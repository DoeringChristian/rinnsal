"""Slurm executor for cluster job submission."""

from __future__ import annotations

import os
import subprocess
import time
import uuid
from concurrent.futures import Future
from pathlib import Path
from typing import TYPE_CHECKING, Any

import cloudpickle

from rinnsal.execution.executor import ExecutionResult, Executor

if TYPE_CHECKING:
    from rinnsal.core.expression import TaskExpression


class SlurmExecutor(Executor):
    """Executor that submits tasks to a Slurm cluster via sbatch.

    Each task is serialized with cloudpickle, wrapped in a Python script,
    and submitted as a Slurm job. Results are read back from pickle files.

    Resources declared on tasks via ``@task(resources=Resources(...))``
    override the executor defaults.
    """

    def __init__(
        self,
        partition: str | None = None,
        account: str | None = None,
        time_min: int = 60,
        mem_gb: int | None = None,
        gpus_per_node: int = 0,
        cpus_per_task: int = 1,
        setup: list[str] | None = None,
        python_bin: str = "python3",
        job_dir: str = ".rinnsal/slurm_jobs",
        capture: bool = True,
        snapshot: bool = True,
    ) -> None:
        super().__init__(capture=capture, snapshot=snapshot)
        self._partition = partition
        self._account = account
        self._time_min = time_min
        self._mem_gb = mem_gb
        self._gpus_per_node = gpus_per_node
        self._cpus_per_task = cpus_per_task
        self._setup = setup or []
        self._python_bin = python_bin
        self._job_dir = Path(job_dir)
        self._job_dir.mkdir(parents=True, exist_ok=True)
        self._active_jobs: list[str] = []

    def submit(
        self,
        expr: TaskExpression,
        resolved_args: tuple[Any, ...],
        resolved_kwargs: dict[str, Any],
    ) -> Future[ExecutionResult]:
        """Submit a task to Slurm via sbatch."""
        # Create job directory
        job_id = str(uuid.uuid4())[:8]
        job_path = self._job_dir / job_id
        job_path.mkdir(parents=True, exist_ok=True)

        # Serialize function and arguments
        submission_pkl = job_path / "submission.pkl"
        with open(submission_pkl, "wb") as f:
            cloudpickle.dump((expr.func, resolved_args, resolved_kwargs), f)

        result_pkl = job_path / "result.pkl"
        stdout_log = job_path / "stdout.log"
        stderr_log = job_path / "stderr.log"

        # Resolve resources: per-task overrides executor defaults
        gpus = self._gpus_per_node
        cpus = self._cpus_per_task
        mem_mb = (self._mem_gb * 1024) if self._mem_gb else None

        resources = expr.task_def.resources
        if resources:
            if resources.gpu:
                gpus = resources.gpu
            if resources.cpu:
                cpus = resources.cpu
            if resources.memory:
                mem_mb = resources.memory

        # Build snapshot PYTHONPATH if enabled
        pythonpath_setup = ""
        if self._snapshot:
            try:
                from rinnsal.core.snapshot import (
                    build_pythonpath,
                    get_snapshot_manager,
                )

                manager = get_snapshot_manager()
                _, snapshot_path = manager.create_snapshot(expr.func)
                if snapshot_path and snapshot_path.exists():
                    remapped = build_pythonpath(snapshot_path)
                    pythonpath_setup = f'export PYTHONPATH="{remapped}"'
            except Exception:
                pass

        # Build checkpoint setup
        checkpoint_setup = ""
        if self._checkpoint_path:
            checkpoint_setup = (
                f'export RINNSAL_CHECKPOINT_PATH="{self._checkpoint_path}"'
            )

        # Generate the Python worker script
        worker_script = _make_worker_script(
            submission_pkl=str(submission_pkl),
            result_pkl=str(result_pkl),
            checkpoint_path=self._checkpoint_path,
        )
        worker_py = job_path / "worker.py"
        worker_py.write_text(worker_script)

        # Generate sbatch script
        sbatch = _make_sbatch_script(
            task_name=expr.task_name,
            partition=self._partition,
            account=self._account,
            time_min=self._time_min,
            mem_mb=mem_mb,
            gpus=gpus,
            cpus=cpus,
            stdout_path=str(stdout_log),
            stderr_path=str(stderr_log),
            setup_commands=self._setup,
            pythonpath_setup=pythonpath_setup,
            checkpoint_setup=checkpoint_setup,
            python_bin=self._python_bin,
            worker_py=str(worker_py),
        )
        sbatch_path = job_path / "run.sh"
        sbatch_path.write_text(sbatch)

        # Submit to Slurm
        try:
            result = subprocess.run(
                ["sbatch", str(sbatch_path)],
                capture_output=True,
                text=True,
            )
            if result.returncode != 0:
                raise RuntimeError(
                    f"sbatch failed: {result.stderr.strip()}"
                )
            # Parse job ID from "Submitted batch job 12345"
            slurm_job_id = result.stdout.strip().split()[-1]
        except FileNotFoundError:
            raise RuntimeError(
                "sbatch not found. Is Slurm installed and in PATH?"
            )

        self._active_jobs.append(slurm_job_id)

        # Return a future that polls for completion
        future: Future[ExecutionResult] = Future()
        _poll_slurm_job(
            future, slurm_job_id, result_pkl, stdout_log, stderr_log
        )
        return future

    def shutdown(self, wait: bool = True) -> None:
        """Cancel active Slurm jobs if not waiting."""
        if not wait:
            for job_id in self._active_jobs:
                try:
                    subprocess.run(
                        ["scancel", job_id],
                        capture_output=True,
                    )
                except Exception:
                    pass
        self._active_jobs.clear()

    def __repr__(self) -> str:
        return (
            f"SlurmExecutor(partition={self._partition!r}, "
            f"gpus={self._gpus_per_node})"
        )


def _make_worker_script(
    submission_pkl: str,
    result_pkl: str,
    checkpoint_path: str | None = None,
) -> str:
    """Generate the Python script that runs inside the Slurm job."""
    checkpoint_block = ""
    if checkpoint_path:
        checkpoint_block = f"""
import os
from pathlib import Path
from rinnsal.context import Checkpoint, current
current._set_checkpoint(Checkpoint(path=Path("{checkpoint_path}")))
"""

    return f"""#!/usr/bin/env python3
\"\"\"Auto-generated rinnsal Slurm worker script.\"\"\"
import sys
import cloudpickle
import traceback

{checkpoint_block}

# Load and execute
with open("{submission_pkl}", "rb") as f:
    func, args, kwargs = cloudpickle.load(f)

try:
    result = func(*args, **kwargs)
    with open("{result_pkl}", "wb") as f:
        cloudpickle.dump(("success", result, None), f)
except Exception as e:
    tb = traceback.format_exc()
    with open("{result_pkl}", "wb") as f:
        cloudpickle.dump(("error", e, tb), f)
"""


def _make_sbatch_script(
    task_name: str,
    partition: str | None,
    account: str | None,
    time_min: int,
    mem_mb: int | None,
    gpus: int,
    cpus: int,
    stdout_path: str,
    stderr_path: str,
    setup_commands: list[str],
    pythonpath_setup: str,
    checkpoint_setup: str,
    python_bin: str,
    worker_py: str,
) -> str:
    """Generate an sbatch submission script."""
    lines = ["#!/bin/bash"]
    lines.append(f"#SBATCH --job-name={task_name}")

    if partition:
        lines.append(f"#SBATCH --partition={partition}")
    if account:
        lines.append(f"#SBATCH --account={account}")

    lines.append(f"#SBATCH --time={time_min}")
    lines.append(f"#SBATCH --cpus-per-task={cpus}")

    if mem_mb:
        lines.append(f"#SBATCH --mem={mem_mb}M")
    if gpus > 0:
        lines.append(f"#SBATCH --gres=gpu:{gpus}")

    lines.append(f"#SBATCH --output={stdout_path}")
    lines.append(f"#SBATCH --error={stderr_path}")
    lines.append("")

    for cmd in setup_commands:
        lines.append(cmd)

    if pythonpath_setup:
        lines.append(pythonpath_setup)
    if checkpoint_setup:
        lines.append(checkpoint_setup)

    lines.append("")
    lines.append(f"{python_bin} {worker_py}")
    lines.append("")

    return "\n".join(lines)


def _poll_slurm_job(
    future: Future[ExecutionResult],
    slurm_job_id: str,
    result_pkl: Path,
    stdout_log: Path,
    stderr_log: Path,
) -> None:
    """Start a background thread to poll Slurm job status."""
    import threading

    def _poll() -> None:
        # Exponential backoff: 2s, 4s, 8s, ..., cap at 30s
        delay = 2.0
        max_delay = 30.0

        while True:
            try:
                state = _get_slurm_job_state(slurm_job_id)
            except Exception:
                state = "UNKNOWN"

            if state in ("COMPLETED", "FAILED", "TIMEOUT", "CANCELLED"):
                break

            if result_pkl.exists():
                # Result file appeared — job finished
                break

            time.sleep(delay)
            delay = min(delay * 1.5, max_delay)

        # Read result
        stdout = ""
        stderr = ""
        if stdout_log.exists():
            stdout = stdout_log.read_text()
        if stderr_log.exists():
            stderr = stderr_log.read_text()

        if result_pkl.exists():
            try:
                with open(result_pkl, "rb") as f:
                    outcome = cloudpickle.load(f)

                if outcome[0] == "success":
                    future.set_result(
                        ExecutionResult(
                            value=outcome[1],
                            stdout=stdout,
                            stderr=stderr,
                            success=True,
                        )
                    )
                else:
                    # ("error", exception, traceback_str)
                    future.set_result(
                        ExecutionResult(
                            value=None,
                            stdout=stdout,
                            stderr=stderr + (outcome[2] or ""),
                            success=False,
                            error=outcome[1],
                        )
                    )
            except Exception as e:
                future.set_result(
                    ExecutionResult(
                        value=None,
                        stdout=stdout,
                        stderr=stderr,
                        success=False,
                        error=RuntimeError(
                            f"Failed to read Slurm job result: {e}"
                        ),
                    )
                )
        else:
            future.set_result(
                ExecutionResult(
                    value=None,
                    stdout=stdout,
                    stderr=stderr,
                    success=False,
                    error=RuntimeError(
                        f"Slurm job {slurm_job_id} ended with state "
                        f"'{state}' but no result file was written"
                    ),
                )
            )

    thread = threading.Thread(target=_poll, daemon=True)
    thread.start()


def _get_slurm_job_state(job_id: str) -> str:
    """Query Slurm for job state via sacct."""
    try:
        result = subprocess.run(
            [
                "sacct",
                "-j", job_id,
                "--format=State",
                "--noheader",
                "--parsable2",
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode == 0 and result.stdout.strip():
            # May return multiple lines for job steps; use the first
            return result.stdout.strip().split("\n")[0].strip()
    except Exception:
        pass
    return "UNKNOWN"
