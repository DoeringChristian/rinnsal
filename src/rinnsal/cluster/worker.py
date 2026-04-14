"""Worker daemon: registers with a coordinator and sends heartbeats.

Phase 1 ships registration + heartbeat only. Job dispatch (long-poll
GET /jobs/next, blob fetch, subprocess execution, result post) lands in
subsequent phases.
"""

from __future__ import annotations

import logging
import os
import socket
import subprocess
import threading
import time
from typing import Any
from urllib.parse import urljoin

from rinnsal.cluster.protocol import Capabilities, WorkerRegisterRequest

log = logging.getLogger("rinnsal.cluster.worker")


def detect_capabilities() -> Capabilities:
    """Best-effort introspection of this machine's resources."""
    cpu = os.cpu_count() or 1
    memory_mb = _read_memory_mb()
    gpu, gpu_memory = _read_gpu()
    return Capabilities(
        cpu=cpu,
        memory=memory_mb,
        gpu=gpu,
        gpu_memory=gpu_memory,
        extras={"hostname": socket.gethostname()},
    )


def _read_memory_mb() -> int:
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemTotal:"):
                    kb = int(line.split()[1])
                    return kb // 1024
    except (OSError, ValueError):
        pass
    return 0


def _read_gpu() -> tuple[int, int]:
    """Return ``(gpu_count, total_memory_mb)`` via nvidia-smi if available."""
    try:
        out = subprocess.run(
            [
                "nvidia-smi",
                "--query-gpu=memory.total",
                "--format=csv,noheader,nounits",
            ],
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (FileNotFoundError, subprocess.SubprocessError):
        return 0, 0
    if out.returncode != 0:
        return 0, 0
    lines = [ln.strip() for ln in out.stdout.splitlines() if ln.strip()]
    try:
        mems = [int(x) for x in lines]
    except ValueError:
        return 0, 0
    return len(mems), sum(mems)


class WorkerDaemon:
    """Long-lived process that keeps a registration alive with a coordinator.

    The HTTP transport is pluggable: ``submit_url(method, path, json=...)``
    can be replaced for testing. By default it uses :mod:`httpx`.
    """

    def __init__(
        self,
        host_url: str,
        *,
        name: str | None = None,
        capabilities: Capabilities | None = None,
        transport: Any = None,
        scratch_dir: Any = None,
        run_in_subprocess: bool | None = None,
    ) -> None:
        self._host_url = host_url.rstrip("/")
        self._name = name or socket.gethostname()
        self._capabilities = capabilities or detect_capabilities()
        self._transport = transport
        self._worker_id: str | None = None
        self._heartbeat_interval = 10.0
        self._stop_event = threading.Event()
        self._heartbeat_thread: threading.Thread | None = None
        self._job_thread: threading.Thread | None = None
        # Where extracted project archives + per-job scratch live.
        if scratch_dir is None:
            from pathlib import Path

            scratch_dir = Path.home() / ".rinnsal" / "worker"
        else:
            from pathlib import Path

            scratch_dir = Path(scratch_dir)
        scratch_dir.mkdir(parents=True, exist_ok=True)
        self._scratch_dir = scratch_dir
        # Default: spawn a subprocess for each job iff we have a project
        # archive to provision against. Inline mode is for tests + the
        # special "single-process self-worker" launched by `cluster up
        # --host`.
        self._run_in_subprocess = run_in_subprocess
        # Cache: project_hash -> (work_dir, python_command) once provisioned.
        self._provisioned: dict[str, tuple[Any, str]] = {}
        self._provision_lock = threading.Lock()

    # ── HTTP plumbing (with default httpx-backed transport) ─────────

    def _request(self, method: str, path: str, json: Any = None) -> Any:
        if self._transport is not None:
            return self._transport(method, path, json=json)
        # Lazy import: in test environments where the daemon isn't
        # actually used, we don't want to require httpx.
        import httpx

        url = urljoin(self._host_url + "/", path.lstrip("/"))
        # The coordinator holds jobs/next open for up to 30s; the client
        # read timeout must exceed that or every idle poll looks like a
        # failure. Short connect/write timeouts still catch real hangs.
        timeouts = httpx.Timeout(60.0, connect=5.0)
        with httpx.Client(timeout=timeouts) as client:
            r = client.request(method, url, json=json)
        if r.status_code == 204:
            # No job ready (long-poll timeout) — matches the test
            # transport's return-None contract.
            return None
        r.raise_for_status()
        if r.headers.get("content-type", "").startswith("application/json"):
            return r.json()
        return r.text

    # ── lifecycle ───────────────────────────────────────────────────

    def register(self) -> str:
        """Synchronously register with the coordinator. Returns worker_id."""
        req = WorkerRegisterRequest(
            name=self._name,
            capabilities=self._capabilities,
        )
        body = self._request(
            "POST",
            "/api/cluster/workers/register",
            json=req.model_dump(),
        )
        self._worker_id = body["worker_id"]
        self._heartbeat_interval = float(body.get("heartbeat_interval", 10.0))
        log.info(
            "registered as %s (worker_id=%s) with %s",
            self._name, self._worker_id, self._host_url,
        )
        return self._worker_id

    def send_heartbeat(self) -> list[str]:
        """One heartbeat tick. Returns any revoked job IDs."""
        assert self._worker_id is not None, "register() first"
        body = self._request(
            "POST",
            f"/api/cluster/workers/{self._worker_id}/heartbeat",
        )
        return list(body.get("revoked_job_ids") or [])

    def _heartbeat_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self.send_heartbeat()
            except Exception as e:
                log.warning("heartbeat failed: %s", e)
            self._stop_event.wait(self._heartbeat_interval)

    def start(self) -> None:
        """Register + start the heartbeat thread (idempotent)."""
        if self._worker_id is None:
            self.register()
        if self._heartbeat_thread is not None:
            return
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            name=f"rinnsal-heartbeat-{self._name}",
            daemon=True,
        )
        self._heartbeat_thread.start()

    def start_job_loop(self) -> None:
        """Spawn the long-poll job loop on a background thread (idempotent).

        Use this for embedded self-workers that share a process with the
        coordinator. Standalone workers call :meth:`run_forever` instead,
        which runs the loop on the main thread.
        """
        if self._job_thread is not None:
            return
        self._job_thread = threading.Thread(
            target=self._job_loop,
            name=f"rinnsal-job-loop-{self._name}",
            daemon=True,
        )
        self._job_thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._heartbeat_thread is not None:
            self._heartbeat_thread.join(timeout=2.0)
        if self._job_thread is not None:
            self._job_thread.join(timeout=2.0)

    def run_forever(self) -> None:
        """Foreground entry: register, start heartbeats, run the job loop."""
        self.start()
        # Run the long-poll job loop on the main thread.
        try:
            self._job_loop()
        except KeyboardInterrupt:
            pass
        finally:
            self.stop()

    # ── job loop ────────────────────────────────────────────────────

    def _request_bytes(self, method: str, path: str) -> bytes:
        """Fetch raw bytes (for blob downloads)."""
        if self._transport is not None:
            return self._transport(method, path, _binary=True)
        import httpx

        url = urljoin(self._host_url + "/", path.lstrip("/"))
        with httpx.Client(timeout=60.0) as client:
            r = client.request(method, url)
        r.raise_for_status()
        return r.content

    def _put_bytes(self, path: str, data: bytes) -> Any:
        """Upload raw bytes (for result blobs)."""
        if self._transport is not None:
            return self._transport("PUT", path, _bytes=data)
        import httpx

        url = urljoin(self._host_url + "/", path.lstrip("/"))
        with httpx.Client(timeout=60.0) as client:
            r = client.request("PUT", url, content=data)
        r.raise_for_status()
        if r.headers.get("content-type", "").startswith("application/json"):
            return r.json()
        return r.text

    def _job_loop(self) -> None:
        assert self._worker_id is not None, "register() first"
        from rinnsal.cluster.protocol import JobAssignment

        while not self._stop_event.is_set():
            try:
                body = self._request(
                    "GET",
                    f"/api/cluster/jobs/next?worker_id={self._worker_id}&timeout=30",
                )
            except Exception as e:
                # Network / coordinator hiccup — back off briefly.
                log.warning("jobs/next failed: %s", e)
                self._stop_event.wait(2.0)
                continue
            if body is None:
                # 204 with no body → no job ready, poll again.
                continue
            try:
                assignment = JobAssignment.model_validate(body)
            except Exception as e:
                log.error("malformed job assignment: %s", e)
                continue
            self._run_one(assignment)

    def _run_one(self, job: Any) -> None:
        """Execute one job + post the result.

        If the job has a ``project_hash`` and ``run_in_subprocess`` is
        not explicitly disabled, the worker provisions the project
        archive (uv/pip/pixi auto-detect) and runs the task in a fresh
        subprocess that uses the provisioned Python. Otherwise it runs
        the task inline (used by tests + the coordinator's self-worker).
        """
        # Decide execution strategy.
        wants_subprocess = (
            self._run_in_subprocess
            if self._run_in_subprocess is not None
            else bool(job.project_hash)
        )

        if wants_subprocess and job.project_hash:
            try:
                work_dir, python_cmd = self._ensure_project(job.project_hash)
            except Exception as e:
                log.exception("project provisioning failed")
                self._post_failure(job.job_id, e, "", "")
                return
            self._run_one_in_subprocess(job, work_dir, python_cmd)
        else:
            self._run_one_inline(job)

    # ── inline path (no project / tests) ────────────────────────────

    def _run_one_inline(self, job: Any) -> None:
        import io
        from contextlib import redirect_stderr, redirect_stdout

        import cloudpickle

        from rinnsal.context import current
        from rinnsal.data.logger.proxy import LoggerProxy

        try:
            func, args, kwargs = self._fetch_payload(job)
        except Exception as e:
            self._post_failure(job.job_id, e, "", "", b"")
            return

        # Install a LoggerProxy so user code that calls
        # ``current.logger.add_*`` collects events in-process; we ship
        # the buffer back to the orchestrator with the result.
        proxy = LoggerProxy()
        current._set_logger(proxy)

        stdout_buf = io.StringIO()
        stderr_buf = io.StringIO()
        try:
            if job.capture:
                with (
                    redirect_stdout(stdout_buf),
                    redirect_stderr(stderr_buf),
                ):
                    value = func(*args, **kwargs)
            else:
                value = func(*args, **kwargs)
        except Exception as e:
            current._reset_logger()
            self._post_failure(
                job.job_id, e,
                stdout_buf.getvalue(), stderr_buf.getvalue(),
                proxy.get_buffer(),
            )
            return

        current._reset_logger()
        self._post_success(
            job.job_id, value,
            stdout_buf.getvalue(), stderr_buf.getvalue(),
            proxy.get_buffer(),
        )

    # ── subprocess path (provisioned project) ───────────────────────

    def _run_one_in_subprocess(
        self,
        job: Any,
        work_dir: Any,
        python_cmd: str,
    ) -> None:
        """Run one job in a fresh subprocess with the project's Python."""
        import os
        import shlex
        import subprocess as _sp
        import tempfile

        # Stage func/args/kwargs as files in a per-job scratch dir.
        scratch = self._scratch_dir / job.project_hash / "scratch" / job.job_id
        scratch.mkdir(parents=True, exist_ok=True)
        try:
            for name, blob_hash in (
                ("func.pkl", job.func_blob_hash),
                ("args.pkl", job.args_blob_hash),
                ("kwargs.pkl", job.kwargs_blob_hash),
            ):
                data = self._request_bytes(
                    "GET", f"/api/cluster/blobs/{blob_hash}"
                )
                (scratch / name).write_bytes(data)
        except Exception as e:
            self._post_failure(job.job_id, e, "", "")
            return

        result_path = scratch / "result.pkl"
        worker_script = (scratch / "worker.py")
        worker_script.write_text(_SUBPROCESS_WORKER_SCRIPT)

        cmd = (
            f"cd {shlex.quote(str(work_dir))} && "
            f"{python_cmd} {shlex.quote(str(worker_script))} "
            f"{shlex.quote(str(scratch))} {shlex.quote(str(result_path))}"
        )
        env = os.environ.copy()
        try:
            proc = _sp.run(
                ["bash", "-lc", cmd],
                capture_output=True,
                text=True,
                timeout=job.timeout,
                env=env,
            )
        except _sp.TimeoutExpired as e:
            self._post_failure(
                job.job_id, e,
                e.stdout or "", e.stderr or "",
            )
            return

        if proc.returncode != 0 or not result_path.exists():
            err = RuntimeError(
                f"worker subprocess exit={proc.returncode}"
            )
            self._post_failure(
                job.job_id, err, proc.stdout, proc.stderr
            )
            return

        # Worker subprocess wrote (status, value_or_error) cloudpickled.
        import cloudpickle

        try:
            status, payload = cloudpickle.loads(result_path.read_bytes())
        except Exception as e:
            self._post_failure(job.job_id, e, proc.stdout, proc.stderr)
            return

        if status == "success":
            self._post_success(job.job_id, payload, proc.stdout, proc.stderr)
        else:
            self._post_failure(job.job_id, payload, proc.stdout, proc.stderr)

    # ── shared helpers ──────────────────────────────────────────────

    def _fetch_payload(self, job: Any):
        import cloudpickle

        func_bytes = self._request_bytes(
            "GET", f"/api/cluster/blobs/{job.func_blob_hash}"
        )
        args_bytes = self._request_bytes(
            "GET", f"/api/cluster/blobs/{job.args_blob_hash}"
        )
        kwargs_bytes = self._request_bytes(
            "GET", f"/api/cluster/blobs/{job.kwargs_blob_hash}"
        )
        return (
            cloudpickle.loads(func_bytes),
            cloudpickle.loads(args_bytes),
            cloudpickle.loads(kwargs_bytes),
        )

    def _post_success(
        self,
        job_id: str,
        value: Any,
        stdout: str,
        stderr: str,
        logger_events: bytes = b"",
    ) -> None:
        try:
            import cloudpickle
            import hashlib

            result_bytes = cloudpickle.dumps(value)
            result_hash = hashlib.sha256(result_bytes).hexdigest()
            self._put_bytes(
                f"/api/cluster/blobs/{result_hash}", result_bytes
            )
            events_hash = self._upload_events(logger_events)
            self._request(
                "POST",
                f"/api/cluster/jobs/{job_id}/result",
                json={
                    "success": True,
                    "result_blob_hash": result_hash,
                    "stdout": stdout,
                    "stderr": stderr,
                    "logger_events_blob_hash": events_hash,
                },
            )
        except Exception as e:
            log.exception("failed to post result for job %s", job_id)
            self._post_failure(job_id, e, stdout, stderr, logger_events)

    def _post_failure(
        self,
        job_id: str,
        error: Exception,
        stdout: str,
        stderr: str,
        logger_events: bytes = b"",
    ) -> None:
        try:
            import cloudpickle
            import hashlib

            error_bytes = cloudpickle.dumps(error)
            error_hash = hashlib.sha256(error_bytes).hexdigest()
            self._put_bytes(
                f"/api/cluster/blobs/{error_hash}", error_bytes
            )
            events_hash = self._upload_events(logger_events)
            self._request(
                "POST",
                f"/api/cluster/jobs/{job_id}/result",
                json={
                    "success": False,
                    "error_blob_hash": error_hash,
                    "stdout": stdout,
                    "stderr": stderr,
                    "logger_events_blob_hash": events_hash,
                },
            )
        except Exception:
            log.exception("failed to post failure for job %s", job_id)

    def _upload_events(self, events: bytes) -> str:
        if not events:
            return ""
        import hashlib

        h = hashlib.sha256(events).hexdigest()
        try:
            self._put_bytes(f"/api/cluster/blobs/{h}", events)
        except Exception:
            log.warning("logger events upload failed", exc_info=True)
            return ""
        return h

    # ── project provisioning ────────────────────────────────────────

    def _ensure_project(self, project_hash: str) -> tuple[Any, str]:
        """Download + extract + provision the project. Returns (work_dir, python_cmd)."""
        from pathlib import Path

        from rinnsal.cluster.archive import extract_archive
        from rinnsal.compute.provisioner import AutoProvisioner

        with self._provision_lock:
            cached = self._provisioned.get(project_hash)
            if cached is not None:
                return cached

            project_dir = self._scratch_dir / project_hash
            work_dir = project_dir / "src"
            ready_marker = project_dir / ".ready"

            if not ready_marker.exists():
                log.info("provisioning project %s", project_hash)
                # Fetch + extract.
                data = self._request_bytes(
                    "GET",
                    f"/api/cluster/projects/{project_hash}/archive",
                )
                extract_archive(data, work_dir)
                # Auto-detect provisioner against the extracted source.
                provisioner = AutoProvisioner(search_dir=work_dir)
                kind = type(getattr(provisioner, "inner", provisioner)).__name__
                log.info(
                    "provisioning %s with %s (work_dir=%s)",
                    project_hash[:8], kind, work_dir,
                )
                script = provisioner.provision_script(str(work_dir))
                import subprocess as _sp

                proc = _sp.run(
                    ["bash", "-lc", script],
                    capture_output=True,
                    text=True,
                    timeout=600,
                )
                if proc.returncode != 0:
                    raise RuntimeError(
                        f"provisioner failed (exit {proc.returncode}):\n"
                        f"stdout:\n{proc.stdout[-2000:]}\n"
                        f"stderr:\n{proc.stderr[-2000:]}"
                    )
                log.info(
                    "provisioner %s succeeded for %s",
                    kind, project_hash[:8],
                )
                if proc.stderr:
                    log.debug(
                        "provisioner stderr:\n%s", proc.stderr[-1000:]
                    )
                # Install rinnsal into the venv too — without this the
                # subprocess can't import cloudpickle / unpickle the
                # task. Best-effort: skip on failure (caller will see
                # ImportError when running the task).
                try:
                    venv_python = AutoProvisioner(
                        search_dir=work_dir
                    ).python_command(str(work_dir))
                except Exception:
                    venv_python = "python3"
                ready_marker.touch()

            python_cmd = AutoProvisioner(
                search_dir=work_dir
            ).python_command(str(work_dir))
            self._provisioned[project_hash] = (work_dir, python_cmd)
            return work_dir, python_cmd


# Worker subprocess script: loads func/args/kwargs from disk, runs the
# task, writes (status, value-or-exception) cloudpickled to result.pkl.
_SUBPROCESS_WORKER_SCRIPT = '''#!/usr/bin/env python3
"""Auto-generated rinnsal cluster worker subprocess."""
import sys
import cloudpickle
from pathlib import Path

scratch = Path(sys.argv[1])
result_path = Path(sys.argv[2])

func = cloudpickle.loads((scratch / "func.pkl").read_bytes())
args = cloudpickle.loads((scratch / "args.pkl").read_bytes())
kwargs = cloudpickle.loads((scratch / "kwargs.pkl").read_bytes())

try:
    value = func(*args, **kwargs)
    payload = ("success", value)
except Exception as e:
    payload = ("error", e)

result_path.write_bytes(cloudpickle.dumps(payload))
'''
