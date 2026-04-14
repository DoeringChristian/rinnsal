"""Subprocess executor for isolated task execution."""

from __future__ import annotations

import logging
import multiprocessing as mp
import os
import sys
import threading
from concurrent.futures import Future, ProcessPoolExecutor
from typing import TYPE_CHECKING, Any

import cloudpickle

from rinnsal.compute.executor import ExecutionResult, Executor

_log = logging.getLogger(__name__)

if TYPE_CHECKING:
    from rinnsal.modeling.expression import TaskExpression


def _worker_execute(
    serialized_func: bytes,
    serialized_args: bytes,
    serialized_kwargs: bytes,
    capture: bool,
    remapped_pythonpath: str | None = None,
    checkpoint_path: str | None = None,
    task_name: str = "",
) -> tuple[bool, Any, str, str, bytes | None]:
    """Worker entry point running in a child process.

    Returns (success, result_or_None, stdout, stderr, serialized_error_or_None).
    """
    import io
    from contextlib import redirect_stderr, redirect_stdout
    from pathlib import Path

    original_path = None
    if remapped_pythonpath:
        original_path = sys.path.copy()
        sys.path = remapped_pythonpath.split(os.pathsep)

    func = cloudpickle.loads(serialized_func)
    args = cloudpickle.loads(serialized_args)
    kwargs = cloudpickle.loads(serialized_kwargs)

    from rinnsal.context import Checkpoint, current

    if task_name:
        current._set_task_name(task_name)

    stdout_capture = io.StringIO()
    stderr_capture = io.StringIO()

    if checkpoint_path:
        current._set_checkpoint(Checkpoint(path=Path(checkpoint_path)))
    try:
        if capture:
            with (
                redirect_stdout(stdout_capture),
                redirect_stderr(stderr_capture),
            ):
                result = func(*args, **kwargs)
        else:
            result = func(*args, **kwargs)

        return (
            True,
            cloudpickle.dumps(result),
            stdout_capture.getvalue(),
            stderr_capture.getvalue(),
            None,
        )
    except Exception as e:
        import traceback

        tb = traceback.format_exception(e)
        stderr_val = stderr_capture.getvalue() + "".join(tb)
        return (
            False,
            None,
            stdout_capture.getvalue(),
            stderr_val,
            cloudpickle.dumps(e),
        )
    finally:
        if original_path is not None:
            sys.path = original_path


class SubprocessExecutor(Executor):
    """Executor that runs tasks in separate processes.

    Each task gets a fresh subprocess so crashes don't poison a shared
    pool. Concurrency is limited by ``max_workers`` via a semaphore.
    """

    def __init__(
        self,
        max_workers: int | None = None,
        capture: bool = True,
        snapshot: bool = True,
    ) -> None:
        super().__init__(capture=capture, snapshot=snapshot)
        self._max_workers = max_workers or os.cpu_count() or 4
        self._mp_context = mp.get_context("spawn")
        self._semaphore = threading.Semaphore(self._max_workers)

    @property
    def max_workers(self) -> int:
        return self._max_workers

    def _prepare_submission(
        self,
        expr: "TaskExpression",
        resolved_args: tuple[Any, ...],
        resolved_kwargs: dict[str, Any],
    ) -> tuple:
        remapped_pythonpath: str | None = None
        if self._snapshot:
            from rinnsal.versioning.snapshot import (
                get_snapshot_manager,
                build_pythonpath,
            )

            manager = get_snapshot_manager()
            _, snapshot_path = manager.create_snapshot(expr.func)
            if snapshot_path and snapshot_path.exists():
                remapped_pythonpath = build_pythonpath(snapshot_path)

        serialized_func = cloudpickle.dumps(expr.func)
        serialized_args = cloudpickle.dumps(resolved_args)
        serialized_kwargs = cloudpickle.dumps(resolved_kwargs)

        return (
            _worker_execute,
            serialized_func,
            serialized_args,
            serialized_kwargs,
            self._capture,
            remapped_pythonpath,
            self._checkpoint_path,
            expr.task_name or "",
        )

    @staticmethod
    def _handle_worker_result(
        f: Future, result_future: Future[ExecutionResult]
    ) -> None:
        try:
            success, result_bytes, stdout, stderr, error_bytes = f.result()

            if success:
                result_future.set_result(
                    ExecutionResult(
                        value=cloudpickle.loads(result_bytes),
                        stdout=stdout,
                        stderr=stderr,
                        success=True,
                    )
                )
            else:
                error = (
                    cloudpickle.loads(error_bytes) if error_bytes else None
                )
                result_future.set_result(
                    ExecutionResult(
                        value=None,
                        stdout=stdout,
                        stderr=stderr,
                        success=False,
                        error=error,
                    )
                )
        except Exception as e:
            result_future.set_result(
                ExecutionResult(value=None, success=False, error=e)
            )

    def submit(
        self,
        expr: "TaskExpression",
        resolved_args: tuple[Any, ...],
        resolved_kwargs: dict[str, Any],
    ) -> Future[ExecutionResult]:
        submit_args = self._prepare_submission(
            expr, resolved_args, resolved_kwargs
        )
        result_future: Future[ExecutionResult] = Future()

        def _run() -> None:
            self._semaphore.acquire()
            proc = ProcessPoolExecutor(
                max_workers=1, mp_context=self._mp_context,
            )
            try:
                future = proc.submit(*submit_args)
                self._handle_worker_result(future, result_future)
            except Exception as e:
                result_future.set_result(
                    ExecutionResult(value=None, success=False, error=e)
                )
            finally:
                proc.shutdown(wait=True)
                self._semaphore.release()

        threading.Thread(target=_run, daemon=True).start()
        return result_future

    def shutdown(self, wait: bool = True) -> None:
        """No persistent pool to shut down."""

    def __repr__(self) -> str:
        return (
            f"SubprocessExecutor(max_workers={self._max_workers}, "
            f"capture={self._capture})"
        )


class ForkExecutor(Executor):
    """Executor that uses fork for task isolation (Unix only)."""

    def __init__(
        self,
        max_workers: int | None = None,
        capture: bool = True,
        snapshot: bool = True,
    ) -> None:
        if sys.platform == "win32":
            raise RuntimeError("ForkExecutor is not available on Windows")

        super().__init__(capture=capture, snapshot=snapshot)
        self._max_workers = max_workers or os.cpu_count() or 4
        self._mp_context = mp.get_context("fork")
        self._semaphore = threading.Semaphore(self._max_workers)

    @property
    def max_workers(self) -> int:
        return self._max_workers

    def _prepare_submission(
        self,
        expr: "TaskExpression",
        resolved_args: tuple[Any, ...],
        resolved_kwargs: dict[str, Any],
    ) -> tuple:
        remapped_pythonpath: str | None = None
        if self._snapshot:
            from rinnsal.versioning.snapshot import (
                get_snapshot_manager,
                build_pythonpath,
            )

            manager = get_snapshot_manager()
            _, snapshot_path = manager.create_snapshot(expr.func)
            if snapshot_path and snapshot_path.exists():
                remapped_pythonpath = build_pythonpath(snapshot_path)

        serialized_func = cloudpickle.dumps(expr.func)
        serialized_args = cloudpickle.dumps(resolved_args)
        serialized_kwargs = cloudpickle.dumps(resolved_kwargs)

        return (
            _worker_execute,
            serialized_func,
            serialized_args,
            serialized_kwargs,
            self._capture,
            remapped_pythonpath,
            self._checkpoint_path,
            expr.task_name or "",
        )

    def submit(
        self,
        expr: "TaskExpression",
        resolved_args: tuple[Any, ...],
        resolved_kwargs: dict[str, Any],
    ) -> Future[ExecutionResult]:
        submit_args = self._prepare_submission(
            expr, resolved_args, resolved_kwargs
        )
        result_future: Future[ExecutionResult] = Future()

        def _run() -> None:
            self._semaphore.acquire()
            proc = ProcessPoolExecutor(
                max_workers=1, mp_context=self._mp_context,
            )
            try:
                future = proc.submit(*submit_args)
                SubprocessExecutor._handle_worker_result(future, result_future)
            except Exception as e:
                result_future.set_result(
                    ExecutionResult(value=None, success=False, error=e)
                )
            finally:
                proc.shutdown(wait=True)
                self._semaphore.release()

        threading.Thread(target=_run, daemon=True).start()
        return result_future

    def shutdown(self, wait: bool = True) -> None:
        """No persistent pool to shut down."""

    def __repr__(self) -> str:
        return (
            f"ForkExecutor(max_workers={self._max_workers}, "
            f"capture={self._capture})"
        )
