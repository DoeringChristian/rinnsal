"""Subprocess executor for isolated task execution."""

from __future__ import annotations

import multiprocessing as mp
import os
import sys
from concurrent.futures import Future, ProcessPoolExecutor
from typing import TYPE_CHECKING, Any, Callable

import cloudpickle

from rinnsal.execution.executor import ExecutionResult, Executor

if TYPE_CHECKING:
    from rinnsal.core.expression import TaskExpression


def _worker_execute(
    serialized_func: bytes,
    serialized_args: bytes,
    serialized_kwargs: bytes,
    capture: bool,
    remapped_pythonpath: str | None = None,
) -> tuple[bool, Any, str, str, bytes | None, list[dict] | None]:
    """Worker function that runs in a subprocess.

    Returns:
        Tuple of (success, result_or_error, stdout, stderr, serialized_error, card)
    """
    import io
    import sys
    from contextlib import redirect_stderr, redirect_stdout

    # If remapped PYTHONPATH provided, replace sys.path
    original_path = None
    if remapped_pythonpath:
        original_path = sys.path.copy()
        sys.path = remapped_pythonpath.split(os.pathsep)

    # Deserialize
    func = cloudpickle.loads(serialized_func)
    args = cloudpickle.loads(serialized_args)
    kwargs = cloudpickle.loads(serialized_kwargs)

    stdout_capture = io.StringIO()
    stderr_capture = io.StringIO()

    from rinnsal.context import Card, current

    current._set_card(Card())
    try:
        if capture:
            with (
                redirect_stdout(stdout_capture),
                redirect_stderr(stderr_capture),
            ):
                result = func(*args, **kwargs)
        else:
            result = func(*args, **kwargs)

        card = current._reset()
        return (
            True,
            cloudpickle.dumps(result),
            stdout_capture.getvalue(),
            stderr_capture.getvalue(),
            None,
            card.serialize() if card else None,
        )
    except Exception as e:
        import traceback

        current._reset()
        tb = traceback.format_exception(e)
        stderr_val = stderr_capture.getvalue()
        stderr_val += "".join(tb)
        return (
            False,
            None,
            stdout_capture.getvalue(),
            stderr_val,
            cloudpickle.dumps(e),
            None,
        )
    finally:
        # Restore original sys.path
        if original_path is not None:
            sys.path = original_path


class SubprocessExecutor(Executor):
    """Executor that runs tasks in separate processes.

    Provides isolation - crashes don't take down the orchestrator.
    Uses ProcessPoolExecutor for parallel execution.
    """

    def __init__(
        self,
        max_workers: int | None = None,
        capture: bool = True,
        snapshot: bool = True,
    ) -> None:
        super().__init__(capture=capture, snapshot=snapshot)
        self._max_workers = max_workers or os.cpu_count() or 4
        self._pool: ProcessPoolExecutor | None = None
        self._mp_context = mp.get_context("spawn")

    @property
    def max_workers(self) -> int:
        return self._max_workers

    def _get_pool(self) -> ProcessPoolExecutor:
        """Get or create the process pool."""
        if self._pool is None:
            self._pool = ProcessPoolExecutor(
                max_workers=self._max_workers,
                mp_context=self._mp_context,
            )
        return self._pool

    def submit(
        self,
        expr: TaskExpression,
        resolved_args: tuple[Any, ...],
        resolved_kwargs: dict[str, Any],
    ) -> Future[ExecutionResult]:
        """Submit a task for subprocess execution."""
        pool = self._get_pool()

        # Create snapshot if enabled
        remapped_pythonpath: str | None = None
        if self._snapshot:
            from rinnsal.core.snapshot import (
                get_snapshot_manager,
                build_pythonpath,
            )

            manager = get_snapshot_manager()
            _, snapshot_path = manager.create_snapshot(expr.func)
            if snapshot_path and snapshot_path.exists():
                remapped_pythonpath = build_pythonpath(snapshot_path)

        # Serialize function and arguments
        serialized_func = cloudpickle.dumps(expr.func)
        serialized_args = cloudpickle.dumps(resolved_args)
        serialized_kwargs = cloudpickle.dumps(resolved_kwargs)

        # Submit to pool
        future = pool.submit(
            _worker_execute,
            serialized_func,
            serialized_args,
            serialized_kwargs,
            self._capture,
            remapped_pythonpath,
        )

        # Wrap the future to return ExecutionResult
        result_future: Future[ExecutionResult] = Future()

        def callback(f: Future) -> None:
            try:
                success, result_bytes, stdout, stderr, error_bytes, card = (
                    f.result()
                )

                if success:
                    result = cloudpickle.loads(result_bytes)
                    result_future.set_result(
                        ExecutionResult(
                            value=result,
                            stdout=stdout,
                            stderr=stderr,
                            success=True,
                            card=card,
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
                    ExecutionResult(
                        value=None,
                        success=False,
                        error=e,
                    )
                )

        future.add_done_callback(callback)
        return result_future

    def shutdown(self, wait: bool = True) -> None:
        """Shutdown the process pool.

        Args:
            wait: If True, wait for workers to finish gracefully.
                  If False, kill worker processes immediately.
        """
        if self._pool is not None:
            if wait:
                self._pool.shutdown(wait=True)
            else:
                self._pool.shutdown(wait=False, cancel_futures=True)
                self._kill_workers()
            self._pool = None

    def _kill_workers(self) -> None:
        """Terminate all worker processes in the pool."""
        # ProcessPoolExecutor stores worker processes in _processes
        processes = getattr(self._pool, "_processes", None)
        if processes:
            for pid, proc in list(processes.items()):
                if proc.is_alive():
                    proc.kill()
                proc.join(timeout=5)

    def __repr__(self) -> str:
        return f"SubprocessExecutor(max_workers={self._max_workers}, capture={self._capture})"


class ForkExecutor(Executor):
    """Executor that uses fork for task isolation (Unix only).

    More efficient than SubprocessExecutor because it shares memory
    at the point of fork. Only available on Unix systems.
    """

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
        self._pool: ProcessPoolExecutor | None = None

    @property
    def max_workers(self) -> int:
        return self._max_workers

    def _get_pool(self) -> ProcessPoolExecutor:
        """Get or create the fork-based process pool."""
        if self._pool is None:
            self._mp_context = mp.get_context("fork")
            self._pool = ProcessPoolExecutor(
                max_workers=self._max_workers,
                mp_context=self._mp_context,
            )
        return self._pool

    def submit(
        self,
        expr: TaskExpression,
        resolved_args: tuple[Any, ...],
        resolved_kwargs: dict[str, Any],
    ) -> Future[ExecutionResult]:
        """Submit a task for fork-based execution."""
        pool = self._get_pool()

        # Create snapshot if enabled
        remapped_pythonpath: str | None = None
        if self._snapshot:
            from rinnsal.core.snapshot import (
                get_snapshot_manager,
                build_pythonpath,
            )

            manager = get_snapshot_manager()
            _, snapshot_path = manager.create_snapshot(expr.func)
            if snapshot_path and snapshot_path.exists():
                remapped_pythonpath = build_pythonpath(snapshot_path)

        # Serialize function and arguments
        serialized_func = cloudpickle.dumps(expr.func)
        serialized_args = cloudpickle.dumps(resolved_args)
        serialized_kwargs = cloudpickle.dumps(resolved_kwargs)

        # Submit to pool
        future = pool.submit(
            _worker_execute,
            serialized_func,
            serialized_args,
            serialized_kwargs,
            self._capture,
            remapped_pythonpath,
        )

        # Wrap the future
        result_future: Future[ExecutionResult] = Future()

        def callback(f: Future) -> None:
            try:
                success, result_bytes, stdout, stderr, error_bytes, card = (
                    f.result()
                )

                if success:
                    result = cloudpickle.loads(result_bytes)
                    result_future.set_result(
                        ExecutionResult(
                            value=result,
                            stdout=stdout,
                            stderr=stderr,
                            success=True,
                            card=card,
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
                    ExecutionResult(
                        value=None,
                        success=False,
                        error=e,
                    )
                )

        future.add_done_callback(callback)
        return result_future

    def shutdown(self, wait: bool = True) -> None:
        """Shutdown the process pool.

        Args:
            wait: If True, wait for workers to finish gracefully.
                  If False, kill worker processes immediately.
        """
        if self._pool is not None:
            if wait:
                self._pool.shutdown(wait=True)
            else:
                self._pool.shutdown(wait=False, cancel_futures=True)
                self._kill_workers()
            self._pool = None

    def _kill_workers(self) -> None:
        """Terminate all worker processes in the pool."""
        processes = getattr(self._pool, "_processes", None)
        if processes:
            for pid, proc in list(processes.items()):
                if proc.is_alive():
                    proc.kill()
                proc.join(timeout=5)

    def __repr__(self) -> str:
        return f"ForkExecutor(max_workers={self._max_workers}, capture={self._capture})"
