"""Subprocess executor for isolated task execution."""

from __future__ import annotations

import logging
import multiprocessing as mp
import os
import sys
import threading
from concurrent.futures import Future, ProcessPoolExecutor
from typing import TYPE_CHECKING, Any

_log = logging.getLogger(__name__)

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
    checkpoint_path: str | None = None,
    event_file: str | None = None,
) -> tuple[bool, Any, str, str, bytes | None, list[dict] | None, bytes]:
    """Worker function that runs in a subprocess.

    Returns:
        Tuple of (success, result_or_error, stdout, stderr,
                  serialized_error, card, logger_events)
    """
    import io
    import sys
    from contextlib import redirect_stderr, redirect_stdout
    from pathlib import Path

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

    from rinnsal.context import Card, Checkpoint, current
    from rinnsal.logger.proxy import LoggerProxy

    current._set_card(Card())
    # When event_file is provided, events are written directly to the
    # flow's events.pb (flushed per event) so the viewer can show live
    # data.  Otherwise fall back to buffer mode (replayed after task).
    proxy = LoggerProxy(event_file=event_file)
    current._set_logger(proxy)
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

        card = current._reset()
        return (
            True,
            cloudpickle.dumps(result),
            stdout_capture.getvalue(),
            stderr_capture.getvalue(),
            None,
            card.serialize() if card else None,
            proxy.get_buffer(),
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
            proxy.get_buffer(),
        )
    finally:
        current._reset_logger()
        # Restore original sys.path
        if original_path is not None:
            sys.path = original_path


class SubprocessExecutor(Executor):
    """Executor that runs tasks in separate processes.

    Provides isolation - each task gets a fresh subprocess, so crashes
    don't take down the orchestrator or poison a shared pool.
    Concurrency is limited by max_workers via a semaphore.
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

    def _get_event_file(self) -> str | None:
        """Resolve events.pb path from the logger for direct file writes."""
        if self._logger is None:
            return None
        writer = getattr(self._logger, "_event_writer", None)
        if writer is None:
            return None
        path = getattr(writer, "_path", None)
        return str(path) if path else None

    def _prepare_submission(
        self,
        expr: TaskExpression,
        resolved_args: tuple[Any, ...],
        resolved_kwargs: dict[str, Any],
    ) -> tuple:
        """Prepare serialized arguments for submission."""
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
            self._get_event_file(),
        )

    @staticmethod
    def _handle_worker_result(
        f: Future, result_future: Future[ExecutionResult]
    ) -> None:
        """Handle the result from a worker future."""
        try:
            (
                success, result_bytes, stdout, stderr,
                error_bytes, card, logger_events,
            ) = f.result()

            if success:
                result = cloudpickle.loads(result_bytes)
                result_future.set_result(
                    ExecutionResult(
                        value=result,
                        stdout=stdout,
                        stderr=stderr,
                        success=True,
                        card=card,
                        logger_events=logger_events,
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
                        logger_events=logger_events,
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

    def submit(
        self,
        expr: TaskExpression,
        resolved_args: tuple[Any, ...],
        resolved_kwargs: dict[str, Any],
    ) -> Future[ExecutionResult]:
        """Submit a task for subprocess execution."""
        submit_args = self._prepare_submission(
            expr, resolved_args, resolved_kwargs
        )
        result_future: Future[ExecutionResult] = Future()

        def _run() -> None:
            self._semaphore.acquire()
            proc = ProcessPoolExecutor(
                max_workers=1,
                mp_context=self._mp_context,
            )
            try:
                future = proc.submit(*submit_args)
                self._handle_worker_result(future, result_future)
            except Exception as e:
                result_future.set_result(
                    ExecutionResult(
                        value=None,
                        success=False,
                        error=e,
                    )
                )
            finally:
                proc.shutdown(wait=True)
                self._semaphore.release()

        thread = threading.Thread(target=_run, daemon=True)
        thread.start()
        return result_future

    def shutdown(self, wait: bool = True) -> None:
        """No persistent pool to shut down."""

    def __repr__(self) -> str:
        return f"SubprocessExecutor(max_workers={self._max_workers}, capture={self._capture})"


class ForkExecutor(Executor):
    """Executor that uses fork for task isolation (Unix only).

    More efficient than SubprocessExecutor because it shares memory
    at the point of fork. Only available on Unix systems.
    Each task gets a fresh process; concurrency is limited by max_workers.
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
        self._mp_context = mp.get_context("fork")
        self._semaphore = threading.Semaphore(self._max_workers)

    @property
    def max_workers(self) -> int:
        return self._max_workers

    def _prepare_submission(
        self,
        expr: TaskExpression,
        resolved_args: tuple[Any, ...],
        resolved_kwargs: dict[str, Any],
    ) -> tuple:
        """Prepare serialized arguments for submission."""
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

        serialized_func = cloudpickle.dumps(expr.func)
        serialized_args = cloudpickle.dumps(resolved_args)
        serialized_kwargs = cloudpickle.dumps(resolved_kwargs)

        # Reuse SubprocessExecutor's event file resolution
        event_file: str | None = None
        if self._logger is not None:
            writer = getattr(self._logger, "_event_writer", None)
            if writer is not None:
                path = getattr(writer, "_path", None)
                event_file = str(path) if path else None

        return (
            _worker_execute,
            serialized_func,
            serialized_args,
            serialized_kwargs,
            self._capture,
            remapped_pythonpath,
            self._checkpoint_path,
            event_file,
        )

    def submit(
        self,
        expr: TaskExpression,
        resolved_args: tuple[Any, ...],
        resolved_kwargs: dict[str, Any],
    ) -> Future[ExecutionResult]:
        """Submit a task for fork-based execution."""
        submit_args = self._prepare_submission(
            expr, resolved_args, resolved_kwargs
        )
        result_future: Future[ExecutionResult] = Future()

        def _run() -> None:
            self._semaphore.acquire()
            proc = ProcessPoolExecutor(
                max_workers=1,
                mp_context=self._mp_context,
            )
            try:
                future = proc.submit(*submit_args)
                SubprocessExecutor._handle_worker_result(future, result_future)
            except Exception as e:
                result_future.set_result(
                    ExecutionResult(
                        value=None,
                        success=False,
                        error=e,
                    )
                )
            finally:
                proc.shutdown(wait=True)
                self._semaphore.release()

        thread = threading.Thread(target=_run, daemon=True)
        thread.start()
        return result_future

    def shutdown(self, wait: bool = True) -> None:
        """No persistent pool to shut down."""

    def __repr__(self) -> str:
        return f"ForkExecutor(max_workers={self._max_workers}, capture={self._capture})"
