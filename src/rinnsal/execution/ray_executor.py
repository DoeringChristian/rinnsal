"""Ray executor for distributed task execution."""

from __future__ import annotations

from concurrent.futures import Future
from typing import TYPE_CHECKING, Any

from rinnsal.execution.executor import ExecutionResult, Executor

if TYPE_CHECKING:
    from rinnsal.core.expression import TaskExpression

try:
    import ray

    HAS_RAY = True
except ImportError:
    HAS_RAY = False


class RayExecutor(Executor):
    """Executor that distributes tasks across a Ray cluster.

    Requires ray to be installed: pip install ray

    Leverages Ray's:
    - Object store for efficient data transfer
    - Scheduling for task placement
    - Fault tolerance for handling failures
    """

    def __init__(
        self,
        capture: bool = True,
        snapshot: bool = False,
        num_cpus: int | None = None,
        address: str | None = None,
        runtime_env: dict[str, Any] | None = None,
    ) -> None:
        if not HAS_RAY:
            raise ImportError(
                "ray is required for RayExecutor. "
                "Install with: pip install rinnsal[ray]"
            )

        super().__init__(capture=capture, snapshot=snapshot)
        self._num_cpus = num_cpus
        self._address = address
        self._runtime_env = runtime_env
        self._initialized = False

    def _ensure_initialized(self) -> None:
        """Initialize Ray if not already initialized."""
        if self._initialized:
            return

        if not ray.is_initialized():
            init_kwargs: dict[str, Any] = {}
            if self._address:
                init_kwargs["address"] = self._address
            if self._num_cpus:
                init_kwargs["num_cpus"] = self._num_cpus
            if self._runtime_env:
                init_kwargs["runtime_env"] = self._runtime_env

            ray.init(**init_kwargs)

        self._initialized = True

    def submit(
        self,
        expr: TaskExpression,
        resolved_args: tuple[Any, ...],
        resolved_kwargs: dict[str, Any],
    ) -> Future[ExecutionResult]:
        """Submit a task for Ray execution."""
        self._ensure_initialized()

        # Create a Ray remote function wrapper
        @ray.remote
        def execute_task(
            func: Any,
            args: tuple,
            kwargs: dict,
            capture: bool,
        ) -> tuple[bool, Any, str, str, Any]:
            import io
            import sys
            from contextlib import redirect_stderr, redirect_stdout

            stdout_capture = io.StringIO()
            stderr_capture = io.StringIO()

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
                    result,
                    stdout_capture.getvalue(),
                    stderr_capture.getvalue(),
                    None,
                )
            except Exception as e:
                return (
                    False,
                    None,
                    stdout_capture.getvalue(),
                    stderr_capture.getvalue(),
                    e,
                )

        # Submit to Ray
        ray_future = execute_task.remote(
            expr.func,
            resolved_args,
            resolved_kwargs,
            self._capture,
        )

        # Wrap in a standard Future
        result_future: Future[ExecutionResult] = Future()

        def fetch_result() -> None:
            try:
                success, result, stdout, stderr, error = ray.get(ray_future)

                if success:
                    result_future.set_result(
                        ExecutionResult(
                            value=result,
                            stdout=stdout,
                            stderr=stderr,
                            success=True,
                        )
                    )
                else:
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

        # Use a thread to wait for the result
        import threading

        thread = threading.Thread(target=fetch_result, daemon=True)
        thread.start()

        return result_future

    def execute_sync(
        self,
        expr: TaskExpression,
        resolved_args: tuple[Any, ...],
        resolved_kwargs: dict[str, Any],
    ) -> ExecutionResult:
        """Execute a task synchronously using Ray."""
        self._ensure_initialized()

        @ray.remote
        def execute_task(
            func: Any,
            args: tuple,
            kwargs: dict,
            capture: bool,
        ) -> tuple[bool, Any, str, str, Any]:
            import io
            from contextlib import redirect_stderr, redirect_stdout

            stdout_capture = io.StringIO()
            stderr_capture = io.StringIO()

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
                    result,
                    stdout_capture.getvalue(),
                    stderr_capture.getvalue(),
                    None,
                )
            except Exception as e:
                return (
                    False,
                    None,
                    stdout_capture.getvalue(),
                    stderr_capture.getvalue(),
                    e,
                )

        try:
            ray_future = execute_task.remote(
                expr.func,
                resolved_args,
                resolved_kwargs,
                self._capture,
            )
            success, result, stdout, stderr, error = ray.get(ray_future)

            if success:
                return ExecutionResult(
                    value=result,
                    stdout=stdout,
                    stderr=stderr,
                    success=True,
                )
            else:
                return ExecutionResult(
                    value=None,
                    stdout=stdout,
                    stderr=stderr,
                    success=False,
                    error=error,
                )
        except Exception as e:
            return ExecutionResult(
                value=None,
                success=False,
                error=e,
            )

    def shutdown(self, wait: bool = True) -> None:
        """Shutdown Ray (if we initialized it)."""
        # Don't shutdown Ray as other code might be using it
        pass

    def __repr__(self) -> str:
        addr = self._address or "local"
        return f"RayExecutor(address={addr})"
