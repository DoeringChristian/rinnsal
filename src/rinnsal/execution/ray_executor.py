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


if HAS_RAY:

    @ray.remote(num_cpus=0)
    class _LoggerRelayActor:
        """Ray actor that receives logger events and writes to events.pb.

        Runs on the head node (where the orchestrator is) and writes
        directly to the Logger's event file.  Workers send events via
        fire-and-forget ``actor.log_event.remote(data)``.
        """

        def __init__(self) -> None:
            self._event_writer = None

        def init_writer(self, events_path: str) -> None:
            from rinnsal.logger.event_file import EventFileWriter

            self._event_writer = EventFileWriter(events_path)

        def log_event(self, event_bytes: bytes) -> None:
            if self._event_writer is None:
                return
            from rinnsal.logger.events_pb2 import Event

            event = Event()
            event.ParseFromString(event_bytes)
            self._event_writer.write(event)
            self._event_writer.flush()

        def shutdown(self) -> None:
            if self._event_writer is not None:
                self._event_writer.flush()

    @ray.remote
    def _ray_execute_task(
        func: Any,
        args: tuple,
        kwargs: dict,
        capture: bool,
        ray_actor_name: str | None = None,
    ) -> tuple[bool, Any, str, str, Any, bytes, list[dict] | None]:
        """Ray remote function that executes a task with logger proxy."""
        import io
        from contextlib import redirect_stderr, redirect_stdout

        from rinnsal.context import Card, current
        from rinnsal.logger.proxy import LoggerProxy

        stdout_capture = io.StringIO()
        stderr_capture = io.StringIO()

        proxy = LoggerProxy(ray_actor_name=ray_actor_name)
        current._set_logger(proxy)
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
                result,
                stdout_capture.getvalue(),
                stderr_capture.getvalue(),
                None,
                proxy.get_buffer(),
                card.serialize() if card else None,
            )
        except Exception as e:
            current._reset()
            return (
                False,
                None,
                stdout_capture.getvalue(),
                stderr_capture.getvalue(),
                e,
                proxy.get_buffer(),
                None,
            )
        finally:
            current._reset_logger()


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
        auto_runtime_env: bool = True,
    ) -> None:
        if not HAS_RAY:
            raise ImportError(
                "ray is required for RayExecutor. "
                "Install with: pip install rinnsal[ray]"
            )

        super().__init__(capture=capture, snapshot=snapshot)
        self._num_cpus = num_cpus
        self._address = address
        self._user_runtime_env = runtime_env
        self._auto_runtime_env = auto_runtime_env
        self._resolved_runtime_env: dict[str, Any] | None = None
        self._initialized = False
        self._logger_actor: Any = None
        self._logger_actor_name: str | None = None

    def set_logger(self, logger: Any) -> None:
        super().set_logger(logger)
        if self._initialized:
            self._create_logger_actor()

    def _create_logger_actor(self) -> None:
        """Create a relay actor on the head node for live logging."""
        if self._logger is None or self._logger_actor is not None:
            return
        if not hasattr(self._logger, "_event_writer"):
            return
        import uuid

        name = f"rinnsal_logger_{uuid.uuid4().hex[:8]}"
        events_path = str(self._logger._event_writer._path)
        actor = _LoggerRelayActor.options(name=name).remote()
        ray.get(actor.init_writer.remote(events_path))
        self._logger_actor = actor
        self._logger_actor_name = name

    def _ensure_initialized(self) -> None:
        """Initialize Ray if not already initialized."""
        if self._initialized:
            return

        # Build runtime_env from provisioner + user overrides.
        # Skip auto-env when connecting to a remote cluster (address is
        # set) — the remote environment is managed separately.  Also
        # skip when running under uv/virtualenv isolation, as Ray
        # rejects pip/uv keys in that context.
        if self._auto_runtime_env and not self._address:
            from rinnsal.execution.provisioner import build_ray_runtime_env

            self._resolved_runtime_env = build_ray_runtime_env(
                user_runtime_env=self._user_runtime_env,
            )
        else:
            self._resolved_runtime_env = self._user_runtime_env

        if not ray.is_initialized():
            init_kwargs: dict[str, Any] = {}
            if self._address:
                init_kwargs["address"] = self._address
            if self._num_cpus:
                init_kwargs["num_cpus"] = self._num_cpus
            if self._resolved_runtime_env:
                init_kwargs["runtime_env"] = self._resolved_runtime_env

            ray.init(**init_kwargs)

        self._initialized = True

        # Create logger actor if logger was set before init
        if self._logger is not None and self._logger_actor is None:
            self._create_logger_actor()

    def submit(
        self,
        expr: TaskExpression,
        resolved_args: tuple[Any, ...],
        resolved_kwargs: dict[str, Any],
    ) -> Future[ExecutionResult]:
        """Submit a task for Ray execution."""
        self._ensure_initialized()

        opts: dict[str, Any] = {}
        if self._resolved_runtime_env:
            opts["runtime_env"] = self._resolved_runtime_env
        task_fn = _ray_execute_task.options(**opts) if opts else _ray_execute_task
        ray_future = task_fn.remote(
            expr.func,
            resolved_args,
            resolved_kwargs,
            self._capture,
            ray_actor_name=self._logger_actor_name,
        )

        # Wrap in a standard Future
        result_future: Future[ExecutionResult] = Future()

        def fetch_result() -> None:
            try:
                (
                    success, result, stdout, stderr,
                    error, logger_events, card,
                ) = ray.get(ray_future)

                if success:
                    result_future.set_result(
                        ExecutionResult(
                            value=result,
                            stdout=stdout,
                            stderr=stderr,
                            success=True,
                            logger_events=logger_events or b"",
                            card=card,
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
                            logger_events=logger_events or b"",
                            card=card,
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

        try:
            opts: dict[str, Any] = {}
            if self._resolved_runtime_env:
                opts["runtime_env"] = self._resolved_runtime_env
            task_fn = (
                _ray_execute_task.options(**opts)
                if opts
                else _ray_execute_task
            )
            ray_future = task_fn.remote(
                expr.func,
                resolved_args,
                resolved_kwargs,
                self._capture,
                ray_actor_name=self._logger_actor_name,
            )
            (
                success, result, stdout, stderr,
                error, logger_events, card,
            ) = ray.get(ray_future)

            if success:
                return ExecutionResult(
                    value=result,
                    stdout=stdout,
                    stderr=stderr,
                    success=True,
                    logger_events=logger_events or b"",
                    card=card,
                )
            else:
                return ExecutionResult(
                    value=None,
                    stdout=stdout,
                    stderr=stderr,
                    success=False,
                    error=error,
                    logger_events=logger_events or b"",
                    card=card,
                )
        except Exception as e:
            return ExecutionResult(
                value=None,
                success=False,
                error=e,
            )

    def shutdown(self, wait: bool = True) -> None:
        """Shutdown the logger relay actor."""
        if self._logger_actor is not None:
            try:
                ray.get(self._logger_actor.shutdown.remote())
                ray.kill(self._logger_actor)
            except Exception:
                pass
            self._logger_actor = None
            self._logger_actor_name = None

    def __repr__(self) -> str:
        addr = self._address or "local"
        return f"RayExecutor(address={addr})"
