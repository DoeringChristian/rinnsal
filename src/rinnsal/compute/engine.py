"""DAG evaluation engine.

Walks the DAG in topological order, resolves dependencies, dispatches
tasks to an :class:`Executor`, handles retries, and persists results to
an optional :class:`Database` for caching. Task-level observability is
now the user's responsibility via :class:`rinnsal.aim.AimLogger`; this
module neither instantiates nor reads a logger.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any, overload

from rinnsal.modeling.expression import TaskExpression, ValueExpression
from rinnsal.modeling.graph import DAG
from rinnsal.modeling.types import Entry
from rinnsal.compute.executor import Executor
from rinnsal.compute.inline import InlineExecutor

if TYPE_CHECKING:
    from rinnsal.data.database import Database


def _summarize_value(v: Any, max_len: int = 200) -> Any:
    """Summarize a value for parameter recording / aim hparams."""
    if v is None or isinstance(v, (bool, int, float)):
        return v
    if isinstance(v, str):
        return v if len(v) <= max_len else v[:max_len] + "..."
    if isinstance(v, dict):
        return {str(k): _summarize_value(val) for k, val in v.items()}
    if hasattr(v, "__dataclass_fields__"):
        return {
            k: _summarize_value(getattr(v, k))
            for k in v.__dataclass_fields__
        }
    if hasattr(v, "__attrs_attrs__"):
        return {
            a.name: _summarize_value(getattr(v, a.name))
            for a in v.__attrs_attrs__
        }
    if hasattr(v, "to_dict"):
        try:
            return _summarize_value(v.to_dict())
        except Exception:
            pass
    if isinstance(v, (list, tuple)):
        t = type(v).__name__
        n = len(v)
        if n <= 5:
            return [_summarize_value(x) for x in v]
        preview = [_summarize_value(x) for x in v[:3]]
        return {"_type": t, "_len": n, "_preview": preview}
    t = type(v).__name__
    shape = getattr(v, "shape", None)
    dtype = getattr(v, "dtype", None)
    if shape is not None:
        info: dict[str, Any] = {"_type": t, "_shape": list(shape)}
        if dtype is not None:
            info["_dtype"] = str(dtype)
        return info
    r = repr(v)
    if len(r) > max_len:
        r = r[:max_len] + "..."
    return {"_type": t, "_repr": r}


def _named_args(
    expr: TaskExpression,
    resolved_args: tuple[Any, ...],
    resolved_kwargs: dict[str, Any],
) -> dict[str, Any]:
    """Match positional args to parameter names for the task.

    Returns ``{param_name: value}`` with positional args resolved via
    :func:`inspect.signature`. Used by the engine to stash the current
    task's inputs on :mod:`rinnsal.context` so ``AimLogger`` can pick
    them up as hparams without the user threading them through.
    """
    import inspect

    out: dict[str, Any] = {}
    try:
        sig = inspect.signature(expr.func)
        names = list(sig.parameters.keys())
        for i, val in enumerate(resolved_args):
            out[names[i] if i < len(names) else f"arg{i}"] = val
    except (ValueError, TypeError):
        for i, val in enumerate(resolved_args):
            out[f"arg{i}"] = val
    out.update(resolved_kwargs)
    return out


class ExecutionEngine:
    """Engine for evaluating task expression DAGs."""

    def __init__(
        self,
        executor: Executor | None = None,
        database: "Database | None" = None,
    ) -> None:
        self._executor = executor or InlineExecutor()
        self._database = database
        self._evaluated: dict[str, Any] = {}

    @property
    def executor(self) -> Executor:
        return self._executor

    @property
    def database(self) -> "Database | None":
        return self._database

    def evaluate(self, *expressions: TaskExpression) -> Any | tuple[Any, ...]:
        if not expressions:
            raise ValueError("At least one expression required")

        dag = DAG.from_expressions(list(expressions))
        ordered = dag.topological_sort()

        for expr in ordered:
            if expr.hash in self._evaluated:
                if not expr.is_evaluated:
                    expr.set_result(self._evaluated[expr.hash])
                continue

            if expr.is_evaluated:
                self._evaluated[expr.hash] = expr.result
                continue

            resolved_args, resolved_kwargs = self._resolve_args(expr)

            checkpoint_path = None
            if self._database is not None and hasattr(
                self._database, "_task_results_dir"
            ):
                task_dir = self._database._task_results_dir(
                    expr.hash, task_name=expr.task_name or None
                )
                task_dir.mkdir(parents=True, exist_ok=True)
                checkpoint_path = task_dir / "checkpoint.dat"

            # Publish the task's identity + inputs for in-task readers
            # (AimLogger pulls task_args to build run["hparams"]).
            from rinnsal.context import current as _ctx

            _ctx._set_task_name(expr.task_name or "")
            _ctx._set_task_hash(expr.hash or "")
            _ctx._set_task_args(
                _named_args(expr, resolved_args, resolved_kwargs)
            )

            result, _log = self._execute_with_retry(
                expr, resolved_args, resolved_kwargs,
                checkpoint_path=checkpoint_path,
            )

            expr.set_result(result)
            self._evaluated[expr.hash] = result

            if self._database is not None:
                metadata: dict[str, Any] = {
                    "task_name": expr.task_name,
                    "func_name": expr.func.__name__,
                }
                if expr.task_def.resources:
                    metadata["resources"] = expr.task_def.resources.as_dict()

                snapshot_obj = None
                try:
                    from rinnsal.versioning.snapshot import get_snapshot_manager
                    from rinnsal.modeling.types import Snapshot

                    manager = get_snapshot_manager()
                    snap_hash, snap_path = manager.create_snapshot(expr.func)
                    if snap_hash:
                        snapshot_obj = Snapshot(
                            hash=snap_hash, path=snap_path
                        )
                except Exception:
                    pass

                entry = Entry(
                    result=result,
                    log=_log,
                    metadata=metadata,
                    timestamp=datetime.now(),
                    snapshot=snapshot_obj,
                )
                self._database.store_task_result(
                    expr.hash, entry, expr.task_name
                )

        if len(expressions) == 1:
            return expressions[0].result
        return tuple(expr.result for expr in expressions)

    def _resolve_args(
        self, expr: TaskExpression
    ) -> tuple[tuple[Any, ...], dict[str, Any]]:
        resolved_args = []
        for arg in expr.args:
            resolved_args.append(self._resolve_value(arg))

        resolved_kwargs = {}
        for key, value in expr.kwargs.items():
            resolved_kwargs[key] = self._resolve_value(value)

        return tuple(resolved_args), resolved_kwargs

    def _resolve_value(self, value: Any) -> Any:
        if isinstance(value, TaskExpression):
            if not value.is_evaluated:
                raise RuntimeError(
                    f"Dependency '{value.task_name}' not yet evaluated"
                )
            return value.result
        if isinstance(value, ValueExpression):
            return value.value
        return value

    def _execute_with_retry(
        self,
        expr: TaskExpression,
        resolved_args: tuple[Any, ...],
        resolved_kwargs: dict[str, Any],
        checkpoint_path: Any = None,
    ) -> tuple[Any, str]:
        from concurrent.futures import TimeoutError as FuturesTimeoutError
        from rinnsal.compute.executor import ExecutionResult

        max_attempts = expr.task_def.retry + 1
        timeout = expr.task_def.timeout
        last_error: Exception | None = None
        combined_log = ""

        if checkpoint_path is not None:
            from rinnsal.context import Checkpoint, current as _current

            _current._set_checkpoint(Checkpoint(path=checkpoint_path))
            self._executor._checkpoint_path = str(checkpoint_path)

        for attempt in range(max_attempts):
            if timeout is not None:
                future = self._executor.submit(
                    expr, resolved_args, resolved_kwargs
                )
                try:
                    result = future.result(timeout=timeout)
                except FuturesTimeoutError:
                    result = ExecutionResult(
                        value=None,
                        success=False,
                        error=TimeoutError(
                            f"Task '{expr.task_name}' timed out "
                            f"after {timeout}s"
                        ),
                    )
            else:
                result = self._executor.execute_sync(
                    expr, resolved_args, resolved_kwargs
                )

            attempt_log = result.stdout + result.stderr
            combined_log += attempt_log

            if result.success:
                return result.value, combined_log

            if attempt_log:
                import sys

                sys.stderr.write(attempt_log)
                sys.stderr.flush()

            last_error = result.error

            if attempt < max_attempts - 1:
                continue

        if expr.task_def.catch_enabled:
            catch_val = expr.task_def.catch
            default = None if catch_val is True else catch_val
            return default, combined_log

        if last_error is not None:
            raise last_error
        raise RuntimeError(
            f"Task '{expr.task_name}' failed after {max_attempts} attempts"
        )

    def clear_cache(self) -> None:
        self._evaluated.clear()

    def shutdown(self, wait: bool = True) -> None:
        self._executor.shutdown(wait=wait)

    def __enter__(self) -> "ExecutionEngine":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.shutdown()


# Default engine instance
_default_engine: ExecutionEngine | None = None


def get_engine() -> ExecutionEngine:
    global _default_engine
    if _default_engine is None:
        _default_engine = _create_default_engine()
    return _default_engine


def _create_default_engine() -> ExecutionEngine:
    import argparse

    from rinnsal.cli.flags import add_builtin_flags, extract_builtin_flags

    parser = argparse.ArgumentParser(add_help=False)
    add_builtin_flags(parser)
    flags, _ = parser.parse_known_args()
    builtin = extract_builtin_flags(flags)

    from rinnsal.compute.factory import create_executor

    executor = create_executor(
        builtin["executor"], capture=not builtin["no_capture"]
    )

    from rinnsal.data.file_store import FileDatabase

    database = FileDatabase(root=builtin["db_path"])

    return ExecutionEngine(executor=executor, database=database)


def set_engine(engine: ExecutionEngine) -> None:
    global _default_engine
    _default_engine = engine


@overload
def eval(expression: TaskExpression) -> Any: ...


@overload
def eval(
    expression: TaskExpression, *expressions: TaskExpression
) -> tuple[Any, ...]: ...


def eval(*expressions: TaskExpression) -> Any | tuple[Any, ...]:
    """Evaluate one or more task expressions."""
    if not expressions:
        raise ValueError("At least one expression required")

    engine = get_engine()
    return engine.evaluate(*expressions)
