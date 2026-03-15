"""DAG evaluation engine."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any, overload

from rinnsal.core.expression import TaskExpression, ValueExpression
from rinnsal.core.graph import DAG
from rinnsal.core.types import Entry
from rinnsal.execution.executor import Executor
from rinnsal.execution.inline import InlineExecutor

if TYPE_CHECKING:
    from rinnsal.persistence.database import Database


class ExecutionEngine:
    """Engine for evaluating task expression DAGs.

    The engine walks the DAG in topological order, resolves dependencies,
    dispatches tasks to an executor, handles retries, and manages results.
    Optionally caches results to a database for persistence.
    """

    def __init__(
        self,
        executor: Executor | None = None,
        database: Database | None = None,
        use_cache: bool = True,
    ) -> None:
        self._executor = executor or InlineExecutor()
        self._database = database
        self._use_cache = use_cache
        self._evaluated: dict[str, Any] = {}

    @property
    def executor(self) -> Executor:
        return self._executor

    @property
    def database(self) -> Database | None:
        return self._database

    @property
    def use_cache(self) -> bool:
        return self._use_cache

    def evaluate(self, *expressions: TaskExpression) -> Any | tuple[Any, ...]:
        """Evaluate one or more task expressions.

        Args:
            *expressions: One or more TaskExpressions to evaluate

        Returns:
            Single value if one expression, tuple if multiple
        """
        if not expressions:
            raise ValueError("At least one expression required")

        # Build DAG from all expressions
        dag = DAG.from_expressions(list(expressions))

        # Get topological order
        ordered = dag.topological_sort()

        # Evaluate each task in order
        for expr in ordered:
            if expr.hash in self._evaluated:
                # Hash found in engine cache - ensure expression has the result
                if not expr.is_evaluated:
                    expr.set_result(self._evaluated[expr.hash])
                continue

            if expr.is_evaluated:
                self._evaluated[expr.hash] = expr.result
                continue

            # Check cache first
            if self._use_cache and self._database is not None:
                cached_entry = self._database.fetch_task_result(expr.hash)
                if cached_entry is not None:
                    expr.set_result(cached_entry.result)
                    self._evaluated[expr.hash] = cached_entry.result
                    continue

            # Resolve arguments
            resolved_args, resolved_kwargs = self._resolve_args(expr)

            # Execute the task
            result, log = self._execute_with_retry(
                expr, resolved_args, resolved_kwargs
            )

            # Store the result
            expr.set_result(result)
            self._evaluated[expr.hash] = result

            # Persist to database
            if self._database is not None:
                entry = Entry(
                    result=result,
                    log=log,
                    metadata={
                        "task_name": expr.task_name,
                        "func_name": expr.func.__name__,
                    },
                    timestamp=datetime.now(),
                )
                self._database.store_task_result(expr.hash, entry)

        # Return results
        if len(expressions) == 1:
            return expressions[0].result
        return tuple(expr.result for expr in expressions)

    def _resolve_args(
        self, expr: TaskExpression
    ) -> tuple[tuple[Any, ...], dict[str, Any]]:
        """Resolve expression arguments to their actual values."""
        resolved_args = []
        for arg in expr.args:
            resolved_args.append(self._resolve_value(arg))

        resolved_kwargs = {}
        for key, value in expr.kwargs.items():
            resolved_kwargs[key] = self._resolve_value(value)

        return tuple(resolved_args), resolved_kwargs

    def _resolve_value(self, value: Any) -> Any:
        """Resolve a single value, unwrapping expressions."""
        if isinstance(value, TaskExpression):
            # The task should already be evaluated
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
    ) -> tuple[Any, str]:
        """Execute a task with retry support.

        Returns:
            Tuple of (result, captured_log)
        """
        max_attempts = expr.task_def.retry + 1
        last_error: Exception | None = None
        combined_log = ""

        for attempt in range(max_attempts):
            result = self._executor.execute_sync(
                expr, resolved_args, resolved_kwargs
            )

            combined_log += result.stdout + result.stderr

            if result.success:
                return result.value, combined_log

            last_error = result.error

            if attempt < max_attempts - 1:
                # Will retry
                continue

        # All attempts failed
        if last_error is not None:
            raise last_error
        raise RuntimeError(
            f"Task '{expr.task_name}' failed after {max_attempts} attempts"
        )

    def clear_cache(self) -> None:
        """Clear the in-memory evaluation cache."""
        self._evaluated.clear()

    def shutdown(self) -> None:
        """Shutdown the executor."""
        self._executor.shutdown()

    def __enter__(self) -> ExecutionEngine:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.shutdown()


# Default engine instance
_default_engine: ExecutionEngine | None = None


def get_engine() -> ExecutionEngine:
    """Get or create the default execution engine."""
    global _default_engine
    if _default_engine is None:
        _default_engine = ExecutionEngine()
    return _default_engine


def set_engine(engine: ExecutionEngine) -> None:
    """Set the default execution engine."""
    global _default_engine
    _default_engine = engine


@overload
def eval(expression: TaskExpression) -> Any: ...


@overload
def eval(
    expression: TaskExpression, *expressions: TaskExpression
) -> tuple[Any, ...]: ...


def eval(*expressions: TaskExpression) -> Any | tuple[Any, ...]:
    """Evaluate one or more task expressions.

    This is the primary entry point for executing tasks. It builds the
    DAG from the expressions and their dependencies, then evaluates
    them in topological order.

    Args:
        *expressions: One or more TaskExpressions to evaluate

    Returns:
        Single value if one expression, tuple of values if multiple

    Examples:
        >>> @task
        ... def source():
        ...     return 10
        ...
        >>> @task
        ... def double(x):
        ...     return x * 2
        ...
        >>> result = eval(double(source()))
        >>> result
        20

        >>> a, b = eval(source(), double(source()))
        >>> a, b
        (10, 20)
    """
    if not expressions:
        raise ValueError("At least one expression required")

    engine = get_engine()
    return engine.evaluate(*expressions)
