"""Task decorator and TaskDef class."""

from __future__ import annotations

import functools
from typing import Any, Callable, ParamSpec, TypeVar, overload

from rinnsal.core.expression import TaskExpression
from rinnsal.core.hashing import compute_task_hash
from rinnsal.core.registry import get_registry

P = ParamSpec("P")
R = TypeVar("R")


class TaskDef:
    """A task definition wrapping a function.

    Created by the @task decorator. When called, returns a TaskExpression
    instead of executing immediately.
    """

    def __init__(
        self,
        func: Callable[..., Any],
        retry: int = 0,
    ) -> None:
        self._func = func
        self._retry = retry
        functools.update_wrapper(self, func)

    @property
    def func(self) -> Callable[..., Any]:
        return self._func

    @property
    def retry(self) -> int:
        return self._retry

    def __call__(self, *args: Any, **kwargs: Any) -> TaskExpression:
        """Create a lazy TaskExpression for this task call.

        The task is not executed immediately - instead, a TaskExpression
        is returned that can be evaluated later.

        If ``name=`` is passed, it is also used to set the expression's
        human-readable name.
        """
        # Compute the hash for deduplication
        hash_key = compute_task_hash(self._func, args, kwargs)

        # Get or create the expression using the registry
        registry = get_registry()

        def create_expression() -> TaskExpression:
            expr = TaskExpression(self, args, kwargs)
            return expr

        expr = registry.get_or_create(hash_key, create_expression)

        if "name" in kwargs:
            expr.name(str(kwargs["name"]))

        from rinnsal.core.flow import notify_task_created
        notify_task_created(expr)

        return expr

    def __repr__(self) -> str:
        return f"TaskDef({self._func.__name__}, retry={self._retry})"


@overload
def task(func: Callable[P, R]) -> TaskDef: ...


@overload
def task(
    *,
    retry: int = 0,
) -> Callable[[Callable[P, R]], TaskDef]: ...


def task(
    func: Callable[P, R] | None = None,
    *,
    retry: int = 0,
) -> TaskDef | Callable[[Callable[P, R]], TaskDef]:
    """Decorator to create a lazy task.

    A task is a lazy computation node. When called, it returns a
    TaskExpression instead of executing immediately. The expression
    captures the function and arguments, forming a node in the DAG.

    Args:
        func: The function to wrap (when used without parentheses)
        retry: Number of retry attempts on failure (default 0)

    Returns:
        A TaskDef that creates TaskExpressions when called

    Examples:
        @task
        def source():
            return 10

        @task(retry=3)
        def flaky_task():
            ...

        # Returns TaskExpression, not the result
        expr = source()

        # Evaluate to get the result
        result = expr.eval()
    """
    if func is not None:
        # Used as @task without parentheses
        return TaskDef(func, retry=retry)

    # Used as @task(...) with arguments
    def decorator(fn: Callable[P, R]) -> TaskDef:
        return TaskDef(fn, retry=retry)

    return decorator


