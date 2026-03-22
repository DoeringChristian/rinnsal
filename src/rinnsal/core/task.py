"""Task decorator and TaskDef class."""

from __future__ import annotations

import functools
from typing import Any, Callable, ParamSpec, TypeVar, overload

from rinnsal.core.expression import TaskExpression
from rinnsal.core.hashing import compute_task_hash
from rinnsal.core.registry import get_registry

P = ParamSpec("P")
R = TypeVar("R")


_SENTINEL = object()


class TaskDef:
    """A task definition wrapping a function.

    Created by the @task decorator. When called, returns a TaskExpression
    instead of executing immediately.
    """

    def __init__(
        self,
        func: Callable[..., Any],
        retry: int = 0,
        timeout: float | None = None,
        catch: Any = _SENTINEL,
    ) -> None:
        self._func = func
        self._retry = retry
        self._timeout = timeout
        self._catch = catch
        functools.update_wrapper(self, func)

    @property
    def func(self) -> Callable[..., Any]:
        return self._func

    @property
    def retry(self) -> int:
        return self._retry

    @property
    def timeout(self) -> float | None:
        return self._timeout

    @property
    def catch(self) -> Any:
        return self._catch

    @property
    def catch_enabled(self) -> bool:
        return self._catch is not _SENTINEL

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

    def map(self, *iterables: Any, **kwargs: Any) -> list[TaskExpression]:
        """Apply this task to each element of the iterable(s).

        With a single iterable, each element is passed as the first arg.
        With multiple iterables, elements are zipped and unpacked as args.

        Returns a list of TaskExpressions, one per element.
        """
        func_name = self._func.__name__
        if len(iterables) == 1:
            results = []
            for i, item in enumerate(iterables[0]):
                expr = self(item, **kwargs)
                if expr._name is None:
                    expr.name(f"{func_name}[{i}]")
                results.append(expr)
            return results
        results = []
        for i, args in enumerate(zip(*iterables)):
            expr = self(*args, **kwargs)
            if expr._name is None:
                expr.name(f"{func_name}[{i}]")
            results.append(expr)
        return results

    def __repr__(self) -> str:
        return f"TaskDef({self._func.__name__}, retry={self._retry})"


@overload
def task(func: Callable[P, R]) -> TaskDef: ...


@overload
def task(
    *,
    retry: int = 0,
    timeout: float | None = None,
    catch: Any = _SENTINEL,
) -> Callable[[Callable[P, R]], TaskDef]: ...


def task(
    func: Callable[P, R] | None = None,
    *,
    retry: int = 0,
    timeout: float | None = None,
    catch: Any = _SENTINEL,
) -> TaskDef | Callable[[Callable[P, R]], TaskDef]:
    """Decorator to create a lazy task.

    A task is a lazy computation node. When called, it returns a
    TaskExpression instead of executing immediately. The expression
    captures the function and arguments, forming a node in the DAG.

    Args:
        func: The function to wrap (when used without parentheses)
        retry: Number of retry attempts on failure (default 0)
        timeout: Maximum seconds per attempt (default None = no limit)
        catch: If set, catch failures and use this value as the result.
            ``catch=True`` uses None, any other value is used directly.

    Returns:
        A TaskDef that creates TaskExpressions when called

    Examples:
        @task
        def source():
            return 10

        @task(retry=3, timeout=60)
        def flaky_task():
            ...

        @task(catch=True)
        def risky():
            ...

        # Returns TaskExpression, not the result
        expr = source()

        # Fan-out over a list
        results = source.map([1, 2, 3])
    """
    if func is not None:
        # Used as @task without parentheses
        return TaskDef(func, retry=retry, timeout=timeout, catch=catch)

    # Used as @task(...) with arguments
    def decorator(fn: Callable[P, R]) -> TaskDef:
        return TaskDef(fn, retry=retry, timeout=timeout, catch=catch)

    return decorator


