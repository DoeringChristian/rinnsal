"""Lazy expression tree nodes for DAG construction."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Callable

from rinnsal.core.hashing import compute_task_hash, hash_value

if TYPE_CHECKING:
    from rinnsal.core.task import TaskDef


class Expression(ABC):
    """Base class for lazy expression nodes.

    Expressions form a DAG where TaskExpressions depend on other expressions
    as arguments. Expressions are content-addressed via their hash.
    """

    _hash: str | None = None

    @property
    @abstractmethod
    def hash(self) -> str:
        """Return the content-addressed hash of this expression."""
        ...

    @abstractmethod
    def get_dependencies(self) -> list[Expression]:
        """Return all expression dependencies of this expression."""
        ...

    def __hash__(self) -> int:
        return hash(self.hash)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Expression):
            return self.hash == other.hash
        return False


class ValueExpression(Expression):
    """An expression wrapping a concrete value.

    Used for non-task arguments that are passed to tasks.
    """

    def __init__(self, value: Any) -> None:
        self._value = value
        self._hash: str | None = None

    @property
    def value(self) -> Any:
        return self._value

    @property
    def hash(self) -> str:
        if self._hash is None:
            self._hash = hash_value(self._value)
        return self._hash

    def get_dependencies(self) -> list[Expression]:
        return []

    def __repr__(self) -> str:
        return f"ValueExpression({self._value!r})"


class TaskExpression(Expression):
    """A lazy task invocation expression.

    Represents a task call that hasn't been executed yet. Contains
    references to the task definition and its arguments (which may
    themselves be expressions).
    """

    def __init__(
        self,
        task_def: TaskDef,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> None:
        self._task_def = task_def
        self._args = args
        self._kwargs = kwargs
        self._name: str | None = None
        self._hash: str | None = None
        self._result: Any = None
        self._evaluated = False

    @property
    def task_def(self) -> TaskDef:
        return self._task_def

    @property
    def func(self) -> Callable[..., Any]:
        return self._task_def.func

    @property
    def args(self) -> tuple[Any, ...]:
        return self._args

    @property
    def kwargs(self) -> dict[str, Any]:
        return self._kwargs

    @property
    def task_name(self) -> str:
        """Return the human-readable name of this task expression."""
        if self._name is not None:
            return self._name
        return self._task_def.func.__name__

    @property
    def hash(self) -> str:
        if self._hash is None:
            self._hash = compute_task_hash(
                self._task_def.func, self._args, self._kwargs
            )
        return self._hash

    @property
    def is_evaluated(self) -> bool:
        return self._evaluated

    @property
    def result(self) -> Any:
        if not self._evaluated:
            raise RuntimeError(f"Task '{self.task_name}' has not been evaluated yet")
        return self._result

    def set_result(self, result: Any) -> None:
        """Set the result of this expression after evaluation."""
        self._result = result
        self._evaluated = True

    def name(self, name: str) -> TaskExpression:
        """Set a human-readable name for this task expression.

        Returns self for method chaining.
        """
        self._name = name
        return self

    def get_dependencies(self) -> list[Expression]:
        """Return all expression dependencies from args and kwargs."""
        deps: list[Expression] = []

        for arg in self._args:
            if isinstance(arg, Expression):
                deps.append(arg)

        for value in self._kwargs.values():
            if isinstance(value, Expression):
                deps.append(value)

        return deps

    def get_all_dependencies(self) -> set[TaskExpression]:
        """Recursively collect all TaskExpression dependencies."""
        all_deps: set[TaskExpression] = set()
        to_visit = [self]

        while to_visit:
            current = to_visit.pop()
            for dep in current.get_dependencies():
                if isinstance(dep, TaskExpression) and dep not in all_deps:
                    all_deps.add(dep)
                    to_visit.append(dep)

        return all_deps

    def eval(self) -> Any:
        """Evaluate this task expression and return its result.

        This is a convenience method that uses the default engine.
        """
        from rinnsal.runtime.engine import eval as engine_eval

        return engine_eval(self)

    def __str__(self) -> str:
        """Evaluate and return string representation of result."""
        return str(self.eval())

    def __repr__(self) -> str:
        return f"TaskExpression({self.task_name}, hash={self.hash[:8]}...)"


def wrap_value(value: Any) -> Expression | Any:
    """Wrap a value in a ValueExpression if it's not already an Expression.

    TaskExpressions are returned as-is to preserve the dependency chain.
    """
    if isinstance(value, Expression):
        return value
    return ValueExpression(value)


def unwrap_value(value: Any) -> Any:
    """Unwrap a ValueExpression to get the underlying value.

    TaskExpressions return their result (must be evaluated first).
    Other values are returned as-is.
    """
    if isinstance(value, TaskExpression):
        return value.result
    if isinstance(value, ValueExpression):
        return value.value
    return value
