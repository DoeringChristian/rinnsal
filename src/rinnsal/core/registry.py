"""Global task registry for content-addressed deduplication."""

from __future__ import annotations

import weakref
from contextlib import contextmanager
from contextvars import ContextVar
from typing import TYPE_CHECKING, Iterator

if TYPE_CHECKING:
    from rinnsal.core.expression import TaskExpression


class TaskRegistry:
    """Registry for task expression deduplication.

    Tasks with the same content hash are deduplicated - the same
    function called with the same arguments returns the same
    TaskExpression object.
    """

    def __init__(self) -> None:
        # Use weak references so expressions can be garbage collected
        self._expressions: dict[str, weakref.ref[TaskExpression]] = {}

    def get_or_create(
        self,
        hash_key: str,
        factory: callable,
    ) -> TaskExpression:
        """Get an existing expression by hash or create a new one.

        Args:
            hash_key: The content hash of the expression
            factory: A callable that creates the expression if not found

        Returns:
            The deduplicated TaskExpression
        """
        ref = self._expressions.get(hash_key)
        if ref is not None:
            expr = ref()
            if expr is not None:
                return expr

        # Create new expression
        expr = factory()
        self._expressions[hash_key] = weakref.ref(expr)
        return expr

    def get(self, hash_key: str) -> TaskExpression | None:
        """Get an expression by hash, or None if not found."""
        ref = self._expressions.get(hash_key)
        if ref is not None:
            return ref()
        return None

    def register(self, expr: TaskExpression) -> None:
        """Register an expression in the registry."""
        self._expressions[expr.hash] = weakref.ref(expr)

    def clear(self) -> None:
        """Clear all registered expressions."""
        self._expressions.clear()

    def cleanup(self) -> int:
        """Remove dead weak references. Returns count of removed entries."""
        dead_keys = [k for k, v in self._expressions.items() if v() is None]
        for key in dead_keys:
            del self._expressions[key]
        return len(dead_keys)

    def __len__(self) -> int:
        return len(self._expressions)

    def __contains__(self, hash_key: str) -> bool:
        ref = self._expressions.get(hash_key)
        return ref is not None and ref() is not None


# Global registry
_global_registry = TaskRegistry()


def get_registry() -> TaskRegistry:
    """Get the global task registry."""
    return _global_registry


class FlowContext:
    """Context for tracking tasks created within a flow.

    Tasks created inside a flow context are tracked separately and
    cleaned up when the context exits, ensuring flow self-containment.
    """

    def __init__(self) -> None:
        self._tasks: list[TaskExpression] = []
        self._parent: FlowContext | None = None

    def add_task(self, expr: TaskExpression) -> None:
        """Register a task expression in this context."""
        self._tasks.append(expr)

    def get_tasks(self) -> list[TaskExpression]:
        """Get all tasks registered in this context."""
        return list(self._tasks)

    def clear(self) -> None:
        """Clear all tasks from this context."""
        self._tasks.clear()

    def __len__(self) -> int:
        return len(self._tasks)


# Context variable for the current flow context
_flow_context: ContextVar[FlowContext | None] = ContextVar(
    "flow_context", default=None
)


def get_flow_context() -> FlowContext | None:
    """Get the current flow context, or None if not in a flow."""
    return _flow_context.get()


@contextmanager
def flow_scope() -> Iterator[FlowContext]:
    """Create a new flow context scope.

    Tasks created within this scope are tracked and can be retrieved
    for DAG construction.
    """
    ctx = FlowContext()
    parent = _flow_context.get()
    ctx._parent = parent

    token = _flow_context.set(ctx)
    try:
        yield ctx
    finally:
        _flow_context.reset(token)


def register_task_in_context(expr: TaskExpression) -> None:
    """Register a task expression in the current flow context if one exists."""
    ctx = _flow_context.get()
    if ctx is not None:
        ctx.add_task(expr)
