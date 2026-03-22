"""Abstract Executor protocol for task execution."""

from __future__ import annotations

from abc import ABC, abstractmethod
from concurrent.futures import Future
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from rinnsal.core.expression import TaskExpression


@dataclass
class ExecutionResult:
    """Result of a task execution."""

    value: Any
    stdout: str = ""
    stderr: str = ""
    success: bool = True
    error: Exception | None = None
    card: list[dict] | None = None


class Executor(ABC):
    """Abstract base class for task executors.

    Executors are responsible for running task functions and returning
    their results. Each executor manages a pool of workers and tasks
    are submitted to this pool for execution.
    """

    def __init__(
        self,
        capture: bool = True,
        snapshot: bool = True,
    ) -> None:
        self._capture = capture
        self._snapshot = snapshot

    @property
    def capture(self) -> bool:
        """Whether to capture stdout/stderr during execution."""
        return self._capture

    @property
    def snapshot(self) -> bool:
        """Whether to create code snapshots before execution."""
        return self._snapshot

    @abstractmethod
    def submit(
        self,
        expr: TaskExpression,
        resolved_args: tuple[Any, ...],
        resolved_kwargs: dict[str, Any],
    ) -> Future[ExecutionResult]:
        """Submit a task for execution.

        Args:
            expr: The task expression to execute
            resolved_args: The resolved positional arguments
            resolved_kwargs: The resolved keyword arguments

        Returns:
            A Future that will contain the ExecutionResult
        """
        ...

    def execute_sync(
        self,
        expr: TaskExpression,
        resolved_args: tuple[Any, ...],
        resolved_kwargs: dict[str, Any],
    ) -> ExecutionResult:
        """Execute a task synchronously.

        Default implementation submits and waits for the future.
        Subclasses may override for more efficient sync execution.
        """
        future = self.submit(expr, resolved_args, resolved_kwargs)
        return future.result()

    @abstractmethod
    def shutdown(self, wait: bool = True) -> None:
        """Shutdown the executor and release resources."""
        ...

    def __enter__(self) -> Executor:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.shutdown(wait=True)
