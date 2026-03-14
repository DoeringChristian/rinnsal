"""Progress reporting with callbacks."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from typing import Any, Callable, Protocol


class EventType(Enum):
    """Types of progress events."""

    TASK_STARTED = auto()
    TASK_COMPLETED = auto()
    TASK_FAILED = auto()
    TASK_CACHED = auto()
    FLOW_STARTED = auto()
    FLOW_COMPLETED = auto()
    FLOW_FAILED = auto()


@dataclass
class ProgressEvent:
    """A progress event."""

    event_type: EventType
    task_name: str | None = None
    flow_name: str | None = None
    result: Any = None
    error: Exception | None = None
    metadata: dict[str, Any] | None = None


class ProgressCallback(Protocol):
    """Protocol for progress callbacks."""

    def __call__(self, event: ProgressEvent) -> None: ...


class ProgressReporter:
    """Event-based progress reporter.

    Allows multiple callbacks to be registered for progress events.
    """

    def __init__(self) -> None:
        self._callbacks: list[ProgressCallback] = []

    def add_callback(self, callback: ProgressCallback) -> None:
        """Register a progress callback."""
        self._callbacks.append(callback)

    def remove_callback(self, callback: ProgressCallback) -> None:
        """Remove a progress callback."""
        if callback in self._callbacks:
            self._callbacks.remove(callback)

    def clear_callbacks(self) -> None:
        """Remove all callbacks."""
        self._callbacks.clear()

    def report(self, event: ProgressEvent) -> None:
        """Report a progress event to all callbacks."""
        for callback in self._callbacks:
            try:
                callback(event)
            except Exception:
                # Don't let callback errors break execution
                pass

    def task_started(self, task_name: str) -> None:
        """Report that a task has started."""
        self.report(ProgressEvent(
            event_type=EventType.TASK_STARTED,
            task_name=task_name,
        ))

    def task_completed(self, task_name: str, result: Any = None) -> None:
        """Report that a task has completed successfully."""
        self.report(ProgressEvent(
            event_type=EventType.TASK_COMPLETED,
            task_name=task_name,
            result=result,
        ))

    def task_failed(self, task_name: str, error: Exception) -> None:
        """Report that a task has failed."""
        self.report(ProgressEvent(
            event_type=EventType.TASK_FAILED,
            task_name=task_name,
            error=error,
        ))

    def task_cached(self, task_name: str, result: Any = None) -> None:
        """Report that a task result was loaded from cache."""
        self.report(ProgressEvent(
            event_type=EventType.TASK_CACHED,
            task_name=task_name,
            result=result,
        ))

    def flow_started(self, flow_name: str) -> None:
        """Report that a flow has started."""
        self.report(ProgressEvent(
            event_type=EventType.FLOW_STARTED,
            flow_name=flow_name,
        ))

    def flow_completed(self, flow_name: str) -> None:
        """Report that a flow has completed."""
        self.report(ProgressEvent(
            event_type=EventType.FLOW_COMPLETED,
            flow_name=flow_name,
        ))

    def flow_failed(self, flow_name: str, error: Exception) -> None:
        """Report that a flow has failed."""
        self.report(ProgressEvent(
            event_type=EventType.FLOW_FAILED,
            flow_name=flow_name,
            error=error,
        ))


# Global reporter instance
_global_reporter = ProgressReporter()


def get_reporter() -> ProgressReporter:
    """Get the global progress reporter."""
    return _global_reporter


def on_progress(callback: ProgressCallback) -> ProgressCallback:
    """Decorator to register a progress callback."""
    _global_reporter.add_callback(callback)
    return callback
