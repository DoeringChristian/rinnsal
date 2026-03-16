"""ANSI progress bar for task execution."""

from __future__ import annotations

import sys
import time
from dataclasses import dataclass, field
from typing import TextIO


@dataclass
class ProgressState:
    """State of progress tracking."""

    total: int = 0
    passed: int = 0
    failed: int = 0
    cached: int = 0
    skipped: int = 0
    current_task: str = ""
    start_time: float = field(default_factory=time.time)

    @property
    def completed(self) -> int:
        return self.passed + self.failed + self.cached + self.skipped

    @property
    def percentage(self) -> float:
        if self.total == 0:
            return 0.0
        return (self.completed / self.total) * 100

    @property
    def elapsed(self) -> float:
        return time.time() - self.start_time


class ProgressBar:
    """ANSI progress bar for flow execution.

    Renders to stderr showing:
    - A visual bar with completion percentage
    - Counts of passed/failed/cached tasks
    - The name of the currently running task
    """

    def __init__(
        self,
        total: int,
        width: int = 40,
        stream: TextIO | None = None,
    ) -> None:
        self._state = ProgressState(total=total)
        self._width = width
        self._stream = stream or sys.stderr
        self._last_line_length = 0

    @property
    def state(self) -> ProgressState:
        return self._state

    def start(self, task_name: str) -> None:
        """Mark a task as started."""
        self._state.current_task = task_name
        self._render()

    def complete(self, task_name: str, cached: bool = False) -> None:
        """Mark a task as completed successfully."""
        if cached:
            self._state.cached += 1
        else:
            self._state.passed += 1
        self._state.current_task = ""
        self._render()

    def fail(self, task_name: str) -> None:
        """Mark a task as failed."""
        self._state.failed += 1
        self._state.current_task = ""
        self._render()

    def skip(self, task_name: str) -> None:
        """Mark a task as skipped (dependency failed)."""
        self._state.skipped += 1
        self._state.current_task = ""
        self._render()

    def finish(self) -> None:
        """Finish progress tracking and render final state."""
        self._state.current_task = ""
        self._render(final=True)
        self._stream.write("\n")
        self._stream.flush()

    def _render(self, final: bool = False) -> None:
        """Render the progress bar."""
        state = self._state

        # Build progress bar
        filled = int(self._width * state.completed / max(state.total, 1))
        bar = "█" * filled + "░" * (self._width - filled)

        # Build status line: 1/5 2 passed, 1 cached
        count = f"{state.completed}/{state.total}"
        parts = []
        if state.passed:
            parts.append(f"{state.passed} passed")
        if state.cached:
            parts.append(f"{state.cached} cached")
        if state.failed:
            parts.append(f"{state.failed} failed")
        if state.skipped:
            parts.append(f"{state.skipped} skipped")
        stats = ", ".join(parts) if parts else ""

        if state.current_task and not final:
            line = f"\r{bar} {count} {stats} [running: {state.current_task}]"
        else:
            line = f"\r{bar} {count} {stats}"

        # Clear previous line if needed
        clear = " " * max(0, self._last_line_length - len(line))

        self._stream.write(line + clear)
        self._stream.flush()
        self._last_line_length = len(line)


class SilentProgress:
    """Silent progress tracker that doesn't output anything."""

    def __init__(self, total: int) -> None:
        self._state = ProgressState(total=total)

    @property
    def state(self) -> ProgressState:
        return self._state

    def start(self, task_name: str) -> None:
        self._state.current_task = task_name

    def complete(self, task_name: str, cached: bool = False) -> None:
        if cached:
            self._state.cached += 1
        else:
            self._state.passed += 1
        self._state.current_task = ""

    def fail(self, task_name: str) -> None:
        self._state.failed += 1
        self._state.current_task = ""

    def skip(self, task_name: str) -> None:
        self._state.skipped += 1
        self._state.current_task = ""

    def finish(self) -> None:
        pass
