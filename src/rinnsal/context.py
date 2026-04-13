"""Task execution context: logger, checkpoint, current task name."""

from __future__ import annotations

from contextvars import ContextVar
from pathlib import Path
from typing import Any


class Checkpoint:
    """Save and load task checkpoint data for resumable execution.

    Checkpoints are stored as pickle files alongside task results.
    A task can call ``current.checkpoint.save(state)`` periodically
    and ``current.checkpoint.load()`` at the start to resume from
    the last checkpoint on retry or ``--resume``.
    """

    def __init__(self, path: Path | None = None) -> None:
        self._path = path

    def save(self, data: Any) -> None:
        """Save checkpoint data to disk."""
        if self._path is None:
            return
        import cloudpickle

        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._path.with_suffix(".tmp")
        with open(tmp, "wb") as f:
            cloudpickle.dump(data, f)
        tmp.rename(self._path)

    def load(self) -> Any:
        """Load checkpoint data. Returns None if no checkpoint exists."""
        if self._path is None or not self._path.exists():
            return None
        import cloudpickle

        with open(self._path, "rb") as f:
            return cloudpickle.load(f)

    def clear(self) -> None:
        """Remove the checkpoint file."""
        if self._path is not None and self._path.exists():
            self._path.unlink()


class _Current:
    """Context-aware accessor for the active task's logger, checkpoint, and name.

    Cards are no longer a separate context construct. Use
    ``current.logger.card("name")`` to compose a card through the unified
    Logger API; see :mod:`rinnsal.data.logger.card`.
    """

    _checkpoint_var: ContextVar[Checkpoint | None] = ContextVar(
        "_checkpoint_var", default=None
    )
    _logger_var: ContextVar[Any] = ContextVar("_logger_var", default=None)
    _task_name_var: ContextVar[str] = ContextVar("_task_name_var", default="")

    @property
    def checkpoint(self) -> Checkpoint:
        """Get the checkpoint for the current task."""
        cp = self._checkpoint_var.get(None)
        if cp is None:
            cp = Checkpoint()
            self._checkpoint_var.set(cp)
        return cp

    @property
    def logger(self) -> Any:
        """Get the logger for the current flow execution.

        Returns None if no logger is active.
        """
        return self._logger_var.get(None)

    @property
    def task_name(self) -> str:
        """Get the name of the currently executing task."""
        return self._task_name_var.get("")

    def _set_checkpoint(self, checkpoint: Checkpoint) -> None:
        self._checkpoint_var.set(checkpoint)

    def _set_logger(self, logger: Any) -> None:
        self._logger_var.set(logger)

    def _set_task_name(self, name: str) -> None:
        self._task_name_var.set(name)

    def _reset_checkpoint(self) -> None:
        self._checkpoint_var.set(None)

    def _reset_logger(self) -> None:
        self._logger_var.set(None)


current = _Current()
