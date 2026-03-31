"""Task execution context with card and checkpoint support."""

from __future__ import annotations

import base64
import io
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class CardItem:
    """A single item in a task card."""

    kind: str  # "text", "image", "html", "table"
    content: Any
    title: str = ""




def _normalize_table(
    data: Any, headers: list[str] | None = None
) -> dict[str, Any]:
    """Normalize table data to a serializable dict."""
    # Handle pandas DataFrame
    if hasattr(data, "to_dict") and hasattr(data, "columns"):
        return {
            "headers": list(data.columns),
            "rows": data.values.tolist(),
        }
    # list-of-lists
    rows = [list(row) for row in data]
    return {"headers": headers, "rows": rows}


def _render_image_bytes(figure: Any) -> bytes:
    """Render a matplotlib figure to PNG bytes."""
    buf = io.BytesIO()
    figure.savefig(buf, format="png", bbox_inches="tight")
    buf.seek(0)
    return buf.read()


class Card:
    """Collects rich content for a task card."""

    def __init__(self) -> None:
        self._items: list[CardItem] = []

    def _log_to_logger(
        self, kind: str, title: str, content: str = "", image: bytes = b""
    ) -> None:
        """Log card item to the current logger if available."""
        # Import here to avoid circular import
        logger = current.logger
        if logger is not None:
            task_name = current.task_name or "unknown"
            logger.add_card(
                task=task_name,
                kind=kind,
                title=title,
                content=content,
                image=image,
            )

    def text(self, content: str, title: str = "") -> None:
        """Add a text/markdown block."""
        self._items.append(CardItem(kind="text", content=content, title=title))
        self._log_to_logger("text", title, content=content)

    def image(self, figure: Any, title: str = "") -> None:
        """Add a matplotlib figure (rendered to PNG immediately)."""
        png_bytes = _render_image_bytes(figure)
        b64_content = base64.b64encode(png_bytes).decode("ascii")
        self._items.append(CardItem(kind="image", content=b64_content, title=title))
        self._log_to_logger("image", title, image=png_bytes)

    def html(self, content: str, title: str = "") -> None:
        """Add raw HTML content."""
        self._items.append(CardItem(kind="html", content=content, title=title))
        self._log_to_logger("html", title, content=content)

    def table(
        self,
        data: Any,
        title: str = "",
        headers: list[str] | None = None,
    ) -> None:
        """Add a table (list-of-lists or pandas DataFrame)."""
        table_data = _normalize_table(data, headers)
        self._items.append(
            CardItem(
                kind="table",
                content=table_data,
                title=title,
            )
        )
        # Log table as markdown text
        import json
        self._log_to_logger("table", title, content=json.dumps(table_data))

    @property
    def items(self) -> list[CardItem]:
        return list(self._items)

    def is_empty(self) -> bool:
        return len(self._items) == 0

    def serialize(self) -> list[dict[str, Any]]:
        """Serialize card items for storage in Entry metadata."""
        return [
            {"kind": item.kind, "content": item.content, "title": item.title}
            for item in self._items
        ]


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
    """Context-aware object for accessing task execution context.

    Provides ``current.card`` for rich content,
    ``current.checkpoint`` for resumable execution, and
    ``current.logger`` for event logging.
    Uses ContextVar for thread/process safety.
    """

    _card_var: ContextVar[Card | None] = ContextVar("_card_var", default=None)
    _checkpoint_var: ContextVar[Checkpoint | None] = ContextVar(
        "_checkpoint_var", default=None
    )
    _logger_var: ContextVar[Any] = ContextVar("_logger_var", default=None)
    _task_name_var: ContextVar[str] = ContextVar("_task_name_var", default="")

    @property
    def card(self) -> Card:
        """Get or create the card for the current task."""
        c = self._card_var.get(None)
        if c is None:
            c = Card()
            self._card_var.set(c)
        return c

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

    def _set_card(self, card: Card) -> None:
        """Set the card for the current task (called by executors)."""
        self._card_var.set(card)

    def _set_checkpoint(self, checkpoint: Checkpoint) -> None:
        """Set the checkpoint for the current task (called by executors)."""
        self._checkpoint_var.set(checkpoint)

    def _set_logger(self, logger: Any) -> None:
        """Set the logger for the current flow (called by flow.run)."""
        self._logger_var.set(logger)

    def _set_task_name(self, name: str) -> None:
        """Set the current task name (called by executors)."""
        self._task_name_var.set(name)

    def _reset(self) -> Card | None:
        """Harvest and clear the card (called by executors after task completes).

        Note: checkpoint is NOT cleared here — it persists across retries
        so a retry attempt can read the checkpoint from the previous attempt.
        """
        c = self._card_var.get(None)
        self._card_var.set(None)
        if c is not None and c.is_empty():
            return None
        return c

    def _reset_checkpoint(self) -> None:
        """Clear the checkpoint context (called after all retries complete)."""
        self._checkpoint_var.set(None)

    def _reset_logger(self) -> None:
        """Clear the logger context (called after flow completes)."""
        self._logger_var.set(None)


current = _Current()
