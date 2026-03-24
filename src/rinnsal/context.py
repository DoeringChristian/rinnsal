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


def _render_image(figure: Any) -> str:
    """Render a matplotlib figure to base64-encoded PNG."""
    buf = io.BytesIO()
    figure.savefig(buf, format="png", bbox_inches="tight")
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("ascii")


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


class Card:
    """Collects rich content for a task card."""

    def __init__(self) -> None:
        self._items: list[CardItem] = []

    def text(self, content: str, title: str = "") -> None:
        """Add a text/markdown block."""
        self._items.append(CardItem(kind="text", content=content, title=title))

    def image(self, figure: Any, title: str = "") -> None:
        """Add a matplotlib figure (rendered to PNG immediately)."""
        self._items.append(
            CardItem(kind="image", content=_render_image(figure), title=title)
        )

    def html(self, content: str, title: str = "") -> None:
        """Add raw HTML content."""
        self._items.append(CardItem(kind="html", content=content, title=title))

    def table(
        self,
        data: Any,
        title: str = "",
        headers: list[str] | None = None,
    ) -> None:
        """Add a table (list-of-lists or pandas DataFrame)."""
        self._items.append(
            CardItem(
                kind="table",
                content=_normalize_table(data, headers),
                title=title,
            )
        )

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

    Provides ``current.card`` for rich content and
    ``current.checkpoint`` for resumable execution.
    Uses ContextVar for thread/process safety.
    """

    _card_var: ContextVar[Card | None] = ContextVar("_card_var", default=None)
    _checkpoint_var: ContextVar[Checkpoint | None] = ContextVar(
        "_checkpoint_var", default=None
    )

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

    def _set_card(self, card: Card) -> None:
        """Set the card for the current task (called by executors)."""
        self._card_var.set(card)

    def _set_checkpoint(self, checkpoint: Checkpoint) -> None:
        """Set the checkpoint for the current task (called by executors)."""
        self._checkpoint_var.set(checkpoint)

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


current = _Current()
