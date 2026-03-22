"""Task execution context with card support."""

from __future__ import annotations

import base64
import io
from contextvars import ContextVar
from dataclasses import dataclass
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


class _Current:
    """Context-aware object for accessing task execution context.

    Provides ``current.card`` for adding rich content to task results.
    Uses ContextVar for thread/process safety.
    """

    _card_var: ContextVar[Card | None] = ContextVar("_card_var", default=None)

    @property
    def card(self) -> Card:
        """Get or create the card for the current task."""
        c = self._card_var.get(None)
        if c is None:
            c = Card()
            self._card_var.set(c)
        return c

    def _set_card(self, card: Card) -> None:
        """Set the card for the current task (called by executors)."""
        self._card_var.set(card)

    def _reset(self) -> Card | None:
        """Harvest and clear the card (called by executors after task completes)."""
        c = self._card_var.get(None)
        self._card_var.set(None)
        if c is not None and c.is_empty():
            return None
        return c


current = _Current()
