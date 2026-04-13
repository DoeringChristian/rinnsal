"""Structured metadata index for fast viewer navigation.

The metadata store indexes flows / runs / task graph for sub-millisecond
``/api/flows`` and friends. Heavy artifacts (figures, plotly JSON,
cards, blobs) live in ``events.pb`` and the content-addressed
``blobs/`` tree, NOT in this DB.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from rinnsal.data.metadata.base import (
    FlowSummary,
    MetadataStore,
    RunSummary,
    RunUpsert,
    TaskHistoryEntry,
    TaskNodeRow,
)
from rinnsal.data.metadata.sqlite import SqliteMetadataStore

if TYPE_CHECKING:
    from rinnsal.data.file_store import FileDatabase


__all__ = [
    "MetadataStore",
    "SqliteMetadataStore",
    "FlowSummary",
    "RunSummary",
    "RunUpsert",
    "TaskNodeRow",
    "TaskHistoryEntry",
    "default_store_for",
]


def default_store_for(database: "FileDatabase") -> MetadataStore:
    """Return the canonical SqliteMetadataStore for this FileDatabase root."""
    db_path = Path(database.root) / "metadata.sqlite"
    return SqliteMetadataStore(db_path)
