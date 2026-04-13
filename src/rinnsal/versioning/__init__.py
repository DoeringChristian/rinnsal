"""Versioning layer: run identity, run metadata, code snapshots."""

from rinnsal.versioning.base import RunStore
from rinnsal.versioning.local import LocalRunStore
from rinnsal.versioning.snapshot import (
    SnapshotManager,
    get_snapshot_manager,
    set_snapshot_manager,
    use_snapshot,
)

__all__ = [
    "RunStore",
    "LocalRunStore",
    "SnapshotManager",
    "get_snapshot_manager",
    "set_snapshot_manager",
    "use_snapshot",
]
