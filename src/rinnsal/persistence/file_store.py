"""File-based database implementation."""

from __future__ import annotations

import json
import os
import shutil
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from rinnsal.core.types import Entry, Snapshot
from rinnsal.persistence.database import BaseDatabase
from rinnsal.persistence.locking import file_lock
from rinnsal.persistence.serializers import HybridSerializer, Serializer


class FileDatabase(BaseDatabase):
    """File-based database for result persistence.

    Directory structure:
        .rinnsal/
        ├── tasks/
        │   └── <task_hash>/
        │       ├── results/
        │       │   └── <timestamp>.dat
        │       └── latest.dat
        ├── flows/
        │   └── <flow_name>/
        │       └── runs/
        │           └── <run_id>.json
        └── snapshots/
            └── <snapshot_hash>/
                └── <files>
    """

    def __init__(
        self,
        root: Path | str = ".rinnsal",
        serializer: Serializer | None = None,
    ) -> None:
        self._root = Path(root)
        self._serializer = serializer or HybridSerializer()

        # Create directory structure
        self._tasks_dir = self._root / "tasks"
        self._flows_dir = self._root / "flows"
        self._snapshots_dir = self._root / "snapshots"

        self._tasks_dir.mkdir(parents=True, exist_ok=True)
        self._flows_dir.mkdir(parents=True, exist_ok=True)
        self._snapshots_dir.mkdir(parents=True, exist_ok=True)

    @property
    def root(self) -> Path:
        return self._root

    def _task_dir(self, task_hash: str) -> Path:
        return self._tasks_dir / task_hash

    def _task_results_dir(self, task_hash: str) -> Path:
        return self._task_dir(task_hash) / "results"

    def _task_latest_path(self, task_hash: str) -> Path:
        return self._task_dir(task_hash) / "latest.dat"

    def _flow_runs_dir(self, flow_name: str) -> Path:
        return self._flows_dir / flow_name / "runs"

    def store_task_result(
        self,
        task_hash: str,
        entry: Entry,
    ) -> None:
        """Store a task execution result."""
        task_dir = self._task_dir(task_hash)
        results_dir = self._task_results_dir(task_hash)
        results_dir.mkdir(parents=True, exist_ok=True)

        # Create timestamp-based filename
        timestamp = entry.timestamp.strftime("%Y%m%d_%H%M%S_%f")
        result_path = results_dir / f"{timestamp}.dat"

        # Serialize entry
        entry_data = self._serialize_entry(entry)

        with file_lock(task_dir):
            # Write result file
            self._serializer.save(entry_data, result_path)

            # Update latest symlink or copy
            latest_path = self._task_latest_path(task_hash)
            self._serializer.save(entry_data, latest_path)

    def fetch_task_result(
        self,
        task_hash: str,
    ) -> Entry | None:
        """Fetch the most recent result for a task."""
        latest_path = self._task_latest_path(task_hash)

        if not latest_path.exists():
            return None

        try:
            entry_data = self._serializer.load(latest_path)
            return self._deserialize_entry(entry_data)
        except Exception:
            return None

    def fetch_task_history(
        self,
        task_hash: str,
    ) -> list[Entry]:
        """Fetch all execution results for a task."""
        results_dir = self._task_results_dir(task_hash)

        if not results_dir.exists():
            return []

        entries: list[Entry] = []

        # List result files sorted by timestamp (newest first)
        result_files = sorted(
            results_dir.glob("*.dat"),
            key=lambda p: p.stem,
            reverse=True,
        )

        for path in result_files:
            try:
                entry_data = self._serializer.load(path)
                entry = self._deserialize_entry(entry_data)
                entries.append(entry)
            except Exception:
                continue

        return entries

    def task_exists(
        self,
        task_hash: str,
    ) -> bool:
        """Check if a task result exists."""
        return self._task_latest_path(task_hash).exists()

    def store_flow_run(
        self,
        flow_name: str,
        task_hashes: list[str],
        metadata: dict[str, Any] | None = None,
    ) -> str:
        """Store a flow run record."""
        runs_dir = self._flow_runs_dir(flow_name)
        runs_dir.mkdir(parents=True, exist_ok=True)

        run_id = str(uuid.uuid4())[:8]
        timestamp = datetime.now()

        run_record = {
            "run_id": run_id,
            "task_hashes": task_hashes,
            "timestamp": timestamp.isoformat(),
            "metadata": metadata or {},
        }

        run_path = runs_dir / f"{timestamp.strftime('%Y%m%d_%H%M%S')}_{run_id}.json"

        with file_lock(runs_dir):
            with open(run_path, "w") as f:
                json.dump(run_record, f, indent=2)

        return run_id

    def fetch_flow_runs(
        self,
        flow_name: str,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch flow run records."""
        runs_dir = self._flow_runs_dir(flow_name)

        if not runs_dir.exists():
            return []

        runs: list[dict[str, Any]] = []

        # List run files sorted by timestamp (newest first)
        run_files = sorted(
            runs_dir.glob("*.json"),
            key=lambda p: p.stem,
            reverse=True,
        )

        if limit is not None:
            run_files = run_files[:limit]

        for path in run_files:
            try:
                with open(path) as f:
                    run_record = json.load(f)
                    runs.append(run_record)
            except Exception:
                continue

        return runs

    def clear(self) -> None:
        """Clear all stored data."""
        if self._root.exists():
            shutil.rmtree(self._root)

        # Recreate directory structure
        self._tasks_dir.mkdir(parents=True, exist_ok=True)
        self._flows_dir.mkdir(parents=True, exist_ok=True)
        self._snapshots_dir.mkdir(parents=True, exist_ok=True)

    def _serialize_entry(self, entry: Entry) -> dict[str, Any]:
        """Serialize an Entry to a dictionary."""
        return {
            "result": entry.result,
            "log": entry.log,
            "metadata": entry.metadata,
            "timestamp": entry.timestamp.isoformat(),
            "snapshot": self._serialize_snapshot(entry.snapshot) if entry.snapshot else None,
        }

    def _deserialize_entry(self, data: dict[str, Any]) -> Entry:
        """Deserialize an Entry from a dictionary."""
        return Entry(
            result=data["result"],
            log=data.get("log", ""),
            metadata=data.get("metadata", {}),
            timestamp=datetime.fromisoformat(data["timestamp"]),
            snapshot=self._deserialize_snapshot(data.get("snapshot")),
        )

    def _serialize_snapshot(self, snapshot: Snapshot) -> dict[str, Any]:
        """Serialize a Snapshot to a dictionary."""
        return {
            "hash": snapshot.hash,
            "path": str(snapshot.path),
        }

    def _deserialize_snapshot(self, data: dict[str, Any] | None) -> Snapshot | None:
        """Deserialize a Snapshot from a dictionary."""
        if data is None:
            return None
        return Snapshot(
            hash=data["hash"],
            path=Path(data["path"]),
        )


# Default database instance
_default_database: FileDatabase | None = None


def get_database() -> FileDatabase:
    """Get or create the default database instance."""
    global _default_database
    if _default_database is None:
        _default_database = FileDatabase()
    return _default_database


def set_database(database: FileDatabase) -> None:
    """Set the default database instance."""
    global _default_database
    _default_database = database
