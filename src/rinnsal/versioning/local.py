"""LocalRunStore — file-backed RunStore wrapping a Database."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from rinnsal.data.database import Database


class LocalRunStore:
    """Default RunStore: timestamp run IDs, runs under <db_root>/flows/<name>/runs/<id>.

    Wraps a ``Database`` for persisting run metadata. The directory layout is
    deliberately the *only* place that knows the on-disk run-path shape — the
    Logger and the rest of the system receive a path from here.
    """

    def __init__(self, database: Database, root: Path) -> None:
        self._db = database
        self._root = Path(root)

    def new_run_id(self, flow_name: str) -> str:
        return datetime.now().strftime("%Y%m%d_%H%M%S")

    def run_dir(self, flow_name: str, run_id: str) -> Path:
        path = self._root / "flows" / flow_name / "runs" / run_id
        path.mkdir(parents=True, exist_ok=True)
        return path

    def record_run(
        self,
        flow_name: str,
        task_hashes: list[str],
        metadata: dict[str, Any],
    ) -> None:
        self._db.store_flow_run(flow_name, task_hashes, metadata=metadata)

    def list_runs(
        self, flow_name: str, limit: int = 10
    ) -> list[dict[str, Any]]:
        return self._db.fetch_flow_runs(flow_name, limit=limit)

    def latest_run(self, flow_name: str) -> dict[str, Any] | None:
        runs = self._db.fetch_flow_runs(flow_name, limit=1)
        return runs[0] if runs else None
