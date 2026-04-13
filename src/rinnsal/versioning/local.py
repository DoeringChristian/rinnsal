"""LocalRunStore — file-backed RunStore wrapping a Database.

list_runs / latest_run prefer the metadata DB when available (one SQL
query) and fall back to JSON sidecars for legacy data. record_run keeps
writing both for one release of back-compat.
"""

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
        # Keep writing the JSON sidecar for back-compat; the metadata DB
        # is updated in parallel by FlowResult.run.
        self._db.store_flow_run(flow_name, task_hashes, metadata=metadata)

    def _meta_store(self):
        if not hasattr(self._db, "metadata_store"):
            return None
        try:
            return self._db.metadata_store()
        except Exception:
            return None

    def _summary_to_dict(
        self,
        s,
        sidecar_lookup: dict[str, dict] | None = None,
    ) -> dict[str, Any]:
        """Convert a RunSummary to the legacy fetch_flow_runs() shape.

        ``task_hashes`` is not yet a column in the v1 DB schema; we look
        it up from the JSON sidecar when needed (e.g. by --resume).
        """
        meta: dict[str, Any] = {}
        if s.tags:
            meta["tags"] = list(s.tags)
        if s.snapshot_hash:
            meta["snapshot"] = s.snapshot_hash

        task_hashes: list[str] = []
        sidecar = (sidecar_lookup or {}).get(s.run_id)
        if sidecar is not None:
            task_hashes = sidecar.get("task_hashes", [])
            for k, v in (sidecar.get("metadata") or {}).items():
                meta.setdefault(k, v)

        return {
            "run_id": s.run_id,
            "task_hashes": task_hashes,
            "timestamp": datetime.fromtimestamp(s.started_at).isoformat(),
            "metadata": meta,
        }

    def list_runs(
        self, flow_name: str, limit: int = 10
    ) -> list[dict[str, Any]]:
        store = self._meta_store()
        if store is not None:
            try:
                rows = store.list_runs(flow_name=flow_name, limit=limit)
                if rows:
                    sidecars = {
                        r["run_id"]: r
                        for r in self._db.fetch_flow_runs(
                            flow_name, limit=limit
                        )
                    }
                    return [self._summary_to_dict(r, sidecars) for r in rows]
            except Exception:
                pass
        return self._db.fetch_flow_runs(flow_name, limit=limit)

    def latest_run(self, flow_name: str) -> dict[str, Any] | None:
        store = self._meta_store()
        if store is not None:
            try:
                rows = store.list_runs(flow_name=flow_name, limit=1)
                if rows:
                    sidecars = {
                        r["run_id"]: r
                        for r in self._db.fetch_flow_runs(flow_name, limit=1)
                    }
                    return self._summary_to_dict(rows[0], sidecars)
            except Exception:
                pass
        runs = self._db.fetch_flow_runs(flow_name, limit=1)
        return runs[0] if runs else None
