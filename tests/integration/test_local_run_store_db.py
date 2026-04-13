"""LocalRunStore reads from the metadata DB when available."""

from __future__ import annotations

from pathlib import Path

import pytest

from rinnsal.data.file_store import FileDatabase
from rinnsal.data.metadata import RunUpsert
from rinnsal.versioning.local import LocalRunStore


@pytest.fixture
def db_and_store(tmp_path):
    db = FileDatabase(root=tmp_path / ".rinnsal")
    store = db.metadata_store()
    return db, store, tmp_path / ".rinnsal"


class TestLocalRunStoreReadPath:
    def test_list_runs_uses_db_when_available(self, db_and_store):
        db, store, root = db_and_store
        rs = LocalRunStore(db, root)

        store.upsert_flow("f")
        store.upsert_run(
            RunUpsert(
                run_id="r1", flow_name="f", run_dir=str(root / "r1"),
                status="success", started_at=100, finished_at=200,
                tags=["a", "b"], snapshot_hash="snap123",
            )
        )

        runs = rs.list_runs("f")
        assert len(runs) == 1
        assert runs[0]["run_id"] == "r1"
        assert runs[0]["metadata"].get("snapshot") == "snap123"
        assert runs[0]["metadata"].get("tags") == ["a", "b"]

    def test_list_runs_augments_with_sidecar_task_hashes(self, db_and_store):
        db, store, root = db_and_store
        rs = LocalRunStore(db, root)

        # Write the sidecar via the legacy path (returns a generated run_id).
        sidecar_run_id = db.store_flow_run(
            "f", task_hashes=["h1", "h2"], metadata={}
        )
        # And mirror to the DB using the same run_id so both paths agree.
        store.upsert_flow("f")
        store.upsert_run(
            RunUpsert(
                run_id=sidecar_run_id, flow_name="f",
                run_dir=str(root / sidecar_run_id),
                status="success", started_at=100, finished_at=200,
            )
        )

        runs = rs.list_runs("f")
        assert len(runs) == 1
        assert runs[0]["task_hashes"] == ["h1", "h2"]

    def test_falls_back_to_sidecar_when_db_empty(self, db_and_store):
        db, store, root = db_and_store
        rs = LocalRunStore(db, root)

        # Only the JSON sidecar exists.
        db.store_flow_run("f", task_hashes=["h1"], metadata={})

        runs = rs.list_runs("f")
        assert len(runs) == 1
        assert runs[0]["task_hashes"] == ["h1"]

    def test_latest_run_uses_db_when_available(self, db_and_store):
        db, store, root = db_and_store
        rs = LocalRunStore(db, root)

        store.upsert_flow("f")
        for i in range(3):
            store.upsert_run(
                RunUpsert(
                    run_id=f"r{i}", flow_name="f",
                    run_dir=str(root / f"r{i}"),
                    status="success", started_at=100 + i,
                )
            )
        latest = rs.latest_run("f")
        assert latest is not None
        assert latest["run_id"] == "r2"
