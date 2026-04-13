"""Backfill: index existing events.pb runs into the metadata DB."""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from rinnsal.data.file_store import FileDatabase
from rinnsal.data.logger.logger import Logger
from rinnsal.data.metadata import SqliteMetadataStore
from rinnsal.data.metadata.backfill import (
    Backfiller,
    _flow_name_from_run_dir,
    index_run,
)


def _write_run_without_db(
    tmp_path: Path,
    flow_name: str = "f",
    run_id: str = "r1",
) -> Path:
    """Write events.pb for a run *without* mirroring to the DB.

    Simulates a pre-DB run on disk that needs backfilling.
    """
    db = FileDatabase(root=tmp_path / ".rinnsal")
    rd = tmp_path / ".rinnsal" / "flows" / flow_name / "runs" / run_id
    rd.mkdir(parents=True, exist_ok=True)
    lg = Logger(rd, database=db, metadata_store=None)
    lg.add_task_node("train", "h1", "success", duration=2.5, params="{}")
    lg.add_task_node("eval", "h2", "success", duration=0.5, params="{}")
    lg.add_task_edge("train", "eval")
    lg.close()
    return rd


class TestFlowNameFromPath:
    def test_well_formed(self, tmp_path):
        p = tmp_path / "x/.rinnsal/flows/myflow/runs/r1"
        assert _flow_name_from_run_dir(p) == "myflow"

    def test_malformed(self, tmp_path):
        assert _flow_name_from_run_dir(Path("/no/flows/here")) is None


class TestIndexRun:
    def test_extracts_nodes_and_edges(self, tmp_path):
        rd = _write_run_without_db(tmp_path)
        store = SqliteMetadataStore(tmp_path / ".rinnsal/metadata.sqlite")

        ok = index_run(store, rd)
        assert ok is True

        runs = store.list_runs(flow_name="f")
        assert len(runs) == 1
        assert runs[0].run_id == "r1"
        assert runs[0].run_dir == str(rd)
        assert runs[0].status == "success"
        # started_at + finished_at derive from event timestamps.
        assert runs[0].started_at > 0
        assert runs[0].finished_at >= runs[0].started_at

        names = {n.task_name for n in store.list_task_nodes("r1")}
        assert names == {"train", "eval"}
        assert store.list_task_edges("r1") == [("train", "eval")]

    def test_no_events_pb_returns_false(self, tmp_path):
        rd = tmp_path / "empty"
        rd.mkdir()
        store = SqliteMetadataStore(tmp_path / "metadata.sqlite")
        assert index_run(store, rd) is False


class TestBackfiller:
    def test_discover_pending_finds_undexed_runs(self, tmp_path):
        _write_run_without_db(tmp_path, run_id="r1")
        _write_run_without_db(tmp_path, run_id="r2")
        store = SqliteMetadataStore(tmp_path / ".rinnsal/metadata.sqlite")

        bf = Backfiller(store, tmp_path / ".rinnsal")
        pending = bf.discover_pending()
        assert len(pending) == 2

    def test_run_sync_indexes_all(self, tmp_path):
        _write_run_without_db(tmp_path, run_id="r1")
        _write_run_without_db(tmp_path, run_id="r2")
        store = SqliteMetadataStore(tmp_path / ".rinnsal/metadata.sqlite")

        bf = Backfiller(store, tmp_path / ".rinnsal")
        n = bf.run_sync()
        assert n == 2

        runs = store.list_runs(flow_name="f")
        assert {r.run_id for r in runs} == {"r1", "r2"}

    def test_progress_lifecycle(self, tmp_path):
        _write_run_without_db(tmp_path, run_id="r1")
        store = SqliteMetadataStore(tmp_path / ".rinnsal/metadata.sqlite")

        bf = Backfiller(store, tmp_path / ".rinnsal")
        # Initial: idle.
        p0 = bf.progress
        assert p0.indexing is False
        assert p0.done == 0

        bf.run_sync()
        p1 = bf.progress
        assert p1.indexing is False
        assert p1.done == 1
        assert p1.total == 1
        assert p1.finished_at is not None
        assert p1.errors == []

    def test_skips_runs_already_in_db(self, tmp_path):
        rd = _write_run_without_db(tmp_path, run_id="r1")
        store = SqliteMetadataStore(tmp_path / ".rinnsal/metadata.sqlite")
        # Pre-populate r1.
        index_run(store, rd)

        # Now write r2 without DB; backfill should only index r2.
        _write_run_without_db(tmp_path, run_id="r2")
        bf = Backfiller(store, tmp_path / ".rinnsal")
        pending = bf.discover_pending()
        assert [p.name for p in pending] == ["r2"]

    def test_async_backfill_completes(self, tmp_path):
        _write_run_without_db(tmp_path, run_id="r1")
        store = SqliteMetadataStore(tmp_path / ".rinnsal/metadata.sqlite")

        bf = Backfiller(store, tmp_path / ".rinnsal")
        thread = bf.run_async()
        thread.join(timeout=10)
        assert not thread.is_alive()
        assert bf.progress.indexing is False
        assert bf.progress.done == 1


class TestIndexStatusEndpoint:
    def test_returns_idle_when_no_backfiller(self, tmp_path):
        pytest.importorskip("httpx")
        from fastapi.testclient import TestClient
        from rinnsal.viewer.backend.main import app
        from rinnsal.data.metadata.backfill import set_global_backfiller

        set_global_backfiller(None)
        client = TestClient(app)
        r = client.get("/api/index/status")
        assert r.status_code == 200
        body = r.json()
        assert body["indexing"] is False
        assert body["total"] == 0

    def test_returns_progress_when_backfiller_set(self, tmp_path):
        pytest.importorskip("httpx")
        from fastapi.testclient import TestClient
        from rinnsal.viewer.backend.main import app
        from rinnsal.data.metadata.backfill import set_global_backfiller

        _write_run_without_db(tmp_path, run_id="r1")
        store = SqliteMetadataStore(tmp_path / ".rinnsal/metadata.sqlite")
        bf = Backfiller(store, tmp_path / ".rinnsal")
        bf.run_sync()
        set_global_backfiller(bf)
        try:
            client = TestClient(app)
            r = client.get("/api/index/status")
            body = r.json()
            assert body["indexing"] is False
            assert body["total"] == 1
            assert body["done"] == 1
        finally:
            set_global_backfiller(None)
