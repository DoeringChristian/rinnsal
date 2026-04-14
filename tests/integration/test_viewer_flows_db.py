"""The viewer's flow endpoints are backed by SqliteMetadataStore."""

from __future__ import annotations

import time
from pathlib import Path

import pytest

fastapi = pytest.importorskip("fastapi", reason="viewer extra not installed")
pytest.importorskip("httpx", reason="httpx required for fastapi.testclient")
from fastapi.testclient import TestClient  # noqa: E402

from rinnsal.data.metadata import (
    RunUpsert,
    SqliteMetadataStore,
    TaskNodeRow,
)
from rinnsal.viewer._data import invalidate_caches


@pytest.fixture
def client():
    from rinnsal.viewer.backend.main import app

    return TestClient(app)


@pytest.fixture(autouse=True)
def _clean_caches():
    invalidate_caches()
    yield
    invalidate_caches()


def _seed_db(root: Path, *, n_runs: int = 3) -> SqliteMetadataStore:
    store = SqliteMetadataStore(root / "metadata.sqlite")
    store.upsert_flow("training")
    base_ts = time.time()
    for i in range(n_runs):
        run_id = f"r{i}"
        run_dir = root / "flows/training/runs" / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        store.upsert_run(
            RunUpsert(
                run_id=run_id,
                flow_name="training",
                run_dir=str(run_dir),
                status="success",
                started_at=base_ts + i,
                finished_at=base_ts + i + 0.5,
            )
        )
        store.upsert_task_node(
            TaskNodeRow(
                run_id=run_id, task_name="train", task_hash=f"h_{run_id}",
                status="success", duration=10.0 + i, error="",
                ts=base_ts + i + 0.1,
            )
        )
        store.upsert_task_node(
            TaskNodeRow(
                run_id=run_id, task_name="eval", task_hash=f"e_{run_id}",
                status="success", duration=2.0, error="",
                ts=base_ts + i + 0.4,
            )
        )
        store.upsert_task_edge(run_id, "train", "eval")
    return store


class TestFlowsEndpoint:
    def test_lists_flows_with_latest_dag(self, tmp_path, client):
        _seed_db(tmp_path, n_runs=3)
        r = client.get(f"/api/flows?root={tmp_path}")
        assert r.status_code == 200
        body = r.json()
        flows = body["flows"]
        assert len(flows) == 1
        f = flows[0]
        assert f["name"] == "training"
        assert f["run_count"] == 3
        assert f["latest_run"] == "r2"
        names = {n["name"] for n in f["nodes"]}
        assert names == {"train", "eval"}
        assert f["edges"] == [{"from": "train", "to": "eval"}]

    def test_emits_last_modified(self, tmp_path, client):
        _seed_db(tmp_path, n_runs=1)
        r = client.get(f"/api/flows?root={tmp_path}")
        assert r.headers.get("last-modified")

    def test_304_on_revalidation(self, tmp_path, client):
        from email.utils import formatdate, parsedate_to_datetime

        _seed_db(tmp_path, n_runs=1)
        r = client.get(f"/api/flows?root={tmp_path}")
        lm = r.headers["last-modified"]
        # Strict revalidation: client must present a timestamp newer
        # than the file's mtime to get a 304 (HTTP dates have 1s
        # resolution; equal timestamps return 200 to avoid masking
        # sub-second writes).
        ts = parsedate_to_datetime(lm).timestamp()
        r2 = client.get(
            f"/api/flows?root={tmp_path}",
            headers={"If-Modified-Since": formatdate(ts + 2, usegmt=True)},
        )
        assert r2.status_code == 304


class TestTaskHistoryEndpoint:
    def test_history_newest_first(self, tmp_path, client):
        _seed_db(tmp_path, n_runs=3)
        r = client.get(
            f"/api/flows/training/tasks/train/history?root={tmp_path}"
        )
        assert r.status_code == 200
        body = r.json()
        assert [h["run_id"] for h in body["history"]] == ["r2", "r1", "r0"]
        assert all(h["status"] == "success" for h in body["history"])
        # Each row carries its own run_dir.
        assert all(h["run_path"].endswith(h["run_id"]) for h in body["history"])

    def test_history_filters_by_flow(self, tmp_path, client):
        store = _seed_db(tmp_path, n_runs=1)
        # Add another flow with the same task name — must not show up.
        store.upsert_flow("other_flow")
        store.upsert_run(
            RunUpsert(
                run_id="x", flow_name="other_flow",
                run_dir=str(tmp_path / "x"),
                status="success", started_at=999, finished_at=1000,
            )
        )
        store.upsert_task_node(
            TaskNodeRow(
                run_id="x", task_name="train", task_hash="hx",
                status="success", duration=99, error="", ts=999,
            )
        )
        r = client.get(
            f"/api/flows/training/tasks/train/history?root={tmp_path}"
        )
        assert all(h["run_id"] != "x" for h in r.json()["history"])


class TestFlowDagEndpoint:
    def test_dag_for_latest_run(self, tmp_path, client):
        _seed_db(tmp_path, n_runs=2)
        r = client.get(f"/api/flows/training/dag?root={tmp_path}")
        assert r.status_code == 200
        body = r.json()
        assert body["run_id"] == "r1"
        names = {n["name"] for n in body["nodes"]}
        assert names == {"train", "eval"}
        assert body["edges"] == [{"from": "train", "to": "eval"}]

    def test_dag_unknown_flow(self, tmp_path, client):
        _seed_db(tmp_path, n_runs=1)
        r = client.get(f"/api/flows/nonexistent/dag?root={tmp_path}")
        assert r.status_code == 200
        body = r.json()
        assert body["nodes"] == []
        assert body["edges"] == []
