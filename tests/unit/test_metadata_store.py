"""Tests for SqliteMetadataStore."""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from rinnsal.data.metadata import (
    FlowSummary,
    RunSummary,
    RunUpsert,
    SqliteMetadataStore,
    TaskNodeRow,
)


@pytest.fixture
def store(tmp_path) -> SqliteMetadataStore:
    return SqliteMetadataStore(tmp_path / "metadata.sqlite")


# --------------------------------------------------------------------------- #
# Schema / migrations
# --------------------------------------------------------------------------- #


class TestMigrations:
    def test_creates_db_file(self, tmp_path):
        SqliteMetadataStore(tmp_path / "metadata.sqlite")
        assert (tmp_path / "metadata.sqlite").exists()

    def test_idempotent_open(self, tmp_path):
        path = tmp_path / "metadata.sqlite"
        SqliteMetadataStore(path)
        # Opening again must not error or duplicate-apply migrations.
        s = SqliteMetadataStore(path)
        assert s.list_flows() == []

    def test_skip_migrations_flag(self, tmp_path):
        # First open creates schema.
        SqliteMetadataStore(tmp_path / "metadata.sqlite")
        # Subsequent open with run_migrations=False must work.
        s = SqliteMetadataStore(
            tmp_path / "metadata.sqlite", run_migrations=False
        )
        assert s.list_flows() == []


# --------------------------------------------------------------------------- #
# Writes + reads
# --------------------------------------------------------------------------- #


def _make_run_row(store, run_id="r1", flow_name="f", started_at=None):
    if started_at is None:
        started_at = time.time()
    store.upsert_flow(flow_name)
    store.upsert_run(
        RunUpsert(
            run_id=run_id,
            flow_name=flow_name,
            run_dir=f"/tmp/{run_id}",
            status="running",
            started_at=started_at,
            tags=["t1"],
        )
    )
    return started_at


class TestFlowsAndRuns:
    def test_upsert_flow_idempotent(self, store):
        store.upsert_flow("f")
        store.upsert_flow("f")
        flows = store.list_flows()
        assert len(flows) == 1
        assert flows[0].name == "f"
        assert flows[0].run_count == 0

    def test_run_count_aggregates(self, store):
        store.upsert_flow("f")
        for i, run_id in enumerate(["a", "b", "c"]):
            store.upsert_run(
                RunUpsert(
                    run_id=run_id,
                    flow_name="f",
                    run_dir=f"/tmp/{run_id}",
                    status="success",
                    started_at=100 + i,
                )
            )
        flows = store.list_flows()
        assert flows[0].run_count == 3
        assert flows[0].latest_run_at == 102
        assert flows[0].latest_run_id == "c"

    def test_list_runs_newest_first(self, store):
        store.upsert_flow("f")
        for i, run_id in enumerate(["a", "b", "c"]):
            store.upsert_run(
                RunUpsert(
                    run_id=run_id,
                    flow_name="f",
                    run_dir=f"/tmp/{run_id}",
                    status="success",
                    started_at=100 + i,
                )
            )
        runs = store.list_runs(flow_name="f")
        assert [r.run_id for r in runs] == ["c", "b", "a"]

    def test_list_runs_limit(self, store):
        store.upsert_flow("f")
        for i in range(5):
            store.upsert_run(
                RunUpsert(
                    run_id=f"r{i}",
                    flow_name="f",
                    run_dir=f"/tmp/r{i}",
                    status="success",
                    started_at=i,
                )
            )
        assert len(store.list_runs(flow_name="f", limit=2)) == 2

    def test_get_run(self, store):
        _make_run_row(store, run_id="r1")
        r = store.get_run("r1")
        assert r is not None
        assert r.run_id == "r1"
        assert r.tags == ["t1"]

    def test_get_run_missing(self, store):
        assert store.get_run("nonexistent") is None

    def test_update_run_status(self, store):
        _make_run_row(store)
        store.update_run_status("r1", "success", finished_at=200, failed_count=0)
        r = store.get_run("r1")
        assert r.status == "success"
        assert r.finished_at == 200
        assert r.failed_count == 0

    def test_run_upsert_overwrites(self, store):
        store.upsert_flow("f")
        store.upsert_run(
            RunUpsert(
                run_id="r1", flow_name="f", run_dir="/tmp/old",
                status="running", started_at=100,
            )
        )
        store.upsert_run(
            RunUpsert(
                run_id="r1", flow_name="f", run_dir="/tmp/new",
                status="success", started_at=100, finished_at=200,
            )
        )
        r = store.get_run("r1")
        assert r.run_dir == "/tmp/new"
        assert r.status == "success"
        assert r.finished_at == 200


class TestTaskNodes:
    def test_upsert_and_list(self, store):
        _make_run_row(store)
        store.upsert_task_node(
            TaskNodeRow(
                run_id="r1", task_name="train", task_hash="h1",
                status="success", duration=1.5, error="", ts=10,
            )
        )
        store.upsert_task_node(
            TaskNodeRow(
                run_id="r1", task_name="eval", task_hash="h2",
                status="success", duration=0.5, error="", ts=20,
            )
        )
        nodes = store.list_task_nodes("r1")
        assert [n.task_name for n in nodes] == ["train", "eval"]

    def test_node_upsert_overwrites(self, store):
        """Re-emitting a task with new status updates the row."""
        _make_run_row(store)
        store.upsert_task_node(
            TaskNodeRow(
                run_id="r1", task_name="train", task_hash="h1",
                status="running", duration=0, error="", ts=10,
            )
        )
        store.upsert_task_node(
            TaskNodeRow(
                run_id="r1", task_name="train", task_hash="h1",
                status="success", duration=2.5, error="", ts=20,
            )
        )
        nodes = store.list_task_nodes("r1")
        assert len(nodes) == 1
        assert nodes[0].status == "success"
        assert nodes[0].duration == 2.5

    def test_task_history_across_runs(self, store):
        store.upsert_flow("f")
        for i, run_id in enumerate(["a", "b", "c"]):
            store.upsert_run(
                RunUpsert(
                    run_id=run_id, flow_name="f", run_dir=f"/tmp/{run_id}",
                    status="success", started_at=100 + i,
                )
            )
            store.upsert_task_node(
                TaskNodeRow(
                    run_id=run_id, task_name="train", task_hash=f"h_{run_id}",
                    status="success", duration=1.0, error="", ts=100 + i,
                )
            )
            # Add an unrelated task in run b only — must not appear in history.
            if run_id == "b":
                store.upsert_task_node(
                    TaskNodeRow(
                        run_id=run_id, task_name="other", task_hash="hx",
                        status="success", duration=0, error="", ts=100 + i,
                    )
                )
        history = store.task_history("f", "train")
        assert [h.run_id for h in history] == ["c", "b", "a"]

    def test_task_history_filters_by_flow(self, store):
        store.upsert_flow("f1")
        store.upsert_flow("f2")
        for flow in ("f1", "f2"):
            store.upsert_run(
                RunUpsert(
                    run_id=f"r_{flow}", flow_name=flow,
                    run_dir=f"/tmp/r_{flow}", status="success",
                    started_at=100,
                )
            )
            store.upsert_task_node(
                TaskNodeRow(
                    run_id=f"r_{flow}", task_name="train", task_hash="h",
                    status="success", duration=1.0, error="", ts=100,
                )
            )
        history = store.task_history("f1", "train")
        assert len(history) == 1
        assert history[0].run_id == "r_f1"


class TestTaskEdges:
    def test_upsert_and_list(self, store):
        _make_run_row(store)
        store.upsert_task_edge("r1", "a", "b")
        store.upsert_task_edge("r1", "b", "c")
        edges = store.list_task_edges("r1")
        assert sorted(edges) == [("a", "b"), ("b", "c")]

    def test_edge_idempotent(self, store):
        _make_run_row(store)
        store.upsert_task_edge("r1", "a", "b")
        store.upsert_task_edge("r1", "a", "b")  # duplicate
        edges = store.list_task_edges("r1")
        assert edges == [("a", "b")]


class TestHousekeeping:
    def test_has_run(self, store):
        _make_run_row(store, run_id="r1")
        assert store.has_run("r1") is True
        assert store.has_run("missing") is False

    def test_runs_missing_index(self, store):
        _make_run_row(store, run_id="r1")
        # r1 indexed; r2 not.
        missing = store.runs_missing_index(
            [Path("/tmp/r1"), Path("/tmp/r2")]
        )
        assert missing == [Path("/tmp/r2")]

    def test_runs_missing_index_empty_input(self, store):
        assert store.runs_missing_index([]) == []

    def test_latest_updated_at_uses_finished_then_started(self, store):
        store.upsert_flow("f")
        store.upsert_run(
            RunUpsert(
                run_id="r1", flow_name="f", run_dir="/tmp/r1",
                status="running", started_at=100,
            )
        )
        # No finished_at yet → falls back to started_at=100.
        assert store.latest_updated_at() == 100
        store.update_run_status("r1", "success", finished_at=150)
        assert store.latest_updated_at() == 150

    def test_latest_updated_at_filters_by_flow(self, store):
        store.upsert_flow("f1")
        store.upsert_flow("f2")
        store.upsert_run(
            RunUpsert(
                run_id="r1", flow_name="f1", run_dir="/tmp/r1",
                status="success", started_at=100, finished_at=200,
            )
        )
        store.upsert_run(
            RunUpsert(
                run_id="r2", flow_name="f2", run_dir="/tmp/r2",
                status="success", started_at=300, finished_at=400,
            )
        )
        assert store.latest_updated_at("f1") == 200
        assert store.latest_updated_at("f2") == 400
        assert store.latest_updated_at() == 400
