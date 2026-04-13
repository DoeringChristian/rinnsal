"""Integration: Logger writes mirror to the metadata store."""

from __future__ import annotations

from pathlib import Path

import pytest

from rinnsal.data.file_store import FileDatabase
from rinnsal.data.logger.logger import Logger
from rinnsal.data.metadata import RunUpsert, SqliteMetadataStore


def _make_logger(tmp_path: Path) -> tuple[Logger, SqliteMetadataStore]:
    db = FileDatabase(root=tmp_path / ".rinnsal")
    store = db.metadata_store()
    rd = tmp_path / ".rinnsal/flows/f/runs/r1"
    rd.mkdir(parents=True)
    store.upsert_flow("f")
    store.upsert_run(
        RunUpsert(
            run_id="r1", flow_name="f", run_dir=str(rd),
            status="running", started_at=100.0,
        )
    )
    lg = Logger(rd, database=db, metadata_store=store)
    lg.set_run_metadata("r1", "f")
    return lg, store


class TestLoggerMirrorsToStore:
    def test_task_node_lands_in_db(self, tmp_path):
        lg, store = _make_logger(tmp_path)
        lg.add_task_node("train", "h1", "success", duration=2.5, params="{}")
        lg.add_task_node("eval", "h2", "success", duration=0.5, params="{}")
        lg.close()

        nodes = store.list_task_nodes("r1")
        names = {n.task_name for n in nodes}
        assert names == {"train", "eval"}
        train = next(n for n in nodes if n.task_name == "train")
        assert train.duration == 2.5
        assert train.task_hash == "h1"
        assert train.status == "success"

    def test_task_edge_lands_in_db(self, tmp_path):
        lg, store = _make_logger(tmp_path)
        lg.add_task_edge("train", "eval")
        lg.add_task_edge("train", "eval")  # duplicate
        lg.close()

        edges = store.list_task_edges("r1")
        assert edges == [("train", "eval")]

    def test_status_transitions_overwrite(self, tmp_path):
        """A 'running' write followed by 'success' must update, not duplicate."""
        lg, store = _make_logger(tmp_path)
        lg.add_task_node("train", "h1", "running", duration=0, params="{}")
        lg.add_task_node("train", "h1", "success", duration=2.5, params="{}")
        lg.close()

        nodes = store.list_task_nodes("r1")
        assert len(nodes) == 1
        assert nodes[0].status == "success"
        assert nodes[0].duration == 2.5

    def test_db_failure_does_not_break_events_pb(self, tmp_path):
        """A broken metadata_store must not corrupt events.pb."""
        from rinnsal.data.logger.event_file import EventFileReader

        lg, store = _make_logger(tmp_path)

        class Broken:
            def upsert_task_node(self, *_a, **_kw):
                raise RuntimeError("DB on fire")

            def upsert_task_edge(self, *_a, **_kw):
                raise RuntimeError("DB on fire")

        lg._metadata_store = Broken()
        lg.add_task_node("train", "h1", "success", duration=1.0, params="{}")
        lg.add_task_edge("train", "eval")
        lg.close()

        # events.pb still readable; contains both events.
        events = list(EventFileReader(lg.log_dir / "events.pb").read_all())
        kinds = [e.WhichOneof("data") for e in events]
        assert "task_node" in kinds
        assert "task_edge" in kinds


@pytest.fixture(autouse=True)
def _reset_default_engine(monkeypatch):
    """Earlier tests may set a global engine without a database; clear it
    so FlowResult.run builds a fresh engine + metadata store per test."""
    import rinnsal.compute.engine as engine_module

    monkeypatch.setattr(engine_module, "_default_engine", None)
    yield


class TestFlowRunPopulatesStore:
    def test_simple_flow_writes_run_and_tasks(self, tmp_path, monkeypatch):
        from rinnsal import flow, task

        monkeypatch.chdir(tmp_path)

        @task
        def step():
            return 42

        @flow
        def my_flow():
            return step()

        my_flow().run()

        store = SqliteMetadataStore(tmp_path / ".rinnsal" / "metadata.sqlite")
        flows = store.list_flows()
        assert len(flows) == 1
        assert flows[0].name == "my_flow"
        runs = store.list_runs(flow_name="my_flow")
        assert len(runs) == 1
        assert runs[0].status == "success"
        assert runs[0].finished_at is not None
        assert runs[0].failed_count == 0

        nodes = store.list_task_nodes(runs[0].run_id)
        assert any(n.task_name == "step" for n in nodes)

    def test_failing_flow_marks_run_failed(self, tmp_path, monkeypatch):
        from rinnsal import flow, task

        monkeypatch.chdir(tmp_path)

        @task
        def boom():
            raise RuntimeError("kaboom")

        @flow
        def my_flow():
            return boom()

        with pytest.raises(RuntimeError):
            my_flow().run()

        store = SqliteMetadataStore(tmp_path / ".rinnsal" / "metadata.sqlite")
        runs = store.list_runs(flow_name="my_flow")
        assert len(runs) == 1
        assert runs[0].status == "failed"
        assert runs[0].failed_count >= 1
