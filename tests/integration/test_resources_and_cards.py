"""Integration tests for Resources and Cards features."""

import pytest

from rinnsal.modeling.task import task
from rinnsal.modeling.flow import flow
from rinnsal.modeling.types import Resources, _normalize_resources
from rinnsal.context import current
from rinnsal.data.database import InMemoryDatabase
from rinnsal.data.file_store import FileDatabase
from rinnsal.data.logger.components import Markdown, Scalar, Table
from rinnsal.data.logger.logger import Logger
from rinnsal.compute.inline import InlineExecutor
from rinnsal.compute.engine import ExecutionEngine, set_engine, eval as rinnsal_eval


@pytest.fixture
def db():
    return InMemoryDatabase()


@pytest.fixture
def engine_with_db(db):
    executor = InlineExecutor()
    engine = ExecutionEngine(executor=executor, database=db)
    set_engine(engine)
    yield engine
    engine.shutdown()


@pytest.fixture
def engine():
    executor = InlineExecutor()
    engine = ExecutionEngine(executor=executor)
    set_engine(engine)
    yield engine
    engine.shutdown()


# ── Resources ────────────────────────────────────────────────────────


class TestResources:
    def test_resources_dataclass(self):
        r = Resources(gpu=1, gpu_memory=16000, cpu=4, memory=8000)
        assert r.gpu == 1
        assert r.gpu_memory == 16000
        assert r.cpu == 4
        assert r.memory == 8000

    def test_resources_as_dict(self):
        r = Resources(gpu=1, memory=8000)
        d = r.as_dict()
        assert d == {"gpu": 1, "memory": 8000}
        assert "gpu_memory" not in d  # zero fields excluded
        assert "cpu" not in d

    def test_resources_as_dict_with_extras(self):
        r = Resources(gpu=1, extras={"tpu": 2})
        d = r.as_dict()
        assert d == {"gpu": 1, "tpu": 2}

    def test_resources_frozen(self):
        r = Resources(gpu=1)
        with pytest.raises(AttributeError):
            r.gpu = 2

    def test_normalize_none(self):
        assert _normalize_resources(None) is None

    def test_normalize_resources_passthrough(self):
        r = Resources(gpu=1)
        assert _normalize_resources(r) is r

    def test_normalize_dict(self):
        r = _normalize_resources({"gpu": 1, "memory": 8000})
        assert isinstance(r, Resources)
        assert r.gpu == 1
        assert r.memory == 8000

    def test_normalize_dict_with_extras(self):
        r = _normalize_resources({"gpu": 1, "tpu": 2})
        assert r.gpu == 1
        assert r.extras == {"tpu": 2}

    def test_task_with_resources_typed(self):
        @task(resources=Resources(gpu=1, gpu_memory=16000))
        def train(data):
            return data

        assert train.resources is not None
        assert train.resources.gpu == 1
        assert train.resources.gpu_memory == 16000

    def test_task_with_resources_dict(self):
        @task(resources={"gpu": 1, "memory": 8000})
        def train(data):
            return data

        assert train.resources is not None
        assert train.resources.gpu == 1
        assert train.resources.memory == 8000

    def test_task_no_resources(self):
        @task
        def simple():
            return 1

        assert simple.resources is None

    def test_resources_stored_in_metadata(self, engine_with_db, db):
        @task(resources=Resources(gpu=1))
        def gpu_task():
            return 42

        rinnsal_eval(gpu_task())

        expr = gpu_task()
        entry = db.fetch_task_result(expr.hash, expr.task_name)
        assert entry is not None
        assert entry.metadata["resources"] == {"gpu": 1}

    def test_resources_not_in_metadata_when_none(self, engine_with_db, db):
        @task
        def simple():
            return 42

        rinnsal_eval(simple())

        expr = simple()
        entry = db.fetch_task_result(expr.hash, expr.task_name)
        assert "resources" not in entry.metadata


# ── Cards (unified Logger+Card API) ──────────────────────────────────
#
# A "card" is a named, user-composed grouping of components emitted
# through the Logger. Cards are addressable by (task, name) and may be
# re-emitted at multiple iterations.


def _read_events(log_dir):
    from rinnsal.data.logger.event_file import EventFileReader

    return list(EventFileReader(log_dir / "events.pb").read_all())


class TestCardBuilderSmoke:
    def test_builder_append_and_commit(self, tmp_path):
        logger = Logger(tmp_path / "logs")
        card = logger.card("report")
        card.append(Markdown("# hello"))
        card.append(Scalar(1.0))
        card.commit()
        logger.close()

        events = _read_events(logger.log_dir)
        assert len(events) == 1
        assert events[0].WhichOneof("data") == "card_event"
        assert events[0].card_event.name == "report"
        assert len(events[0].card_event.components) == 2

    def test_context_manager(self, tmp_path):
        logger = Logger(tmp_path / "logs")
        with logger.card("auto") as c:
            c.append(Markdown("auto-committed"))
            c.append(Table([[1, 2]], headers=["a", "b"]))
        logger.close()

        events = _read_events(logger.log_dir)
        assert len(events) == 1
        assert len(events[0].card_event.components) == 2

    def test_iteration_slider(self, tmp_path):
        logger = Logger(tmp_path / "logs")
        for it in (1, 2, 3):
            logger.set_iteration(it)
            with logger.card("train") as c:
                c.append(Scalar(1.0 / it))
        logger.close()

        events = _read_events(logger.log_dir)
        assert [e.iteration for e in events] == [1, 2, 3]
        assert all(e.card_event.name == "train" for e in events)


class TestCardsFromTasks:
    """A task composes a card through current.logger; the card lands in
    the flow run's events.pb."""

    def test_card_from_task_in_flow(self, tmp_path, monkeypatch):
        # Run from tmp_path so FlowResult's default db_path=".rinnsal"
        # lands inside the test directory.
        monkeypatch.chdir(tmp_path)
        db = FileDatabase(root=tmp_path / ".rinnsal")

        @task
        def analyze():
            with current.logger.card("summary") as card:
                card.append(Markdown("## summary"))
                card.append(Scalar(0.99))
            return 42

        @flow
        def f():
            return analyze()

        # Drive the flow with an engine bound to our FileDatabase so the
        # run directory (and events.pb) are created under tmp_path.
        executor = InlineExecutor()
        engine = ExecutionEngine(executor=executor, database=db)
        set_engine(engine)
        try:
            result = f().run()
        finally:
            engine.shutdown()
        assert result.result == 42

        # The logger writes events.pb into the run dir under .rinnsal/flows/...
        # (sibling JSON files from FileDatabase.store_flow_run are excluded).
        runs_root = tmp_path / ".rinnsal" / "flows" / "f" / "runs"
        run_dirs = sorted(p for p in runs_root.iterdir() if p.is_dir())
        assert run_dirs, "no run directory created"
        events = _read_events(run_dirs[-1])
        card_events = [e for e in events if e.WhichOneof("data") == "card_event"]
        assert len(card_events) == 1
        assert card_events[0].card_event.name == "summary"
        assert card_events[0].card_event.task == "analyze"
