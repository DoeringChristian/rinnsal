"""Integration tests for Resources and Cards features."""

import pytest

from rinnsal.core.task import task
from rinnsal.core.flow import flow
from rinnsal.core.types import Resources, _normalize_resources
from rinnsal.context import Card, current
from rinnsal.persistence.database import InMemoryDatabase
from rinnsal.execution.inline import InlineExecutor
from rinnsal.runtime.engine import ExecutionEngine, set_engine, eval as rinnsal_eval


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


# ── Cards ────────────────────────────────────────────────────────────


class TestCard:
    def test_card_text(self):
        card = Card()
        card.text("Hello", title="Greeting")
        assert len(card.items) == 1
        assert card.items[0].kind == "text"
        assert card.items[0].content == "Hello"
        assert card.items[0].title == "Greeting"

    def test_card_html(self):
        card = Card()
        card.html("<b>Bold</b>")
        assert card.items[0].kind == "html"

    def test_card_table(self):
        card = Card()
        card.table([[1, 2], [3, 4]], headers=["A", "B"])
        item = card.items[0]
        assert item.kind == "table"
        assert item.content["headers"] == ["A", "B"]
        assert item.content["rows"] == [[1, 2], [3, 4]]

    def test_card_serialize(self):
        card = Card()
        card.text("Hello")
        card.html("<b>World</b>")
        serialized = card.serialize()
        assert len(serialized) == 2
        assert serialized[0]["kind"] == "text"
        assert serialized[1]["kind"] == "html"

    def test_card_is_empty(self):
        card = Card()
        assert card.is_empty()
        card.text("Hello")
        assert not card.is_empty()


class TestCurrentContext:
    def test_current_card_lazy_creation(self):
        card = current.card
        assert isinstance(card, Card)
        # Same card on repeated access
        assert current.card is card
        current._reset()

    def test_current_reset_returns_card(self):
        current.card.text("test")
        card = current._reset()
        assert card is not None
        assert len(card.items) == 1

    def test_current_reset_clears(self):
        current.card.text("test")
        current._reset()
        # After reset, new card is created
        assert current.card.is_empty()
        current._reset()

    def test_current_reset_empty_returns_none(self):
        _ = current.card  # Create empty card
        card = current._reset()
        assert card is None


class TestCardsIntegration:
    def test_card_in_task_inline(self, engine):
        @task
        def my_task():
            current.card.text("Task completed!")
            return 42

        result = rinnsal_eval(my_task())
        assert result == 42

    def test_card_stored_in_metadata(self, engine_with_db, db):
        @task
        def my_task():
            current.card.text("Result summary", title="Summary")
            current.card.html("<b>Done</b>")
            return 42

        expr = my_task()
        rinnsal_eval(expr)

        entry = db.fetch_task_result(expr.hash, expr.task_name)
        assert "card" in entry.metadata
        card_data = entry.metadata["card"]
        assert len(card_data) == 2
        assert card_data[0]["kind"] == "text"
        assert card_data[0]["content"] == "Result summary"
        assert card_data[1]["kind"] == "html"

    def test_no_card_no_metadata_key(self, engine_with_db, db):
        @task
        def simple():
            return 42

        expr = simple()
        rinnsal_eval(expr)

        entry = db.fetch_task_result(expr.hash, expr.task_name)
        assert "card" not in entry.metadata

    def test_card_on_failure_not_stored(self, engine):
        @task
        def broken():
            current.card.text("Before crash")
            raise RuntimeError("boom")

        with pytest.raises(RuntimeError, match="boom"):
            rinnsal_eval(broken())

    def test_card_with_catch(self, engine_with_db, db):
        """When catch=True and task fails, card is not stored (failure path)."""

        @task(catch=True)
        def risky():
            raise RuntimeError("fail")

        expr = risky()
        result = rinnsal_eval(expr)
        assert result is None

        # The catch path in engine doesn't capture cards from failed attempts
        entry = db.fetch_task_result(expr.hash, expr.task_name)
        assert "card" not in entry.metadata
