"""Integration tests for caching."""

import pytest

from rinnsal.core.task import task
from rinnsal.core.registry import get_registry
from rinnsal.persistence.database import InMemoryDatabase
from rinnsal.persistence.file_store import FileDatabase
from rinnsal.runtime.engine import ExecutionEngine
from rinnsal.execution.inline import InlineExecutor


class TestInMemoryDeduplication:
    """Tests for within-run deduplication via _evaluated dict."""

    @pytest.fixture
    def engine(self):
        db = InMemoryDatabase()
        executor = InlineExecutor()
        engine = ExecutionEngine(executor=executor, database=db)
        yield engine
        engine.shutdown()

    @pytest.fixture(autouse=True)
    def clean_registry(self):
        registry = get_registry()
        registry.clear()
        yield
        registry.clear()

    def test_same_expression_deduped_within_run(self, engine):
        """Within a single run, same hash is evaluated once."""
        call_count = 0

        @task
        def counter():
            nonlocal call_count
            call_count += 1
            return call_count

        expr = counter()
        result = engine.evaluate(expr)
        assert result == 1
        assert call_count == 1

        # Same expression again (still in _evaluated)
        result2 = engine.evaluate(expr)
        assert result2 == 1
        assert call_count == 1  # Not re-executed

    def test_fresh_execution_after_clear_cache(self, engine):
        """After clear_cache(), tasks execute fresh."""
        call_count = 0

        @task
        def compute(x):
            nonlocal call_count
            call_count += 1
            return x * 2

        expr = compute(1)
        result1 = engine.evaluate(expr)
        assert result1 == 2
        assert call_count == 1

        engine.clear_cache()
        get_registry().clear()

        @task
        def compute(x):
            nonlocal call_count
            call_count += 1
            return x * 2

        expr2 = compute(1)
        result2 = engine.evaluate(expr2)
        assert result2 == 2
        assert call_count == 2  # Executed again


class TestPersistence:
    """Tests for database persistence (store, not cache-on-read)."""

    @pytest.fixture
    def db(self, tmp_path):
        return FileDatabase(root=tmp_path / ".rinnsal")

    @pytest.fixture
    def engine(self, db):
        executor = InlineExecutor()
        engine = ExecutionEngine(executor=executor, database=db)
        yield engine
        engine.shutdown()

    @pytest.fixture(autouse=True)
    def clean_registry(self):
        registry = get_registry()
        registry.clear()
        yield
        registry.clear()

    def test_results_persisted_to_database(self, db):
        """Results are stored in the database after execution."""

        @task
        def source():
            return 42

        engine = ExecutionEngine(
            executor=InlineExecutor(),
            database=db,
        )

        expr = source()
        engine.evaluate(expr)

        # Result should be in the database
        entry = db.fetch_task_result(expr.hash, expr.task_name)
        assert entry is not None
        assert entry.result == 42

        engine.shutdown()


class TestDependencyDeduplication:
    """Tests for deduplication with dependencies within a run."""

    @pytest.fixture(autouse=True)
    def clean_registry(self):
        registry = get_registry()
        registry.clear()
        yield
        registry.clear()

    def test_shared_dependency_runs_once(self):
        db = InMemoryDatabase()
        call_counts = {"source": 0, "double": 0}

        @task
        def source():
            call_counts["source"] += 1
            return 10

        @task
        def double(x):
            call_counts["double"] += 1
            return x * 2

        engine = ExecutionEngine(
            executor=InlineExecutor(),
            database=db,
        )

        result = engine.evaluate(double(source()))
        assert result == 20
        assert call_counts["source"] == 1
        assert call_counts["double"] == 1

        engine.shutdown()
