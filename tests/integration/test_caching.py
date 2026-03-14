"""Integration tests for caching."""

import pytest

from rinnsal.core.task import task
from rinnsal.core.registry import get_registry
from rinnsal.persistence.database import InMemoryDatabase
from rinnsal.persistence.file_store import FileDatabase
from rinnsal.runtime.engine import ExecutionEngine
from rinnsal.execution.inline import InlineExecutor


class TestCachingWithInMemoryDatabase:
    """Tests for caching with InMemoryDatabase."""

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

    def test_cache_hit(self, engine):
        call_count = 0

        @task
        def counter():
            nonlocal call_count
            call_count += 1
            return call_count

        # First call
        expr1 = counter()
        result1 = engine.evaluate(expr1)
        assert result1 == 1
        assert call_count == 1

        # Clear engine cache but keep database
        engine.clear_cache()

        # Second call - different expression object but same hash
        @task
        def counter():
            nonlocal call_count
            call_count += 1
            return call_count

        expr2 = counter()
        result2 = engine.evaluate(expr2)

        # Should use cached result
        assert result2 == 1
        assert call_count == 1  # Not incremented

    def test_cache_disabled(self):
        """Test that use_cache=False ignores database cache."""
        call_count = 0

        @task
        def compute(x):
            nonlocal call_count
            call_count += 1
            return x * 2

        # Create engine with caching disabled
        db = InMemoryDatabase()
        no_cache_engine = ExecutionEngine(
            executor=InlineExecutor(),
            database=db,
            use_cache=False,
        )

        # First call with x=1
        expr1 = compute(1)
        result1 = no_cache_engine.evaluate(expr1)
        assert result1 == 2
        assert call_count == 1

        # Manually store a different result in the database for the same hash
        from rinnsal.core.types import Entry
        db.store_task_result(expr1.hash, Entry(result=999))

        # Clear engine's in-memory cache
        no_cache_engine.clear_cache()
        get_registry().clear()

        # Redefine to get fresh expression with same hash
        @task
        def compute(x):
            nonlocal call_count
            call_count += 1
            return x * 2

        # Second call - even though DB has result, should execute because use_cache=False
        expr2 = compute(1)
        result2 = no_cache_engine.evaluate(expr2)

        # Should execute again (not using database cache)
        assert result2 == 2
        assert call_count == 2

        no_cache_engine.shutdown()


class TestCachingWithFileDatabase:
    """Tests for caching with FileDatabase."""

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

    def test_cache_persists(self, db, tmp_path):
        call_count = 0

        @task
        def counter():
            nonlocal call_count
            call_count += 1
            return call_count

        # First engine
        engine1 = ExecutionEngine(
            executor=InlineExecutor(),
            database=db,
        )

        expr = counter()
        result1 = engine1.evaluate(expr)
        assert result1 == 1
        engine1.shutdown()

        # Create new engine with same database path
        db2 = FileDatabase(root=tmp_path / ".rinnsal")
        engine2 = ExecutionEngine(
            executor=InlineExecutor(),
            database=db2,
        )

        # Clear global registry
        get_registry().clear()

        @task
        def counter():
            nonlocal call_count
            call_count += 1
            return call_count

        expr2 = counter()
        result2 = engine2.evaluate(expr2)

        # Should use cached result from disk
        assert result2 == 1
        assert call_count == 1
        engine2.shutdown()


class TestCachingWithDependencies:
    """Tests for caching with task dependencies."""

    @pytest.fixture(autouse=True)
    def clean_registry(self):
        registry = get_registry()
        registry.clear()
        yield
        registry.clear()

    def test_downstream_uses_cached_upstream(self):
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

        # First run
        result = engine.evaluate(double(source()))
        assert result == 20
        assert call_counts["source"] == 1
        assert call_counts["double"] == 1

        # Clear engine cache
        engine.clear_cache()
        get_registry().clear()

        # Redefine tasks
        @task
        def source():
            call_counts["source"] += 1
            return 10

        @task
        def double(x):
            call_counts["double"] += 1
            return x * 2

        # Second run - both should be cached
        result = engine.evaluate(double(source()))
        assert result == 20
        assert call_counts["source"] == 1  # Not incremented
        assert call_counts["double"] == 1  # Not incremented

        engine.shutdown()
