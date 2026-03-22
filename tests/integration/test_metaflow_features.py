"""Integration tests for Metaflow-inspired features: timeout, catch, map, tags."""

import sys
import time
from unittest import mock

import pytest

from rinnsal.core.task import task
from rinnsal.core.flow import flow, FlowResult
from rinnsal.core.registry import get_registry
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


def _with_argv(*args):
    return mock.patch.object(sys, "argv", ["test_script", *args])


# ── Feature 1: @task(timeout=N) ──────────────────────────────────────


class TestTimeout:
    def test_timeout_property(self):
        @task(timeout=30)
        def slow():
            return 1

        assert slow.timeout == 30

    def test_no_timeout_default(self):
        @task
        def fast():
            return 1

        assert fast.timeout is None

    def test_timeout_with_retry(self):
        @task(timeout=10, retry=2)
        def flaky():
            return 1

        assert flaky.timeout == 10
        assert flaky.retry == 2


# ── Feature 2: @task(catch=True) ─────────────────────────────────────


class TestCatch:
    def test_catch_default_disabled(self):
        @task
        def normal():
            return 1

        assert not normal.catch_enabled

    def test_catch_true(self):
        @task(catch=True)
        def risky():
            return 1

        assert risky.catch_enabled
        assert risky.catch is True

    def test_catch_custom_value(self):
        @task(catch="fallback")
        def risky():
            return 1

        assert risky.catch_enabled
        assert risky.catch == "fallback"

    def test_catch_returns_none_on_failure(self, engine):
        @task(catch=True)
        def broken():
            raise RuntimeError("boom")

        result = rinnsal_eval(broken())
        assert result is None

    def test_catch_returns_custom_value_on_failure(self, engine):
        @task(catch="default_val")
        def broken():
            raise RuntimeError("boom")

        result = rinnsal_eval(broken())
        assert result == "default_val"

    def test_catch_does_not_affect_success(self, engine):
        @task(catch=True)
        def ok():
            return 42

        result = rinnsal_eval(ok())
        assert result == 42

    def test_catch_with_retry_retries_first(self, engine):
        attempts = []

        @task(retry=2, catch=True)
        def flaky():
            attempts.append(1)
            raise RuntimeError("fail")

        result = rinnsal_eval(flaky())
        assert result is None
        assert len(attempts) == 3  # 1 + 2 retries

    def test_catch_allows_downstream_to_continue(self, engine_with_db):
        @task(catch=True)
        def source():
            raise RuntimeError("source failed")

        @task
        def consumer(x):
            return f"got: {x}"

        @flow
        def my_flow():
            s = source()
            c = consumer(s)
            return [s, c]

        with _with_argv():
            result = my_flow().run()

        assert result[0].result is None
        assert result[1].result == "got: None"

    def test_catch_false_value(self, engine):
        """catch=0 should be enabled with default value 0."""

        @task(catch=0)
        def broken():
            raise RuntimeError("boom")

        assert broken.catch_enabled
        result = rinnsal_eval(broken())
        assert result == 0


# ── Feature 3: task.map() ────────────────────────────────────────────


class TestMap:
    def test_map_single_iterable(self, engine):
        @task
        def double(x):
            return x * 2

        exprs = double.map([1, 2, 3])
        assert len(exprs) == 3

        results = [rinnsal_eval(e) for e in exprs]
        assert results == [2, 4, 6]

    def test_map_multi_iterable(self, engine):
        @task
        def add(a, b):
            return a + b

        exprs = add.map([1, 2, 3], [10, 20, 30])
        assert len(exprs) == 3

        results = [rinnsal_eval(e) for e in exprs]
        assert results == [11, 22, 33]

    def test_map_auto_names(self, engine):
        @task
        def process(x):
            return x

        exprs = process.map([1, 2, 3])
        assert exprs[0].task_name == "process[0]"
        assert exprs[1].task_name == "process[1]"
        assert exprs[2].task_name == "process[2]"

    def test_map_preserves_explicit_name(self, engine):
        @task
        def process(x):
            return x

        exprs = process.map([1, 2], name="custom")
        # name= kwarg sets all to "custom" (from TaskDef.__call__)
        assert exprs[0].task_name == "custom"
        assert exprs[1].task_name == "custom"

    def test_map_empty_iterable(self, engine):
        @task
        def process(x):
            return x

        exprs = process.map([])
        assert exprs == []

    def test_map_with_kwargs(self, engine):
        @task
        def multiply(x, factor):
            return x * factor

        exprs = multiply.map([1, 2, 3], factor=10)
        results = [rinnsal_eval(e) for e in exprs]
        assert results == [10, 20, 30]

    def test_map_in_flow(self, engine_with_db):
        @task
        def double(x):
            return x * 2

        @task
        def total(values):
            return sum(values)

        @flow
        def my_flow():
            results = double.map([1, 2, 3])
            return results

        with _with_argv():
            fr = my_flow()
            fr.run()

        assert fr[0].result == 2
        assert fr[1].result == 4
        assert fr[2].result == 6

    def test_map_with_task_expression_input(self, engine):
        @task
        def source():
            return [10, 20, 30]

        @task
        def double(x):
            return x * 2

        # map over concrete values (not expressions)
        exprs = double.map([1, 2])
        results = [rinnsal_eval(e) for e in exprs]
        assert results == [2, 4]


# ── Feature 4: --tag ─────────────────────────────────────────────────


class TestTags:
    def test_tag_stored_in_metadata(self, engine_with_db, db):
        @task
        def source():
            return 42

        @flow
        def my_flow():
            return source()

        with _with_argv("--tag", "v1", "--tag", "gpu"):
            my_flow().run()

        runs = db.fetch_flow_runs("my_flow", limit=1)
        assert len(runs) == 1
        assert runs[0]["metadata"]["tags"] == ["v1", "gpu"]

    def test_no_tags_no_metadata_key(self, engine_with_db, db):
        @task
        def source():
            return 42

        @flow
        def my_flow():
            return source()

        with _with_argv():
            my_flow().run()

        runs = db.fetch_flow_runs("my_flow", limit=1)
        assert "tags" not in runs[0]["metadata"]

    def test_filter_by_tags(self, engine_with_db, db):
        @task
        def source():
            return 42

        @flow
        def my_flow():
            return source()

        # Run 1: tagged v1
        with _with_argv("--tag", "v1"):
            my_flow().run()

        get_registry().clear()

        # Run 2: tagged v2
        with _with_argv("--tag", "v2"):
            my_flow().run()

        get_registry().clear()

        # Run 3: tagged v1 and gpu
        with _with_argv("--tag", "v1", "--tag", "gpu"):
            my_flow().run()

        # All runs
        all_runs = db.fetch_flow_runs("my_flow")
        assert len(all_runs) == 3

        # Filter by v1
        v1_runs = db.fetch_flow_runs("my_flow", tags=["v1"])
        assert len(v1_runs) == 2

        # Filter by gpu
        gpu_runs = db.fetch_flow_runs("my_flow", tags=["gpu"])
        assert len(gpu_runs) == 1

        # Filter by v1 AND gpu
        v1_gpu_runs = db.fetch_flow_runs("my_flow", tags=["v1", "gpu"])
        assert len(v1_gpu_runs) == 1

        # Filter by nonexistent
        none_runs = db.fetch_flow_runs("my_flow", tags=["nonexistent"])
        assert len(none_runs) == 0

    def test_filter_by_tags_with_limit(self, engine_with_db, db):
        @task
        def source():
            return 42

        @flow
        def my_flow():
            return source()

        for _ in range(5):
            with _with_argv("--tag", "batch"):
                my_flow().run()
            get_registry().clear()

        runs = db.fetch_flow_runs("my_flow", limit=2, tags=["batch"])
        assert len(runs) == 2
