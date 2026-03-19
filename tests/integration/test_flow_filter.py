"""Integration tests for flow --filter functionality."""

import sys
from unittest import mock

import pytest

from rinnsal.core.task import task
from rinnsal.core.flow import flow, FlowResult
from rinnsal.core.registry import get_registry
from rinnsal.persistence.database import InMemoryDatabase
from rinnsal.execution.inline import InlineExecutor
from rinnsal.runtime.engine import ExecutionEngine, set_engine


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


def _with_argv(*args):
    """Context manager to mock sys.argv for CLI flag parsing."""
    return mock.patch.object(sys, "argv", ["test_script", *args])


class TestFilterByName:
    """Test filtering tasks by name pattern."""

    def test_filter_matches_task_name(self, engine_with_db):
        """Only the matched task runs; others are not touched."""
        call_log = []

        @task
        def process(x):
            call_log.append(x)
            return x * 2

        @flow
        def my_flow():
            a = process(1).name("alpha")
            b = process(2).name("beta")
            c = process(3).name("gamma")
            return [a, b, c]

        with _with_argv("--filter", "beta"):
            fr = my_flow()
            fr.run()

        assert call_log == [2]

    def test_filter_matches_function_name(self, engine_with_db):
        """Filter can match on the underlying function name."""
        call_log = []

        @task
        def train(x):
            call_log.append(("train", x))
            return x

        @task
        def evaluate(x):
            call_log.append(("eval", x))
            return x * 2

        @flow
        def my_flow():
            t = train(10)
            e = evaluate(t)
            return [t, e]

        # Full run to populate cache (evaluate depends on train)
        with _with_argv():
            my_flow().run()

        call_log.clear()
        get_registry().clear()

        # Filter by function name — train loaded from cache
        with _with_argv("--filter", "evaluate"):
            fr = my_flow()
            fr.run()

        # evaluate re-runs (not cached because use_cache default),
        # train is loaded from cache — not re-executed
        assert ("train", 10) not in call_log

    def test_filter_regex_pattern(self, engine_with_db):
        """Regex patterns match across multiple tasks."""
        call_log = []

        @task
        def process(x):
            call_log.append(x)
            return x * 2

        @flow
        def my_flow():
            a = process(1).name("train_model_a")
            b = process(2).name("train_model_b")
            c = process(3).name("eval_model_a")
            return [a, b, c]

        with _with_argv("--filter", "train"):
            fr = my_flow()
            fr.run()

        assert sorted(call_log) == [1, 2]

    def test_filter_no_match_raises(self, engine_with_db):
        @task
        def process(x):
            return x

        @flow
        def my_flow():
            return [process(1).name("alpha"), process(2).name("beta")]

        with _with_argv("--filter", "nonexistent"):
            fr = my_flow()
            with pytest.raises(ValueError, match="No tasks match"):
                fr.run()


class TestFilterWithDependencies:
    """Test that filter loads dependencies from cache."""

    def test_dependency_loaded_from_cache(self, engine_with_db):
        """Matched task executes; its dependency comes from cache."""
        call_log = []

        @task
        def source():
            call_log.append("source")
            return 10

        @task
        def transform(x):
            call_log.append("transform")
            return x * 2

        @flow
        def my_flow():
            s = source()
            t = transform(s)
            return [s, t]

        # Full run to populate cache
        with _with_argv():
            my_flow().run()

        assert "source" in call_log
        assert "transform" in call_log
        call_log.clear()
        get_registry().clear()

        # Filter to only "transform" — source should come from cache
        with _with_argv("--filter", "transform"):
            fr = my_flow()
            fr.run()

        assert "source" not in call_log

    def test_deep_dependency_chain(self, engine_with_db):
        """Only the matched leaf task executes; the full chain is cached."""
        call_log = []

        @task
        def step_a():
            call_log.append("a")
            return 1

        @task
        def step_b(x):
            call_log.append("b")
            return x + 1

        @task
        def step_c(x):
            call_log.append("c")
            return x + 1

        @task
        def step_d(x):
            call_log.append("d")
            return x + 1

        @flow
        def my_flow():
            a = step_a()
            b = step_b(a)
            c = step_c(b)
            d = step_d(c)
            return [a, b, c, d]

        # Full run
        with _with_argv():
            my_flow().run()

        call_log.clear()
        get_registry().clear()

        # Filter to step_d — a, b, c should come from cache
        with _with_argv("--filter", "step_d"):
            fr = my_flow()
            fr.run()

        assert "a" not in call_log
        assert "b" not in call_log
        assert "c" not in call_log

    def test_diamond_dependency(self, engine_with_db):
        """Filter on merge node; source, left, right come from cache."""
        call_log = []

        @task
        def source():
            call_log.append("source")
            return 10

        @task
        def left(x):
            call_log.append("left")
            return x + 1

        @task
        def right(x):
            call_log.append("right")
            return x + 2

        @task
        def merge(a, b):
            call_log.append("merge")
            return a + b

        @flow
        def my_flow():
            s = source()
            l = left(s)
            r = right(s)
            m = merge(l, r)
            return [s, l, r, m]

        # Full run
        with _with_argv():
            my_flow().run()

        call_log.clear()
        get_registry().clear()

        # Filter to merge — source, left, right from cache
        with _with_argv("--filter", "merge"):
            fr = my_flow()
            fr.run()

        assert "source" not in call_log
        assert "left" not in call_log
        assert "right" not in call_log

    def test_dependency_not_cached_raises(self, engine_with_db):
        """If a dependency has no cached result, raise a clear error."""

        @task
        def source():
            return 10

        @task
        def transform(x):
            return x * 2

        @flow
        def my_flow():
            s = source()
            t = transform(s)
            return [s, t]

        # Don't run full flow — no cache
        with _with_argv("--filter", "transform"):
            fr = my_flow()
            with pytest.raises(ValueError, match="No cached result"):
                fr.run()

    def test_filter_middle_of_chain(self, engine_with_db):
        """Filter a task in the middle; upstream cached, downstream excluded."""
        call_log = []

        @task
        def step_a():
            call_log.append("a")
            return 1

        @task
        def step_b(x):
            call_log.append("b")
            return x + 1

        @task
        def step_c(x):
            call_log.append("c")
            return x + 1

        @flow
        def my_flow():
            a = step_a()
            b = step_b(a)
            c = step_c(b)
            return [a, b, c]

        # Full run
        with _with_argv():
            my_flow().run()

        call_log.clear()
        get_registry().clear()

        # Filter to step_b only — step_a cached, step_c not processed
        with _with_argv("--filter", "step_b"):
            fr = my_flow()
            fr.run()

        assert "a" not in call_log
        assert "c" not in call_log


class TestFilterWithoutDependencies:
    """Test filtering independent tasks (no deps between them)."""

    def test_independent_tasks(self, engine_with_db):
        call_log = []

        @task
        def job(label):
            call_log.append(label)
            return label

        @flow
        def my_flow():
            a = job("alpha").name("alpha")
            b = job("beta").name("beta")
            c = job("gamma").name("gamma")
            return [a, b, c]

        with _with_argv("--filter", "beta"):
            fr = my_flow()
            fr.run()

        assert call_log == ["beta"]

    def test_filter_multiple_independent_matches(self, engine_with_db):
        call_log = []

        @task
        def job(label):
            call_log.append(label)
            return label

        @flow
        def my_flow():
            a = job("train_a").name("train_a")
            b = job("train_b").name("train_b")
            c = job("eval_a").name("eval_a")
            return [a, b, c]

        with _with_argv("--filter", "train"):
            fr = my_flow()
            fr.run()

        assert sorted(call_log) == ["train_a", "train_b"]


class TestFilterWithNoReturn:
    """Test filtering with flows that don't return tasks (captured tasks)."""

    def test_captured_tasks_filtered(self, engine_with_db):
        call_log = []

        @task
        def process(x):
            call_log.append(x)
            return x * 2

        @flow
        def my_flow():
            process(1).name("alpha")
            process(2).name("beta")
            process(3).name("gamma")

        with _with_argv("--filter", "gamma"):
            fr = my_flow()
            fr.run()

        assert call_log == [3]


class TestFilterReExecution:
    """Test that matched tasks actually re-execute even if cached."""

    def test_matched_task_reexecutes_after_full_run(self, engine_with_db):
        """Full run, then filtered re-run: matched task must re-execute."""
        call_log = []

        @task
        def source():
            call_log.append("source")
            return 10

        @task
        def add(x):
            call_log.append("add")
            return x + 1

        @flow
        def my_flow():
            s = source()
            a = add(s)
            return [s, a]

        # Full run populates cache
        with _with_argv():
            my_flow().run()

        assert call_log.count("source") == 1
        assert call_log.count("add") == 1
        call_log.clear()
        get_registry().clear()

        # Filtered re-run: add must re-execute, source loaded from cache
        with _with_argv("--filter", "add"):
            my_flow().run()

        assert "add" in call_log
        assert "source" not in call_log

    def test_run_always_executes_even_if_cached(self, engine_with_db):
        """run() without filter always executes all tasks fresh."""
        call_log = []

        @task
        def work():
            call_log.append("work")
            return 42

        @flow
        def my_flow():
            return work()

        # First run
        with _with_argv():
            my_flow().run()

        assert call_log.count("work") == 1

        get_registry().clear()

        # Second run — should still execute
        with _with_argv():
            my_flow().run()

        assert call_log.count("work") == 2

    def test_filtered_result_uses_cached_dep_value(self, engine_with_db):
        """Matched task receives the correct value from its cached dependency."""

        @task
        def source():
            return 42

        @task
        def double(x):
            return x * 2

        @flow
        def my_flow():
            s = source()
            d = double(s)
            return d

        # Full run — populates cache with source=42, double=84
        with _with_argv():
            my_flow().run()

        get_registry().clear()

        # Filtered re-run — double should get 42 from cache and return 84
        with _with_argv("--filter", "double"):
            rv = my_flow().run()

        assert rv.result == 84

    def test_downstream_of_matched_is_excluded(self, engine_with_db):
        """Tasks downstream of the matched task are NOT processed."""
        call_log = []

        @task
        def step_a():
            call_log.append("a")
            return 1

        @task
        def step_b(x):
            call_log.append("b")
            return x + 1

        @task
        def step_c(x):
            call_log.append("c")
            return x + 1

        @flow
        def my_flow():
            a = step_a()
            b = step_b(a)
            c = step_c(b)
            return [a, b, c]

        # Full run
        with _with_argv():
            my_flow().run()

        call_log.clear()
        get_registry().clear()

        # Filter to step_b — step_a loaded from cache, step_c excluded
        with _with_argv("--filter", "step_b"):
            my_flow().run()

        assert "b" in call_log
        assert "a" not in call_log
        assert "c" not in call_log

    def test_matched_task_error_propagates(self, engine_with_db):
        """If a matched task raises, the error propagates to the caller."""

        @task
        def source():
            return 1

        @task
        def broken(x):
            raise RuntimeError("boom")

        @flow
        def my_flow():
            s = source()
            b = broken(s)
            return [s, b]

        # Full run to cache source
        with _with_argv():
            with pytest.raises(RuntimeError, match="boom"):
                my_flow().run()

        get_registry().clear()

        # Filtered re-run — same error
        with _with_argv("--filter", "broken"):
            with pytest.raises(RuntimeError, match="boom"):
                my_flow().run()

    def test_dep_failure_skips_matched_task(self, engine_with_db):
        """If a dependency has no cached result, the matched task is skipped."""

        @task
        def uncached():
            return 1

        @task
        def consumer(x):
            return x + 1

        @flow
        def my_flow():
            u = uncached()
            c = consumer(u)
            return [u, c]

        # Do NOT run full flow — cache is empty
        with _with_argv("--filter", "consumer"):
            with pytest.raises(ValueError, match="No cached result"):
                my_flow().run()


class TestFilterRequiresDatabase:
    """Test that filter mode requires a database."""

    def test_no_database_raises(self):
        executor = InlineExecutor()
        engine = ExecutionEngine(executor=executor, database=None)
        set_engine(engine)

        @task
        def process(x):
            return x

        @flow
        def my_flow():
            return process(1)

        try:
            with _with_argv("--filter", "process"):
                fr = my_flow()
                with pytest.raises(ValueError, match="requires a database"):
                    fr.run()
        finally:
            engine.shutdown()


class TestNoFilterRunsAll:
    """Verify that without --filter, all tasks run normally."""

    def test_all_tasks_run(self, engine_with_db):
        call_log = []

        @task
        def process(x):
            call_log.append(x)
            return x * 2

        @flow
        def my_flow():
            a = process(1).name("alpha")
            b = process(2).name("beta")
            c = process(3).name("gamma")
            return [a, b, c]

        with _with_argv():
            fr = my_flow()
            fr.run()

        assert sorted(call_log) == [1, 2, 3]
