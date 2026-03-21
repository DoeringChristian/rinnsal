"""Integration tests for flow --resume functionality."""

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


class TestResumeBasic:
    """Test basic --resume functionality."""

    def test_resume_no_previous_run_raises(self, engine_with_db):
        """--resume with no prior run raises ValueError."""

        @task
        def process(x):
            return x

        @flow
        def my_flow():
            return process(1)

        with _with_argv("--resume"):
            fr = my_flow()
            with pytest.raises(ValueError, match="No previous runs"):
                fr.run()

    def test_resume_skips_successful_tasks(self, engine_with_db):
        """Tasks that succeeded in the prior run are loaded from cache."""
        call_log = []
        fail_flag = [True]  # Mutable flag to control failure

        @task
        def source():
            call_log.append("source")
            return 10

        @task
        def transform(x):
            call_log.append("transform")
            if fail_flag[0]:
                raise RuntimeError("transform failed")
            return x * 2

        @flow
        def my_flow():
            s = source()
            t = transform(s)
            return [s, t]

        # First run: source succeeds, transform fails
        with _with_argv():
            with pytest.raises(RuntimeError, match="transform failed"):
                my_flow().run()

        assert "source" in call_log
        assert "transform" in call_log
        call_log.clear()
        get_registry().clear()

        # Resume: source should be cached, transform re-executes
        fail_flag[0] = False
        with _with_argv("--resume"):
            result = my_flow().run()

        assert "source" not in call_log  # loaded from cache
        assert "transform" in call_log  # re-executed
        assert result[1].result == 20

    def test_resume_all_passed_loads_all_from_cache(self, engine_with_db):
        """If all tasks passed, resume loads everything from cache."""
        call_log = []

        @task
        def step_a():
            call_log.append("a")
            return 1

        @task
        def step_b(x):
            call_log.append("b")
            return x + 1

        @flow
        def my_flow():
            a = step_a()
            b = step_b(a)
            return [a, b]

        # Full successful run
        with _with_argv():
            my_flow().run()

        call_log.clear()
        get_registry().clear()

        # Resume — nothing to re-execute
        with _with_argv("--resume"):
            my_flow().run()

        assert "a" not in call_log
        assert "b" not in call_log

    def test_resume_reruns_downstream_of_failed(self, engine_with_db):
        """If a task failed, its downstream dependents also re-execute."""
        call_log = []
        fail_flag = [True]

        @task
        def step_a():
            call_log.append("a")
            return 1

        @task
        def step_b(x):
            call_log.append("b")
            if fail_flag[0]:
                raise RuntimeError("b failed")
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

        # First run: a passes, b fails, c is skipped
        with _with_argv():
            with pytest.raises(RuntimeError, match="b failed"):
                my_flow().run()

        call_log.clear()
        get_registry().clear()

        # Resume: a from cache, b and c re-execute
        fail_flag[0] = False
        with _with_argv("--resume"):
            result = my_flow().run()

        assert "a" not in call_log
        assert "b" in call_log
        assert "c" in call_log
        assert result[2].result == 3


class TestResumeWithFilter:
    """Test --resume combined with --filter."""

    def test_resume_and_filter_narrows_execution(self, engine_with_db):
        """--resume + --filter only re-runs tasks that failed AND match."""
        call_log = []
        fail_flag = [True]

        @task
        def alpha():
            call_log.append("alpha")
            if fail_flag[0]:
                raise RuntimeError("alpha failed")
            return 1

        @task
        def beta():
            call_log.append("beta")
            if fail_flag[0]:
                raise RuntimeError("beta failed")
            return 2

        @task
        def gamma():
            call_log.append("gamma")
            return 3

        @flow
        def my_flow():
            a = alpha().name("alpha")
            b = beta().name("beta")
            g = gamma().name("gamma")
            return [a, b, g]

        # First run: alpha and beta fail, gamma passes
        with _with_argv():
            with pytest.raises(RuntimeError):
                my_flow().run()

        call_log.clear()
        get_registry().clear()

        # Resume with filter: only re-run alpha (not beta)
        fail_flag[0] = False
        with _with_argv("--resume", "--filter", "alpha"):
            my_flow().run()

        assert "alpha" in call_log
        assert "beta" not in call_log
        assert "gamma" not in call_log


class TestResumeRequiresDatabase:
    """Test that resume mode requires a database."""

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
            with _with_argv("--resume"):
                fr = my_flow()
                with pytest.raises(ValueError, match="requires a database"):
                    fr.run()
        finally:
            engine.shutdown()


class TestFlowRunMetadata:
    """Test that flow runs store task name metadata."""

    def test_flow_run_stores_task_names(self, engine_with_db, db):
        @task
        def source():
            return 10

        @task
        def double(x):
            return x * 2

        @flow
        def my_flow():
            s = source().name("src")
            d = double(s).name("dbl")
            return [s, d]

        with _with_argv():
            my_flow().run()

        runs = db.fetch_flow_runs("my_flow", limit=1)
        assert len(runs) == 1
        assert "task_names" in runs[0]["metadata"]
        task_names = runs[0]["metadata"]["task_names"]
        assert "src" in task_names
        assert "dbl" in task_names
