"""Integration tests for checkpointing, Slurm executor, and batch execution."""

import sys
import time
from pathlib import Path
from unittest import mock

import pytest

from rinnsal.core.task import task
from rinnsal.core.flow import flow, FlowResult
from rinnsal.core.registry import get_registry
from rinnsal.context import Checkpoint, Card, current
from rinnsal.persistence.database import InMemoryDatabase
from rinnsal.persistence.file_store import FileDatabase
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


@pytest.fixture
def file_db(tmp_path):
    return FileDatabase(root=str(tmp_path / ".rinnsal"))


@pytest.fixture
def engine_with_file_db(file_db):
    executor = InlineExecutor()
    engine = ExecutionEngine(executor=executor, database=file_db)
    set_engine(engine)
    yield engine
    engine.shutdown()


def _with_argv(*args):
    return mock.patch.object(sys, "argv", ["test_script", *args])


# ── Checkpointing ────────────────────────────────────────────────────


class TestCheckpoint:
    def test_checkpoint_save_load(self, tmp_path):
        path = tmp_path / "checkpoint.dat"
        cp = Checkpoint(path=path)

        assert cp.load() is None

        cp.save({"epoch": 5, "loss": 0.1})
        assert path.exists()

        loaded = cp.load()
        assert loaded["epoch"] == 5
        assert loaded["loss"] == 0.1

    def test_checkpoint_clear(self, tmp_path):
        path = tmp_path / "checkpoint.dat"
        cp = Checkpoint(path=path)
        cp.save({"data": 42})
        assert path.exists()

        cp.clear()
        assert not path.exists()
        assert cp.load() is None

    def test_checkpoint_no_path(self):
        cp = Checkpoint(path=None)
        cp.save({"data": 42})  # no-op
        assert cp.load() is None

    def test_checkpoint_atomic_write(self, tmp_path):
        """Checkpoint write uses atomic rename (no .tmp left behind)."""
        path = tmp_path / "checkpoint.dat"
        cp = Checkpoint(path=path)
        cp.save({"data": 42})

        assert not (tmp_path / "checkpoint.tmp").exists()
        assert path.exists()

    def test_current_checkpoint_access(self):
        cp = Checkpoint(path=None)
        current._set_checkpoint(cp)
        assert current.checkpoint is cp
        current._reset()

    def test_checkpoint_in_task_with_file_db(self, engine_with_file_db):
        """Checkpoint is accessible inside a task when using FileDatabase."""
        checkpoint_data = []

        @task
        def my_task():
            state = current.checkpoint.load()
            checkpoint_data.append(state)
            current.checkpoint.save({"step": 1})
            return 42

        result = rinnsal_eval(my_task())
        assert result == 42
        assert checkpoint_data[0] is None  # First run, no checkpoint

    def test_checkpoint_resume_with_retry(self, engine_with_file_db):
        """On retry, checkpoint from previous attempt is available."""
        attempts = []

        @task(retry=1)
        def resumable():
            state = current.checkpoint.load()
            attempts.append(state)

            if state is None:
                current.checkpoint.save({"attempt": 1})
                raise RuntimeError("First attempt fails")

            return state["attempt"]

        result = rinnsal_eval(resumable())
        assert result == 1
        assert attempts[0] is None  # First attempt
        assert attempts[1] == {"attempt": 1}  # Second attempt sees checkpoint


# ── Batch Execution ──────────────────────────────────────────────────


class TestBatchExecution:
    def test_independent_tasks_all_run(self, engine_with_db):
        """Independent tasks at the same level all execute."""
        call_log = []

        @task
        def job(label):
            call_log.append(label)
            return label

        @flow
        def my_flow():
            a = job("a")
            b = job("b")
            c = job("c")
            return [a, b, c]

        with _with_argv():
            result = my_flow().run()

        assert sorted(call_log) == ["a", "b", "c"]
        assert result[0].result == "a"
        assert result[1].result == "b"
        assert result[2].result == "c"

    def test_dependency_order_preserved(self, engine_with_db):
        """Tasks with dependencies run after their deps."""
        call_log = []

        @task
        def source():
            call_log.append("source")
            return 10

        @task
        def double(x):
            call_log.append("double")
            return x * 2

        @task
        def triple(x):
            call_log.append("triple")
            return x * 3

        @flow
        def my_flow():
            s = source()
            d = double(s)
            t = triple(s)
            return [s, d, t]

        with _with_argv():
            my_flow().run()

        # Source must run before double and triple
        assert call_log.index("source") < call_log.index("double")
        assert call_log.index("source") < call_log.index("triple")

    def test_diamond_dependency(self, engine_with_db):
        """Diamond dependency pattern works correctly."""
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
            return m

        with _with_argv():
            result = my_flow().run()

        assert result.result == 23  # (10+1) + (10+2)
        assert call_log.index("source") < call_log.index("left")
        assert call_log.index("source") < call_log.index("right")
        assert call_log.index("left") < call_log.index("merge")
        assert call_log.index("right") < call_log.index("merge")

    def test_failure_skips_dependents(self, engine_with_db):
        """When a task fails, its dependents are skipped."""
        call_log = []

        @task
        def source():
            call_log.append("source")
            raise RuntimeError("fail")

        @task
        def consumer(x):
            call_log.append("consumer")
            return x

        @flow
        def my_flow():
            s = source()
            c = consumer(s)
            return [s, c]

        with _with_argv():
            with pytest.raises(RuntimeError, match="fail"):
                my_flow().run()

        assert "source" in call_log
        assert "consumer" not in call_log


# ── Slurm Executor ───────────────────────────────────────────────────


class TestSlurmExecutor:
    def test_sbatch_script_generation(self, tmp_path):
        from rinnsal.execution.slurm import _make_sbatch_script

        script = _make_sbatch_script(
            task_name="train",
            partition="gpu",
            account="myaccount",
            time_min=120,
            mem_mb=32000,
            gpus=2,
            cpus=4,
            stdout_path="/tmp/out.log",
            stderr_path="/tmp/err.log",
            setup_commands=["module load cuda"],
            pythonpath_setup='export PYTHONPATH="/snap/src"',
            checkpoint_setup="",
            python_bin="python3",
            worker_py="/tmp/worker.py",
        )

        assert "#SBATCH --job-name=train" in script
        assert "#SBATCH --partition=gpu" in script
        assert "#SBATCH --account=myaccount" in script
        assert "#SBATCH --time=120" in script
        assert "#SBATCH --mem=32000M" in script
        assert "#SBATCH --gres=gpu:2" in script
        assert "#SBATCH --cpus-per-task=4" in script
        assert "module load cuda" in script
        assert 'export PYTHONPATH="/snap/src"' in script
        assert "python3 /tmp/worker.py" in script

    def test_sbatch_script_no_gpu(self):
        from rinnsal.execution.slurm import _make_sbatch_script

        script = _make_sbatch_script(
            task_name="preprocess",
            partition=None,
            account=None,
            time_min=30,
            mem_mb=None,
            gpus=0,
            cpus=1,
            stdout_path="/tmp/out.log",
            stderr_path="/tmp/err.log",
            setup_commands=[],
            pythonpath_setup="",
            checkpoint_setup="",
            python_bin="python3",
            worker_py="/tmp/worker.py",
        )

        assert "#SBATCH --gres" not in script
        assert "#SBATCH --partition" not in script
        assert "#SBATCH --mem" not in script

    def test_worker_script_generation(self):
        from rinnsal.execution.slurm import _make_worker_script

        script = _make_worker_script(
            submission_pkl="/tmp/sub.pkl",
            result_pkl="/tmp/res.pkl",
        )

        assert "cloudpickle.load" in script
        assert "cloudpickle.dump" in script
        assert '("success"' in script
        assert '("error"' in script

    def test_worker_script_with_checkpoint(self):
        from rinnsal.execution.slurm import _make_worker_script

        script = _make_worker_script(
            submission_pkl="/tmp/sub.pkl",
            result_pkl="/tmp/res.pkl",
            checkpoint_path="/tmp/checkpoint.dat",
        )

        assert "Checkpoint" in script
        assert "/tmp/checkpoint.dat" in script

    def test_slurm_executor_init(self):
        from rinnsal.execution.slurm import SlurmExecutor

        executor = SlurmExecutor(
            partition="gpu",
            gpus_per_node=1,
            mem_gb=32,
            time_min=60,
        )
        assert executor._partition == "gpu"
        assert executor._gpus_per_node == 1
        assert repr(executor) == "SlurmExecutor(partition='gpu', gpus=1)"

    def test_slurm_executor_in_cli_choices(self):
        """Slurm is a valid executor choice."""
        from rinnsal.cli.flags import add_builtin_flags
        import argparse

        parser = argparse.ArgumentParser()
        add_builtin_flags(parser)
        ns = parser.parse_args(["--executor", "slurm"])
        assert ns.executor == "slurm"
