"""Integration tests for task and flow execution."""

import pytest

from rinnsal.core.task import task
from rinnsal.core.flow import flow, FlowResult
from rinnsal.runtime.engine import eval as rinnsal_eval


class TestBasicExecution:
    """Tests for basic task execution."""

    def test_simple_task(self, engine):
        @task
        def source():
            return 42

        result = rinnsal_eval(source())
        assert result == 42

    def test_task_with_args(self, engine):
        @task
        def add(x, y):
            return x + y

        result = rinnsal_eval(add(10, 20))
        assert result == 30

    def test_chained_tasks(self, engine):
        @task
        def source():
            return 10

        @task
        def double(x):
            return x * 2

        result = rinnsal_eval(double(source()))
        assert result == 20

    def test_multiple_tasks(self, engine):
        @task
        def source():
            return 10

        @task
        def double(x):
            return x * 2

        a, b = rinnsal_eval(source(), double(source()))
        assert a == 10
        assert b == 20

    def test_diamond_dependency(self, engine):
        @task
        def source():
            return 10

        @task
        def left(x):
            return x + 1

        @task
        def right(x):
            return x + 2

        @task
        def merge(a, b):
            return a + b

        src = source()
        result = rinnsal_eval(merge(left(src), right(src)))
        assert result == 23  # (10 + 1) + (10 + 2)


class TestTaskDeduplication:
    """Tests for task deduplication."""

    def test_same_task_runs_once(self, engine):
        call_count = 0

        @task
        def counter():
            nonlocal call_count
            call_count += 1
            return call_count

        # Same call, should be deduplicated
        expr1 = counter()
        expr2 = counter()

        assert expr1 is expr2

        result = rinnsal_eval(expr1)
        assert result == 1
        assert call_count == 1

    def test_diamond_runs_shared_once(self, engine):
        call_count = 0

        @task
        def source():
            nonlocal call_count
            call_count += 1
            return 10

        @task
        def left(x):
            return x + 1

        @task
        def right(x):
            return x + 2

        @task
        def merge(a, b):
            return a + b

        src = source()
        result = rinnsal_eval(merge(left(src), right(src)))

        # Source should only run once
        assert call_count == 1
        assert result == 23


class TestTaskRetry:
    """Tests for task retry functionality."""

    def test_retry_on_failure(self, engine):
        attempts = 0

        @task(retry=2)
        def flaky():
            nonlocal attempts
            attempts += 1
            if attempts < 2:
                raise ValueError("Not yet!")
            return "success"

        result = rinnsal_eval(flaky())
        assert result == "success"
        assert attempts == 2

    def test_retry_exhausted(self, engine):
        @task(retry=2)
        def always_fails():
            raise ValueError("Always fails")

        with pytest.raises(ValueError, match="Always fails"):
            rinnsal_eval(always_fails())


class TestFlowExecution:
    """Tests for flow execution."""

    def test_simple_flow(self, engine):
        @task
        def source():
            return 10

        @task
        def double(x):
            return x * 2

        @flow
        def my_flow():
            src = source()
            d = double(src)
            return [src, d]

        result = my_flow()
        assert isinstance(result, FlowResult)
        assert len(result) == 2

        outputs = result.run()
        assert outputs[0].result == 10
        assert outputs[1].result == 20

    def test_flow_with_params(self, engine):
        @task
        def multiply(x, factor):
            return x * factor

        @flow
        def my_flow(factor=2):
            return multiply(10, factor)

        result = my_flow(factor=3).run()
        assert result.result == 30

    def test_flow_result_indexing(self, engine):
        @task
        def source():
            return 10

        @task
        def double(x):
            return x * 2

        @flow
        def my_flow():
            src = source()
            d = double(src)
            return [src, d]

        fr = my_flow()
        fr.run()

        # Integer indexing
        assert fr[0].result == 10
        assert fr[-1].result == 20

    def test_flow_result_string_indexing(self, engine):
        @task
        def source():
            return 10

        @task
        def double(x):
            return x * 2

        @flow
        def my_flow():
            src = source()
            d = double(src)
            return [src, d]

        fr = my_flow()
        fr.run()

        # String indexing (function name)
        src = fr["source"]
        assert src.result == 10

        dbl = fr["double"]
        assert dbl.result == 20

    def test_flow_result_named_task(self, engine):
        @task
        def process(x):
            return x * 2

        @flow
        def my_flow():
            a = process(10).name("step1")
            b = process(20).name("step2")
            return [a, b]

        fr = my_flow()
        fr.run()

        # Access by name
        step1 = fr["step1"]
        assert step1.result == 20

        step2 = fr["step2"]
        assert step2.result == 40

    def test_flow_dict_return(self, engine):
        @task
        def source():
            return 10

        @task
        def double(x):
            return x * 2

        @flow
        def my_flow():
            src = source()
            d = double(src)
            return {"source": src, "doubled": d}

        outputs = my_flow().run()
        assert outputs["source"].result == 10
        assert outputs["doubled"].result == 20

    def test_flow_single_return(self, engine):
        @task
        def source():
            return 42

        @flow
        def my_flow():
            return source()

        output = my_flow().run()
        assert output.result == 42


class TestTaskEval:
    """Tests for task.eval() method."""

    def test_eval_method(self, engine):
        @task
        def source():
            return 42

        expr = source()
        result = expr.eval()
        assert result == 42

    def test_eval_method_chained(self, engine):
        @task
        def source():
            return 10

        @task
        def double(x):
            return x * 2

        result = double(source()).eval()
        assert result == 20
