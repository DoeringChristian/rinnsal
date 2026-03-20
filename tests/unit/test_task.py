"""Tests for the task decorator."""

import pytest

from rinnsal.core.task import TaskDef, task
from rinnsal.core.expression import TaskExpression


class TestTaskDecorator:
    """Tests for the @task decorator."""

    def test_decorator_without_parens(self):
        @task
        def my_func():
            return 42

        assert isinstance(my_func, TaskDef)
        assert my_func.func.__name__ == "my_func"

    def test_decorator_with_parens(self):
        @task()
        def my_func():
            return 42

        assert isinstance(my_func, TaskDef)

    def test_decorator_with_retry(self):
        @task(retry=3)
        def my_func():
            return 42

        assert my_func.retry == 3

    def test_calling_returns_expression(self):
        @task
        def my_func():
            return 42

        result = my_func()
        assert isinstance(result, TaskExpression)

    def test_calling_with_args(self):
        @task
        def add(x, y):
            return x + y

        expr = add(1, 2)
        assert isinstance(expr, TaskExpression)
        assert expr.args == (1, 2)

    def test_calling_with_kwargs(self):
        @task
        def add(x, y=0):
            return x + y

        expr = add(1, y=2)
        assert expr.args == (1,)
        assert expr.kwargs == {"y": 2}

    def test_deduplication(self):
        @task
        def my_func(x):
            return x * 2

        expr1 = my_func(10)
        expr2 = my_func(10)

        # Same call should return same expression object
        assert expr1 is expr2

    def test_different_args_different_expressions(self):
        @task
        def my_func(x):
            return x * 2

        expr1 = my_func(10)
        expr2 = my_func(20)

        assert expr1 is not expr2
        assert expr1.hash != expr2.hash


class TestTaskDef:
    """Tests for the TaskDef class."""

    def test_func_property(self):
        def my_func():
            return 42

        task_def = TaskDef(my_func)
        assert task_def.func is my_func

    def test_retry_default(self):
        def my_func():
            pass

        task_def = TaskDef(my_func)
        assert task_def.retry == 0

    def test_retry_custom(self):
        def my_func():
            pass

        task_def = TaskDef(my_func, retry=5)
        assert task_def.retry == 5

    def test_functools_wrapper(self):
        def my_func():
            """My docstring."""
            pass

        task_def = TaskDef(my_func)
        assert task_def.__name__ == "my_func"
        assert task_def.__doc__ == "My docstring."


