"""Tests for the expression system."""

import pytest

from rinnsal.core.expression import (
    Expression,
    TaskExpression,
    ValueExpression,
    unwrap_value,
    wrap_value,
)
from rinnsal.core.task import TaskDef


class TestValueExpression:
    """Tests for ValueExpression."""

    def test_value_access(self):
        expr = ValueExpression(42)
        assert expr.value == 42

    def test_hash_is_deterministic(self):
        expr1 = ValueExpression(42)
        expr2 = ValueExpression(42)
        assert expr1.hash == expr2.hash

    def test_different_values_different_hash(self):
        expr1 = ValueExpression(42)
        expr2 = ValueExpression(43)
        assert expr1.hash != expr2.hash

    def test_equality(self):
        expr1 = ValueExpression(42)
        expr2 = ValueExpression(42)
        expr3 = ValueExpression(43)

        assert expr1 == expr2
        assert expr1 != expr3

    def test_no_dependencies(self):
        expr = ValueExpression(42)
        assert expr.get_dependencies() == []


class TestTaskExpression:
    """Tests for TaskExpression."""

    def test_creation(self):
        def my_func(x):
            return x * 2

        task_def = TaskDef(my_func)
        expr = TaskExpression(task_def, (10,), {})

        assert expr.func is my_func
        assert expr.args == (10,)
        assert expr.kwargs == {}

    def test_task_name_default(self):
        def my_func():
            pass

        task_def = TaskDef(my_func)
        expr = TaskExpression(task_def, (), {})

        assert expr.task_name == "my_func"

    def test_task_name_custom(self):
        def my_func():
            pass

        task_def = TaskDef(my_func)
        expr = TaskExpression(task_def, (), {})
        expr.name("custom_name")

        assert expr.task_name == "custom_name"

    def test_name_chaining(self):
        def my_func():
            pass

        task_def = TaskDef(my_func)
        expr = TaskExpression(task_def, (), {})

        result = expr.name("test")
        assert result is expr  # Returns self for chaining

    def test_hash_deterministic(self):
        def my_func(x):
            return x * 2

        task_def = TaskDef(my_func)
        expr1 = TaskExpression(task_def, (10,), {})
        expr2 = TaskExpression(task_def, (10,), {})

        assert expr1.hash == expr2.hash

    def test_different_args_different_hash(self):
        def my_func(x):
            return x * 2

        task_def = TaskDef(my_func)
        expr1 = TaskExpression(task_def, (10,), {})
        expr2 = TaskExpression(task_def, (20,), {})

        assert expr1.hash != expr2.hash

    def test_not_evaluated_initially(self):
        def my_func():
            return 42

        task_def = TaskDef(my_func)
        expr = TaskExpression(task_def, (), {})

        assert not expr.is_evaluated

    def test_result_before_evaluation_raises(self):
        def my_func():
            return 42

        task_def = TaskDef(my_func)
        expr = TaskExpression(task_def, (), {})

        with pytest.raises(RuntimeError, match="not been evaluated"):
            _ = expr.result

    def test_set_result(self):
        def my_func():
            return 42

        task_def = TaskDef(my_func)
        expr = TaskExpression(task_def, (), {})

        expr.set_result(42)

        assert expr.is_evaluated
        assert expr.result == 42

    def test_dependencies(self):
        def source():
            return 10

        def double(x):
            return x * 2

        source_def = TaskDef(source)
        double_def = TaskDef(double)

        source_expr = TaskExpression(source_def, (), {})
        double_expr = TaskExpression(double_def, (source_expr,), {})

        deps = double_expr.get_dependencies()
        assert len(deps) == 1
        assert deps[0] is source_expr

    def test_dependencies_with_kwargs(self):
        def my_func(x, y=None):
            return x + (y or 0)

        task_def = TaskDef(my_func)
        dep1 = TaskExpression(TaskDef(lambda: 1), (), {})
        dep2 = TaskExpression(TaskDef(lambda: 2), (), {})

        expr = TaskExpression(task_def, (dep1,), {"y": dep2})

        deps = expr.get_dependencies()
        assert len(deps) == 2
        assert dep1 in deps
        assert dep2 in deps


class TestWrapUnwrap:
    """Tests for wrap_value and unwrap_value."""

    def test_wrap_primitive(self):
        wrapped = wrap_value(42)
        assert isinstance(wrapped, ValueExpression)
        assert wrapped.value == 42

    def test_wrap_expression_unchanged(self):
        expr = ValueExpression(42)
        wrapped = wrap_value(expr)
        assert wrapped is expr

    def test_wrap_task_expression_unchanged(self):
        def my_func():
            pass

        task_def = TaskDef(my_func)
        expr = TaskExpression(task_def, (), {})

        wrapped = wrap_value(expr)
        assert wrapped is expr

    def test_unwrap_value_expression(self):
        expr = ValueExpression(42)
        assert unwrap_value(expr) == 42

    def test_unwrap_task_expression(self):
        def my_func():
            return 42

        task_def = TaskDef(my_func)
        expr = TaskExpression(task_def, (), {})
        expr.set_result(42)

        assert unwrap_value(expr) == 42

    def test_unwrap_primitive(self):
        assert unwrap_value(42) == 42
        assert unwrap_value("hello") == "hello"
