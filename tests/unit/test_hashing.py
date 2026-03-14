"""Tests for the hashing module."""

import pytest

from rinnsal.core.hashing import (
    ContentHasher,
    compute_task_hash,
    hash_args,
    hash_function,
    hash_value,
)


class TestHashFunction:
    """Tests for function hashing."""

    def test_same_function_same_hash(self):
        def my_func():
            return 42

        h1 = hash_function(my_func)
        h2 = hash_function(my_func)
        assert h1 == h2

    def test_different_functions_different_hash(self):
        def func1():
            return 1

        def func2():
            return 2

        assert hash_function(func1) != hash_function(func2)

    def test_lambda_functions(self):
        f1 = lambda x: x + 1
        f2 = lambda x: x + 2

        # Different lambdas should have different hashes
        assert hash_function(f1) != hash_function(f2)

    def test_hash_is_deterministic(self):
        def my_func(x):
            return x * 2

        hashes = [hash_function(my_func) for _ in range(5)]
        assert all(h == hashes[0] for h in hashes)


class TestHashValue:
    """Tests for value hashing."""

    def test_hash_primitives(self):
        assert hash_value(42) == hash_value(42)
        assert hash_value(42) != hash_value(43)

        assert hash_value("hello") == hash_value("hello")
        assert hash_value("hello") != hash_value("world")

        assert hash_value(3.14) == hash_value(3.14)
        assert hash_value(True) == hash_value(True)
        assert hash_value(None) == hash_value(None)

    def test_hash_tuple(self):
        assert hash_value((1, 2, 3)) == hash_value((1, 2, 3))
        assert hash_value((1, 2, 3)) != hash_value((1, 2, 4))

    def test_hash_list(self):
        assert hash_value([1, 2, 3]) == hash_value([1, 2, 3])
        assert hash_value([1, 2, 3]) != hash_value([1, 2, 4])

    def test_hash_dict(self):
        assert hash_value({"a": 1}) == hash_value({"a": 1})
        assert hash_value({"a": 1}) != hash_value({"a": 2})

    def test_hash_complex_object(self):
        class MyClass:
            def __init__(self, value):
                self.value = value

        obj1 = MyClass(42)
        obj2 = MyClass(42)
        # Different instances may have same hash if cloudpickle serializes them identically
        h1 = hash_value(obj1)
        h2 = hash_value(obj2)
        assert isinstance(h1, str) and len(h1) == 64  # SHA-256 hex


class TestHashArgs:
    """Tests for argument hashing."""

    def test_same_args_same_hash(self):
        args1 = (1, 2, 3)
        kwargs1 = {"a": 4}

        args2 = (1, 2, 3)
        kwargs2 = {"a": 4}

        assert hash_args(args1, kwargs1) == hash_args(args2, kwargs2)

    def test_different_args_different_hash(self):
        assert hash_args((1,), {}) != hash_args((2,), {})
        assert hash_args((), {"a": 1}) != hash_args((), {"a": 2})
        assert hash_args((), {"a": 1}) != hash_args((), {"b": 1})

    def test_kwarg_order_independent(self):
        # Same kwargs in different order should have same hash
        h1 = hash_args((), {"a": 1, "b": 2})
        h2 = hash_args((), {"b": 2, "a": 1})
        assert h1 == h2


class TestComputeTaskHash:
    """Tests for complete task hash computation."""

    def test_same_call_same_hash(self):
        def my_func(x):
            return x * 2

        h1 = compute_task_hash(my_func, (10,), {})
        h2 = compute_task_hash(my_func, (10,), {})
        assert h1 == h2

    def test_different_args_different_hash(self):
        def my_func(x):
            return x * 2

        h1 = compute_task_hash(my_func, (10,), {})
        h2 = compute_task_hash(my_func, (20,), {})
        assert h1 != h2

    def test_different_funcs_different_hash(self):
        def func1(x):
            return x * 2

        def func2(x):
            return x * 3

        h1 = compute_task_hash(func1, (10,), {})
        h2 = compute_task_hash(func2, (10,), {})
        assert h1 != h2


class TestContentHasher:
    """Tests for the ContentHasher class."""

    def test_function_caching(self):
        hasher = ContentHasher()

        def my_func():
            return 42

        h1 = hasher.hash_function(my_func)
        h2 = hasher.hash_function(my_func)
        assert h1 == h2

    def test_hash_task(self):
        hasher = ContentHasher()

        def my_func(x):
            return x * 2

        h1 = hasher.hash_task(my_func, (10,), {})
        h2 = hasher.hash_task(my_func, (10,), {})
        assert h1 == h2

    def test_clear_cache(self):
        hasher = ContentHasher()

        def my_func():
            return 42

        hasher.hash_function(my_func)
        assert len(hasher._func_cache) > 0

        hasher.clear_cache()
        assert len(hasher._func_cache) == 0
