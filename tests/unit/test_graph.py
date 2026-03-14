"""Tests for the DAG class."""

import pytest

from rinnsal.core.graph import DAG
from rinnsal.core.task import task


class TestDAG:
    """Tests for the DAG class."""

    def test_add_node(self):
        @task
        def my_func():
            return 42

        dag = DAG()
        expr = my_func()
        dag.add_node(expr)

        assert len(dag) == 1
        assert expr.hash in dag

    def test_add_edge(self):
        @task
        def source():
            return 10

        @task
        def double(x):
            return x * 2

        dag = DAG()
        src = source()
        dbl = double(src)

        dag.add_node(src)
        dag.add_node(dbl)
        dag.add_edge(dbl.hash, src.hash)

        assert src.hash in dag.get_dependencies(dbl.hash)
        assert dbl.hash in dag.get_dependents(src.hash)

    def test_topological_sort_simple(self):
        @task
        def source():
            return 10

        @task
        def double(x):
            return x * 2

        src = source()
        dbl = double(src)

        dag = DAG.from_expressions([dbl])
        ordered = dag.topological_sort()

        # Source should come before double
        src_idx = next(i for i, e in enumerate(ordered) if e.hash == src.hash)
        dbl_idx = next(i for i, e in enumerate(ordered) if e.hash == dbl.hash)
        assert src_idx < dbl_idx

    def test_topological_sort_diamond(self):
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
        l = left(src)
        r = right(src)
        m = merge(l, r)

        dag = DAG.from_expressions([m])
        ordered = dag.topological_sort()

        # Check order: source before left/right, left/right before merge
        src_idx = next(i for i, e in enumerate(ordered) if e.hash == src.hash)
        l_idx = next(i for i, e in enumerate(ordered) if e.hash == l.hash)
        r_idx = next(i for i, e in enumerate(ordered) if e.hash == r.hash)
        m_idx = next(i for i, e in enumerate(ordered) if e.hash == m.hash)

        assert src_idx < l_idx
        assert src_idx < r_idx
        assert l_idx < m_idx
        assert r_idx < m_idx

    def test_get_ready_tasks_initial(self):
        @task
        def source():
            return 10

        @task
        def double(x):
            return x * 2

        src = source()
        dbl = double(src)

        dag = DAG.from_expressions([dbl])
        ready = dag.get_ready_tasks(completed=set())

        # Only source should be ready initially
        assert len(ready) == 1
        assert ready[0].hash == src.hash

    def test_get_ready_tasks_after_completion(self):
        @task
        def source():
            return 10

        @task
        def double(x):
            return x * 2

        src = source()
        dbl = double(src)

        dag = DAG.from_expressions([dbl])

        # After completing source, double should be ready
        ready = dag.get_ready_tasks(completed={src.hash})

        assert len(ready) == 1
        assert ready[0].hash == dbl.hash

    def test_from_expressions_builds_deps(self):
        @task
        def source():
            return 10

        @task
        def double(x):
            return x * 2

        src = source()
        dbl = double(src)

        dag = DAG.from_expressions([dbl])

        # Should include both nodes
        assert len(dag) == 2
        assert src.hash in dag
        assert dbl.hash in dag

    def test_deduplication_in_diamond(self):
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
        l = left(src)
        r = right(src)
        m = merge(l, r)

        dag = DAG.from_expressions([m])

        # Source should appear only once
        assert len(dag) == 4
