"""DAG representation and manipulation."""

from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING, Iterator

if TYPE_CHECKING:
    from rinnsal.core.expression import TaskExpression


class DAG:
    """Directed Acyclic Graph for task dependencies.

    Provides methods for topological sorting, dependency tracking,
    and identifying ready-to-execute tasks.
    """

    def __init__(self) -> None:
        self._nodes: dict[str, TaskExpression] = {}
        self._edges: dict[str, set[str]] = defaultdict(
            set
        )  # node -> dependencies
        self._reverse_edges: dict[str, set[str]] = defaultdict(
            set
        )  # node -> dependents

    def add_node(self, expr: TaskExpression) -> None:
        """Add a task expression to the graph."""
        if expr.hash not in self._nodes:
            self._nodes[expr.hash] = expr

    def add_edge(self, from_hash: str, to_hash: str) -> None:
        """Add a dependency edge (from depends on to)."""
        self._edges[from_hash].add(to_hash)
        self._reverse_edges[to_hash].add(from_hash)

    def get_node(self, hash_key: str) -> TaskExpression | None:
        """Get a node by its hash."""
        return self._nodes.get(hash_key)

    def get_dependencies(self, hash_key: str) -> set[str]:
        """Get all direct dependencies of a node."""
        return self._edges.get(hash_key, set())

    def get_dependents(self, hash_key: str) -> set[str]:
        """Get all nodes that depend on this node."""
        return self._reverse_edges.get(hash_key, set())

    @property
    def nodes(self) -> list[TaskExpression]:
        """Get all nodes in the graph."""
        return list(self._nodes.values())

    def __len__(self) -> int:
        return len(self._nodes)

    def __contains__(self, hash_key: str) -> bool:
        return hash_key in self._nodes

    def topological_sort(self) -> list[TaskExpression]:
        """Return nodes in topological order (dependencies first).

        Uses Kahn's algorithm for topological sorting.
        Raises ValueError if the graph contains cycles.
        """
        # Calculate in-degrees
        in_degree: dict[str, int] = {h: 0 for h in self._nodes}
        for deps in self._edges.values():
            for dep in deps:
                if dep in in_degree:
                    pass  # Dependencies might not be in this subgraph

        for node_hash in self._nodes:
            for dep_hash in self._edges.get(node_hash, set()):
                if dep_hash in in_degree:
                    # This is wrong - in_degree should count incoming edges
                    pass

        # Recalculate properly: in_degree[x] = number of nodes that depend on x
        # Actually, for execution order, we need nodes with no dependencies first
        in_degree = {
            h: len(self._edges.get(h, set()) & set(self._nodes.keys()))
            for h in self._nodes
        }

        # Start with nodes that have no dependencies
        queue = [h for h, d in in_degree.items() if d == 0]
        result: list[TaskExpression] = []

        while queue:
            current = queue.pop(0)
            result.append(self._nodes[current])

            # Reduce in-degree for dependents
            for dependent in self._reverse_edges.get(current, set()):
                if dependent in in_degree:
                    in_degree[dependent] -= 1
                    if in_degree[dependent] == 0:
                        queue.append(dependent)

        if len(result) != len(self._nodes):
            raise ValueError("Graph contains a cycle")

        return result

    def get_ready_tasks(self, completed: set[str]) -> list[TaskExpression]:
        """Get tasks that are ready to execute.

        A task is ready if all its dependencies have been completed.
        """
        ready: list[TaskExpression] = []

        for hash_key, expr in self._nodes.items():
            if hash_key in completed:
                continue

            deps = self._edges.get(hash_key, set())
            # Only consider dependencies that are in this graph
            relevant_deps = deps & set(self._nodes.keys())

            if relevant_deps <= completed:
                ready.append(expr)

        return ready

    def has_cycle(self) -> bool:
        """Check if the graph contains a cycle."""
        try:
            self.topological_sort()
            return False
        except ValueError:
            return True

    @classmethod
    def from_expressions(cls, expressions: list[TaskExpression]) -> DAG:
        """Build a DAG from a list of task expressions.

        Traverses all dependencies and adds them to the graph.
        """
        dag = cls()

        # Add all expressions and collect dependencies
        to_process = list(expressions)
        seen: set[str] = set()

        while to_process:
            expr = to_process.pop()
            if expr.hash in seen:
                continue
            seen.add(expr.hash)

            dag.add_node(expr)

            for dep in expr.get_dependencies():
                from rinnsal.core.expression import TaskExpression

                if isinstance(dep, TaskExpression):
                    dag.add_node(dep)
                    dag.add_edge(expr.hash, dep.hash)
                    if dep.hash not in seen:
                        to_process.append(dep)

        return dag

    def __repr__(self) -> str:
        return f"DAG(nodes={len(self._nodes)}, edges={sum(len(e) for e in self._edges.values())})"
