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
        self._insertion_order: dict[str, int] = {}
        self._edges: dict[str, set[str]] = defaultdict(
            set
        )  # node -> dependencies
        self._reverse_edges: dict[str, set[str]] = defaultdict(
            set
        )  # node -> dependents

    def add_node(self, expr: TaskExpression) -> None:
        """Add a task expression to the graph."""
        if expr.hash not in self._nodes:
            self._insertion_order[expr.hash] = len(self._nodes)
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
        Among nodes at the same topological level, insertion order is preserved.
        Raises ValueError if the graph contains cycles.
        """
        in_degree = {
            h: len(self._edges.get(h, set()) & set(self._nodes.keys()))
            for h in self._nodes
        }

        # Start with nodes that have no dependencies, in insertion order
        queue = sorted(
            [h for h, d in in_degree.items() if d == 0],
            key=lambda h: self._insertion_order[h],
        )
        result: list[TaskExpression] = []

        while queue:
            current = queue.pop(0)
            result.append(self._nodes[current])

            # Collect newly ready dependents, then sort by insertion order
            newly_ready = []
            for dependent in self._reverse_edges.get(current, set()):
                if dependent in in_degree:
                    in_degree[dependent] -= 1
                    if in_degree[dependent] == 0:
                        newly_ready.append(dependent)
            newly_ready.sort(key=lambda h: self._insertion_order[h])
            queue.extend(newly_ready)

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
        The original list order is used as the preferred execution order
        among tasks at the same topological level.
        """
        dag = cls()

        # Record the original expression order for stable tiebreaking.
        # Dependencies not in the original list get order based on
        # when they are first discovered (after all explicit ones).
        original_order: dict[str, int] = {}
        for i, expr in enumerate(expressions):
            if expr.hash not in original_order:
                original_order[expr.hash] = i

        # Add all expressions and collect dependencies
        to_process = list(expressions)
        seen: set[str] = set()
        next_order = len(expressions)

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
                        if dep.hash not in original_order:
                            original_order[dep.hash] = next_order
                            next_order += 1
                        to_process.append(dep)

        # Set insertion order to match the original expression order
        dag._insertion_order = original_order
        return dag

    def __repr__(self) -> str:
        return f"DAG(nodes={len(self._nodes)}, edges={sum(len(e) for e in self._edges.values())})"
