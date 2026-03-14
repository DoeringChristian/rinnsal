"""Task scheduling for DAG execution."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from rinnsal.core.expression import TaskExpression
    from rinnsal.core.graph import DAG


@dataclass
class Worker:
    """Represents an execution worker."""

    id: str
    executor_name: str
    capabilities: dict[str, Any] = field(default_factory=dict)
    current_load: int = 0
    max_load: int = 1

    @property
    def available(self) -> bool:
        return self.current_load < self.max_load


@dataclass
class TaskAssignment:
    """Assignment of a task to a worker."""

    task: TaskExpression
    worker: Worker
    priority: int = 0


class Scheduler(ABC):
    """Abstract base class for task schedulers.

    The scheduler decides which worker runs which task and when.
    """

    @abstractmethod
    def schedule(
        self,
        dag: DAG,
        workers: list[Worker],
        completed: set[str],
    ) -> list[TaskAssignment]:
        """Schedule ready tasks to workers.

        Args:
            dag: The task DAG
            workers: Available workers
            completed: Set of completed task hashes

        Returns:
            List of task assignments
        """
        ...


class FIFOScheduler(Scheduler):
    """Simple FIFO scheduler.

    Assigns ready tasks to available workers in topological order.
    """

    def schedule(
        self,
        dag: DAG,
        workers: list[Worker],
        completed: set[str],
    ) -> list[TaskAssignment]:
        ready_tasks = dag.get_ready_tasks(completed)
        available_workers = [w for w in workers if w.available]

        assignments: list[TaskAssignment] = []

        for task, worker in zip(ready_tasks, available_workers):
            assignments.append(TaskAssignment(task=task, worker=worker))

        return assignments


class LocalityAwareScheduler(Scheduler):
    """Scheduler that minimizes data transfer.

    Places dependent tasks on the same worker when possible to
    reduce network traffic for large intermediate results.
    """

    def __init__(self) -> None:
        # Track which worker executed each task
        self._task_worker_map: dict[str, str] = {}

    def schedule(
        self,
        dag: DAG,
        workers: list[Worker],
        completed: set[str],
    ) -> list[TaskAssignment]:
        ready_tasks = dag.get_ready_tasks(completed)
        available_workers = [w for w in workers if w.available]

        assignments: list[TaskAssignment] = []

        for task in ready_tasks:
            # Find the best worker based on locality
            best_worker = self._find_best_worker(task, dag, available_workers)

            if best_worker:
                assignments.append(TaskAssignment(task=task, worker=best_worker))
                available_workers.remove(best_worker)

        return assignments

    def _find_best_worker(
        self,
        task: TaskExpression,
        dag: DAG,
        available_workers: list[Worker],
    ) -> Worker | None:
        if not available_workers:
            return None

        # Get dependencies
        deps = dag.get_dependencies(task.hash)

        # Find workers that executed dependencies
        dep_workers: dict[str, int] = {}
        for dep_hash in deps:
            if dep_hash in self._task_worker_map:
                worker_id = self._task_worker_map[dep_hash]
                dep_workers[worker_id] = dep_workers.get(worker_id, 0) + 1

        # Prefer worker with most dependencies
        if dep_workers:
            sorted_workers = sorted(dep_workers.items(), key=lambda x: -x[1])
            for worker_id, _ in sorted_workers:
                for w in available_workers:
                    if w.id == worker_id:
                        return w

        # Fall back to first available
        return available_workers[0]

    def record_execution(self, task_hash: str, worker_id: str) -> None:
        """Record which worker executed a task."""
        self._task_worker_map[task_hash] = worker_id


class ResourceMatchingScheduler(Scheduler):
    """Scheduler that matches tasks to workers by resource requirements.

    Tasks can declare resource requirements (GPU, memory, etc.) and
    the scheduler matches them to capable workers.
    """

    def schedule(
        self,
        dag: DAG,
        workers: list[Worker],
        completed: set[str],
    ) -> list[TaskAssignment]:
        ready_tasks = dag.get_ready_tasks(completed)
        available_workers = [w for w in workers if w.available]

        assignments: list[TaskAssignment] = []

        for task in ready_tasks:
            # Get task resource requirements (from metadata)
            requirements = self._get_requirements(task)

            # Find matching worker
            worker = self._find_matching_worker(requirements, available_workers)

            if worker:
                assignments.append(TaskAssignment(task=task, worker=worker))
                available_workers.remove(worker)

        return assignments

    def _get_requirements(self, task: TaskExpression) -> dict[str, Any]:
        """Get resource requirements for a task."""
        # Could be extended to read from task decorator or metadata
        return {}

    def _find_matching_worker(
        self,
        requirements: dict[str, Any],
        available_workers: list[Worker],
    ) -> Worker | None:
        if not available_workers:
            return None

        for worker in available_workers:
            if self._worker_matches(worker, requirements):
                return worker

        return None

    def _worker_matches(
        self,
        worker: Worker,
        requirements: dict[str, Any],
    ) -> bool:
        """Check if a worker meets the requirements."""
        for key, value in requirements.items():
            if key not in worker.capabilities:
                return False
            if worker.capabilities[key] < value:
                return False
        return True


class LoadBalancingScheduler(Scheduler):
    """Scheduler that balances load across workers.

    Distributes work evenly, avoiding situations where one machine
    is saturated while others are idle.
    """

    def schedule(
        self,
        dag: DAG,
        workers: list[Worker],
        completed: set[str],
    ) -> list[TaskAssignment]:
        ready_tasks = dag.get_ready_tasks(completed)

        # Sort workers by current load (least loaded first)
        sorted_workers = sorted(workers, key=lambda w: w.current_load)

        assignments: list[TaskAssignment] = []

        for task in ready_tasks:
            # Find least loaded available worker
            for worker in sorted_workers:
                if worker.available:
                    assignments.append(TaskAssignment(task=task, worker=worker))
                    worker.current_load += 1
                    break

        return assignments
