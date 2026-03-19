"""Abstract Database protocol for result persistence."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Protocol, runtime_checkable

from rinnsal.core.types import Entry


@runtime_checkable
class Database(Protocol):
    """Protocol for result persistence.

    Defines the interface for storing and retrieving task results.
    Implementations can be file-based, database-backed, or remote.
    """

    def store_task_result(
        self,
        task_hash: str,
        entry: Entry,
        task_name: str = "",
    ) -> None:
        """Store a task execution result.

        Args:
            task_hash: The content hash of the task
            entry: The execution result entry
            task_name: Human-readable task name for directory naming
        """
        ...

    def fetch_task_result(
        self,
        task_hash: str,
        task_name: str = "",
    ) -> Entry | None:
        """Fetch the most recent result for a task.

        Args:
            task_hash: The content hash of the task
            task_name: Human-readable task name for directory naming

        Returns:
            The most recent Entry, or None if not found
        """
        ...

    def fetch_task_history(
        self,
        task_hash: str,
        task_name: str = "",
    ) -> list[Entry]:
        """Fetch all execution results for a task.

        Args:
            task_hash: The content hash of the task
            task_name: Human-readable task name for directory naming

        Returns:
            List of all Entry objects, newest first
        """
        ...

    def task_exists(
        self,
        task_hash: str,
        task_name: str = "",
    ) -> bool:
        """Check if a task result exists.

        Args:
            task_hash: The content hash of the task
            task_name: Human-readable task name for directory naming

        Returns:
            True if at least one result exists
        """
        ...

    def store_flow_run(
        self,
        flow_name: str,
        task_hashes: list[str],
        metadata: dict[str, Any] | None = None,
    ) -> str:
        """Store a flow run record.

        Args:
            flow_name: Name of the flow
            task_hashes: List of task hashes in execution order
            metadata: Optional flow-level metadata

        Returns:
            A unique run ID
        """
        ...

    def fetch_flow_runs(
        self,
        flow_name: str,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch flow run records.

        Args:
            flow_name: Name of the flow
            limit: Maximum number of runs to return

        Returns:
            List of run records, newest first
        """
        ...

    def clear(self) -> None:
        """Clear all stored data."""
        ...


class BaseDatabase(ABC):
    """Abstract base class implementing the Database protocol.

    Provides common functionality that can be shared across implementations.
    """

    @abstractmethod
    def store_task_result(
        self,
        task_hash: str,
        entry: Entry,
        task_name: str = "",
    ) -> None: ...

    @abstractmethod
    def fetch_task_result(
        self,
        task_hash: str,
        task_name: str = "",
    ) -> Entry | None: ...

    @abstractmethod
    def fetch_task_history(
        self,
        task_hash: str,
        task_name: str = "",
    ) -> list[Entry]: ...

    @abstractmethod
    def task_exists(
        self,
        task_hash: str,
        task_name: str = "",
    ) -> bool: ...

    @abstractmethod
    def store_flow_run(
        self,
        flow_name: str,
        task_hashes: list[str],
        metadata: dict[str, Any] | None = None,
    ) -> str: ...

    @abstractmethod
    def fetch_flow_runs(
        self,
        flow_name: str,
        limit: int | None = None,
    ) -> list[dict[str, Any]]: ...

    @abstractmethod
    def clear(self) -> None: ...


class InMemoryDatabase(BaseDatabase):
    """In-memory database for testing purposes.

    Does not persist data across process restarts.
    """

    def __init__(self) -> None:
        self._task_results: dict[str, list[Entry]] = {}
        self._flow_runs: dict[str, list[dict[str, Any]]] = {}
        self._run_counter = 0

    def _key(self, task_hash: str, task_name: str) -> str:
        return f"{task_name}-{task_hash}" if task_name else task_hash

    def store_task_result(
        self,
        task_hash: str,
        entry: Entry,
        task_name: str = "",
    ) -> None:
        key = self._key(task_hash, task_name)
        if key not in self._task_results:
            self._task_results[key] = []
        self._task_results[key].insert(0, entry)

    def fetch_task_result(
        self,
        task_hash: str,
        task_name: str = "",
    ) -> Entry | None:
        key = self._key(task_hash, task_name)
        results = self._task_results.get(key, [])
        return results[0] if results else None

    def fetch_task_history(
        self,
        task_hash: str,
        task_name: str = "",
    ) -> list[Entry]:
        key = self._key(task_hash, task_name)
        return list(self._task_results.get(key, []))

    def task_exists(
        self,
        task_hash: str,
        task_name: str = "",
    ) -> bool:
        key = self._key(task_hash, task_name)
        return (
            key in self._task_results
            and len(self._task_results[key]) > 0
        )

    def store_flow_run(
        self,
        flow_name: str,
        task_hashes: list[str],
        metadata: dict[str, Any] | None = None,
    ) -> str:
        self._run_counter += 1
        run_id = f"run_{self._run_counter}"

        if flow_name not in self._flow_runs:
            self._flow_runs[flow_name] = []

        run_record = {
            "run_id": run_id,
            "task_hashes": task_hashes,
            "timestamp": datetime.now().isoformat(),
            "metadata": metadata or {},
        }

        self._flow_runs[flow_name].insert(0, run_record)
        return run_id

    def fetch_flow_runs(
        self,
        flow_name: str,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        runs = self._flow_runs.get(flow_name, [])
        if limit is not None:
            return runs[:limit]
        return runs

    def clear(self) -> None:
        self._task_results.clear()
        self._flow_runs.clear()
        self._run_counter = 0
