"""Protocol + dataclasses for the metadata store.

The metadata store indexes flows / runs / task graph for fast viewer
navigation. Heavy artifacts (figures, plotly JSON, cards, blobs) live
in ``events.pb`` and the content-addressed ``blobs/`` tree, NOT here.

Implementations live in sibling modules (``sqlite.py``); a future
key-value time-series store would be a separate Protocol with no
overlap with this one.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol, runtime_checkable


@dataclass(frozen=True, slots=True)
class FlowSummary:
    name: str
    run_count: int
    latest_run_at: float | None
    latest_run_id: str | None


@dataclass(frozen=True, slots=True)
class RunSummary:
    run_id: str
    flow_name: str
    run_dir: str
    status: str
    started_at: float
    finished_at: float | None
    snapshot_hash: str | None
    tags: list[str] = field(default_factory=list)
    task_count: int = 0
    failed_count: int = 0


@dataclass(frozen=True, slots=True)
class TaskNodeRow:
    run_id: str
    task_name: str
    task_hash: str
    status: str
    duration: float
    error: str
    ts: float
    params_json: str = ""


@dataclass(frozen=True, slots=True)
class TaskHistoryEntry:
    run_id: str
    run_dir: str
    task_hash: str
    status: str
    duration: float
    ts: float
    error: str
    params_json: str


@dataclass(frozen=True, slots=True)
class RunUpsert:
    run_id: str
    flow_name: str
    run_dir: str
    status: str
    started_at: float
    finished_at: float | None = None
    snapshot_hash: str | None = None
    tags: list[str] = field(default_factory=list)
    params_json: str = "{}"
    task_count: int = 0
    failed_count: int = 0


@runtime_checkable
class MetadataStore(Protocol):
    """Index of structured metadata for the viewer's navigation queries."""

    # ── writes ───────────────────────────────────────────────────────
    def upsert_flow(self, name: str) -> None: ...

    def upsert_run(self, run: RunUpsert) -> None: ...

    def update_run_status(
        self,
        run_id: str,
        status: str,
        finished_at: float | None = None,
        failed_count: int | None = None,
        snapshot_hash: str | None = None,
        task_count: int | None = None,
    ) -> None: ...

    def upsert_task_node(self, row: TaskNodeRow) -> None: ...

    def upsert_task_edge(
        self, run_id: str, from_task: str, to_task: str
    ) -> None: ...

    # ── reads ────────────────────────────────────────────────────────
    def list_flows(self) -> list[FlowSummary]: ...

    def list_runs(
        self,
        flow_name: str | None = None,
        limit: int | None = None,
    ) -> list[RunSummary]: ...

    def get_run(self, run_id: str) -> RunSummary | None: ...

    def list_task_nodes(self, run_id: str) -> list[TaskNodeRow]: ...

    def list_task_edges(self, run_id: str) -> list[tuple[str, str]]: ...

    def task_history(
        self, flow_name: str, task_name: str
    ) -> list[TaskHistoryEntry]: ...

    # ── housekeeping ────────────────────────────────────────────────
    def has_run(self, run_id: str) -> bool: ...

    def runs_missing_index(
        self, expected_run_dirs: list[Path]
    ) -> list[Path]: ...

    def latest_updated_at(
        self, flow_name: str | None = None
    ) -> float | None: ...
