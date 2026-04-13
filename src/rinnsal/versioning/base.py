"""RunStore protocol — owns run identity and run metadata."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class RunStore(Protocol):
    """Owns run IDs, run directories, and run-level metadata.

    Storage of the bytes themselves lives in the Data layer
    (e.g. ``FileDatabase``). The RunStore decides *what* counts as a run
    and *where* its directory lives; Data decides *how* the bytes land.
    """

    def new_run_id(self, flow_name: str) -> str: ...

    def run_dir(self, flow_name: str, run_id: str) -> Path: ...

    def record_run(
        self,
        flow_name: str,
        task_hashes: list[str],
        metadata: dict[str, Any],
    ) -> None: ...

    def list_runs(
        self, flow_name: str, limit: int = 10
    ) -> list[dict[str, Any]]: ...

    def latest_run(self, flow_name: str) -> dict[str, Any] | None: ...
