"""Orchestrator protocol — walks a DAG, dispatching tasks to the engine."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from rinnsal.compute.engine import ExecutionEngine
    from rinnsal.data.database import Database
    from rinnsal.data.logger import Logger
    from rinnsal.modeling.expression import TaskExpression
    from rinnsal.modeling.graph import DAG


@dataclass
class RunPlan:
    """Everything an orchestrator needs to walk one flow run."""

    flow_name: str
    dag: DAG
    ordered: list[TaskExpression]
    matched_hashes: set[str]      # tasks to execute fresh
    ordered_hashes: set[str]
    engine: ExecutionEngine
    database: Database | None
    logger: Logger
    progress: Any                  # ProgressBar or SilentProgress
    resume: bool


@dataclass
class RunOutcome:
    n_passed: int = 0
    n_cached: int = 0
    n_failed: int = 0
    completed: set[str] = field(default_factory=set)
    failed_hashes: set[str] = field(default_factory=set)
    errors: list[tuple[str, Exception]] = field(default_factory=list)
    interrupted: bool = False


@runtime_checkable
class Orchestrator(Protocol):
    """Walks the DAG of a single run and returns its outcome."""

    def run(self, plan: RunPlan) -> RunOutcome: ...
