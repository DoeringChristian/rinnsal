"""Task execution context.

Holds per-task state addressable from inside a ``@task`` body without
threading it through every function. The accessible surface:

``current.checkpoint``
    Resumable state for long-running tasks (pickle-backed).
``current.task_name`` / ``current.task_hash``
    Identity of the currently executing task.
``current.flow_name`` / ``current.run_id``
    Identity of the enclosing flow run.
``current.snapshot_hash``
    Hash of the code snapshot that the engine captured for this run.
``current.db_path``
    Absolute path to ``<db>/`` (the ``.rinnsal/`` root), useful for
    choosing an aim-repo default that lives next to the task cache.
``current.executor``
    The active :class:`Executor` instance (or ``None`` outside a flow).
``current.task_args``
    ``{name: value}`` dict of the current task's resolved inputs,
    captured at dispatch time. :class:`rinnsal.aim.AimLogger` reads
    this to auto-populate ``run["hparams"]``.
"""

from __future__ import annotations

from contextvars import ContextVar
from pathlib import Path
from typing import Any


class Checkpoint:
    """Save and load task checkpoint data for resumable execution."""

    def __init__(self, path: Path | None = None) -> None:
        self._path = path

    def save(self, data: Any) -> None:
        if self._path is None:
            return
        import cloudpickle

        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._path.with_suffix(".tmp")
        with open(tmp, "wb") as f:
            cloudpickle.dump(data, f)
        tmp.rename(self._path)

    def load(self) -> Any:
        if self._path is None or not self._path.exists():
            return None
        import cloudpickle

        with open(self._path, "rb") as f:
            return cloudpickle.load(f)

    def clear(self) -> None:
        if self._path is not None and self._path.exists():
            self._path.unlink()


class _Current:
    """Context-aware accessor for the active task's execution context."""

    _checkpoint_var: ContextVar[Checkpoint | None] = ContextVar(
        "_checkpoint_var", default=None
    )
    _task_name_var: ContextVar[str] = ContextVar("_task_name_var", default="")
    _task_hash_var: ContextVar[str] = ContextVar("_task_hash_var", default="")
    _task_args_var: ContextVar[dict[str, Any] | None] = ContextVar(
        "_task_args_var", default=None
    )
    _flow_name_var: ContextVar[str] = ContextVar("_flow_name_var", default="")
    _run_id_var: ContextVar[str] = ContextVar("_run_id_var", default="")
    _snapshot_hash_var: ContextVar[str] = ContextVar(
        "_snapshot_hash_var", default=""
    )
    _db_path_var: ContextVar[Path | None] = ContextVar(
        "_db_path_var", default=None
    )
    _executor_var: ContextVar[Any] = ContextVar(
        "_executor_var", default=None
    )

    # ── getters ────────────────────────────────────────────────────

    @property
    def checkpoint(self) -> Checkpoint:
        cp = self._checkpoint_var.get(None)
        if cp is None:
            cp = Checkpoint()
            self._checkpoint_var.set(cp)
        return cp

    @property
    def task_name(self) -> str:
        return self._task_name_var.get("")

    @property
    def task_hash(self) -> str:
        return self._task_hash_var.get("")

    @property
    def task_args(self) -> dict[str, Any] | None:
        return self._task_args_var.get(None)

    @property
    def flow_name(self) -> str:
        return self._flow_name_var.get("")

    @property
    def run_id(self) -> str:
        return self._run_id_var.get("")

    @property
    def snapshot_hash(self) -> str:
        return self._snapshot_hash_var.get("")

    @property
    def db_path(self) -> Path | None:
        return self._db_path_var.get(None)

    @property
    def executor(self) -> Any:
        return self._executor_var.get(None)

    # ── setters (framework-internal) ───────────────────────────────

    def _set_checkpoint(self, checkpoint: Checkpoint) -> None:
        self._checkpoint_var.set(checkpoint)

    def _set_task_name(self, name: str) -> None:
        self._task_name_var.set(name)

    def _set_task_hash(self, h: str) -> None:
        self._task_hash_var.set(h)

    def _set_task_args(self, args: dict[str, Any] | None) -> None:
        self._task_args_var.set(args)

    def _set_flow_name(self, name: str) -> None:
        self._flow_name_var.set(name)

    def _set_run_id(self, rid: str) -> None:
        self._run_id_var.set(rid)

    def _set_snapshot_hash(self, h: str) -> None:
        self._snapshot_hash_var.set(h)

    def _set_db_path(self, p: Path | None) -> None:
        self._db_path_var.set(p)

    def _set_executor(self, ex: Any) -> None:
        self._executor_var.set(ex)

    def _reset_checkpoint(self) -> None:
        self._checkpoint_var.set(None)


current = _Current()
