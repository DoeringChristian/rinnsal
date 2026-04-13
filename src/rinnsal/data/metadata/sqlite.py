"""SQLite-backed implementation of MetadataStore.

Engine instances are cached per absolute path so multiple callers in
the same process share a connection pool. WAL mode + per-call short
transactions handle the multi-writer flow case (multiple concurrent
flow processes writing to the same DB).
"""

from __future__ import annotations

import json
import logging
import threading
import time
from pathlib import Path
from typing import Any

from sqlalchemy import Engine, create_engine, event, func, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from rinnsal.data.metadata.base import (
    FlowSummary,
    RunSummary,
    RunUpsert,
    TaskHistoryEntry,
    TaskNodeRow,
)
from rinnsal.data.metadata.models import Flow, Run, TaskEdge, TaskNode

log = logging.getLogger("rinnsal.metadata")

_engines: dict[Path, Engine] = {}
_engines_lock = threading.Lock()


def _get_engine(db_path: Path) -> Engine:
    """Return a cached Engine for the given absolute path."""
    abs_path = db_path.resolve()
    with _engines_lock:
        eng = _engines.get(abs_path)
        if eng is not None:
            return eng

        abs_path.parent.mkdir(parents=True, exist_ok=True)
        eng = create_engine(
            f"sqlite:///{abs_path}",
            future=True,
            connect_args={"check_same_thread": False, "timeout": 30.0},
        )

        @event.listens_for(eng, "connect")
        def _set_pragmas(dbapi_connection, _record) -> None:
            cur = dbapi_connection.cursor()
            cur.execute("PRAGMA journal_mode=WAL")
            cur.execute("PRAGMA synchronous=NORMAL")
            cur.execute("PRAGMA foreign_keys=ON")
            cur.close()

        _engines[abs_path] = eng
        return eng


def _alembic_config(db_path: Path):
    """Build an in-memory Alembic Config pointing at our migrations dir."""
    from alembic.config import Config

    cfg = Config()
    migrations_dir = Path(__file__).parent / "migrations"
    cfg.set_main_option("script_location", str(migrations_dir))
    cfg.set_main_option("sqlalchemy.url", f"sqlite:///{db_path.resolve()}")
    return cfg


def _current_revision(engine: Engine) -> str | None:
    from alembic.runtime.migration import MigrationContext

    with engine.connect() as conn:
        ctx = MigrationContext.configure(conn)
        return ctx.get_current_revision()


def _head_revision(cfg) -> str | None:
    from alembic.script import ScriptDirectory

    script = ScriptDirectory.from_config(cfg)
    return script.get_current_head()


def _detect_nfs_warning(db_path: Path) -> None:
    """Best-effort warning when the DB lives on a network filesystem."""
    try:
        import psutil  # type: ignore[import-not-found]
    except ImportError:
        return
    try:
        partitions = psutil.disk_partitions(all=True)
    except Exception:
        return
    abs_path = str(db_path.resolve())
    matches = [p for p in partitions if abs_path.startswith(p.mountpoint)]
    if not matches:
        return
    # Longest mountpoint wins (most specific match).
    best = max(matches, key=lambda p: len(p.mountpoint))
    fstype = (best.fstype or "").lower()
    if fstype in {"nfs", "nfs4", "smbfs", "cifs"}:
        log.warning(
            "rinnsal metadata DB lives on %s (%s). SQLite WAL mode is "
            "unsafe over network filesystems; consider a local path.",
            best.mountpoint,
            fstype,
        )


class SqliteMetadataStore:
    """SQLite-backed MetadataStore. Implements the protocol in base.py."""

    def __init__(
        self,
        db_path: Path | str,
        *,
        run_migrations: bool = True,
    ) -> None:
        self._db_path = Path(db_path)
        _detect_nfs_warning(self._db_path)
        self._engine = _get_engine(self._db_path)
        if run_migrations:
            self._maybe_upgrade()

    # ------------------------------------------------------------------
    # Migrations
    # ------------------------------------------------------------------

    def _maybe_upgrade(self) -> None:
        from alembic import command

        cfg = _alembic_config(self._db_path)
        head = _head_revision(cfg)
        current = _current_revision(self._engine)
        if current == head:
            return

        if current is not None:
            log.info(
                "rinnsal metadata DB at %s: current=%s head=%s — applying migrations…",
                self._db_path,
                current,
                head,
            )
        t0 = time.perf_counter()
        # Pass our existing connection so Alembic doesn't open a second one
        # (which would fight WAL mode locks on Windows / NFS edge cases).
        with self._engine.begin() as conn:
            cfg.attributes["connection"] = conn
            command.upgrade(cfg, "head")
        elapsed = time.perf_counter() - t0
        if elapsed > 5.0:
            log.warning(
                "rinnsal metadata migration took %.1fs — consider running "
                "`rinnsal db upgrade` explicitly before launching the viewer.",
                elapsed,
            )

    # ------------------------------------------------------------------
    # Writes
    # ------------------------------------------------------------------

    def upsert_flow(self, name: str) -> None:
        now = time.time()
        stmt = (
            sqlite_insert(Flow)
            .values(name=name, created_at=now, last_run_at=None)
            .on_conflict_do_nothing(index_elements=[Flow.name])
        )
        with self._engine.begin() as conn:
            conn.execute(stmt)

    def upsert_run(self, run: RunUpsert) -> None:
        values = {
            "run_id": run.run_id,
            "flow_name": run.flow_name,
            "run_dir": run.run_dir,
            "status": run.status,
            "started_at": run.started_at,
            "finished_at": run.finished_at,
            "snapshot_hash": run.snapshot_hash,
            "tags_json": json.dumps(run.tags),
            "params_json": run.params_json,
            "task_count": run.task_count,
            "failed_count": run.failed_count,
        }
        stmt = (
            sqlite_insert(Run)
            .values(**values)
            .on_conflict_do_update(
                index_elements=[Run.run_id],
                set_={k: v for k, v in values.items() if k != "run_id"},
            )
        )
        with self._engine.begin() as conn:
            conn.execute(stmt)
            # Bump the flow's last_run_at watermark.
            conn.execute(
                Flow.__table__.update()
                .where(Flow.name == run.flow_name)
                .where(
                    (Flow.last_run_at.is_(None))
                    | (Flow.last_run_at < run.started_at)
                )
                .values(last_run_at=run.started_at)
            )

    def update_run_status(
        self,
        run_id: str,
        status: str,
        finished_at: float | None = None,
        failed_count: int | None = None,
        snapshot_hash: str | None = None,
        task_count: int | None = None,
    ) -> None:
        update_values: dict[str, Any] = {"status": status}
        if finished_at is not None:
            update_values["finished_at"] = finished_at
        if failed_count is not None:
            update_values["failed_count"] = failed_count
        if snapshot_hash is not None:
            update_values["snapshot_hash"] = snapshot_hash
        if task_count is not None:
            update_values["task_count"] = task_count
        with self._engine.begin() as conn:
            conn.execute(
                Run.__table__.update()
                .where(Run.run_id == run_id)
                .values(**update_values)
            )

    def upsert_task_node(self, row: TaskNodeRow) -> None:
        values = {
            "run_id": row.run_id,
            "task_name": row.task_name,
            "task_hash": row.task_hash,
            "status": row.status,
            "duration": row.duration,
            "error": row.error,
            "ts": row.ts,
            "params_json": row.params_json,
        }
        stmt = (
            sqlite_insert(TaskNode)
            .values(**values)
            .on_conflict_do_update(
                index_elements=[TaskNode.run_id, TaskNode.task_name],
                set_={
                    k: v
                    for k, v in values.items()
                    if k not in ("run_id", "task_name")
                },
            )
        )
        with self._engine.begin() as conn:
            conn.execute(stmt)

    def upsert_task_edge(
        self, run_id: str, from_task: str, to_task: str
    ) -> None:
        stmt = (
            sqlite_insert(TaskEdge)
            .values(run_id=run_id, from_task=from_task, to_task=to_task)
            .on_conflict_do_nothing(
                index_elements=[
                    TaskEdge.run_id,
                    TaskEdge.from_task,
                    TaskEdge.to_task,
                ]
            )
        )
        with self._engine.begin() as conn:
            conn.execute(stmt)

    # ------------------------------------------------------------------
    # Reads
    # ------------------------------------------------------------------

    def list_flows(self) -> list[FlowSummary]:
        # Aggregate run counts + latest run id with one query per flow.
        count_subq = (
            select(
                Run.flow_name,
                func.count(Run.run_id).label("run_count"),
                func.max(Run.started_at).label("latest_started_at"),
            )
            .group_by(Run.flow_name)
            .subquery()
        )
        latest_subq = (
            select(Run.flow_name, Run.run_id, Run.started_at)
            .where(Run.flow_name == count_subq.c.flow_name)
            .where(Run.started_at == count_subq.c.latest_started_at)
        )
        with self._engine.begin() as conn:
            counts = {
                row.flow_name: (row.run_count, row.latest_started_at)
                for row in conn.execute(select(count_subq)).all()
            }
            latest_ids = {
                row.flow_name: row.run_id
                for row in conn.execute(latest_subq).all()
            }
            flow_rows = conn.execute(
                select(Flow.name).order_by(Flow.name)
            ).all()

        out: list[FlowSummary] = []
        for (name,) in flow_rows:
            run_count, latest_started_at = counts.get(name, (0, None))
            out.append(
                FlowSummary(
                    name=name,
                    run_count=run_count,
                    latest_run_at=latest_started_at,
                    latest_run_id=latest_ids.get(name),
                )
            )
        return out

    _RUN_COLS = (
        Run.run_id,
        Run.flow_name,
        Run.run_dir,
        Run.status,
        Run.started_at,
        Run.finished_at,
        Run.snapshot_hash,
        Run.tags_json,
        Run.task_count,
        Run.failed_count,
    )

    def list_runs(
        self,
        flow_name: str | None = None,
        limit: int | None = None,
    ) -> list[RunSummary]:
        stmt = select(*self._RUN_COLS)
        if flow_name is not None:
            stmt = stmt.where(Run.flow_name == flow_name)
        stmt = stmt.order_by(Run.started_at.desc())
        if limit is not None:
            stmt = stmt.limit(limit)
        with self._engine.begin() as conn:
            rows = conn.execute(stmt).all()
        return [self._row_to_summary(r) for r in rows]

    def get_run(self, run_id: str) -> RunSummary | None:
        with self._engine.begin() as conn:
            row = conn.execute(
                select(*self._RUN_COLS).where(Run.run_id == run_id)
            ).first()
        return self._row_to_summary(row) if row else None

    def list_task_nodes(self, run_id: str) -> list[TaskNodeRow]:
        with self._engine.begin() as conn:
            rows = conn.execute(
                select(
                    TaskNode.run_id,
                    TaskNode.task_name,
                    TaskNode.task_hash,
                    TaskNode.status,
                    TaskNode.duration,
                    TaskNode.error,
                    TaskNode.ts,
                    TaskNode.params_json,
                )
                .where(TaskNode.run_id == run_id)
                .order_by(TaskNode.ts)
            ).all()
        return [
            TaskNodeRow(
                run_id=r.run_id,
                task_name=r.task_name,
                task_hash=r.task_hash,
                status=r.status,
                duration=r.duration,
                error=r.error,
                ts=r.ts,
                params_json=r.params_json,
            )
            for r in rows
        ]

    def list_task_edges(self, run_id: str) -> list[tuple[str, str]]:
        with self._engine.begin() as conn:
            rows = conn.execute(
                select(TaskEdge.from_task, TaskEdge.to_task).where(
                    TaskEdge.run_id == run_id
                )
            ).all()
        return [(r.from_task, r.to_task) for r in rows]

    def task_history(
        self, flow_name: str, task_name: str
    ) -> list[TaskHistoryEntry]:
        stmt = (
            select(
                Run.run_id,
                Run.run_dir,
                TaskNode.task_hash,
                TaskNode.status,
                TaskNode.duration,
                TaskNode.ts,
                TaskNode.error,
                TaskNode.params_json,
            )
            .join(TaskNode, TaskNode.run_id == Run.run_id)
            .where(Run.flow_name == flow_name)
            .where(TaskNode.task_name == task_name)
            .order_by(Run.started_at.desc())
        )
        with self._engine.begin() as conn:
            rows = conn.execute(stmt).all()
        return [
            TaskHistoryEntry(
                run_id=r.run_id,
                run_dir=r.run_dir,
                task_hash=r.task_hash,
                status=r.status,
                duration=r.duration,
                ts=r.ts,
                error=r.error,
                params_json=r.params_json,
            )
            for r in rows
        ]

    # ------------------------------------------------------------------
    # Housekeeping
    # ------------------------------------------------------------------

    def has_run(self, run_id: str) -> bool:
        with self._engine.begin() as conn:
            return (
                conn.execute(
                    select(func.count()).select_from(Run).where(
                        Run.run_id == run_id
                    )
                ).scalar_one()
                > 0
            )

    def runs_missing_index(
        self, expected_run_dirs: list[Path]
    ) -> list[Path]:
        if not expected_run_dirs:
            return []
        as_strings = [str(p) for p in expected_run_dirs]
        with self._engine.begin() as conn:
            present = {
                r.run_dir
                for r in conn.execute(
                    select(Run.run_dir).where(Run.run_dir.in_(as_strings))
                ).all()
            }
        return [p for p in expected_run_dirs if str(p) not in present]

    def latest_updated_at(
        self, flow_name: str | None = None
    ) -> float | None:
        stmt = select(
            func.max(func.coalesce(Run.finished_at, Run.started_at))
        )
        if flow_name is not None:
            stmt = stmt.where(Run.flow_name == flow_name)
        with self._engine.begin() as conn:
            return conn.execute(stmt).scalar_one_or_none()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _row_to_summary(row) -> RunSummary:
        try:
            tags = json.loads(row.tags_json or "[]")
            if not isinstance(tags, list):
                tags = []
        except (TypeError, ValueError):
            tags = []
        return RunSummary(
            run_id=row.run_id,
            flow_name=row.flow_name,
            run_dir=row.run_dir,
            status=row.status,
            started_at=row.started_at,
            finished_at=row.finished_at,
            snapshot_hash=row.snapshot_hash,
            tags=tags,
            task_count=row.task_count,
            failed_count=row.failed_count,
        )
