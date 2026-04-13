"""Backfill the metadata DB from existing events.pb files.

Run dirs that exist on disk but have no row in ``runs`` are scanned
once via :class:`TaskGraphCache` (the wire-format scanner that skips
heavy figure/card/plotly payloads), then upserted into the DB.

Designed to run as a daemon thread on viewer startup so the user
can interact with the viewer immediately while older runs are
indexed in the background.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from rinnsal.data.metadata.base import MetadataStore, RunUpsert, TaskNodeRow

log = logging.getLogger("rinnsal.metadata.backfill")


@dataclass
class BackfillProgress:
    """Snapshot of an in-flight backfill, exposed via /api/index/status."""

    indexing: bool = False
    done: int = 0
    total: int = 0
    current: str | None = None
    started_at: float | None = None
    finished_at: float | None = None
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "indexing": self.indexing,
            "done": self.done,
            "total": self.total,
            "current": self.current,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "errors": list(self.errors),
        }


def _flow_name_from_run_dir(run_dir: Path) -> str | None:
    """Recover the flow name from a path like ``…/flows/<flow>/runs/<run>``."""
    parts = run_dir.parts
    if "flows" not in parts or "runs" not in parts:
        return None
    try:
        i = parts.index("flows")
        return parts[i + 1]
    except (IndexError, ValueError):
        return None


def index_run(store: MetadataStore, run_dir: Path) -> bool:
    """Scan one run's events.pb and upsert flow/run/task rows.

    Returns True on success, False if the run could not be parsed.
    """
    from rinnsal.data.logger.logger import EVENTS_FILE
    from rinnsal.viewer._data import TaskGraphCache

    events_path = run_dir / EVENTS_FILE
    if not events_path.exists():
        return False

    cache = TaskGraphCache()
    try:
        cache.load(events_path)
    except OSError as e:
        log.warning("backfill: could not read %s: %s", events_path, e)
        return False

    flow_name = _flow_name_from_run_dir(run_dir) or "unknown"
    run_id = run_dir.name

    # Derive timestamps: earliest task_node ts → started_at,
    # latest ts → finished_at. Falls back to events.pb mtime.
    if cache.task_nodes:
        timestamps = [n[5] for n in cache.task_nodes]
        started = min(timestamps)
        finished = max(timestamps)
    else:
        try:
            mt = events_path.stat().st_mtime
        except OSError:
            mt = time.time()
        started = mt
        finished = mt

    # Status: failed if any node has a non-empty error; otherwise success
    # for completed runs, running for fresh ones (heuristic: events.pb
    # mtime within last 60s = "still writing").
    has_error = any(n[4] for n in cache.task_nodes)
    failed_count = sum(1 for n in cache.task_nodes if n[2] == "failed")
    age = time.time() - finished
    if has_error or failed_count > 0:
        status = "failed"
    elif age < 60 and not cache.task_nodes:
        status = "running"
    else:
        status = "success"

    store.upsert_flow(flow_name)
    store.upsert_run(
        RunUpsert(
            run_id=run_id,
            flow_name=flow_name,
            run_dir=str(run_dir),
            status=status,
            started_at=started,
            finished_at=finished,
            failed_count=failed_count,
        )
    )
    for (
        task_name,
        task_hash,
        node_status,
        duration,
        error,
        ts,
        params,
    ) in cache.task_nodes:
        if not task_name:
            continue
        store.upsert_task_node(
            TaskNodeRow(
                run_id=run_id,
                task_name=task_name,
                task_hash=task_hash,
                status=node_status,
                duration=duration,
                error=error,
                ts=ts,
                params_json=params,
            )
        )
    for from_t, to_t in cache.task_edges:
        if from_t and to_t:
            store.upsert_task_edge(run_id, from_t, to_t)
    return True


class Backfiller:
    """Discovers + indexes events.pb runs that are missing from the DB."""

    def __init__(self, store: MetadataStore, root: Path) -> None:
        self._store = store
        self._root = Path(root)
        self._progress = BackfillProgress()
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None

    @property
    def progress(self) -> BackfillProgress:
        with self._lock:
            return BackfillProgress(
                indexing=self._progress.indexing,
                done=self._progress.done,
                total=self._progress.total,
                current=self._progress.current,
                started_at=self._progress.started_at,
                finished_at=self._progress.finished_at,
                errors=list(self._progress.errors),
            )

    def discover_pending(self) -> list[Path]:
        """Walk the flow tree and return run dirs not yet in the DB."""
        from rinnsal.viewer._data import discover_flows

        flows_map = discover_flows(self._root)
        all_runs: list[Path] = []
        for runs in flows_map.values():
            all_runs.extend(runs)
        if not all_runs:
            return []
        return self._store.runs_missing_index(all_runs)

    def run_sync(self, on_progress: Callable[[], None] | None = None) -> int:
        """Backfill all pending runs synchronously. Returns count indexed."""
        pending = self.discover_pending()
        with self._lock:
            self._progress.indexing = True
            self._progress.started_at = time.time()
            self._progress.done = 0
            self._progress.total = len(pending)
            self._progress.errors.clear()

        n_ok = 0
        for run_dir in pending:
            with self._lock:
                self._progress.current = str(run_dir)
            try:
                if index_run(self._store, run_dir):
                    n_ok += 1
            except Exception as e:
                log.exception("backfill failed for %s", run_dir)
                with self._lock:
                    self._progress.errors.append(f"{run_dir}: {e}")
            with self._lock:
                self._progress.done += 1
            if on_progress is not None:
                try:
                    on_progress()
                except Exception:
                    pass

        with self._lock:
            self._progress.indexing = False
            self._progress.finished_at = time.time()
            self._progress.current = None
        return n_ok

    def run_async(
        self, on_progress: Callable[[], None] | None = None
    ) -> threading.Thread:
        """Spawn a daemon thread that runs the backfill."""
        t = threading.Thread(
            target=self.run_sync,
            kwargs={"on_progress": on_progress},
            daemon=True,
            name="rinnsal-backfill",
        )
        self._thread = t
        t.start()
        return t


# Module-level singleton: viewer startup creates a backfiller and stores
# it here so /api/index/status can read its progress without re-walking.
_global_backfiller: Backfiller | None = None
_global_lock = threading.Lock()


def set_global_backfiller(b: Backfiller | None) -> None:
    global _global_backfiller
    with _global_lock:
        _global_backfiller = b


def get_global_backfiller() -> Backfiller | None:
    with _global_lock:
        return _global_backfiller
