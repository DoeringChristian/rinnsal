"""Data loading utilities for the viewer."""

from __future__ import annotations

import math
import pickle
from pathlib import Path
from typing import Any

from rinnsal.data.logger.logger import EVENTS_FILE, MARKER_FILE

DEFAULT_MAX_POINTS = 1000


def _read_varint(buf: bytes, idx: int) -> tuple[int | None, int]:
    """Decode one protobuf varint from ``buf`` starting at ``idx``.

    Returns ``(value, new_idx)`` or ``(None, idx)`` if the buffer ends
    mid-varint.
    """
    result = 0
    shift = 0
    start = idx
    n = len(buf)
    while idx < n:
        b = buf[idx]
        idx += 1
        result |= (b & 0x7F) << shift
        if not (b & 0x80):
            return result, idx
        shift += 7
        if shift >= 64:  # malformed
            return None, start
    return None, start


def _resolve_blob_root(log_path: Path) -> Path | None:
    """Walk up from a run dir to find the FileDatabase root.

    Layout produced by ``LocalRunStore``:
        <db_root>/flows/<flow_name>/runs/<run_id>/events.pb
    Returns the directory that contains a ``blobs/`` subdirectory, or
    ``None`` if none is found within four levels.
    """
    cur = log_path.resolve()
    for _ in range(5):
        if (cur / "blobs").is_dir():
            return cur
        if cur == cur.parent:
            break
        cur = cur.parent
    return None


def _card_component_to_dict(cc: Any) -> dict[str, Any]:
    """Convert a CardComponent proto into a JSON-friendly dict.

    Heavy payloads (image bytes, plotly JSON, pickled data) are NOT
    inlined here — only their hashes / sizes / metadata. The viewer
    fetches blobs lazily via the ``/api/blob`` endpoint.
    """
    import base64

    kind = cc.WhichOneof("data")
    out: dict[str, Any] = {"kind": kind, "tag": cc.tag}
    if kind == "scalar":
        out["value"] = cc.scalar.value
    elif kind == "text":
        out["content"] = cc.text.value
    elif kind == "markdown":
        out["content"] = cc.markdown.content
    elif kind == "table":
        out["headers_json"] = cc.table.headers_json
        out["rows_json"] = cc.table.rows_json
    elif kind == "code":
        out["source"] = cc.code.source
        out["language"] = cc.code.language
    elif kind == "progress":
        out["value"] = cc.progress.value
        out["total"] = cc.progress.total
        out["label"] = cc.progress.label
    elif kind == "image":
        out["width"] = cc.image.width
        out["height"] = cc.image.height
        out["blob_hash"] = cc.image.blob_hash
        if not cc.image.blob_hash and cc.image.data:
            out["inline_b64"] = base64.b64encode(bytes(cc.image.data)).decode()
    elif kind == "figure":
        out["interactive"] = cc.figure.interactive
        out["format"] = cc.figure.format or "matplotlib"
        out["image_blob_hash"] = cc.figure.image_blob_hash
        if not cc.figure.image_blob_hash and cc.figure.image:
            out["inline_b64"] = base64.b64encode(bytes(cc.figure.image)).decode()
    elif kind == "plotly":
        out["title"] = cc.plotly.title
        out["n_traces"] = cc.plotly.n_traces
        out["blob_hash"] = cc.plotly.blob_hash
        out["png_blob_hash"] = cc.plotly.png_blob_hash
        if not cc.plotly.blob_hash and cc.plotly.inline_json:
            out["inline_json"] = cc.plotly.inline_json
    elif kind == "artifact":
        # Artifact component (Checkpoint proto + description / type_name)
        out["description"] = cc.artifact.description
        out["type_name"] = cc.artifact.type_name
        out["blob_hash"] = cc.artifact.blob_hash
    return out


def lttb_downsample(
    data: list[tuple[int, float]], threshold: int
) -> list[tuple[int, float]]:
    """Downsample using Largest Triangle Three Buckets algorithm."""
    if len(data) <= threshold or threshold < 3:
        return data

    sampled = [data[0]]
    bucket_size = (len(data) - 2) / (threshold - 2)
    a = 0

    for i in range(threshold - 2):
        bucket_start = int(math.floor((i + 1) * bucket_size)) + 1
        bucket_end = int(math.floor((i + 2) * bucket_size)) + 1
        bucket_end = min(bucket_end, len(data) - 1)

        next_bucket_start = bucket_end
        next_bucket_end = (
            int(math.floor((i + 3) * bucket_size)) + 1
        )
        next_bucket_end = min(next_bucket_end, len(data))

        avg_x = sum(
            data[j][0]
            for j in range(next_bucket_start, next_bucket_end)
        )
        avg_y = sum(
            data[j][1]
            for j in range(next_bucket_start, next_bucket_end)
        )
        count = next_bucket_end - next_bucket_start
        if count > 0:
            avg_x /= count
            avg_y /= count
        else:
            avg_x, avg_y = data[-1]

        max_area = -1
        max_idx = bucket_start
        point_a = data[a]
        for j in range(bucket_start, bucket_end):
            area = abs(
                (point_a[0] - avg_x) * (data[j][1] - point_a[1])
                - (point_a[0] - data[j][0])
                * (avg_y - point_a[1])
            )
            if area > max_area:
                max_area = area
                max_idx = j

        sampled.append(data[max_idx])
        a = max_idx

    sampled.append(data[-1])
    return sampled


class RunCache:
    """Holds all parsed event data for one run, loaded in a single pass."""

    __slots__ = (
        "scalars",
        "text",
        "figures",
        "images",
        "markdown",
        "tables",
        "code",
        "progress",
        "plotly",
        "cards",
        "task_nodes",
        "task_edges",
        "file_mtime",
        "file_size",
        "blob_root",
    )

    def __init__(self) -> None:
        self.scalars: dict[str, list[tuple[int, float, float | None]]] = {}
        self.text: dict[str, list[tuple[int, str]]] = {}
        # tag → [(it, image_bytes, image_blob_hash, data_bytes, data_blob_hash, interactive, format)]
        self.figures: dict[
            str,
            list[tuple[int, bytes, str, bytes, str, bool, str]],
        ] = {}
        # tag → [(it, png_bytes, blob_hash, w, h)]
        self.images: dict[str, list[tuple[int, bytes, str, int, int]]] = {}
        self.markdown: dict[str, list[tuple[int, str]]] = {}
        # tag → [(it, headers_json, rows_json)]
        self.tables: dict[str, list[tuple[int, str, str]]] = {}
        # tag → [(it, source, language)]
        self.code: dict[str, list[tuple[int, str, str]]] = {}
        # tag → [(it, value, total, label)]
        self.progress: dict[str, list[tuple[int, float, float, str]]] = {}
        # tag → [(it, inline_json, blob_hash, png_blob_hash, n_traces, title)]
        self.plotly: dict[
            str,
            list[tuple[int, str, str, str, int, str]],
        ] = {}
        # (task, name) → [(it, [CardComponent serialized as dict])]
        self.cards: dict[tuple[str, str], list[tuple[int, list[dict]]]] = {}
        # List of (task_name, task_hash, status, duration, error, timestamp, params)
        self.task_nodes: list[tuple[str, str, str, float, str, float, str]] = []
        # List of (from_task, to_task)
        self.task_edges: list[tuple[str, str]] = []
        self.file_mtime: float = 0.0
        self.file_size: int = 0
        # Path to the FileDatabase root (containing blobs/) for content-
        # addressed payloads. Set by get_cache() when the run dir lives
        # under a recognized .rinnsal layout.
        self.blob_root: Path | None = None

    def load(self, events_path: Path) -> None:
        """Single-pass read of events.pb, populating all caches."""
        from rinnsal.data.logger.event_file import EventFileReader

        stat = events_path.stat()
        self.file_mtime = stat.st_mtime
        self.file_size = stat.st_size

        self.scalars.clear()
        self.text.clear()
        self.figures.clear()
        self.images.clear()
        self.markdown.clear()
        self.tables.clear()
        self.code.clear()
        self.progress.clear()
        self.plotly.clear()
        self.cards.clear()
        self.task_nodes.clear()
        self.task_edges.clear()

        reader = EventFileReader(events_path)
        for event in reader:
            data_type = event.WhichOneof("data")
            it = event.iteration

            if data_type == "scalar":
                tag = event.scalar.tag
                if tag not in self.scalars:
                    self.scalars[tag] = []
                self.scalars[tag].append(
                    (it, event.scalar.value, event.timestamp)
                )

            elif data_type == "text":
                tag = event.text.tag
                if tag not in self.text:
                    self.text[tag] = []
                self.text[tag].append(
                    (it, str(event.text.value))
                )

            elif data_type == "figure":
                tag = event.figure.tag
                if tag not in self.figures:
                    self.figures[tag] = []
                # Copy bytes so the protobuf Event can be GC'd
                self.figures[tag].append(
                    (
                        it,
                        bytes(event.figure.image),
                        event.figure.image_blob_hash,
                        bytes(event.figure.data),
                        event.figure.data_blob_hash,
                        event.figure.interactive,
                        event.figure.format or "matplotlib",
                    )
                )

            elif data_type == "image":
                tag = event.image.tag
                if tag not in self.images:
                    self.images[tag] = []
                self.images[tag].append(
                    (
                        it,
                        bytes(event.image.data),
                        event.image.blob_hash,
                        event.image.width,
                        event.image.height,
                    )
                )

            elif data_type == "markdown":
                tag = event.markdown.tag
                self.markdown.setdefault(tag, []).append(
                    (it, event.markdown.content)
                )

            elif data_type == "table":
                tag = event.table.tag
                self.tables.setdefault(tag, []).append(
                    (it, event.table.headers_json, event.table.rows_json)
                )

            elif data_type == "code":
                tag = event.code.tag
                self.code.setdefault(tag, []).append(
                    (it, event.code.source, event.code.language)
                )

            elif data_type == "progress":
                tag = event.progress.tag
                self.progress.setdefault(tag, []).append(
                    (
                        it,
                        event.progress.value,
                        event.progress.total,
                        event.progress.label,
                    )
                )

            elif data_type == "plotly":
                tag = event.plotly.tag
                self.plotly.setdefault(tag, []).append(
                    (
                        it,
                        event.plotly.inline_json,
                        event.plotly.blob_hash,
                        event.plotly.png_blob_hash,
                        event.plotly.n_traces,
                        event.plotly.title,
                    )
                )

            elif data_type == "card_event":
                ce = event.card_event
                key = (ce.task, ce.name)
                components = [_card_component_to_dict(cc) for cc in ce.components]
                self.cards.setdefault(key, []).append((it, components))

            elif data_type == "task_node":
                tn = event.task_node
                self.task_nodes.append(
                    (
                        tn.task_name,
                        tn.task_hash,
                        tn.status,
                        tn.duration,
                        tn.error,
                        event.timestamp,
                        getattr(tn, "params", ""),
                    )
                )

            elif data_type == "task_edge":
                te = event.task_edge
                self.task_edges.append((te.from_task, te.to_task))

        # Sort all by iteration
        for d in (
            self.scalars,
            self.text,
            self.figures,
            self.images,
            self.markdown,
            self.tables,
            self.code,
            self.progress,
            self.plotly,
        ):
            for tag in d:
                d[tag].sort(key=lambda x: x[0])
        for key in self.cards:
            self.cards[key].sort(key=lambda x: x[0])

    def tags(self) -> list[dict[str, Any]]:
        """Return a unified listing of every (tag, kind) in the run.

        Used by the viewer's Tags section. Each entry includes the list
        of iterations the tag was emitted at.
        """
        out: list[dict[str, Any]] = []
        for kind, store in (
            ("scalar", self.scalars),
            ("text", self.text),
            ("figure", self.figures),
            ("image", self.images),
            ("markdown", self.markdown),
            ("table", self.tables),
            ("code", self.code),
            ("progress", self.progress),
            ("plotly", self.plotly),
        ):
            for tag, entries in store.items():
                out.append(
                    {
                        "tag": tag,
                        "kind": kind,
                        "iterations": [e[0] for e in entries],
                        "count": len(entries),
                    }
                )
        return out

    def get_blob(self, blob_hash: str) -> bytes | None:
        """Resolve a content-addressed blob from the run's FileDatabase root."""
        if not blob_hash or self.blob_root is None:
            return None
        path = self.blob_root / "blobs" / blob_hash[:2] / blob_hash[2:4] / blob_hash[4:]
        try:
            return path.read_bytes()
        except OSError:
            return None

    def is_stale(self, events_path: Path) -> bool:
        """Check if the file has changed since we last loaded."""
        try:
            stat = events_path.stat()
            return (
                stat.st_mtime != self.file_mtime
                or stat.st_size != self.file_size
            )
        except OSError:
            return True


import threading

# Module-level cache stores
_run_caches: dict[Path, RunCache] = {}
_task_graph_caches: dict[Path, "TaskGraphCache"] = {}

# Per-path locks: prevents N concurrent /api/flows requests from each
# loading the same multi-GB events.pb in parallel. The first request
# loads, the others wait and reuse the populated cache.
_task_graph_locks_lock = threading.Lock()
_task_graph_locks: dict[Path, threading.Lock] = {}


def _task_graph_lock(path: Path) -> threading.Lock:
    with _task_graph_locks_lock:
        lock = _task_graph_locks.get(path)
        if lock is None:
            lock = threading.Lock()
            _task_graph_locks[path] = lock
        return lock


def get_cache(log_path: Path) -> RunCache:
    """Return cached data, reloading only if file changed on disk."""
    events_path = log_path / EVENTS_FILE
    cache = _run_caches.get(log_path)

    if cache is not None and not cache.is_stale(events_path):
        return cache

    cache = RunCache()
    cache.blob_root = _resolve_blob_root(log_path)
    if events_path.exists():
        try:
            cache.load(events_path)
        except (IOError, OSError):
            pass
    _run_caches[log_path] = cache
    return cache


class TaskGraphCache:
    """Lightweight cache of a run's task_node + task_edge events.

    The /api/flows aggregator walks every run under a flow dir. It only
    needs task graph data, so skip the full event parse — figures,
    plotly payloads, cards, blobs. This is orders of magnitude faster
    on runs with lots of figures or a long training loop.
    """

    __slots__ = ("task_nodes", "task_edges", "file_mtime", "file_size")

    def __init__(self) -> None:
        self.task_nodes: list[tuple[str, str, str, float, str, float, str]] = []
        self.task_edges: list[tuple[str, str]] = []
        self.file_mtime: float = 0.0
        self.file_size: int = 0

    def load(self, events_path: Path) -> None:
        """Scan events.pb reading only header bytes per record.

        Each rinnsal Event has fixed leading fields ``timestamp``
        (field 1, fixed64) and ``iteration`` (field 2, varint). The
        payload arrives as a single ``oneof data`` field — `task_node`
        is field 9, `task_edge` is field 10, every other event type is
        a heavy payload (Figure, Image, Plotly, CardEvent, …).

        We parse the wire-format header for each record up to the
        oneof field tag; if it's not 9 or 10 we ``f.seek`` past the
        record without reading its body. Result: a multi-GB file with
        hundreds of figures touches only a few KB of disk.
        """
        import logging
        import struct
        import time

        from rinnsal.data.logger.events_pb2 import Event

        log = logging.getLogger("rinnsal.viewer")
        t0 = time.perf_counter()

        stat = events_path.stat()
        self.file_mtime = stat.st_mtime
        self.file_size = stat.st_size
        self.task_nodes.clear()
        self.task_edges.clear()

        n_records = 0
        n_header_only = 0   # records skipped after a header-only peek
        bytes_read = 0

        # The header bytes we always care about:
        #   tag(1)=fixed64 (0x09) + 8 bytes timestamp = 9
        #   tag(2)=varint  (0x10) + 1..10 bytes iteration
        # The oneof data tag immediately follows. Read 32 bytes — that
        # comfortably covers the longest plausible varint iteration.
        HEADER_PROBE = 32

        with open(events_path, "rb") as f:
            while True:
                length_bytes = f.read(4)
                if len(length_bytes) < 4:
                    break
                record_len = struct.unpack("<I", length_bytes)[0]
                n_records += 1
                bytes_read += 4

                probe_size = min(HEADER_PROBE, record_len)
                head = f.read(probe_size)
                bytes_read += len(head)
                if len(head) < probe_size:
                    break

                # Walk the header to locate the data-oneof field tag.
                idx = 0
                data_tag: int | None = None
                while idx < len(head):
                    # Each field starts with a varint "tag" combining
                    # field number (>> 3) and wire type (& 7).
                    tag, idx = _read_varint(head, idx)
                    if tag is None:
                        break
                    field_no = tag >> 3
                    wire = tag & 7

                    if field_no in (1, 2):  # timestamp / iteration
                        if wire == 0:                       # varint
                            _, idx = _read_varint(head, idx)
                        elif wire == 1:                     # fixed64
                            idx += 8
                        elif wire == 5:                     # fixed32
                            idx += 4
                        else:
                            # Unexpected wire type for a known field;
                            # bail out and full-parse to be safe.
                            data_tag = -1
                            break
                        continue

                    # Anything else is the data oneof payload.
                    data_tag = field_no
                    break

                # Decide: skip, or read+parse.
                if data_tag in (None, 9, 10):
                    # 9 = task_node, 10 = task_edge — read the rest.
                    if record_len > probe_size:
                        extra = f.read(record_len - probe_size)
                        bytes_read += len(extra)
                        head += extra
                    if len(head) < record_len:
                        break

                    event = Event()
                    event.ParseFromString(head)
                    dt = event.WhichOneof("data")
                    if dt == "task_node":
                        tn = event.task_node
                        self.task_nodes.append(
                            (
                                tn.task_name,
                                tn.task_hash,
                                tn.status,
                                tn.duration,
                                tn.error,
                                event.timestamp,
                                getattr(tn, "params", ""),
                            )
                        )
                    elif dt == "task_edge":
                        te = event.task_edge
                        self.task_edges.append((te.from_task, te.to_task))
                else:
                    # Heavy event (Figure, Image, Plotly, CardEvent…).
                    # Skip its body without reading it.
                    remaining = record_len - probe_size
                    if remaining > 0:
                        f.seek(remaining, 1)
                    n_header_only += 1

        elapsed = (time.perf_counter() - t0) * 1000
        if log.isEnabledFor(logging.DEBUG):
            log.debug(
                "task_graph %s: %.1fMB file, %d records (%d header-only), "
                "%d nodes, %d edges, %.2fMB read in %.0fms",
                events_path,
                stat.st_size / 1e6,
                n_records,
                n_header_only,
                len(self.task_nodes),
                len(self.task_edges),
                bytes_read / 1e6,
                elapsed,
            )

    def is_stale(self, events_path: Path) -> bool:
        try:
            stat = events_path.stat()
        except OSError:
            return True
        return (
            stat.st_mtime != self.file_mtime
            or stat.st_size != self.file_size
        )


def get_task_graph(log_path: Path) -> TaskGraphCache:
    """Return a task-graph-only cache; orders of magnitude cheaper than
    ``get_cache`` when the caller only needs ``task_nodes``/``task_edges``.

    Concurrent callers for the same path are serialized via a per-path
    lock so we don't load the same multi-GB events.pb N times in
    parallel.
    """
    import logging
    log = logging.getLogger("rinnsal.viewer")
    events_path = log_path / EVENTS_FILE

    cache = _task_graph_caches.get(log_path)
    if cache is not None and not cache.is_stale(events_path):
        log.debug("task_graph CACHE HIT %s", events_path)
        return cache

    lock = _task_graph_lock(log_path)
    with lock:
        # Re-check under the lock: a concurrent caller may have loaded
        # this same path while we were waiting.
        cache = _task_graph_caches.get(log_path)
        if cache is not None and not cache.is_stale(events_path):
            log.debug("task_graph CACHE HIT (post-lock) %s", events_path)
            return cache

        cache = TaskGraphCache()
        if events_path.exists():
            try:
                cache.load(events_path)
            except (IOError, OSError) as e:
                log.debug("task_graph load error %s: %s", events_path, e)
        else:
            log.debug("task_graph no events.pb at %s", events_path)
        _task_graph_caches[log_path] = cache
        return cache


def invalidate_caches() -> None:
    """Force reload on next access (for Refresh button)."""
    _run_caches.clear()
    _task_graph_caches.clear()


def is_run_directory(path: Path) -> bool:
    return path.is_dir() and (path / MARKER_FILE).exists()


def discover_runs(root_path: Path) -> list[Path]:
    runs = []
    if not root_path.exists():
        return runs

    if is_run_directory(root_path):
        runs.append(root_path)

    for item in root_path.iterdir():
        if item.is_dir() and not item.name.isdigit():
            if is_run_directory(item):
                runs.append(item)
            else:
                runs.extend(discover_runs(item))

    return sorted(runs)


def discover_flows(root_path: Path) -> dict[str, list[Path]]:
    """Find flows and their runs under ``<root>/flows/<flow_name>/runs/*``.

    Returns a dict mapping flow name to a list of run directories,
    sorted newest-first by directory name.
    """
    result: dict[str, list[Path]] = {}
    flows_dir = root_path / "flows"
    if not flows_dir.is_dir():
        return result

    for flow_dir in flows_dir.iterdir():
        if not flow_dir.is_dir():
            continue
        runs_dir = flow_dir / "runs"
        if not runs_dir.is_dir():
            continue
        runs = [
            p for p in runs_dir.iterdir()
            if p.is_dir() and is_run_directory(p)
        ]
        if runs:
            result[flow_dir.name] = sorted(runs, reverse=True)
    return result


def load_scalars_timeseries(
    log_path: Path, max_points: int = DEFAULT_MAX_POINTS
) -> dict[str, list[tuple[int, float, float | None]]]:
    """Load all scalars as time series (from cache)."""
    cache = get_cache(log_path)
    timeseries: dict[str, list[tuple[int, float, float | None]]] = {}

    for tag, data in cache.scalars.items():
        if len(data) > max_points:
            points_2d = [(it, val) for it, val, _ in data]
            ts_map = {it: ts for it, _, ts in data}
            downsampled = lttb_downsample(points_2d, max_points)
            timeseries[tag] = [
                (it, val, ts_map.get(it)) for it, val in downsampled
            ]
        else:
            timeseries[tag] = list(data)

    return timeseries


def load_text_timeseries(
    log_path: Path,
) -> dict[str, list[tuple[int, str]]]:
    """Load all text as time series (from cache)."""
    cache = get_cache(log_path)
    return {tag: list(data) for tag, data in cache.text.items()}


def load_figure(fig_path_or_data: Path | bytes) -> Any:
    import cloudpickle

    if isinstance(fig_path_or_data, bytes):
        try:
            return cloudpickle.loads(fig_path_or_data)
        except (EOFError, pickle.UnpicklingError):
            return None

    with open(fig_path_or_data, "rb") as f:
        try:
            return cloudpickle.load(f)
        except (EOFError, pickle.UnpicklingError):
            return None
