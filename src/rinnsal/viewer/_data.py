"""Data loading utilities for the viewer."""

from __future__ import annotations

import math
import pickle
from pathlib import Path
from typing import Any

from rinnsal.data.logger.logger import EVENTS_FILE, MARKER_FILE

DEFAULT_MAX_POINTS = 1000


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


# Module-level cache stores
_run_caches: dict[Path, RunCache] = {}
_task_graph_caches: dict[Path, "TaskGraphCache"] = {}


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
        """Scan events.pb skipping any record that can't contain a
        task_node/task_edge payload. Critical for runs with hundreds of
        MB of figures: we seek past them instead of parsing.

        Wire-format peek: task_node = field 9 (tag byte 0x4A, length-
        delimited), task_edge = field 10 (tag byte 0x52). If neither
        byte appears in the first ~32 bytes of a record we skip.
        """
        import struct

        from rinnsal.data.logger.events_pb2 import Event

        stat = events_path.stat()
        self.file_mtime = stat.st_mtime
        self.file_size = stat.st_size
        self.task_nodes.clear()
        self.task_edges.clear()

        TASK_NODE_TAG = 0x4A  # (field_number=9 << 3) | wire_type=2
        TASK_EDGE_TAG = 0x52  # (field_number=10 << 3) | wire_type=2
        PEEK_WINDOW = 64      # bytes at the start of each record to inspect

        with open(events_path, "rb") as f:
            while True:
                length_bytes = f.read(4)
                if len(length_bytes) < 4:
                    break
                length = struct.unpack("<I", length_bytes)[0]

                peek = f.read(min(PEEK_WINDOW, length))
                if TASK_NODE_TAG not in peek and TASK_EDGE_TAG not in peek:
                    # No chance this record is a task_node/task_edge;
                    # skip the remaining bytes without parsing.
                    remaining = length - len(peek)
                    if remaining > 0:
                        f.seek(remaining, 1)
                    continue

                # Candidate — read the rest and parse.
                if len(peek) < length:
                    peek += f.read(length - len(peek))
                if len(peek) < length:
                    break

                event = Event()
                event.ParseFromString(peek)
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
    """
    events_path = log_path / EVENTS_FILE
    cache = _task_graph_caches.get(log_path)
    if cache is not None and not cache.is_stale(events_path):
        return cache
    cache = TaskGraphCache()
    if events_path.exists():
        try:
            cache.load(events_path)
        except (IOError, OSError):
            pass
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
