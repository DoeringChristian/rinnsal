"""Data loading utilities for the viewer."""

from __future__ import annotations

import math
import pickle
from pathlib import Path
from typing import Any

from rinnsal.logger.logger import EVENTS_FILE, MARKER_FILE

DEFAULT_MAX_POINTS = 1000


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
        "file_mtime",
        "file_size",
    )

    def __init__(self) -> None:
        self.scalars: dict[str, list[tuple[int, float, float | None]]] = {}
        self.text: dict[str, list[tuple[int, str]]] = {}
        self.figures: dict[str, list[tuple[int, bytes, bytes, bool]]] = {}
        self.file_mtime: float = 0.0
        self.file_size: int = 0

    def load(self, events_path: Path) -> None:
        """Single-pass read of events.pb, populating all caches."""
        from rinnsal.logger.event_file import EventFileReader

        stat = events_path.stat()
        self.file_mtime = stat.st_mtime
        self.file_size = stat.st_size

        self.scalars.clear()
        self.text.clear()
        self.figures.clear()

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
                        bytes(event.figure.data),
                        event.figure.interactive,
                    )
                )

        # Sort all by iteration
        for tag in self.scalars:
            self.scalars[tag].sort(key=lambda x: x[0])
        for tag in self.text:
            self.text[tag].sort(key=lambda x: x[0])
        for tag in self.figures:
            self.figures[tag].sort(key=lambda x: x[0])

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


# Module-level cache store
_run_caches: dict[Path, RunCache] = {}


def get_cache(log_path: Path) -> RunCache:
    """Return cached data, reloading only if file changed on disk."""
    events_path = log_path / EVENTS_FILE
    cache = _run_caches.get(log_path)

    if cache is not None and not cache.is_stale(events_path):
        return cache

    cache = RunCache()
    if events_path.exists():
        try:
            cache.load(events_path)
        except (IOError, OSError):
            pass
    _run_caches[log_path] = cache
    return cache


def invalidate_caches() -> None:
    """Force reload on next access (for Refresh button)."""
    _run_caches.clear()


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
