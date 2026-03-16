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
    """Load all scalars as time series.

    Returns dict mapping tag to list of (iteration, value, timestamp).
    """
    timeseries: dict[str, list[tuple[int, float, float | None]]] = (
        {}
    )

    events_path = log_path / EVENTS_FILE
    if not events_path.exists():
        return timeseries

    try:
        from rinnsal.logger.event_file import EventFileReader

        reader = EventFileReader(events_path)
        for event in reader:
            if event.WhichOneof("data") == "scalar":
                tag = event.scalar.tag
                if tag not in timeseries:
                    timeseries[tag] = []
                timeseries[tag].append(
                    (
                        event.iteration,
                        event.scalar.value,
                        event.timestamp,
                    )
                )
    except (IOError, OSError):
        pass

    for tag in timeseries:
        timeseries[tag].sort(key=lambda x: x[0])
        if len(timeseries[tag]) > max_points:
            points_2d = [
                (it, val) for it, val, _ in timeseries[tag]
            ]
            ts_map = {
                it: ts for it, _, ts in timeseries[tag]
            }
            downsampled = lttb_downsample(points_2d, max_points)
            timeseries[tag] = [
                (it, val, ts_map.get(it))
                for it, val in downsampled
            ]

    return timeseries


def load_text_timeseries(
    log_path: Path,
) -> dict[str, list[tuple[int, str]]]:
    timeseries: dict[str, list[tuple[int, str]]] = {}

    events_path = log_path / EVENTS_FILE
    if not events_path.exists():
        return timeseries

    try:
        from rinnsal.logger.event_file import EventFileReader

        reader = EventFileReader(events_path)
        for event in reader:
            if event.WhichOneof("data") == "text":
                tag = event.text.tag
                if tag not in timeseries:
                    timeseries[tag] = []
                timeseries[tag].append(
                    (event.iteration, event.text.value)
                )
    except (IOError, OSError):
        pass

    for tag in timeseries:
        timeseries[tag].sort(key=lambda x: x[0])

    return timeseries


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


def load_figures_info(
    log_path: Path,
) -> dict[str, list[tuple[int, Path | bytes, bool]]]:
    """Load all figure data and metadata.

    Returns dict mapping tag to list of (iteration, data_bytes,
    interactive).
    """
    figures: dict[str, list[tuple[int, Path | bytes, bool]]] = {}

    events_path = log_path / EVENTS_FILE
    if not events_path.exists():
        return figures

    try:
        from rinnsal.logger.event_file import EventFileReader

        reader = EventFileReader(events_path)
        for event in reader:
            if event.WhichOneof("data") == "figure":
                tag = event.figure.tag
                if tag not in figures:
                    figures[tag] = []
                figures[tag].append(
                    (
                        event.iteration,
                        event.figure.data,
                        event.figure.interactive,
                    )
                )
    except (IOError, OSError):
        pass

    for tag in figures:
        figures[tag].sort(key=lambda x: x[0])
    return figures
