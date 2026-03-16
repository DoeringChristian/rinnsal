"""Data loading utilities for the viewer."""

from __future__ import annotations

import json
import math
import pickle
from pathlib import Path
from typing import Any

from rinnsal.logger.logger import EVENTS_FILE, MARKER_FILE, SCALARS_FILE, TEXT_FILE

DEFAULT_MAX_POINTS = 1000


def lttb_downsample(
    data: list[tuple[int, float]], threshold: int
) -> list[tuple[int, float]]:
    """Downsample using Largest Triangle Three Buckets algorithm.

    Preserves visual shape while reducing points.
    """
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
        next_bucket_end = int(math.floor((i + 3) * bucket_size)) + 1
        next_bucket_end = min(next_bucket_end, len(data))

        avg_x = sum(data[j][0] for j in range(next_bucket_start, next_bucket_end))
        avg_y = sum(data[j][1] for j in range(next_bucket_start, next_bucket_end))
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
                - (point_a[0] - data[j][0]) * (avg_y - point_a[1])
            )
            if area > max_area:
                max_area = area
                max_idx = j

        sampled.append(data[max_idx])
        a = max_idx

    sampled.append(data[-1])
    return sampled


def is_run_directory(path: Path) -> bool:
    """Check if a directory is a rinnsal run."""
    return path.is_dir() and (path / MARKER_FILE).exists()


def discover_runs(root_path: Path) -> list[Path]:
    """Recursively discover all run directories under the given root."""
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
    timeseries: dict[str, list[tuple[int, float, float | None]]] = {}

    events_path = log_path / EVENTS_FILE
    if events_path.exists():
        try:
            from rinnsal.logger.event_file import EventFileReader

            reader = EventFileReader(events_path)
            for event in reader:
                if event.WhichOneof("data") == "scalar":
                    tag = event.scalar.tag
                    if tag not in timeseries:
                        timeseries[tag] = []
                    timeseries[tag].append(
                        (event.iteration, event.scalar.value, event.timestamp)
                    )
        except (IOError, OSError):
            pass
    else:
        scalars_path = log_path / SCALARS_FILE
        if scalars_path.exists():
            try:
                with open(scalars_path) as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            entry = json.loads(line)
                            tag = entry["tag"]
                            if tag not in timeseries:
                                timeseries[tag] = []
                            ts = entry.get("ts")
                            timeseries[tag].append(
                                (entry["it"], entry["value"], ts)
                            )
                        except (json.JSONDecodeError, KeyError):
                            continue
            except (IOError, OSError):
                pass

    for tag in timeseries:
        timeseries[tag].sort(key=lambda x: x[0])
        if len(timeseries[tag]) > max_points:
            points_2d = [(it, val) for it, val, _ in timeseries[tag]]
            ts_map = {it: ts for it, _, ts in timeseries[tag]}
            downsampled = lttb_downsample(points_2d, max_points)
            timeseries[tag] = [
                (it, val, ts_map.get(it)) for it, val in downsampled
            ]

    return timeseries


def load_text_timeseries(
    log_path: Path,
) -> dict[str, list[tuple[int, str]]]:
    """Load all text as time series."""
    timeseries: dict[str, list[tuple[int, str]]] = {}

    events_path = log_path / EVENTS_FILE
    if events_path.exists():
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
    else:
        text_path = log_path / TEXT_FILE
        if text_path.exists():
            try:
                with open(text_path) as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            entry = json.loads(line)
                            tag = entry["tag"]
                            if tag not in timeseries:
                                timeseries[tag] = []
                            timeseries[tag].append(
                                (entry["it"], entry["text"])
                            )
                        except (json.JSONDecodeError, KeyError):
                            continue
            except (IOError, OSError):
                pass

    for tag in timeseries:
        timeseries[tag].sort(key=lambda x: x[0])

    return timeseries


def load_iterations(log_path: Path) -> list[int]:
    """Load all iteration numbers from log directory."""
    if not log_path.exists():
        return []

    iterations: set[int] = set()

    events_path = log_path / EVENTS_FILE
    if events_path.exists():
        try:
            from rinnsal.logger.event_file import EventFileReader

            reader = EventFileReader(events_path)
            for event in reader:
                iterations.add(event.iteration)
        except (IOError, OSError):
            pass
    else:
        for values in load_scalars_timeseries(log_path).values():
            for it, *_ in values:
                iterations.add(it)
        for values in load_text_timeseries(log_path).values():
            for it, *_ in values:
                iterations.add(it)
        for d in log_path.iterdir():
            if d.is_dir() and d.name.isdigit():
                iterations.add(int(d.name))

    return sorted(iterations)


def load_figure(fig_path_or_data: Path | bytes) -> Any:
    """Load a figure from cloudpickle file or bytes data."""
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


def load_figure_meta(fig_path: Path) -> dict:
    """Load metadata for a figure."""
    meta_path = fig_path.with_suffix(".meta")
    if meta_path.exists():
        try:
            return json.loads(meta_path.read_text())
        except (json.JSONDecodeError, ValueError):
            pass
    return {"interactive": True}


def load_figures_info(
    log_path: Path,
) -> dict[str, list[tuple[int, Path | bytes, bool]]]:
    """Load all figure paths/data and metadata.

    Returns dict mapping tag to list of (iteration, path_or_data, interactive).
    """
    figures: dict[str, list[tuple[int, Path | bytes, bool]]] = {}

    events_path = log_path / EVENTS_FILE
    if events_path.exists():
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
    else:
        for it in load_iterations(log_path):
            figures_dir = log_path / str(it) / "figures"
            if figures_dir.exists():
                for fig_path in figures_dir.glob("*.cpkl"):
                    tag = fig_path.stem
                    meta = load_figure_meta(fig_path)
                    if tag not in figures:
                        figures[tag] = []
                    figures[tag].append(
                        (it, fig_path, meta.get("interactive", True))
                    )

    for tag in figures:
        figures[tag].sort(key=lambda x: x[0])
    return figures
