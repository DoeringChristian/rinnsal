"""FastAPI backend for the rinnsal viewer."""

from __future__ import annotations

import base64
import os
from pathlib import Path
from typing import Annotated

from fastapi import FastAPI, Query, Response
from fastapi.staticfiles import StaticFiles

from rinnsal.viewer._data import (
    discover_flows,
    discover_runs,
    get_cache,
    is_run_directory,
)

app = FastAPI(title="Rinnsal Viewer")


def _resolve_run_path(run_path: str) -> Path:
    """Resolve a URL-encoded run path to an absolute Path."""
    path = Path(run_path)
    if not path.is_absolute():
        path = Path("/") / run_path
    return path


@app.get("/api/config")
def get_config() -> dict:
    """Get initial configuration from environment."""
    log_dir = os.environ.get("RINNSAL_LOG_DIR", "")
    return {"logDir": log_dir}


@app.get("/api/runs")
def list_runs(
    root: Annotated[str, Query(description="Root directory to search for runs")]
) -> list[dict]:
    """List all runs under the given root directory.

    Returns a list of ``{path, name, flow}`` dicts.  The *flow* field
    is extracted from the directory structure when the run lives under
    ``<root>/flows/<flow_name>/runs/<run_id>/``.
    """
    root_path = Path(root).resolve()
    if not root_path.exists():
        return []

    runs = discover_runs(root_path)
    result: list[dict] = []
    for r in runs:
        name = r.name
        # Detect flow name from path: .../flows/<flow>/runs/<run_id>
        flow: str | None = None
        try:
            parts = r.relative_to(root_path).parts
            if len(parts) >= 3 and parts[0] == "flows" and parts[2] == "runs":
                flow = parts[1]
        except ValueError:
            pass
        result.append({"path": str(r), "name": name, "flow": flow})
    return result


@app.get("/api/scalars/{run_path:path}")
def get_scalars(run_path: str) -> dict:
    """Get scalar data for a run. Returns {tag: [{it, value, timestamp}, ...]}."""
    cache = get_cache(_resolve_run_path(run_path))
    result: dict[str, list[dict]] = {}
    for tag, data in cache.scalars.items():
        result[tag] = [
            {"it": it, "value": val, "ts": ts}
            for it, val, ts in data
        ]
    return result


@app.get("/api/text/{run_path:path}")
def get_text(run_path: str) -> dict:
    """Get text data for a run. Returns {tag: [{it, value}, ...]}."""
    cache = get_cache(_resolve_run_path(run_path))
    result: dict[str, list[dict]] = {}
    for tag, data in cache.text.items():
        result[tag] = [
            {"it": it, "value": val}
            for it, val in data
        ]
    return result


@app.get("/api/figures/{run_path:path}")
def get_figures_meta(run_path: str) -> dict:
    """Get figure metadata (no image bytes). Returns {tag: [{it}, ...]}."""
    cache = get_cache(_resolve_run_path(run_path))
    result: dict[str, list[dict]] = {}
    for tag, data in cache.figures.items():
        result[tag] = [{"it": entry[0]} for entry in data]
    return result


@app.get("/api/figure/{run_path:path}")
def get_figure_image(
    run_path: str,
    tag: str = Query(...),
    it: int = Query(...),
) -> Response:
    """Get a single figure image as PNG."""
    cache = get_cache(_resolve_run_path(run_path))
    figures = cache.figures.get(tag, [])
    for fig_it, image, image_blob_hash, _data, _data_blob, _interactive, _fmt in figures:
        if fig_it == it:
            png = image or (cache.get_blob(image_blob_hash) or b"")
            if png:
                return Response(content=png, media_type="image/png")
    return Response(status_code=404, content=b"Figure not found")


@app.get("/api/images/{run_path:path}")
def get_images_meta(run_path: str) -> dict:
    """Get image metadata (no pixel data). Returns {tag: [{it, width, height}]}."""
    cache = get_cache(_resolve_run_path(run_path))
    result: dict[str, list[dict]] = {}
    for tag, data in cache.images.items():
        result[tag] = [
            {"it": it, "width": w, "height": h}
            for it, _png, _blob, w, h in data
        ]
    return result


@app.get("/api/image/{run_path:path}")
def get_image(
    run_path: str,
    tag: str = Query(...),
    it: int = Query(...),
) -> Response:
    """Get a single image as PNG."""
    cache = get_cache(_resolve_run_path(run_path))
    images = cache.images.get(tag, [])
    for img_it, png_data, blob_hash, _w, _h in images:
        if img_it == it:
            png = png_data or (cache.get_blob(blob_hash) or b"")
            if png:
                return Response(content=png, media_type="image/png")
    return Response(status_code=404, content=b"Image not found")


@app.get("/api/cards/{run_path:path}")
def get_cards_index(run_path: str) -> dict:
    """List every named card in a run.

    Returns ``{"cards": [{name, task, iterations, component_kinds}]}``.
    Iteration order is ascending; ``component_kinds`` previews the
    composition of the latest emission.
    """
    cache = get_cache(_resolve_run_path(run_path))
    out: list[dict] = []
    for (task, name), entries in cache.cards.items():
        latest = entries[-1] if entries else (0, [])
        out.append(
            {
                "name": name,
                "task": task,
                "iterations": [it for it, _ in entries],
                "component_kinds": [c["kind"] for c in latest[1]],
            }
        )
    out.sort(key=lambda c: (c["task"], c["name"]))
    return {"cards": out}


@app.get("/api/card/{run_path:path}")
def get_card(
    run_path: str,
    name: str = Query(...),
    task: str = Query(""),
    it: int | None = Query(None),
) -> dict:
    """Get a single card snapshot.

    When *it* is omitted, returns the latest emission. Components are
    returned with metadata + (small) inline payloads; heavy payloads
    must be fetched via the ``/api/blob`` endpoint using the hashes in
    each component dict.
    """
    cache = get_cache(_resolve_run_path(run_path))
    entries = cache.cards.get((task, name), [])
    if not entries:
        return {"name": name, "task": task, "it": None, "components": []}
    if it is None:
        chosen_it, components = entries[-1]
    else:
        chosen_it, components = entries[-1]
        for ev_it, comps in entries:
            if ev_it == it:
                chosen_it, components = ev_it, comps
                break
    return {
        "name": name,
        "task": task,
        "it": chosen_it,
        "components": components,
    }


@app.get("/api/tags/{run_path:path}")
def get_tags(run_path: str) -> dict:
    """Unified listing of every (tag, kind) emitted in a run."""
    cache = get_cache(_resolve_run_path(run_path))
    return {"tags": cache.tags()}


@app.get("/api/blob/{run_path:path}/{blob_hash}")
def get_blob(run_path: str, blob_hash: str) -> Response:
    """Stream a content-addressed blob from the run's FileDatabase root.

    The blob_hash is sha256 hex; payload type (PNG, plotly JSON,
    pickled bytes) is determined by the caller.
    """
    cache = get_cache(_resolve_run_path(run_path))
    data = cache.get_blob(blob_hash)
    if data is None:
        return Response(status_code=404, content=b"blob not found")
    return Response(content=data, media_type="application/octet-stream")


@app.get("/api/flows")
def list_flows(
    root: Annotated[str, Query(description="Root directory containing flows/")]
) -> dict:
    """List all flows and their aggregated task DAGs.

    Returns ``{flows: [{name, run_count, latest_run, nodes, edges}]}``.

    For each flow, walks every run's events.pb and aggregates
    TaskNode/TaskEdge events by task_name. Each node's info reflects
    the most recent run; edges are a union across all runs.
    """
    root_path = Path(root).resolve()
    flows_map = discover_flows(root_path)

    result_flows: list[dict] = []
    for flow_name, runs in flows_map.items():
        # Aggregate across runs (oldest first so newer overwrites)
        nodes: dict[str, dict] = {}
        node_run_counts: dict[str, int] = {}
        edges: set[tuple[str, str]] = set()
        latest_ts_per_node: dict[str, float] = {}

        for run_dir in reversed(runs):  # oldest first
            cache = get_cache(run_dir)
            # Dedupe nodes within one run by task_name — prefer
            # terminal states ("success"/"failed") over "cached", which
            # only indicates the task was already computed earlier in
            # the same run (engine.evaluate is called once per
            # top-level task, so cached repeats are noise here).
            priority = {"cached": 0, "running": 1, "success": 2, "failed": 2}
            run_nodes: dict[str, tuple[str, str, float, str, float, str]] = {}
            for (
                task_name,
                task_hash,
                status,
                duration,
                error,
                ts,
                params,
            ) in cache.task_nodes:
                if not task_name:
                    continue
                existing = run_nodes.get(task_name)
                if existing is not None:
                    if priority.get(status, 0) < priority.get(existing[1], 0):
                        continue
                run_nodes[task_name] = (task_hash, status, duration, error, ts, params)

            for task_name, data in run_nodes.items():
                task_hash, status, duration, error, ts, params = data
                node_run_counts[task_name] = (
                    node_run_counts.get(task_name, 0) + 1
                )
                if (
                    task_name not in latest_ts_per_node
                    or ts >= latest_ts_per_node[task_name]
                ):
                    latest_ts_per_node[task_name] = ts
                    nodes[task_name] = {
                        "name": task_name,
                        "task_hash": task_hash,
                        "status": status,
                        "duration": duration,
                        "error": error,
                        "timestamp": ts,
                        "params": params,
                    }

            for from_t, to_t in cache.task_edges:
                if from_t and to_t:
                    edges.add((from_t, to_t))

        for name, count in node_run_counts.items():
            nodes[name]["run_count"] = count

        result_flows.append(
            {
                "name": flow_name,
                "run_count": len(runs),
                "latest_run": runs[0].name if runs else None,
                "nodes": list(nodes.values()),
                "edges": [
                    {"from": f, "to": t} for f, t in sorted(edges)
                ],
            }
        )

    result_flows.sort(key=lambda f: f["name"])
    return {"flows": result_flows}


@app.get("/api/flows/{flow_name}/tasks/{task_name}/history")
def task_history(
    flow_name: str,
    task_name: str,
    root: Annotated[str, Query(description="Root directory containing flows/")],
) -> dict:
    """Return per-run history for one task within a flow.

    Returns ``{history: [{run_id, status, duration, timestamp, error,
    task_hash}]}`` sorted newest-first.
    """
    root_path = Path(root).resolve()
    flows_map = discover_flows(root_path)
    runs = flows_map.get(flow_name, [])

    priority = {"cached": 0, "running": 1, "success": 2, "failed": 2}

    history: list[dict] = []
    for run_dir in runs:
        cache = get_cache(run_dir)
        # Find the "best" TaskNode matching task_name in this run —
        # terminal states (success/failed) win over "cached", which only
        # marks a repeated reference within the same run.
        match: tuple[str, str, float, str, float, str] | None = None
        for (
            name,
            task_hash,
            status,
            duration,
            error,
            ts,
            params,
        ) in cache.task_nodes:
            if name != task_name:
                continue
            if (
                match is None
                or priority.get(status, 0) >= priority.get(match[1], 0)
            ):
                match = (task_hash, status, duration, error, ts, params)
        if match is None:
            continue
        task_hash, status, duration, error, ts, params = match
        history.append(
            {
                "run_id": run_dir.name,
                "run_path": str(run_dir),
                "status": status,
                "duration": duration,
                "timestamp": ts,
                "error": error,
                "task_hash": task_hash,
                "params": params,
            }
        )

    return {"history": history}


def get_frontend_dist_path() -> Path:
    """Get the path to the frontend dist directory."""
    return Path(__file__).parent.parent / "frontend" / "dist"


def create_app_with_static() -> FastAPI:
    """Create the FastAPI app with static file serving for production."""
    dist_path = get_frontend_dist_path()

    if dist_path.exists():
        # Serve frontend static files at root
        app.mount(
            "/",
            StaticFiles(directory=str(dist_path), html=True),
            name="frontend",
        )

    return app
