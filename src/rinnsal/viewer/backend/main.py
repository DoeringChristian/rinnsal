"""FastAPI backend for the rinnsal viewer."""

from __future__ import annotations

import base64
import logging
import os
import time
from email.utils import formatdate, parsedate_to_datetime
from pathlib import Path
from typing import Annotated

from fastapi import FastAPI, Header, Query, Request, Response
from fastapi.staticfiles import StaticFiles

from rinnsal.data.logger.logger import EVENTS_FILE
from rinnsal.viewer._data import (
    discover_flows,
    discover_runs,
    get_cache,
    is_run_directory,
)

# Dedicated logger so --debug on the viewer CLI controls viewer output
# without changing application-wide logging behavior.
log = logging.getLogger("rinnsal.viewer")


def enable_debug_logging(level: int = logging.DEBUG) -> None:
    """Turn on verbose per-request timing + I/O tracing.

    Called from ``rinnsal-viewer --debug``. Formats each log line so
    timings are easy to scan over a slow link:

        [viewer] GET /api/flows?root=... -> 200 in 142ms
        [viewer]   discover_flows(/..): 8 flows, 42 runs in 12ms
        [viewer]   get_task_graph(.../runs/r7): events.pb 120MB scan 9.4s
    """
    log.setLevel(level)
    if not log.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("[viewer] %(message)s"))
        log.addHandler(handler)
    log.propagate = False


app = FastAPI(title="Rinnsal Viewer")


@app.middleware("http")
async def _time_requests(request: Request, call_next):
    """Log every request's path + status + duration when debug is on."""
    if not log.isEnabledFor(logging.INFO):
        return await call_next(request)

    t0 = time.perf_counter()
    response = await call_next(request)
    elapsed = (time.perf_counter() - t0) * 1000

    # Truncate the query string at a sensible length so log lines stay readable.
    qs = request.url.query
    if len(qs) > 120:
        qs = qs[:120] + "…"
    path = f"{request.url.path}?{qs}" if qs else request.url.path
    log.info(
        "%s %s -> %d in %.0fms",
        request.method,
        path,
        response.status_code,
        elapsed,
    )
    return response


def _resolve_run_path(run_path: str) -> Path:
    """Resolve a URL-encoded run path to an absolute Path."""
    path = Path(run_path)
    if not path.is_absolute():
        path = Path("/") / run_path
    return path


def _events_mtime(run_path: Path) -> float | None:
    """Return events.pb mtime for a run, or None if missing."""
    try:
        return (run_path / EVENTS_FILE).stat().st_mtime
    except OSError:
        return None


def _http_date(ts: float) -> str:
    return formatdate(ts, usegmt=True)


def _parse_http_date(value: str) -> float | None:
    try:
        return parsedate_to_datetime(value).timestamp()
    except (TypeError, ValueError):
        return None


def _listing_headers(run_path: Path) -> dict[str, str]:
    """Cache headers for per-run JSON listings.

    The events.pb mtime is the single source of freshness — if the file
    hasn't changed, listings are guaranteed unchanged (RunCache.is_stale
    uses the same predicate). Browser revalidates with
    If-Modified-Since; we return 304 when the timestamps match.
    """
    mtime = _events_mtime(run_path)
    if mtime is None:
        return {"Cache-Control": "no-cache"}
    return {
        "Cache-Control": "public, max-age=0, must-revalidate",
        "Last-Modified": _http_date(mtime),
    }


def _not_modified(
    run_path: Path, if_modified_since: str | None
) -> Response | None:
    """Return a 304 response if the client's cached copy is still fresh."""
    if not if_modified_since:
        return None
    mtime = _events_mtime(run_path)
    if mtime is None:
        return None
    client_ts = _parse_http_date(if_modified_since)
    if client_ts is None:
        return None
    # Compare at second granularity (HTTP dates have 1s resolution).
    if int(mtime) <= int(client_ts):
        return Response(
            status_code=304,
            headers={
                "Last-Modified": _http_date(mtime),
                "Cache-Control": "public, max-age=0, must-revalidate",
            },
        )
    return None


def _blob_headers(blob_hash: str) -> dict[str, str]:
    """Cache headers for content-addressed blobs.

    Blobs are immutable by construction (sha256 hash = content identity).
    Tell the browser to keep them forever and never revalidate.
    """
    return {
        "Cache-Control": "public, max-age=31536000, immutable",
        "ETag": f'"{blob_hash}"',
    }


@app.get("/api/config")
def get_config() -> dict:
    """Get initial configuration from environment."""
    log_dir = os.environ.get("RINNSAL_LOG_DIR", "")
    return {"logDir": log_dir}


@app.get("/api/index/status")
def index_status() -> dict:
    """Backfill progress for the viewer's sidebar spinner.

    Returns ``{indexing, done, total, current, started_at, finished_at,
    errors}``. When no backfill is registered (e.g. tests, manual server),
    returns an idle snapshot.
    """
    from rinnsal.data.metadata.backfill import get_global_backfiller

    bf = get_global_backfiller()
    if bf is None:
        return {
            "indexing": False,
            "done": 0,
            "total": 0,
            "current": None,
            "started_at": None,
            "finished_at": None,
            "errors": [],
        }
    return bf.progress.to_dict()


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


def _cached_json(
    run_path: Path,
    if_modified_since: str | None,
    build: callable,  # type: ignore[valid-type]
) -> Response:
    """Shared flow for JSON listing endpoints: 304 when possible, else 200 with Last-Modified."""
    from fastapi.responses import JSONResponse

    not_mod = _not_modified(run_path, if_modified_since)
    if not_mod is not None:
        return not_mod
    body = build()
    return JSONResponse(content=body, headers=_listing_headers(run_path))


def _blob_response(
    data: bytes,
    blob_hash: str,
    if_none_match: str | None,
    media_type: str = "application/octet-stream",
) -> Response:
    """Serve a content-addressed payload with long-lived cache + ETag."""
    etag = f'"{blob_hash}"'
    if if_none_match and if_none_match.strip() == etag:
        return Response(status_code=304, headers=_blob_headers(blob_hash))
    return Response(
        content=data,
        media_type=media_type,
        headers=_blob_headers(blob_hash),
    )


@app.get("/api/scalars/{run_path:path}")
def get_scalars(
    run_path: str,
    if_modified_since: str | None = Header(default=None),
) -> Response:
    """Get scalar data for a run. Returns {tag: [{it, value, timestamp}, ...]}."""
    rp = _resolve_run_path(run_path)

    def build() -> dict:
        cache = get_cache(rp)
        return {
            tag: [{"it": it, "value": val, "ts": ts} for it, val, ts in data]
            for tag, data in cache.scalars.items()
        }

    return _cached_json(rp, if_modified_since, build)


@app.get("/api/text/{run_path:path}")
def get_text(
    run_path: str,
    if_modified_since: str | None = Header(default=None),
) -> Response:
    """Get text data for a run. Returns {tag: [{it, value}, ...]}."""
    rp = _resolve_run_path(run_path)

    def build() -> dict:
        cache = get_cache(rp)
        return {
            tag: [{"it": it, "value": val} for it, val in data]
            for tag, data in cache.text.items()
        }

    return _cached_json(rp, if_modified_since, build)


@app.get("/api/figures/{run_path:path}")
def get_figures_meta(
    run_path: str,
    if_modified_since: str | None = Header(default=None),
) -> Response:
    """Get figure metadata (no image bytes). Returns {tag: [{it}, ...]}."""
    rp = _resolve_run_path(run_path)

    def build() -> dict:
        cache = get_cache(rp)
        return {
            tag: [{"it": entry[0]} for entry in data]
            for tag, data in cache.figures.items()
        }

    return _cached_json(rp, if_modified_since, build)


def _figure_etag(run_path: Path, tag: str, it: int, image_blob_hash: str) -> str:
    """Stable ETag for a figure bytes response.

    Prefer the content hash when available (true immutability); fall
    back to run mtime + tag + it for inline figures. Either way, the
    same figure from the same run returns the same ETag.
    """
    if image_blob_hash:
        return f'"{image_blob_hash}"'
    mtime = _events_mtime(run_path) or 0
    return f'"{int(mtime)}-{tag}-{it}"'


@app.get("/api/figure/{run_path:path}")
def get_figure_image(
    run_path: str,
    tag: str = Query(...),
    it: int = Query(...),
    if_none_match: str | None = Header(default=None),
) -> Response:
    """Get a single figure image as PNG."""
    rp = _resolve_run_path(run_path)
    cache = get_cache(rp)
    figures = cache.figures.get(tag, [])
    for fig_it, image, image_blob_hash, _data, _data_blob, _interactive, _fmt in figures:
        if fig_it == it:
            png = image or (cache.get_blob(image_blob_hash) or b"")
            if png:
                etag = _figure_etag(rp, tag, it, image_blob_hash)
                if if_none_match and if_none_match.strip() == etag:
                    return Response(
                        status_code=304,
                        headers={
                            "Cache-Control": "public, max-age=31536000, immutable",
                            "ETag": etag,
                        },
                    )
                return Response(
                    content=png,
                    media_type="image/png",
                    headers={
                        "Cache-Control": "public, max-age=31536000, immutable",
                        "ETag": etag,
                    },
                )
    return Response(status_code=404, content=b"Figure not found")


@app.get("/api/images/{run_path:path}")
def get_images_meta(
    run_path: str,
    if_modified_since: str | None = Header(default=None),
) -> Response:
    """Get image metadata (no pixel data). Returns {tag: [{it, width, height}]}."""
    rp = _resolve_run_path(run_path)

    def build() -> dict:
        cache = get_cache(rp)
        return {
            tag: [
                {"it": it, "width": w, "height": h}
                for it, _png, _blob, w, h in data
            ]
            for tag, data in cache.images.items()
        }

    return _cached_json(rp, if_modified_since, build)


@app.get("/api/image/{run_path:path}")
def get_image(
    run_path: str,
    tag: str = Query(...),
    it: int = Query(...),
    if_none_match: str | None = Header(default=None),
) -> Response:
    """Get a single image as PNG."""
    rp = _resolve_run_path(run_path)
    cache = get_cache(rp)
    images = cache.images.get(tag, [])
    for img_it, png_data, blob_hash, _w, _h in images:
        if img_it == it:
            png = png_data or (cache.get_blob(blob_hash) or b"")
            if png:
                etag = _figure_etag(rp, tag, it, blob_hash)
                if if_none_match and if_none_match.strip() == etag:
                    return Response(
                        status_code=304,
                        headers={
                            "Cache-Control": "public, max-age=31536000, immutable",
                            "ETag": etag,
                        },
                    )
                return Response(
                    content=png,
                    media_type="image/png",
                    headers={
                        "Cache-Control": "public, max-age=31536000, immutable",
                        "ETag": etag,
                    },
                )
    return Response(status_code=404, content=b"Image not found")


@app.get("/api/cards/{run_path:path}")
def get_cards_index(
    run_path: str,
    if_modified_since: str | None = Header(default=None),
) -> Response:
    """List every named card in a run."""
    rp = _resolve_run_path(run_path)

    def build() -> dict:
        cache = get_cache(rp)
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

    return _cached_json(rp, if_modified_since, build)


@app.get("/api/card/{run_path:path}")
def get_card(
    run_path: str,
    name: str = Query(...),
    task: str = Query(""),
    it: int | None = Query(None),
    if_modified_since: str | None = Header(default=None),
) -> Response:
    """Get a single card snapshot.

    When *it* is omitted, returns the latest emission. Components are
    returned with metadata + (small) inline payloads; heavy payloads
    must be fetched via the ``/api/blob`` endpoint using the hashes in
    each component dict.
    """
    rp = _resolve_run_path(run_path)

    def build() -> dict:
        cache = get_cache(rp)
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

    return _cached_json(rp, if_modified_since, build)


@app.get("/api/tags/{run_path:path}")
def get_tags(
    run_path: str,
    if_modified_since: str | None = Header(default=None),
) -> Response:
    """Unified listing of every (tag, kind) emitted in a run."""
    rp = _resolve_run_path(run_path)

    def build() -> dict:
        cache = get_cache(rp)
        return {"tags": cache.tags()}

    return _cached_json(rp, if_modified_since, build)


@app.get("/api/blob/{run_path:path}/{blob_hash}")
def get_blob(
    run_path: str,
    blob_hash: str,
    if_none_match: str | None = Header(default=None),
) -> Response:
    """Stream a content-addressed blob from the run's FileDatabase root.

    Content-addressed: the blob_hash IS the content identity, so the
    response can be cached forever. A matching If-None-Match yields 304.
    """
    cache = get_cache(_resolve_run_path(run_path))
    data = cache.get_blob(blob_hash)
    if data is None:
        return Response(status_code=404, content=b"blob not found")
    return _blob_response(data, blob_hash, if_none_match)


def _metadata_store_for(root_path: Path):
    """Return a SqliteMetadataStore rooted at the given DB root.

    Engines are process-cached by absolute path, so repeat callers
    share connections.
    """
    from rinnsal.data.metadata import SqliteMetadataStore

    return SqliteMetadataStore(root_path / "metadata.sqlite")


def _flows_last_modified(root_path: Path) -> float | None:
    """Max(finished_at, started_at) across every run under the flow tree.

    Backed by a single SQL query (no filesystem walk).
    """
    try:
        store = _metadata_store_for(root_path)
    except Exception:
        return None
    return store.latest_updated_at()


def _flows_not_modified(
    root_path: Path, if_modified_since: str | None
) -> Response | None:
    if not if_modified_since:
        return None
    latest = _flows_last_modified(root_path)
    if latest is None:
        return None
    client_ts = _parse_http_date(if_modified_since)
    if client_ts is None:
        return None
    if int(latest) <= int(client_ts):
        return Response(
            status_code=304,
            headers={
                "Last-Modified": _http_date(latest),
                "Cache-Control": "public, max-age=0, must-revalidate",
            },
        )
    return None


def _flows_headers(root_path: Path) -> dict[str, str]:
    latest = _flows_last_modified(root_path)
    if latest is None:
        return {"Cache-Control": "no-cache"}
    return {
        "Cache-Control": "public, max-age=0, must-revalidate",
        "Last-Modified": _http_date(latest),
    }


@app.get("/api/flows")
def list_flows(
    root: Annotated[str, Query(description="Root directory containing flows/")],
    if_modified_since: str | None = Header(default=None),
) -> Response:
    """List flows + their latest-run task DAG.

    Backed by the metadata DB (SqliteMetadataStore). Sub-millisecond
    regardless of how many figures/cards each run contains. The DAG
    aggregation walks only the LATEST run per flow (a SQL query) —
    legacy pre-DB code aggregated across every run via events.pb scan.
    """
    root_path = Path(root).resolve()
    not_mod = _flows_not_modified(root_path, if_modified_since)
    if not_mod is not None:
        log.debug("list_flows %s: 304 Not Modified", root_path)
        return not_mod

    t0 = time.perf_counter()
    store = _metadata_store_for(root_path)
    flows = store.list_flows()
    log.debug(
        "list_flows %s: %d flows from DB in %.0fms",
        root_path, len(flows), (time.perf_counter() - t0) * 1000,
    )

    result_flows: list[dict] = []
    for fs in flows:
        nodes: list[dict] = []
        edges: list[dict] = []
        if fs.latest_run_id is not None:
            for n in store.list_task_nodes(fs.latest_run_id):
                nodes.append(
                    {
                        "name": n.task_name,
                        "task_hash": n.task_hash,
                        "status": n.status,
                        "duration": n.duration,
                        "error": n.error,
                        "timestamp": n.ts,
                        "params": n.params_json,
                        "run_count": fs.run_count,
                    }
                )
            edges = [
                {"from": f, "to": t}
                for f, t in store.list_task_edges(fs.latest_run_id)
            ]
        result_flows.append(
            {
                "name": fs.name,
                "run_count": fs.run_count,
                "latest_run": fs.latest_run_id,
                "nodes": nodes,
                "edges": edges,
            }
        )

    from fastapi.responses import JSONResponse
    return JSONResponse(
        content={"flows": result_flows},
        headers=_flows_headers(root_path),
    )


@app.get("/api/flows/{flow_name}/tasks/{task_name}/history")
def task_history(
    flow_name: str,
    task_name: str,
    root: Annotated[str, Query(description="Root directory containing flows/")],
    if_modified_since: str | None = Header(default=None),
) -> Response:
    """Per-run history for one task within a flow (newest-first).

    Backed by a single SQL JOIN on (runs, task_nodes).
    """
    root_path = Path(root).resolve()
    not_mod = _flows_not_modified(root_path, if_modified_since)
    if not_mod is not None:
        return not_mod
    store = _metadata_store_for(root_path)
    history = [
        {
            "run_id": h.run_id,
            "run_path": h.run_dir,
            "status": h.status,
            "duration": h.duration,
            "timestamp": h.ts,
            "error": h.error,
            "task_hash": h.task_hash,
            "params": h.params_json,
        }
        for h in store.task_history(flow_name, task_name)
    ]
    from fastapi.responses import JSONResponse
    return JSONResponse(
        content={"history": history},
        headers=_flows_headers(root_path),
    )


@app.get("/api/flows/{flow_name}/dag")
def flow_dag(
    flow_name: str,
    root: Annotated[str, Query(description="Root directory containing flows/")],
    if_modified_since: str | None = Header(default=None),
) -> Response:
    """Task DAG (nodes + edges) for the latest run of a flow."""
    root_path = Path(root).resolve()
    not_mod = _flows_not_modified(root_path, if_modified_since)
    if not_mod is not None:
        return not_mod
    store = _metadata_store_for(root_path)
    runs = store.list_runs(flow_name=flow_name, limit=1)
    if not runs:
        from fastapi.responses import JSONResponse
        return JSONResponse(
            content={"nodes": [], "edges": []},
            headers=_flows_headers(root_path),
        )
    run = runs[0]
    nodes = [
        {
            "name": n.task_name,
            "task_hash": n.task_hash,
            "status": n.status,
            "duration": n.duration,
            "error": n.error,
            "timestamp": n.ts,
            "params": n.params_json,
        }
        for n in store.list_task_nodes(run.run_id)
    ]
    edges = [
        {"from": f, "to": t}
        for f, t in store.list_task_edges(run.run_id)
    ]
    from fastapi.responses import JSONResponse
    return JSONResponse(
        content={"run_id": run.run_id, "nodes": nodes, "edges": edges},
        headers=_flows_headers(root_path),
    )


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
