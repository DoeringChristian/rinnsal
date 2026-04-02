"""FastAPI backend for the rinnsal viewer."""

from __future__ import annotations

import base64
import os
from pathlib import Path
from typing import Annotated

from fastapi import FastAPI, Query, Response
from fastapi.staticfiles import StaticFiles

from rinnsal.viewer._data import (
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
) -> list[str]:
    """List all runs under the given root directory."""
    root_path = Path(root).resolve()
    if not root_path.exists():
        return []

    runs = discover_runs(root_path)
    return [str(r) for r in runs]


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
        result[tag] = [{"it": it} for it, _img, _data, _interactive in data]
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
    for fig_it, image, _data, _interactive in figures:
        if fig_it == it:
            return Response(content=image, media_type="image/png")
    return Response(status_code=404, content=b"Figure not found")


@app.get("/api/cards/{run_path:path}")
def get_cards(run_path: str) -> dict:
    """Get card data for a run. Returns {task: [{it, kind, title, content, image?}, ...]}."""
    cache = get_cache(_resolve_run_path(run_path))
    # Cards are stored differently — need to read from events directly
    # For now, read from the events.pb if cards exist
    from rinnsal.logger.event_file import EventFileReader
    from rinnsal.logger.logger import EVENTS_FILE

    events_path = _resolve_run_path(run_path) / EVENTS_FILE
    result: dict[str, list[dict]] = {}

    if not events_path.exists():
        return result

    try:
        for event in EventFileReader(events_path):
            if event.WhichOneof("data") == "card":
                task = event.card.task
                if task not in result:
                    result[task] = []
                card_data: dict = {
                    "it": event.iteration,
                    "kind": event.card.kind,
                    "title": event.card.title,
                    "content": event.card.content,
                }
                if event.card.image:
                    card_data["image"] = base64.b64encode(
                        event.card.image
                    ).decode()
                result[task].append(card_data)
    except (IOError, OSError):
        pass

    return result


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
