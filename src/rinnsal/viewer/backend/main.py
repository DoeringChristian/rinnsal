"""FastAPI backend for the rinnsal viewer."""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import Annotated

from fastapi import FastAPI, Query, Response, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles

from rinnsal.logger.logger import EVENTS_FILE
from rinnsal.viewer._data import discover_runs, is_run_directory

app = FastAPI(title="Rinnsal Viewer")


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


@app.get("/api/events/{run_path:path}")
def get_events(run_path: str) -> Response:
    """Get raw protobuf events for a run."""
    path = Path(run_path)

    # Handle URL-encoded paths
    if not path.is_absolute():
        path = Path("/") / run_path

    events_file = path / EVENTS_FILE

    if not events_file.exists():
        return Response(content=b"", media_type="application/x-protobuf")

    return Response(
        content=events_file.read_bytes(),
        media_type="application/x-protobuf",
    )


@app.websocket("/api/events/{run_path:path}/stream")
async def stream_events(websocket: WebSocket, run_path: str) -> None:
    """Stream new events as they're written to events.pb."""
    await websocket.accept()

    path = Path(run_path)
    if not path.is_absolute():
        path = Path("/") / run_path

    events_file = path / EVENTS_FILE

    # Track file position
    last_size = events_file.stat().st_size if events_file.exists() else 0

    try:
        while True:
            await asyncio.sleep(0.5)  # Poll interval

            if not events_file.exists():
                continue

            try:
                current_size = events_file.stat().st_size
            except OSError:
                continue

            if current_size > last_size:
                # Send only new bytes
                with open(events_file, "rb") as f:
                    f.seek(last_size)
                    new_bytes = f.read()

                if new_bytes:
                    await websocket.send_bytes(new_bytes)

                last_size = current_size

    except WebSocketDisconnect:
        pass


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
