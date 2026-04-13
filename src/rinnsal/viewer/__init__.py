"""Rinnsal log viewer - a web-based experiment dashboard.

Install with: pip install rinnsal[viewer]
Run with: python -m rinnsal.viewer

By default, looks for runs in .rinnsal/ in the current directory.
"""

from __future__ import annotations

import argparse
import os
import socket
import subprocess
import sys
from pathlib import Path


def _find_free_port(start: int, max_attempts: int = 100) -> int:
    """Find a free port starting from the given port number."""
    for offset in range(max_attempts):
        port = start + offset
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(("127.0.0.1", port))
                return port
            except OSError:
                continue
    raise RuntimeError(
        f"No free port found in range {start}-{start + max_attempts - 1}"
    )


def _get_frontend_dist() -> Path:
    """Get path to frontend dist directory."""
    return Path(__file__).parent / "frontend" / "dist"


def _build_frontend_if_needed() -> bool:
    """Build frontend if dist doesn't exist. Returns True if available."""
    dist = _get_frontend_dist()
    if (dist / "index.html").exists():
        return True

    frontend_dir = Path(__file__).parent / "frontend"
    if not (frontend_dir / "package.json").exists():
        return False

    print("Frontend dist not found. Attempting to build...")
    print(f"  (looked in {dist})")
    try:
        subprocess.run(
            ["npm", "install"],
            cwd=frontend_dir,
            check=True,
            capture_output=True,
        )
        subprocess.run(
            ["npm", "run", "build"],
            cwd=frontend_dir,
            check=True,
            capture_output=True,
        )
        return True
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        print(f"Failed to build frontend: {e}")
        print("Hint: run 'npm run build' in src/rinnsal/viewer/frontend/ "
              "and commit the dist/ folder.")
        return False


def run(
    log_path: str | Path | None = None,
    port: int = 8765,
    host: str = "127.0.0.1",
    debug: bool = False,
) -> None:
    """Run the viewer server.

    Args:
        log_path: Optional log directory to open on start.
        port: Port to run the server on. If busy, the next free port is used.
        host: Interface to bind to. Defaults to ``127.0.0.1`` (loopback only).
            Use ``0.0.0.0`` to accept connections from other machines on the
            network (e.g. over Tailscale or LAN).
        debug: When True, print every HTTP request with its duration plus
            I/O breakdown for slow endpoints (per-run task-graph load
            times, file sizes, record counts).
    """
    try:
        import uvicorn
    except ImportError:
        print(
            "Viewer dependencies not installed. "
            "Install with: pip install rinnsal[viewer]"
        )
        raise SystemExit(1)

    # Build frontend if needed
    if not _build_frontend_if_needed():
        print(
            "Warning: Frontend not available. "
            "API-only mode (no web UI)."
        )

    if log_path:
        os.environ["RINNSAL_LOG_DIR"] = str(Path(log_path).resolve())

    # Initialize/upgrade the metadata DB and queue a background backfill
    # for any runs that exist on disk but have no DB row yet. The viewer
    # is responsive immediately; a sidebar spinner shows progress via
    # /api/index/status.
    if log_path:
        try:
            from rinnsal.data.metadata import SqliteMetadataStore
            from rinnsal.data.metadata.backfill import (
                Backfiller,
                set_global_backfiller,
            )

            log_root = Path(log_path).resolve()
            if log_root.exists():
                store = SqliteMetadataStore(log_root / "metadata.sqlite")
                bf = Backfiller(store, log_root)
                set_global_backfiller(bf)
                bf.run_async()
        except Exception as e:
            print(f"warning: backfill not started ({e})")

    port = _find_free_port(port)
    display_host = "localhost" if host in ("127.0.0.1", "localhost") else host
    print(f"Starting rinnsal viewer on http://{display_host}:{port}")
    if host == "0.0.0.0":
        print(
            "  (accepting connections from all network interfaces — "
            "anyone on this network can reach the viewer)"
        )

    # Import and configure the app
    from rinnsal.viewer.backend.main import (
        create_app_with_static,
        enable_debug_logging,
    )

    if debug:
        enable_debug_logging()
        print("  (debug logging enabled — will print every request + I/O timings)")

    app = create_app_with_static()

    uvicorn.run(
        app,
        host=host,
        port=port,
        log_level="info" if debug else "warning",
    )


def main() -> None:
    """CLI entry point for the viewer."""
    parser = argparse.ArgumentParser(description="Rinnsal Log Viewer")
    parser.add_argument(
        "log_dir",
        nargs="?",
        default=".rinnsal",
        help="Log directory to view (default: .rinnsal)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8765,
        help="Port to run the server on (default: 8765)",
    )
    parser.add_argument(
        "--host",
        type=str,
        default="127.0.0.1",
        help=(
            "Interface to bind to. Default 127.0.0.1 (loopback only). "
            "Pass 0.0.0.0 to accept connections from other machines "
            "(e.g. over Tailscale or LAN)."
        ),
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help=(
            "Print every HTTP request with its duration, plus I/O "
            "breakdowns for slow endpoints. Useful for diagnosing "
            "'why is the viewer hanging' over a slow connection."
        ),
    )
    args = parser.parse_args()

    run(args.log_dir, args.port, host=args.host, debug=args.debug)
