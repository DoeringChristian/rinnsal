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


def run(log_path: str | Path | None = None, port: int = 8765) -> None:
    """Run the viewer server.

    Args:
        log_path: Optional log directory to open on start.
        port: Port to run the server on. If busy, the next free port is used.
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

    port = _find_free_port(port)
    print(f"Starting rinnsal viewer on http://localhost:{port}")

    # Import and configure the app
    from rinnsal.viewer.backend.main import create_app_with_static

    app = create_app_with_static()

    uvicorn.run(
        app,
        host="127.0.0.1",
        port=port,
        log_level="warning",
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
    args = parser.parse_args()

    run(args.log_dir, args.port)
