"""Unified ``rinnsal`` CLI dispatcher.

Subcommands:
  rinnsal cluster up --host [--aim]    Start a coordinator (+ optional aim server)
  rinnsal cluster up <URL>             Register as a worker with a coordinator

The viewer and metadata-DB subcommands were removed when rinnsal
switched to aim for logging — ``aim up`` is the UI. ``rinnsal cluster
up --host --aim`` will also spawn an ``aim server`` subprocess next to
the coordinator so remote workers have a network-reachable aim repo.
"""

from __future__ import annotations

import argparse
import sys


# Default port for the embedded aim tracking server when ``--aim`` is
# passed. Fixed rather than derived from the coordinator port so users
# can hard-code it in scripts. Override with ``--aim-port``.
DEFAULT_AIM_PORT = 43800


def _add_cluster(sub: argparse._SubParsersAction) -> None:
    p = sub.add_parser("cluster", help="Cluster mode (coordinator + workers)")
    cs = p.add_subparsers(dest="cluster_cmd", required=True)

    up = cs.add_parser(
        "up",
        help=(
            "Start a coordinator (--host) or a worker daemon (positional URL)"
        ),
    )
    up.add_argument(
        "host_url",
        nargs="?",
        default=None,
        help="Coordinator URL to register with (worker mode)",
    )
    up.add_argument(
        "--host",
        action="store_true",
        help=(
            "Run as the coordinator (also a local worker by default; "
            "see --no-worker)"
        ),
    )
    up.add_argument(
        "--port",
        type=int,
        default=8800,
        help="Port for the coordinator (default 8800)",
    )
    up.add_argument(
        "--bind",
        type=str,
        default="0.0.0.0",
        help="Bind interface for the coordinator (default 0.0.0.0)",
    )
    up.add_argument(
        "--name",
        type=str,
        default=None,
        help="Worker name (defaults to hostname)",
    )
    up.add_argument(
        "--no-worker",
        action="store_true",
        help="Coordinator only — don't also register as a worker",
    )
    up.add_argument(
        "--aim",
        action="store_true",
        help=(
            "Also spawn an aim tracking server alongside the coordinator. "
            "Workers using ``rinnsal.aim.AimLogger`` will auto-discover it "
            "via GET /api/cluster/aim."
        ),
    )
    up.add_argument(
        "--aim-port",
        type=int,
        default=DEFAULT_AIM_PORT,
        help=f"Port for the aim tracking server (default {DEFAULT_AIM_PORT})",
    )
    up.add_argument(
        "--aim-repo",
        type=str,
        default=".aim",
        help="aim repo path (default ./.aim)",
    )


def _cmd_cluster_up(args: argparse.Namespace) -> int:
    """Coordinator (--host) or worker (positional URL)."""
    if args.host:
        return _run_coordinator(args)
    if not args.host_url:
        print(
            "rinnsal cluster up: pass --host to start a coordinator, "
            "or a coordinator URL to register as a worker."
        )
        return 2
    return _run_worker(args)


def _spawn_aim_server(repo: str, host: str, port: int) -> object | None:
    """Start ``aim server --repo <repo> --host <host> --port <port>``.

    Returns the Popen handle so the caller can terminate it on shutdown.
    None when aim isn't available or the server fails to start.
    """
    import shutil
    import subprocess
    from pathlib import Path

    if shutil.which("aim") is None:
        print(
            "warning: --aim requested but `aim` binary not on PATH. "
            "Install with: pip install 'aim>=3.25,<4'",
            file=sys.stderr,
        )
        return None

    repo_path = Path(repo).expanduser().resolve()
    repo_path.mkdir(parents=True, exist_ok=True)
    # `aim init` is idempotent and only writes if the repo is empty.
    try:
        subprocess.run(
            ["aim", "init", "--repo", str(repo_path)],
            capture_output=True, check=False, timeout=30,
        )
    except Exception as e:
        print(f"warning: aim init failed: {e}", file=sys.stderr)
        return None

    try:
        proc = subprocess.Popen(
            [
                "aim", "server",
                "--repo", str(repo_path),
                "--host", host,
                "--port", str(port),
            ],
        )
    except Exception as e:
        print(f"warning: failed to spawn aim server: {e}", file=sys.stderr)
        return None
    return proc


def _run_coordinator(args: argparse.Namespace) -> int:
    try:
        import uvicorn  # noqa: F401
        from fastapi import FastAPI  # noqa: F401
    except ImportError:
        print(
            "Cluster mode requires fastapi + uvicorn. "
            "Install with: pip install 'rinnsal[cluster]'"
        )
        return 1

    from fastapi import FastAPI

    from rinnsal.cluster.coordinator import (
        CoordinatorState,
        router as cluster_router,
    )

    state = CoordinatorState()
    app = FastAPI(title="rinnsal coordinator")
    app.state.cluster = state
    app.include_router(cluster_router, prefix="/api/cluster")

    # Optional aim tracking server.
    aim_proc = None
    aim_repo_url: str | None = None
    if args.aim:
        aim_proc = _spawn_aim_server(args.aim_repo, args.bind, args.aim_port)
        if aim_proc is not None:
            # Advertise a reachable URL — bind "0.0.0.0" in the URL is
            # never useful to clients, replace with the coordinator
            # host so the workers know where to connect.
            host_for_url = args.bind if args.bind not in ("0.0.0.0", "::") \
                else "0.0.0.0"
            aim_repo_url = f"aim://{host_for_url}:{args.aim_port}"
            state.aim_repo_url = aim_repo_url

    # Self-register as a local worker (unless --no-worker).
    local_worker = None
    if not args.no_worker:
        from rinnsal.cluster.worker import WorkerDaemon

        local_url = f"http://127.0.0.1:{args.port}"
        local_worker = WorkerDaemon(local_url, name=args.name or "self")

        def _self_register() -> None:
            import time
            import urllib.error
            import urllib.request

            for _ in range(50):
                try:
                    urllib.request.urlopen(
                        f"{local_url}/api/cluster/health", timeout=1.0
                    )
                    break
                except (urllib.error.URLError, OSError):
                    time.sleep(0.1)
            try:
                local_worker.start()
                # ``start()`` only spawns the heartbeat thread. The
                # self-worker also needs the long-poll job loop, or the
                # coordinator will queue jobs indefinitely with no one to
                # pick them up.
                local_worker.start_job_loop()
            except Exception as e:
                print(
                    f"warning: local worker registration failed: {e}",
                    file=sys.stderr,
                )

        import threading

        threading.Thread(target=_self_register, daemon=True).start()

    print(f"[rinnsal] Cluster coordinator on http://{args.bind}:{args.port}")
    if aim_repo_url is not None:
        print(f"[rinnsal]   Aim tracking : {aim_repo_url}")
        print(
            f"[rinnsal]   Aim UI       : run `aim up --repo {args.aim_repo}`"
        )
    if not args.no_worker:
        print(f"[rinnsal]   Workers      : 1 (self)")
    print(
        f"[rinnsal] Other machines: rinnsal cluster up "
        f"http://<this-host>:{args.port}"
    )
    print(
        f"[rinnsal] Submit flows : python my_flow.py --executor "
        f"cluster:http://<this-host>:{args.port}"
    )

    try:
        import uvicorn

        uvicorn.run(
            app,
            host=args.bind,
            port=args.port,
            log_level="warning",
        )
    finally:
        if local_worker is not None:
            local_worker.stop()
        if aim_proc is not None:
            try:
                aim_proc.terminate()
                aim_proc.wait(timeout=5)
            except Exception:
                pass
    return 0


def _run_worker(args: argparse.Namespace) -> int:
    try:
        import httpx  # noqa: F401
    except ImportError:
        print(
            "Worker mode requires httpx. "
            "Install with: pip install 'rinnsal[cluster]'"
        )
        return 1

    from rinnsal.cluster.worker import WorkerDaemon

    url = args.host_url
    if not url.startswith(("http://", "https://")):
        url = f"http://{url}"
    daemon = WorkerDaemon(url, name=args.name)
    print(f"[rinnsal] Worker registering with {url}…")
    daemon.run_forever()
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="rinnsal",
        description=(
            "rinnsal — declarative DAG execution. "
            "Use `aim up` for the UI; `rinnsal cluster up` for clusters."
        ),
    )
    sub = parser.add_subparsers(dest="cmd", required=True)
    _add_cluster(sub)

    args = parser.parse_args(argv)

    if args.cmd == "cluster":
        if args.cluster_cmd == "up":
            return _cmd_cluster_up(args)
    parser.print_help()
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
