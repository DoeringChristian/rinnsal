"""Unified ``rinnsal`` CLI dispatcher.

Subcommands:
  rinnsal up [LOG_DIR]      Start the viewer + auto-init/upgrade the DB.
  rinnsal db upgrade        Apply pending DB migrations.
  rinnsal db rebuild        Drop the DB file and re-init.
  rinnsal db status         Show schema rev + row counts.

``rinnsal-viewer`` script remains as a back-compat alias of ``rinnsal up``.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


def _add_up(sub: argparse._SubParsersAction) -> None:
    p = sub.add_parser(
        "up",
        help="Start the viewer + auto-init the metadata DB",
    )
    p.add_argument(
        "log_dir",
        nargs="?",
        default=".rinnsal",
        help="Log directory to view (default: .rinnsal)",
    )
    p.add_argument(
        "--port",
        type=int,
        default=8765,
        help="Port to bind (default 8765; increments if busy)",
    )
    p.add_argument(
        "--host",
        type=str,
        default="127.0.0.1",
        help=(
            "Interface to bind. Default 127.0.0.1 (loopback only). "
            "Use 0.0.0.0 to expose for LAN/Tailscale."
        ),
    )
    p.add_argument(
        "--debug",
        action="store_true",
        help="Verbose request + I/O timing logs",
    )


def _add_db(sub: argparse._SubParsersAction) -> None:
    p = sub.add_parser("db", help="Metadata database operations")
    db_sub = p.add_subparsers(dest="db_cmd", required=True)

    up = db_sub.add_parser(
        "upgrade",
        help="Apply pending Alembic migrations",
    )
    up.add_argument(
        "--db-path",
        type=str,
        default=".rinnsal",
        help=(
            "Path to the rinnsal directory (containing metadata.sqlite); "
            "default .rinnsal"
        ),
    )

    rb = db_sub.add_parser(
        "rebuild",
        help=(
            "Drop the metadata DB and re-create it. "
            "Backfill from events.pb arrives in DB Phase 4."
        ),
    )
    rb.add_argument("--db-path", type=str, default=".rinnsal")
    rb.add_argument(
        "--yes",
        action="store_true",
        help="Skip confirmation prompt",
    )

    st = db_sub.add_parser(
        "status",
        help="Show schema revision + row counts",
    )
    st.add_argument("--db-path", type=str, default=".rinnsal")


def _resolve_db_path(arg: str) -> Path:
    p = Path(arg).expanduser().resolve()
    if p.suffix == ".sqlite":
        return p
    return p / "metadata.sqlite"


def _cmd_up(args: argparse.Namespace) -> int:
    """rinnsal up — viewer + auto-init DB."""
    from rinnsal.viewer import run as viewer_run

    # Eagerly initialize/upgrade the DB so first request to /api/flows is
    # snappy (no migration delay during the user's first interaction).
    db_path = _resolve_db_path(args.log_dir)
    if db_path.parent.exists():
        try:
            from rinnsal.data.metadata import SqliteMetadataStore

            SqliteMetadataStore(db_path)  # auto-applies migrations
        except Exception as e:
            print(
                f"warning: could not initialize metadata DB at {db_path}: {e}",
                file=sys.stderr,
            )

    viewer_run(
        log_path=args.log_dir,
        port=args.port,
        host=args.host,
        debug=args.debug,
    )
    return 0


def _cmd_db_upgrade(args: argparse.Namespace) -> int:
    db_path = _resolve_db_path(args.db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    from rinnsal.data.metadata import SqliteMetadataStore

    print(f"applying migrations to {db_path}…")
    SqliteMetadataStore(db_path)  # __init__ runs migrations
    print("done.")
    return 0


def _cmd_db_rebuild(args: argparse.Namespace) -> int:
    db_path = _resolve_db_path(args.db_path)
    if db_path.exists():
        if not args.yes:
            confirm = input(
                f"This will delete {db_path}. Continue? [y/N] "
            ).strip().lower()
            if confirm not in ("y", "yes"):
                print("aborted.")
                return 1
        # Reset the engine cache so re-creating against the same path
        # opens a fresh DB instead of reusing the cached engine.
        from rinnsal.data.metadata.sqlite import _engines

        _engines.pop(db_path.resolve(), None)
        db_path.unlink()
        for sib in (db_path.with_suffix(".sqlite-wal"),
                    db_path.with_suffix(".sqlite-shm")):
            if sib.exists():
                sib.unlink()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    from rinnsal.data.metadata import SqliteMetadataStore
    from rinnsal.data.metadata.backfill import Backfiller

    store = SqliteMetadataStore(db_path)
    print(f"recreated {db_path}")

    # Backfill from events.pb under the DB root.
    bf = Backfiller(store, db_path.parent)
    pending = bf.discover_pending()
    if not pending:
        print("no existing run dirs to index.")
        return 0
    print(f"indexing {len(pending)} run(s)…")
    n = bf.run_sync()
    print(f"indexed {n} run(s).")
    return 0


def _cmd_db_status(args: argparse.Namespace) -> int:
    db_path = _resolve_db_path(args.db_path)
    if not db_path.exists():
        print(f"no DB at {db_path} (run `rinnsal db upgrade` to create it)")
        return 1
    from sqlalchemy import func, select

    from rinnsal.data.metadata import SqliteMetadataStore
    from rinnsal.data.metadata.models import Flow, Run, TaskEdge, TaskNode

    store = SqliteMetadataStore(db_path)
    print(f"db: {db_path}")
    with store._engine.begin() as conn:  # noqa: SLF001 — admin tool
        for tbl in (Flow, Run, TaskNode, TaskEdge):
            n = conn.execute(
                select(func.count()).select_from(tbl)
            ).scalar_one()
            print(f"  {tbl.__tablename__:14s} {n}")
    # Schema revision via Alembic.
    from rinnsal.data.metadata.sqlite import _current_revision

    rev = _current_revision(store._engine)  # noqa: SLF001
    print(f"  schema_rev    {rev}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="rinnsal",
        description="Rinnsal: declarative DAG execution + viewer.",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)
    _add_up(sub)
    _add_db(sub)

    args = parser.parse_args(argv)

    if args.cmd == "up":
        return _cmd_up(args)
    if args.cmd == "db":
        if args.db_cmd == "upgrade":
            return _cmd_db_upgrade(args)
        if args.db_cmd == "rebuild":
            return _cmd_db_rebuild(args)
        if args.db_cmd == "status":
            return _cmd_db_status(args)
    parser.print_help()
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
