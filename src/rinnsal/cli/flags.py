"""Built-in CLI flags for flow execution."""

from __future__ import annotations

import argparse
from typing import Any


def add_builtin_flags(parser: argparse.ArgumentParser) -> None:
    """Add built-in flags to a parser.

    Built-in flags:
    - --executor NAME: Select an executor by name
    - --filter PATTERN: Only execute tasks matching this pattern
    - -s/--no-capture: Disable stdout/stderr capture
    """
    builtin_group = parser.add_argument_group("rinnsal options")

    builtin_group.add_argument(
        "--executor",
        type=str,
        default="subprocess",
        help="Executor to use for task execution",
        choices=["inline", "subprocess", "ssh", "ray"],
    )

    builtin_group.add_argument(
        "--filter",
        type=str,
        default=None,
        metavar="PATTERN",
        help="Only execute tasks matching this pattern (regex)",
    )

    builtin_group.add_argument(
        "-s",
        "--no-capture",
        action="store_true",
        default=False,
        help="Disable stdout/stderr capture during execution",
    )

    builtin_group.add_argument(
        "--db-path",
        type=str,
        default=".rinnsal",
        help="Path to the database directory",
    )

    builtin_group.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Print the task DAG without executing",
    )

    builtin_group.add_argument(
        "--resume",
        action="store_true",
        default=False,
        help="Re-run only failed/incomplete tasks from the last run",
    )

    builtin_group.add_argument(
        "--tag",
        type=str,
        action="append",
        default=[],
        metavar="TAG",
        help="Tag this run for later filtering (repeatable)",
    )


def extract_builtin_flags(namespace: argparse.Namespace) -> dict[str, Any]:
    """Extract built-in flags from a parsed namespace.

    Returns:
        Dictionary with keys: executor, filter, no_capture, db_path, etc.
    """
    return {
        "executor": getattr(namespace, "executor", "subprocess"),
        "filter": getattr(namespace, "filter", None),
        "no_capture": getattr(namespace, "no_capture", False),
        "db_path": getattr(namespace, "db_path", ".rinnsal"),
        "dry_run": getattr(namespace, "dry_run", False),
        "resume": getattr(namespace, "resume", False),
        "tags": getattr(namespace, "tag", []),
    }


def remove_builtin_flags(kwargs: dict[str, Any]) -> dict[str, Any]:
    """Remove built-in flags from a kwargs dict.

    Returns a new dict with only user-defined arguments.
    """
    builtin_keys = {
        "executor", "filter", "no_capture", "db_path", "dry_run", "resume",
        "tag", "tags",
    }
    return {k: v for k, v in kwargs.items() if k not in builtin_keys}
