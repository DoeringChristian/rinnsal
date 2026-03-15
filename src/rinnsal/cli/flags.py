"""Built-in CLI flags for flow execution."""

from __future__ import annotations

import argparse
from typing import Any


def add_builtin_flags(parser: argparse.ArgumentParser) -> None:
    """Add built-in flags to a parser.

    Built-in flags:
    - --executor NAME: Select an executor by name
    - --spin TASK_NAME: Re-run only one task (spin mode)
    - -s/--no-capture: Disable stdout/stderr capture
    """
    builtin_group = parser.add_argument_group("rinnsal options")

    builtin_group.add_argument(
        "--executor",
        type=str,
        default="inline",
        help="Executor to use for task execution",
        choices=["inline", "subprocess", "ssh", "ray"],
    )

    builtin_group.add_argument(
        "--spin",
        type=str,
        default=None,
        metavar="TASK_NAME",
        help="Re-run only the specified task (spin mode)",
    )

    builtin_group.add_argument(
        "-s",
        "--no-capture",
        action="store_true",
        default=False,
        help="Disable stdout/stderr capture during execution",
    )

    builtin_group.add_argument(
        "--no-cache",
        action="store_true",
        default=False,
        help="Disable result caching",
    )

    builtin_group.add_argument(
        "--db-path",
        type=str,
        default=".rinnsal",
        help="Path to the database directory",
    )


def extract_builtin_flags(namespace: argparse.Namespace) -> dict[str, Any]:
    """Extract built-in flags from a parsed namespace.

    Returns:
        Dictionary with keys: executor, spin, no_capture, no_cache, db_path
    """
    return {
        "executor": getattr(namespace, "executor", "inline"),
        "spin": getattr(namespace, "spin", None),
        "no_capture": getattr(namespace, "no_capture", False),
        "no_cache": getattr(namespace, "no_cache", False),
        "db_path": getattr(namespace, "db_path", ".rinnsal"),
    }


def remove_builtin_flags(kwargs: dict[str, Any]) -> dict[str, Any]:
    """Remove built-in flags from a kwargs dict.

    Returns a new dict with only user-defined arguments.
    """
    builtin_keys = {"executor", "spin", "no_capture", "no_cache", "db_path"}
    return {k: v for k, v in kwargs.items() if k not in builtin_keys}
