"""CLI runner for flow execution."""

from __future__ import annotations

import argparse
import sys
from typing import TYPE_CHECKING, Any

from rinnsal.cli.flags import add_builtin_flags, extract_builtin_flags, remove_builtin_flags
from rinnsal.cli.parser import create_parser_from_signature
from rinnsal.execution.inline import InlineExecutor
from rinnsal.persistence.file_store import FileDatabase
from rinnsal.runtime.engine import ExecutionEngine, set_engine

if TYPE_CHECKING:
    from rinnsal.core.flow import FlowDef


def create_flow_parser(flow: FlowDef) -> argparse.ArgumentParser:
    """Create a CLI parser for a flow.

    Combines the flow's function signature arguments with built-in flags.
    """
    parser = create_parser_from_signature(
        flow.func,
        description=flow.func.__doc__,
    )
    add_builtin_flags(parser)
    return parser


def run_flow_from_cli(flow: FlowDef, args: list[str] | None = None) -> Any:
    """Run a flow from command-line arguments.

    Args:
        flow: The flow to run
        args: Command-line arguments (defaults to sys.argv[1:])

    Returns:
        The FlowResult from executing the flow
    """
    parser = create_flow_parser(flow)
    namespace = parser.parse_args(args)

    # Extract built-in flags
    builtin_flags = extract_builtin_flags(namespace)

    # Get user-defined arguments
    all_kwargs = vars(namespace)
    user_kwargs = remove_builtin_flags(all_kwargs)

    # Create executor based on flag
    executor = _create_executor(
        builtin_flags["executor"],
        capture=not builtin_flags["no_capture"],
    )

    # Create database
    database = FileDatabase(root=builtin_flags["db_path"])

    # Create and set engine
    engine = ExecutionEngine(
        executor=executor,
        database=database,
        use_cache=not builtin_flags["no_cache"],
    )
    set_engine(engine)

    try:
        # Handle spin mode
        if builtin_flags["spin"]:
            return _run_spin_mode(flow, builtin_flags["spin"], user_kwargs)

        # Normal execution
        return flow(**user_kwargs)
    finally:
        engine.shutdown()


def _create_executor(name: str, capture: bool = True) -> Any:
    """Create an executor by name."""
    if name == "inline":
        return InlineExecutor(capture=capture)
    elif name == "subprocess":
        # Import here to avoid circular imports
        try:
            from rinnsal.execution.subprocess import SubprocessExecutor
            return SubprocessExecutor(capture=capture)
        except ImportError:
            raise ValueError("Subprocess executor not available")
    elif name == "ssh":
        raise ValueError("SSH executor requires additional configuration")
    elif name == "ray":
        try:
            from rinnsal.execution.ray_executor import RayExecutor
            return RayExecutor(capture=capture)
        except ImportError:
            raise ValueError("Ray executor requires ray to be installed")
    else:
        raise ValueError(f"Unknown executor: {name}")


def _run_spin_mode(flow: FlowDef, task_name: str, kwargs: dict[str, Any]) -> Any:
    """Run a flow in spin mode, re-executing only the specified task.

    All other tasks load their most recent cached result.
    """
    from rinnsal.core.graph import DAG
    from rinnsal.core.registry import flow_scope
    from rinnsal.persistence.file_store import get_database
    from rinnsal.runtime.engine import get_engine

    engine = get_engine()
    database = engine.database

    if database is None:
        raise ValueError("Spin mode requires a database for cached results")

    # Build the DAG
    with flow_scope() as ctx:
        flow.func(**kwargs)
        tasks = ctx.get_tasks()

    if not tasks:
        raise ValueError("Flow produced no tasks")

    # Find the target task
    target_task = None
    for task in tasks:
        if task.task_name == task_name or task.func.__name__ == task_name:
            target_task = task
            break

    if target_task is None:
        available = [t.task_name for t in tasks]
        raise ValueError(
            f"Task '{task_name}' not found. Available tasks: {available}"
        )

    # Build DAG
    dag = DAG.from_expressions(tasks)
    ordered = dag.topological_sort()

    # Load all tasks from cache except the target
    for expr in ordered:
        if expr.hash == target_task.hash:
            # Skip target - will execute it
            continue

        cached = database.fetch_task_result(expr.hash)
        if cached is None:
            raise ValueError(
                f"No cached result for task '{expr.task_name}'. "
                "Spin mode requires a previous full run."
            )

        expr.set_result(cached.result)

    # Execute the target task
    engine.evaluate(target_task)

    # Create result
    from rinnsal.core.flow import FlowResult
    return FlowResult(tasks, flow.name)


def main() -> None:
    """Main entry point for the rinnsal CLI."""
    print("Usage: python your_script.py [options]")
    print("Run a flow script directly with --help for available options.")
    sys.exit(0)


if __name__ == "__main__":
    main()
