"""CLI runner for flow execution."""

from __future__ import annotations

import argparse
import sys
from typing import TYPE_CHECKING, Any

from rinnsal.cli.flags import (
    add_builtin_flags,
    extract_builtin_flags,
    remove_builtin_flags,
)
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
        # Handle filter mode
        if builtin_flags["filter"]:
            return _run_filter_mode(flow, builtin_flags["filter"], user_kwargs)

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


def _run_filter_mode(
    flow: FlowDef, pattern: str, kwargs: dict[str, Any]
) -> Any:
    """Run a flow with task filtering.

    Only tasks matching the pattern are executed. Dependencies of matched
    tasks are loaded from cache. Tasks that don't match and aren't
    dependencies are skipped.

    Args:
        flow: The flow to run
        pattern: Regex pattern to match task names
        kwargs: Flow arguments

    Returns:
        FlowResult containing only the matched tasks
    """
    import re

    from rinnsal.core.flow import FlowResult
    from rinnsal.core.graph import DAG
    from rinnsal.core.registry import flow_scope
    from rinnsal.progress.bar import ProgressBar, SilentProgress
    from rinnsal.runtime.engine import get_engine

    engine = get_engine()
    database = engine.database

    if database is None:
        raise ValueError("Filter mode requires a database for cached results")

    # Build the DAG
    with flow_scope() as ctx:
        flow.func(**kwargs)
        tasks = ctx.get_tasks()

    if not tasks:
        raise ValueError("Flow produced no tasks")

    # Build DAG and get topological order
    dag = DAG.from_expressions(tasks)
    ordered = dag.topological_sort()

    # Find matching tasks (regex on task_name or func.__name__)
    regex = re.compile(pattern)
    matched_hashes = {
        t.hash
        for t in tasks
        if regex.search(t.task_name) or regex.search(t.func.__name__)
    }

    if not matched_hashes:
        available = [t.task_name for t in tasks]
        raise ValueError(
            f"No tasks match pattern '{pattern}'. "
            f"Available tasks: {available}"
        )

    # Find all dependencies of matched tasks (recursive)
    required_dep_hashes: set[str] = set()

    def collect_deps(task_hash: str) -> None:
        for dep_hash in dag.get_dependencies(task_hash):
            if dep_hash not in required_dep_hashes:
                required_dep_hashes.add(dep_hash)
                collect_deps(dep_hash)

    for h in matched_hashes:
        collect_deps(h)

    # Setup progress tracking
    from rinnsal.core.flow import get_progress

    tasks_to_process = [
        e
        for e in ordered
        if e.hash in matched_hashes or e.hash in required_dep_hashes
    ]
    if get_progress():
        progress = ProgressBar(total=len(tasks_to_process))
    else:
        progress = SilentProgress(total=len(tasks_to_process))

    # Execute in topological order
    for expr in ordered:
        if expr.hash in matched_hashes:
            # Execute this task (matches filter)
            progress.start(expr.task_name)
            engine.evaluate(expr)
            progress.complete(expr.task_name, cached=False)

        elif expr.hash in required_dep_hashes:
            # Load from cache (dependency of matched task)
            progress.start(expr.task_name)
            cached = database.fetch_task_result(expr.hash)
            if cached is None:
                progress.fail(expr.task_name)
                raise ValueError(
                    f"No cached result for dependency '{expr.task_name}'. "
                    "Run the full flow first to populate the cache."
                )
            expr.set_result(cached.result)
            progress.complete(expr.task_name, cached=True)

        # else: skip (not matched, not a dependency)

    progress.finish()

    # Return only matched tasks in result
    matched_tasks = [t for t in tasks if t.hash in matched_hashes]
    return FlowResult(matched_tasks, flow.name)


def main() -> None:
    """Main entry point for the rinnsal CLI."""
    print("Usage: python your_script.py [options]")
    print("Run a flow script directly with --help for available options.")
    sys.exit(0)


if __name__ == "__main__":
    main()
