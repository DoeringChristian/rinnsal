"""Flow decorator and FlowResult class."""

from __future__ import annotations

import functools
import re
import inspect
from typing import Any, Callable, ParamSpec, TypeVar, overload

from rinnsal.core.expression import TaskExpression
from rinnsal.core.graph import DAG
from rinnsal.core.registry import flow_scope, get_flow_context
from rinnsal.core.types import Entry, Runs
from rinnsal.runtime.engine import get_engine
from rinnsal.progress.bar import ProgressBar, SilentProgress

# Global setting for progress display
_show_progress: bool = True


def set_progress(enabled: bool) -> None:
    """Enable or disable progress bar display."""
    global _show_progress
    _show_progress = enabled


def get_progress() -> bool:
    """Check if progress display is enabled."""
    return _show_progress


P = ParamSpec("P")
R = TypeVar("R")


class FlowResult:
    """Result of a flow execution.

    An indexable collection of all evaluated tasks with support for:
    - Integer indexing (positional)
    - String indexing (regex match on task name)
    - Callable indexing (filter by task arguments)
    """

    def __init__(
        self,
        tasks: list[TaskExpression],
        flow_name: str,
    ) -> None:
        self._tasks = tasks
        self._flow_name = flow_name

    @property
    def tasks(self) -> list[TaskExpression]:
        return list(self._tasks)

    @property
    def flow_name(self) -> str:
        return self._flow_name

    def __len__(self) -> int:
        return len(self._tasks)

    def __iter__(self):
        return iter(self._tasks)

    @overload
    def __getitem__(self, index: int) -> TaskExpression: ...

    @overload
    def __getitem__(self, pattern: str) -> TaskExpression | FlowResult: ...

    @overload
    def __getitem__(
        self, filter_fn: Callable[..., bool]
    ) -> TaskExpression | FlowResult: ...

    def __getitem__(
        self, key: int | str | Callable[..., bool]
    ) -> TaskExpression | FlowResult:
        if isinstance(key, int):
            return self._tasks[key]
        elif isinstance(key, str):
            return self._filter_by_pattern(key)
        elif callable(key):
            return self._filter_by_callable(key)
        else:
            raise TypeError(f"Invalid key type: {type(key)}")

    def _filter_by_pattern(self, pattern: str) -> TaskExpression | FlowResult:
        """Filter tasks by regex pattern on name or function name."""
        regex = re.compile(pattern)
        matches: list[TaskExpression] = []

        for task in self._tasks:
            if regex.search(task.task_name) or regex.search(task.func.__name__):
                matches.append(task)

        if len(matches) == 1:
            return matches[0]
        return FlowResult(matches, self._flow_name)

    def _filter_by_callable(
        self, filter_fn: Callable[..., bool]
    ) -> TaskExpression | FlowResult:
        """Filter tasks by a callable that inspects task arguments."""
        sig = inspect.signature(filter_fn)
        param_names = set(sig.parameters.keys())

        matches: list[TaskExpression] = []

        for task in self._tasks:
            # Get the task's resolved argument values
            try:
                task_sig = inspect.signature(task.func)
                task_params = list(task_sig.parameters.keys())

                # Build a dict of parameter names to resolved values
                arg_dict: dict[str, Any] = {}

                # Map positional args
                for i, (param, value) in enumerate(zip(task_params, task.args)):
                    if param in param_names:
                        if task.is_evaluated:
                            from rinnsal.core.expression import unwrap_value

                            arg_dict[param] = unwrap_value(value)
                        else:
                            # For unevaluated tasks, try to get the value
                            from rinnsal.core.expression import ValueExpression

                            if isinstance(value, ValueExpression):
                                arg_dict[param] = value.value
                            elif not isinstance(value, TaskExpression):
                                arg_dict[param] = value

                # Add keyword args
                for key, value in task.kwargs.items():
                    if key in param_names:
                        if task.is_evaluated:
                            from rinnsal.core.expression import unwrap_value

                            arg_dict[key] = unwrap_value(value)
                        else:
                            from rinnsal.core.expression import ValueExpression

                            if isinstance(value, ValueExpression):
                                arg_dict[key] = value.value
                            elif not isinstance(value, TaskExpression):
                                arg_dict[key] = value

                # Only match if we have all required parameters
                if param_names <= set(arg_dict.keys()):
                    filter_args = {k: arg_dict[k] for k in param_names}
                    if filter_fn(**filter_args):
                        matches.append(task)

            except (ValueError, TypeError):
                # Skip tasks that don't match the filter signature
                continue

        if len(matches) == 1:
            return matches[0]
        return FlowResult(matches, self._flow_name)

    def __repr__(self) -> str:
        return f"FlowResult({self._flow_name}, tasks={len(self._tasks)})"


class FlowDef:
    """A flow definition wrapping a function.

    Created by the @flow decorator. When called, executes the flow
    function to build a DAG, then evaluates all tasks.
    """

    def __init__(
        self,
        func: Callable[..., Any],
    ) -> None:
        self._func = func
        self._runs: Runs[FlowResult] = Runs()
        functools.update_wrapper(self, func)

    @property
    def func(self) -> Callable[..., Any]:
        return self._func

    @property
    def name(self) -> str:
        return self._func.__name__

    def __call__(self, **kwargs: Any) -> FlowResult:
        """Execute the flow and return a FlowResult.

        Parses CLI arguments (--filter, -s, --executor, etc.) from
        sys.argv, sets up the engine, then executes the flow.
        """
        from rinnsal.cli.flags import (
            add_builtin_flags,
            extract_builtin_flags,
            remove_builtin_flags,
        )
        from rinnsal.cli.parser import create_parser_from_signature
        from rinnsal.persistence.file_store import FileDatabase
        from rinnsal.runtime.engine import ExecutionEngine, set_engine

        # Parse CLI arguments
        parser = create_parser_from_signature(
            self._func, description=self._func.__doc__
        )
        add_builtin_flags(parser)
        namespace, _ = parser.parse_known_args()

        builtin_flags = extract_builtin_flags(namespace)
        cli_kwargs = remove_builtin_flags(vars(namespace))
        # Programmatic kwargs override CLI defaults
        merged_kwargs = {**cli_kwargs, **kwargs}

        # Setup engine
        executor = _create_executor(
            builtin_flags["executor"],
            capture=not builtin_flags["no_capture"],
        )
        database = FileDatabase(root=builtin_flags["db_path"])
        engine = ExecutionEngine(
            executor=executor,
            database=database,
            use_cache=not builtin_flags["no_cache"],
        )
        set_engine(engine)

        try:
            if builtin_flags["filter"]:
                return self._run_filtered(
                    builtin_flags["filter"], merged_kwargs
                )
            return self._execute(merged_kwargs)
        finally:
            engine.shutdown()

    def _execute(self, kwargs: dict[str, Any]) -> FlowResult:
        """Execute the flow with the given kwargs."""
        with flow_scope() as ctx:
            self._func(**kwargs)
            tasks = ctx.get_tasks()

            if not tasks:
                return FlowResult([], self.name)

            dag = DAG.from_expressions(tasks)
            ordered = dag.topological_sort()

            if _show_progress:
                progress = ProgressBar(total=len(ordered))
            else:
                progress = SilentProgress(total=len(ordered))

            engine = get_engine()
            failed_hashes: set[str] = set()
            errors: list[tuple[str, Exception]] = []

            for expr in ordered:
                deps = dag.get_dependencies(expr.hash)
                if deps & failed_hashes:
                    failed_hashes.add(expr.hash)
                    progress.skip(expr.task_name)
                    continue

                if expr.is_evaluated:
                    progress.complete(expr.task_name, cached=True)
                else:
                    progress.start(expr.task_name)
                    was_cached = self._check_cached(engine, expr)
                    try:
                        engine.evaluate(expr)
                        progress.complete(
                            expr.task_name, cached=was_cached
                        )
                    except Exception as e:
                        failed_hashes.add(expr.hash)
                        errors.append((expr.task_name, e))
                        progress.fail(expr.task_name)

            progress.finish()

            if errors:
                if len(errors) == 1:
                    raise errors[0][1]
                msg = f"{len(errors)} tasks failed:\n"
                for name, err in errors:
                    msg += f"  - {name}: {err}\n"
                raise RuntimeError(msg)

            result = FlowResult(tasks, self.name)
            self._runs.append(result)
            return result

    def _run_filtered(
        self, pattern: str, kwargs: dict[str, Any]
    ) -> FlowResult:
        """Execute only tasks matching the pattern.

        Dependencies of matched tasks are loaded from cache.
        """
        import re as re_mod

        engine = get_engine()
        database = engine.database

        if database is None:
            raise ValueError(
                "Filter mode requires a database for cached results"
            )

        with flow_scope() as ctx:
            self._func(**kwargs)
            tasks = ctx.get_tasks()

        if not tasks:
            raise ValueError("Flow produced no tasks")

        dag = DAG.from_expressions(tasks)
        ordered = dag.topological_sort()

        regex = re_mod.compile(pattern)
        matched_hashes = {
            t.hash
            for t in tasks
            if regex.search(t.task_name)
            or regex.search(t.func.__name__)
        }

        if not matched_hashes:
            available = [t.task_name for t in tasks]
            raise ValueError(
                f"No tasks match pattern '{pattern}'. "
                f"Available tasks: {available}"
            )

        # Collect all dependencies of matched tasks
        required_dep_hashes: set[str] = set()

        def collect_deps(task_hash: str) -> None:
            for dep_hash in dag.get_dependencies(task_hash):
                if dep_hash not in required_dep_hashes:
                    required_dep_hashes.add(dep_hash)
                    collect_deps(dep_hash)

        for h in matched_hashes:
            collect_deps(h)

        tasks_to_process = [
            e
            for e in ordered
            if e.hash in matched_hashes
            or e.hash in required_dep_hashes
        ]
        if _show_progress:
            progress = ProgressBar(total=len(tasks_to_process))
        else:
            progress = SilentProgress(total=len(tasks_to_process))

        for expr in ordered:
            if expr.hash in matched_hashes:
                progress.start(expr.task_name)
                engine.evaluate(expr)
                progress.complete(expr.task_name, cached=False)
            elif expr.hash in required_dep_hashes:
                progress.start(expr.task_name)
                cached = database.fetch_task_result(expr.hash)
                if cached is None:
                    progress.fail(expr.task_name)
                    raise ValueError(
                        f"No cached result for dependency "
                        f"'{expr.task_name}'. Run the full flow "
                        "first to populate the cache."
                    )
                expr.set_result(cached.result)
                progress.complete(expr.task_name, cached=True)

        progress.finish()

        matched_tasks = [
            t for t in tasks if t.hash in matched_hashes
        ]
        return FlowResult(matched_tasks, self.name)

    def _check_cached(self, engine: Any, expr: TaskExpression) -> bool:
        """Check if expression result is in database cache."""
        if engine.database is not None and engine.use_cache:
            cached = engine.database.fetch_task_result(expr.hash)
            return cached is not None
        return False

    def results(self, **kwargs: Any) -> FlowResult:
        """Re-run the flow function to rebuild DAG, load cached results.

        This rebuilds the DAG structure without re-executing tasks,
        loading results from the cache/database instead.
        """
        # For now, this just runs the flow normally
        # TODO: Implement cache loading
        return self(**kwargs)

    @overload
    def __getitem__(self, index: int) -> FlowResult: ...

    @overload
    def __getitem__(self, pattern: str) -> FlowResult: ...

    def __getitem__(self, key: int | str) -> FlowResult:
        """Access historical flow results."""
        if isinstance(key, int):
            return self._runs[key]
        raise TypeError(f"Invalid key type: {type(key)}")

    def __repr__(self) -> str:
        return f"FlowDef({self.name})"


def _create_executor(name: str, capture: bool = True) -> Any:
    """Create an executor by name."""
    from rinnsal.execution.inline import InlineExecutor

    if name == "inline":
        return InlineExecutor(capture=capture)
    elif name == "subprocess":
        try:
            from rinnsal.execution.subprocess import SubprocessExecutor

            return SubprocessExecutor(capture=capture)
        except ImportError:
            raise ValueError("Subprocess executor not available")
    elif name == "ssh":
        raise ValueError(
            "SSH executor requires additional configuration"
        )
    elif name == "ray":
        try:
            from rinnsal.execution.ray_executor import RayExecutor

            return RayExecutor(capture=capture)
        except ImportError:
            raise ValueError(
                "Ray executor requires ray to be installed"
            )
    else:
        raise ValueError(f"Unknown executor: {name}")


def flow(func: Callable[P, R]) -> FlowDef:
    """Decorator to create a flow.

    A flow wraps a function that builds a task DAG. The flow function
    calls tasks, and the system collects all registered tasks to form
    the DAG.

    Args:
        func: The function that builds the task DAG

    Returns:
        A FlowDef that executes the DAG when called

    Examples:
        @flow
        def my_flow(learning_rate=0.01, epochs=10):
            data = load_data()
            model = train(data, lr=learning_rate, epochs=epochs)
            evaluate(model)

        # Run the flow
        result = my_flow()
        result = my_flow(learning_rate=0.001)  # Override defaults
    """
    return FlowDef(func)
