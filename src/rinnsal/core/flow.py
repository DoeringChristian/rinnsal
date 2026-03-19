"""Flow decorator and FlowResult class."""

from __future__ import annotations

import functools
import re
import inspect
from typing import Any, Callable, ParamSpec, TypeVar, overload

from contextvars import ContextVar

from rinnsal.core.expression import TaskExpression
from rinnsal.core.graph import DAG
from rinnsal.core.types import Entry, Runs
from rinnsal.runtime.engine import get_engine
from rinnsal.progress.bar import ProgressBar, SilentProgress

# Context variable for capturing tasks created during flow execution
_capture_stack: ContextVar[list[TaskExpression] | None] = ContextVar(
    "_capture_stack", default=None
)


def notify_task_created(expr: TaskExpression) -> None:
    """Called by TaskDef.__call__ to register a newly created expression."""
    capture = _capture_stack.get(None)
    if capture is not None:
        capture.append(expr)


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


def _extract_tasks(value: Any) -> list[TaskExpression]:
    """Recursively extract all TaskExpression objects from a structure."""
    if isinstance(value, TaskExpression):
        return [value]
    if isinstance(value, dict):
        tasks: list[TaskExpression] = []
        for v in value.values():
            tasks.extend(_extract_tasks(v))
        return tasks
    if isinstance(value, (list, tuple)):
        tasks = []
        for v in value:
            tasks.extend(_extract_tasks(v))
        return tasks
    return []


class FlowResult:
    """Result of calling a flow function (unevaluated).

    Wraps the return value of a flow function. Provides run() to
    execute and results() to load from cache.
    """

    def __init__(
        self,
        return_value: Any,
        flow_name: str,
        builtin_flags: dict[str, Any],
        captured_tasks: list[TaskExpression] | None = None,
    ) -> None:
        self._return_value = return_value
        self._flow_name = flow_name
        self._builtin_flags = builtin_flags
        returned_tasks = _extract_tasks(return_value)
        # Merge captured tasks (all created during flow body) with returned tasks,
        # preserving order and deduplicating by hash
        if captured_tasks:
            seen = {t.hash for t in returned_tasks}
            extra = [t for t in captured_tasks if t.hash not in seen]
            self._tasks = returned_tasks + extra
        else:
            self._tasks = returned_tasks

    @property
    def tasks(self) -> list[TaskExpression]:
        return list(self._tasks)

    @property
    def flow_name(self) -> str:
        return self._flow_name

    def run(self) -> Any:
        """Execute the returned tasks (+ dependencies) and return the original structure."""
        if not self._tasks:
            return self._return_value

        filter_pattern = self._builtin_flags.get("filter")
        if filter_pattern:
            return self._run_filtered(filter_pattern)

        # Collect all tasks: returned + transitive deps
        all_tasks: set[TaskExpression] = set(self._tasks)
        for t in self._tasks:
            all_tasks.update(t.get_all_dependencies())

        dag = DAG.from_expressions(list(all_tasks))
        ordered = dag.topological_sort()

        engine = self._get_or_create_engine()

        if _show_progress:
            progress = ProgressBar(total=len(ordered))
        else:
            progress = SilentProgress(total=len(ordered))

        try:
            failed_hashes: set[str] = set()
            errors: list[tuple[str, Exception]] = []

            for expr in ordered:
                deps = dag.get_dependencies(expr.hash)
                if deps & failed_hashes:
                    failed_hashes.add(expr.hash)
                    progress.skip(expr.task_name)
                    continue

                progress.start(expr.task_name)
                try:
                    engine.execute(expr)
                    progress.complete(expr.task_name, cached=False)
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

            return self._return_value
        finally:
            if not self._builtin_flags.get("_engine_preset"):
                engine.shutdown()

    def _run_filtered(self, pattern: str) -> Any:
        """Execute only tasks matching ``pattern``; load their deps from the DB.

        Semantics
        ---------
        1. **Match** — ``pattern`` is compiled as a regex and tested against
           every top-level task's ``task_name`` *and* ``func.__name__``.
           A task matches if either search succeeds.  Only top-level tasks
           (those returned or captured by the flow body) are candidates;
           transitive dependencies are never matched directly.
        2. **Matched tasks** always execute fresh via ``engine.execute()``
           — the database and in-memory caches are bypassed.
        3. **Dependencies** of matched tasks are loaded from the database.
           They are never re-executed.  If a dependency has no stored
           result, a ``ValueError`` is raised telling the user to run the
           full flow first.
        4. **Everything else** (tasks that are neither matched nor a
           dependency of a match) is ignored entirely.
        5. **Error propagation** — if a dependency fails to load or a
           matched task raises, every downstream task that transitively
           depends on it is skipped.
        """
        engine = self._get_or_create_engine()
        database = engine.database

        if database is None:
            raise ValueError(
                "Filter mode requires a database for cached results"
            )

        tasks = self._tasks

        # Build the full DAG
        all_tasks: set[TaskExpression] = set(tasks)
        for t in tasks:
            all_tasks.update(t.get_all_dependencies())

        dag = DAG.from_expressions(list(all_tasks))
        ordered = dag.topological_sort()

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
            if e.hash in matched_hashes or e.hash in required_dep_hashes
        ]

        if _show_progress:
            progress = ProgressBar(total=len(tasks_to_process))
        else:
            progress = SilentProgress(total=len(tasks_to_process))

        try:
            failed_hashes: set[str] = set()
            errors: list[tuple[str, Exception]] = []

            for expr in tasks_to_process:
                deps = dag.get_dependencies(expr.hash)
                if deps & failed_hashes:
                    failed_hashes.add(expr.hash)
                    progress.skip(expr.task_name)
                    continue

                if expr.is_evaluated:
                    progress.complete(expr.task_name, cached=True)
                elif expr.hash in matched_hashes:
                    progress.start(expr.task_name)
                    try:
                        engine.execute(expr)
                        progress.complete(expr.task_name, cached=False)
                    except Exception as e:
                        failed_hashes.add(expr.hash)
                        errors.append((expr.task_name, e))
                        progress.fail(expr.task_name)
                else:
                    # Dependency — load from cache
                    progress.start(expr.task_name)
                    cached = database.fetch_task_result(
                        expr.hash, expr.task_name
                    )
                    if cached is None:
                        failed_hashes.add(expr.hash)
                        errors.append((
                            expr.task_name,
                            ValueError(
                                f"No cached result for dependency "
                                f"'{expr.task_name}'. Run the full flow "
                                "first to populate the cache."
                            ),
                        ))
                        progress.fail(expr.task_name)
                    else:
                        expr.set_result(cached.result)
                        progress.complete(expr.task_name, cached=True)

            progress.finish()

            if errors:
                if len(errors) == 1:
                    raise errors[0][1]
                msg = f"{len(errors)} tasks failed:\n"
                for name, err in errors:
                    msg += f"  - {name}: {err}\n"
                raise RuntimeError(msg)

            return self._return_value
        finally:
            if not self._builtin_flags.get("_engine_preset"):
                engine.shutdown()

    def results(self) -> Any:
        """Load cached results for all returned tasks and return the original structure."""
        if not self._tasks:
            return self._return_value

        all_tasks: set[TaskExpression] = set(self._tasks)
        for t in self._tasks:
            all_tasks.update(t.get_all_dependencies())

        dag = DAG.from_expressions(list(all_tasks))
        ordered = dag.topological_sort()

        engine = self._get_or_create_engine()
        database = engine.database
        if database is None:
            raise ValueError("results() requires a database for cached results")

        try:
            for expr in ordered:
                if expr.is_evaluated:
                    continue
                cached = database.fetch_task_result(expr.hash, expr.task_name)
                if cached is None:
                    raise ValueError(
                        f"No cached result for task '{expr.task_name}'. "
                        "Run the flow first to populate the cache."
                    )
                expr.set_result(cached.result)

            return self._return_value
        finally:
            if not self._builtin_flags.get("_engine_preset"):
                engine.shutdown()

    def _get_or_create_engine(self) -> Any:
        """Get existing engine or create one from builtin flags."""
        from rinnsal.runtime.engine import (
            ExecutionEngine,
            set_engine,
            _default_engine,
        )

        if _default_engine is not None:
            self._builtin_flags["_engine_preset"] = True
            return _default_engine

        executor = _create_executor(
            self._builtin_flags["executor"],
            capture=not self._builtin_flags["no_capture"],
        )
        from rinnsal.persistence.file_store import FileDatabase

        database = FileDatabase(root=self._builtin_flags["db_path"])
        engine = ExecutionEngine(
            executor=executor,
            database=database,
        )
        set_engine(engine)

        from rinnsal.core.snapshot import SnapshotManager, set_snapshot_manager

        set_snapshot_manager(
            SnapshotManager(snapshot_dir=database._snapshots_dir)
        )

        return engine

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
        return FlowResult._from_tasks(matches, self._flow_name)

    def _filter_by_callable(
        self, filter_fn: Callable[..., bool]
    ) -> TaskExpression | FlowResult:
        """Filter tasks by a callable that inspects task arguments."""
        sig = inspect.signature(filter_fn)
        param_names = set(sig.parameters.keys())

        matches: list[TaskExpression] = []

        for task in self._tasks:
            try:
                task_sig = inspect.signature(task.func)
                task_params = list(task_sig.parameters.keys())

                arg_dict: dict[str, Any] = {}

                for i, (param, value) in enumerate(zip(task_params, task.args)):
                    if param in param_names:
                        if task.is_evaluated:
                            from rinnsal.core.expression import unwrap_value

                            arg_dict[param] = unwrap_value(value)
                        else:
                            from rinnsal.core.expression import ValueExpression

                            if isinstance(value, ValueExpression):
                                arg_dict[param] = value.value
                            elif not isinstance(value, TaskExpression):
                                arg_dict[param] = value

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

                if param_names <= set(arg_dict.keys()):
                    filter_args = {k: arg_dict[k] for k in param_names}
                    if filter_fn(**filter_args):
                        matches.append(task)

            except (ValueError, TypeError):
                continue

        if len(matches) == 1:
            return matches[0]
        return FlowResult._from_tasks(matches, self._flow_name)

    @classmethod
    def _from_tasks(
        cls, tasks: list[TaskExpression], flow_name: str
    ) -> FlowResult:
        """Create a FlowResult wrapping a plain task list (for filtering)."""
        result = cls.__new__(cls)
        result._return_value = tasks
        result._flow_name = flow_name
        result._builtin_flags = {}
        result._tasks = tasks
        return result

    def __repr__(self) -> str:
        return f"FlowResult({self._flow_name}, tasks={len(self._tasks)})"


class FlowDef:
    """A flow definition wrapping a function.

    Created by the @flow decorator. When called, captures the return
    value and returns a FlowResult for lazy execution.
    """

    def __init__(
        self,
        func: Callable[..., Any],
        capture_tasks: bool = True,
    ) -> None:
        self._func = func
        self._capture_tasks = capture_tasks
        functools.update_wrapper(self, func)

    @property
    def func(self) -> Callable[..., Any]:
        return self._func

    @property
    def name(self) -> str:
        return self._func.__name__

    def __call__(self, **kwargs: Any) -> FlowResult:
        """Call the flow function and return a FlowResult (unevaluated).

        Parses CLI arguments from sys.argv, merges with programmatic
        kwargs, calls the flow function, and wraps the return value.
        """
        from rinnsal.cli.flags import (
            add_builtin_flags,
            extract_builtin_flags,
            remove_builtin_flags,
        )
        from rinnsal.cli.parser import create_parser_from_signature

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

        # Call flow function to build expression graph,
        # optionally capturing all created tasks
        captured: list[TaskExpression] | None = None
        if self._capture_tasks:
            captured = []
            token = _capture_stack.set(captured)

        try:
            return_value = self._func(**merged_kwargs)
        finally:
            if self._capture_tasks:
                _capture_stack.reset(token)

        return FlowResult(
            return_value,
            self.name,
            builtin_flags,
            captured_tasks=captured,
        )

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
        raise ValueError("SSH executor requires additional configuration")
    elif name == "ray":
        try:
            from rinnsal.execution.ray_executor import RayExecutor

            return RayExecutor(capture=capture)
        except ImportError:
            raise ValueError("Ray executor requires ray to be installed")
    else:
        raise ValueError(f"Unknown executor: {name}")


@overload
def flow(func: Callable[P, R]) -> FlowDef: ...


@overload
def flow(
    *, capture_tasks: bool = True
) -> Callable[[Callable[P, R]], FlowDef]: ...


def flow(
    func: Callable[P, R] | None = None,
    *,
    capture_tasks: bool = True,
) -> FlowDef | Callable[[Callable[P, R]], FlowDef]:
    """Decorator to create a flow.

    A flow wraps a function that builds a task DAG. The flow function
    calls tasks and returns the expressions to execute. Use .run()
    on the result to execute, or .results() to load from cache.

    Args:
        func: The function to wrap (when used without parentheses)
        capture_tasks: If True (default), all tasks created inside the
            flow body are captured and evaluated on .run(), even if
            they are not included in the return value.

    Returns:
        A FlowDef that returns a FlowResult when called

    Examples:
        @flow
        def my_flow(learning_rate=0.01, epochs=10):
            data = load_data()
            model = train(data, lr=learning_rate, epochs=epochs)
            return evaluate(model)

        # Lazy — just builds expressions:
        result = my_flow()

        # Execute:
        outputs = result.run()
    """
    if func is not None:
        return FlowDef(func, capture_tasks=capture_tasks)

    def decorator(fn: Callable[P, R]) -> FlowDef:
        return FlowDef(fn, capture_tasks=capture_tasks)

    return decorator
