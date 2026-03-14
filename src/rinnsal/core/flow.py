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
    def __getitem__(self, filter_fn: Callable[..., bool]) -> TaskExpression | FlowResult: ...

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

    def _filter_by_callable(self, filter_fn: Callable[..., bool]) -> TaskExpression | FlowResult:
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

        The flow function is called to build the DAG, then all tasks
        are evaluated in topological order.
        """
        with flow_scope() as ctx:
            # Call the flow function to build the DAG
            self._func(**kwargs)

            # Get all tasks registered during flow execution
            tasks = ctx.get_tasks()

            if not tasks:
                return FlowResult([], self.name)

            # Build DAG and evaluate
            dag = DAG.from_expressions(tasks)
            ordered = dag.topological_sort()

            # Evaluate all tasks
            engine = get_engine()
            for expr in ordered:
                if not expr.is_evaluated:
                    engine.evaluate(expr)

            # Create and store result
            result = FlowResult(tasks, self.name)
            self._runs.append(result)

            return result

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
