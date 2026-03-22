"""Inline executor that runs tasks in the calling process."""

from __future__ import annotations

import io
import sys
from concurrent.futures import Future
from contextlib import redirect_stderr, redirect_stdout
from typing import TYPE_CHECKING, Any

from rinnsal.execution.executor import ExecutionResult, Executor

if TYPE_CHECKING:
    from rinnsal.core.expression import TaskExpression


class InlineExecutor(Executor):
    """Executor that runs tasks in the calling process.

    Simple, no serialization overhead. Useful for debugging and
    lightweight pipelines. Tasks execute synchronously.
    """

    def submit(
        self,
        expr: TaskExpression,
        resolved_args: tuple[Any, ...],
        resolved_kwargs: dict[str, Any],
    ) -> Future[ExecutionResult]:
        """Submit a task for inline execution.

        The task is executed immediately in the current thread.
        Returns a completed Future with the result.
        """
        future: Future[ExecutionResult] = Future()

        try:
            result = self._execute(expr, resolved_args, resolved_kwargs)
            future.set_result(result)
        except Exception as e:
            # Even on exception, we set a result with the error
            future.set_result(
                ExecutionResult(
                    value=None,
                    success=False,
                    error=e,
                )
            )

        return future

    def _execute(
        self,
        expr: TaskExpression,
        resolved_args: tuple[Any, ...],
        resolved_kwargs: dict[str, Any],
    ) -> ExecutionResult:
        """Execute a task and capture its output."""
        from rinnsal.context import Card, current

        stdout_capture = io.StringIO()
        stderr_capture = io.StringIO()

        current._set_card(Card())
        try:
            if self._capture:
                with (
                    redirect_stdout(stdout_capture),
                    redirect_stderr(stderr_capture),
                ):
                    value = expr.func(*resolved_args, **resolved_kwargs)
            else:
                value = expr.func(*resolved_args, **resolved_kwargs)

            card = current._reset()
            return ExecutionResult(
                value=value,
                stdout=stdout_capture.getvalue(),
                stderr=stderr_capture.getvalue(),
                success=True,
                card=card.serialize() if card else None,
            )
        except Exception as e:
            current._reset()
            return ExecutionResult(
                value=None,
                stdout=stdout_capture.getvalue(),
                stderr=stderr_capture.getvalue(),
                success=False,
                error=e,
            )

    def execute_sync(
        self,
        expr: TaskExpression,
        resolved_args: tuple[Any, ...],
        resolved_kwargs: dict[str, Any],
    ) -> ExecutionResult:
        """Execute a task synchronously.

        For InlineExecutor, this is the same as submit().result().
        """
        return self._execute(expr, resolved_args, resolved_kwargs)

    def shutdown(self, wait: bool = True) -> None:
        """No-op for inline executor - nothing to shut down."""
        pass

    def __repr__(self) -> str:
        return f"InlineExecutor(capture={self._capture})"
