"""LocalOrchestrator — in-process DAG walker."""

from __future__ import annotations

from rinnsal.context import current
from rinnsal.orchestration.base import RunOutcome, RunPlan


class LocalOrchestrator:
    """In-process orchestrator: walks the DAG level-by-level, dispatches via engine."""

    def run(self, plan: RunPlan) -> RunOutcome:
        outcome = RunOutcome()

        def process_ready_tasks() -> None:
            ready = plan.dag.get_ready_tasks(
                outcome.completed | outcome.failed_hashes
            )
            ready.sort(
                key=lambda e: plan.dag._insertion_order.get(
                    e.hash, float("inf")
                )
            )

            for expr in ready:
                if expr.hash not in plan.ordered_hashes:
                    continue
                if (
                    expr.hash in outcome.completed
                    or expr.hash in outcome.failed_hashes
                ):
                    continue

                deps = plan.dag.get_dependencies(expr.hash)
                if deps & outcome.failed_hashes:
                    outcome.failed_hashes.add(expr.hash)
                    plan.progress.skip(expr.task_name)
                    outcome.n_failed += 1
                    continue

                if expr.hash in plan.matched_hashes:
                    plan.progress.start(expr.task_name)
                    current._set_task_name(expr.task_name)
                    try:
                        plan.engine.evaluate(expr)
                        plan.progress.complete(expr.task_name, cached=False)
                        outcome.n_passed += 1
                        outcome.completed.add(expr.hash)
                        plan.logger.add_text(
                            f"task/{expr.task_name}/status", "completed"
                        )
                    except Exception as e:
                        outcome.failed_hashes.add(expr.hash)
                        outcome.errors.append((expr.task_name, e))
                        plan.progress.fail(expr.task_name)
                        outcome.n_failed += 1
                        plan.logger.add_text(
                            f"task/{expr.task_name}/status",
                            f"failed: {e}",
                        )
                    finally:
                        current._set_task_name("")
                elif expr.is_evaluated:
                    plan.progress.complete(expr.task_name, cached=True)
                    outcome.n_cached += 1
                    outcome.completed.add(expr.hash)
                else:
                    plan.progress.start(expr.task_name)
                    cached = plan.database.fetch_task_result(
                        expr.hash, expr.task_name
                    )
                    if cached is None:
                        outcome.failed_hashes.add(expr.hash)
                        if not plan.resume:
                            outcome.errors.append(
                                (
                                    expr.task_name,
                                    ValueError(
                                        f"No cached result for "
                                        f"dependency '{expr.task_name}'. "
                                        "Run the full flow first to "
                                        "populate the cache."
                                    ),
                                )
                            )
                        plan.progress.fail(expr.task_name)
                        outcome.n_failed += 1
                    else:
                        expr.set_result(cached.result)
                        plan.progress.complete(expr.task_name, cached=True)
                        outcome.n_cached += 1
                        outcome.completed.add(expr.hash)

        try:
            while (
                len(outcome.completed) + len(outcome.failed_hashes)
                < len(plan.ordered_hashes)
            ):
                prev_total = len(outcome.completed) + len(outcome.failed_hashes)
                process_ready_tasks()
                new_total = len(outcome.completed) + len(outcome.failed_hashes)
                if new_total == prev_total:
                    break
        except KeyboardInterrupt:
            outcome.interrupted = True

        return outcome
