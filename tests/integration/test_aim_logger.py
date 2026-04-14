"""Smoke test: :class:`rinnsal.aim.AimLogger` populates from task context."""

from __future__ import annotations

from pathlib import Path

import pytest

aim = pytest.importorskip("aim", reason="aim extra not installed")

from rinnsal import flow, task  # noqa: E402
from rinnsal.aim import AimLogger  # noqa: E402
from rinnsal.context import current  # noqa: E402


def test_aimlogger_auto_populates_from_task_context(tmp_path, monkeypatch):
    """Running a task under a flow stamps the aim Run with rinnsal
    metadata (flow/run_id/task/etc.) and copies the task's arguments
    into ``run["hparams"]``. No manual wiring should be required."""

    monkeypatch.chdir(tmp_path)

    captured: dict[str, object] = {}

    @task
    def train(lr: float, epochs: int):
        logger = AimLogger(
            repo=str(tmp_path / ".aim"),
            experiment="unit-test",
            system_tracking_interval=None,
            log_system_params=False,
        )
        logger.track(0.5, name="loss", step=0)
        captured["rinnsal"] = dict(logger["rinnsal"])
        captured["hparams"] = dict(logger["hparams"])
        captured["flow"] = current.flow_name
        captured["run_id"] = current.run_id
        logger.close()
        return "ok"

    @flow
    def pipeline():
        return train(lr=1e-3, epochs=42)

    pipeline().run()

    assert captured["flow"] == "pipeline"
    # Run id is the timestamp the engine assigned.
    assert captured["run_id"]
    rinnsal_meta = captured["rinnsal"]
    assert rinnsal_meta["flow"] == "pipeline"
    assert rinnsal_meta["task"] == "train"
    # Snapshot hash comes from the engine's code-snapshot manager.
    assert rinnsal_meta["snapshot_hash"] is not None
    # executor tag should be one of the known kinds.
    assert rinnsal_meta["executor"] in {
        "inline", "subprocess", "cluster", "ssh",
        "pssh", "slurm", "fork",
    }
    # Hparams come from the @task's resolved arguments.
    assert captured["hparams"]["lr"] == pytest.approx(1e-3)
    assert captured["hparams"]["epochs"] == 42
