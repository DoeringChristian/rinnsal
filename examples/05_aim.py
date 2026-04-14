"""Example: rinnsal @task + aim logging.

``AimLogger`` is a thin subclass of ``aim.Run`` that auto-fills from
the current rinnsal task context:

* flow / run_id / task / task_hash / snapshot_hash → ``run["rinnsal"]``
* the task's resolved arguments → ``run["hparams"]``
* repo: local ``.aim/`` by default, or the cluster coordinator's aim
  server when you run with ``--executor cluster:…``.

Run locally::

    python examples/05_aim.py

Then inspect with::

    aim up --repo .rinnsal/.aim
"""

import math

from rinnsal import task, flow


@task
def train(lr: float = 1e-3, epochs: int = 200) -> float:
    from rinnsal.aim import AimLogger

    logger = AimLogger(experiment="training")
    loss = 1.0
    for it in range(epochs):
        loss = math.exp(-lr * it) + 0.01 * ((it % 20) - 10) / 10
        logger.track(loss, name="loss", step=it)
    return loss


@flow
def experiment():
    return train(lr=5e-3, epochs=500)


if __name__ == "__main__":
    experiment().run()
