#!/usr/bin/env python3
"""Task capture in flows: all tasks created inside a flow body are
evaluated on .run(), even if they are not part of the return value.

By default ``capture_tasks=True``, so side-effect tasks (logging,
checkpointing, metrics, ...) just work without being returned.

Use ``@flow(capture_tasks=False)`` to opt out and only evaluate
the tasks that appear in the return value (the old behaviour).
"""

from rinnsal import task, flow


@task
def load_data():
    print("Loading data...")
    return [1, 2, 3]


@task
def train(data, lr=0.01):
    print(f"Training with lr={lr}...")
    return {"acc": 0.9 + lr}


@task
def log_metrics(model):
    print(f"Logging metrics: {model}")
    return "logged"


@task
def save_checkpoint(model):
    print(f"Saving checkpoint: {model}")
    return "saved"


# ---------- capture_tasks=True (default) ----------

@flow
def pipeline_with_capture(lr=0.01):
    data = load_data()
    model = train(data, lr=lr)

    # Side-effect tasks: not returned, but still evaluated
    log_metrics(model)
    save_checkpoint(model)

    return model


# ---------- capture_tasks=False ----------

@flow(capture_tasks=False)
def pipeline_without_capture(lr=0.01):
    data = load_data()
    model = train(data, lr=lr)

    # These will NOT be evaluated because they are not returned
    log_metrics(model)
    save_checkpoint(model)

    return model


if __name__ == "__main__":
    print("=== With capture (default) ===")
    fr = pipeline_with_capture(lr=0.05)
    print(f"Tasks to run: {len(fr.tasks)}")
    for t in fr.tasks:
        print(f"  - {t.task_name}")
    fr.run()

    print()
    print("=== Without capture ===")
    fr2 = pipeline_without_capture(lr=0.05)
    print(f"Tasks to run: {len(fr2.tasks)}")
    for t in fr2.tasks:
        print(f"  - {t.task_name}")
    fr2.run()
