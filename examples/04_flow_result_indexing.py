#!/usr/bin/env python3
"""FlowResult indexing: by int, string/regex, or callable."""

from rinnsal import task, flow


@task
def load(name):
    return {"name": name, "size": 100}


@task
def train(data, lr=0.01):
    return {"model": data["name"], "lr": lr, "acc": 0.9 + lr}


@task
def evaluate(model):
    return {"model": model["model"], "score": model["acc"] * 0.95}


@flow
def pipeline():
    mnist = load("mnist").name("load_mnist")
    cifar = load("cifar").name("load_cifar")

    m1 = train(mnist, lr=0.01).name("train_slow")
    m2 = train(mnist, lr=0.1).name("train_fast")
    m3 = train(cifar, lr=0.01).name("train_cifar")

    evaluate(m1).name("eval_slow")
    evaluate(m2).name("eval_fast")
    evaluate(m3).name("eval_cifar")


if __name__ == "__main__":
    result = pipeline()

    # Integer indexing
    print(f"First: {result[0].task_name}")
    print(f"Last: {result[-1].task_name}")

    # String/regex indexing
    print(f"\nTraining tasks: {[t.task_name for t in result['train_.*']]}")

    # Callable indexing (filter by argument)
    fast = result[lambda lr: lr == 0.1]
    print(f"Fast (lr=0.1): {fast.task_name}")
