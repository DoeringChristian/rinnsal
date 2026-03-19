#!/usr/bin/env python3
"""Remote execution over SSH with automatic provisioning.

The SSHExecutor auto-provisions remote hosts with a Python venv and
cloudpickle before running tasks.  By default an ``AutoProvisioner``
detects your local package manager (uv / pixi / pip) and mirrors the
setup on the remote.

Install the SSH extra first:

    pip install rinnsal[ssh]

Usage:

    python examples/11_ssh_executor.py --host gpu-server --user alice
    python examples/11_ssh_executor.py --host 192.168.1.50 --user bob --key ~/.ssh/id_ed25519
    python examples/11_ssh_executor.py --host node1 --host node2 -s
"""

import platform

from rinnsal import task, flow
from rinnsal.execution.ssh import SSHExecutor, SSHHost
from rinnsal.runtime.engine import ExecutionEngine, set_engine


@task
def whoami():
    """Return hostname and Python version of the machine that runs this."""
    return {
        "hostname": platform.node(),
        "python": platform.python_version(),
    }


@task
def compute(n):
    """A small computation to verify round-tripping of args and results."""
    return {"n": n, "result": sum(i**2 for i in range(n))}


@flow
def pipeline():
    info = whoami()
    r1 = compute(100).name("compute_100")
    r2 = compute(1000).name("compute_1000")
    return [info, r1, r2]


if __name__ == "__main__":

    hosts = [
        SSHHost(
            hostname="rgllab",
            username="doeringc",
            port="22",
        )
    ]

    # AutoProvisioner is the default — it detects uv/pixi/pip from your
    # local project and provisions the remote automatically.
    executor = SSHExecutor(hosts=hosts)
    set_engine(ExecutionEngine(executor=executor))

    print(f"Running on: {', '.join(host.hostname for host in hosts)}")
    fr = pipeline()
    fr.run()

    for t in fr:
        print(f"{t.task_name}: {t.result}")
