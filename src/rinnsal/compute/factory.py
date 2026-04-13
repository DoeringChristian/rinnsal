"""Factory for constructing executors from CLI-style names."""

from __future__ import annotations

from typing import Any


def _parse_ssh_host(spec: str) -> Any:
    """Parse an SSH host spec like 'host', 'user@host', or 'user@host:port'."""
    from rinnsal.compute.ssh import SSHHost

    username = None
    port = 22

    if "@" in spec:
        username, spec = spec.rsplit("@", 1)

    if ":" in spec:
        spec, port_str = spec.rsplit(":", 1)
        try:
            port = int(port_str)
        except ValueError:
            raise ValueError(f"Invalid SSH port: {port_str}")

    return SSHHost(hostname=spec, username=username, port=port)


def create_executor(name: str, capture: bool = True) -> Any:
    """Create an executor by name.

    Supports:
        - inline, subprocess, slurm
        - ssh:[user@]host[:port][,[user@]host[:port]...]
          e.g. ssh:rgllab, ssh:user@host1,host2, ssh:user@host1:22,user@host2:2222
        - pssh:[user@]host[:port][,...]   (persistent SSH)
    """
    from rinnsal.compute.inline import InlineExecutor

    if name == "inline":
        return InlineExecutor(capture=capture)
    elif name == "subprocess":
        try:
            from rinnsal.compute.subprocess import SubprocessExecutor

            return SubprocessExecutor(capture=capture)
        except ImportError:
            raise ValueError("Subprocess executor not available")
    elif name.startswith("ssh:"):
        from rinnsal.compute.provisioner import AutoProvisioner
        from rinnsal.compute.ssh import SSHExecutor

        host_specs = name[4:]
        if not host_specs:
            raise ValueError(
                "SSH executor requires at least one host: "
                "--executor ssh:[user@]host[:port]"
            )

        hosts = [_parse_ssh_host(s.strip()) for s in host_specs.split(",")]
        return SSHExecutor(
            hosts=hosts,
            capture=capture,
            provisioner=AutoProvisioner(),
        )
    elif name == "ssh":
        raise ValueError(
            "SSH executor requires a host: "
            "--executor ssh:[user@]host[:port] "
            "e.g. --executor ssh:rgllab or --executor ssh:user@host1,host2"
        )
    elif name.startswith("pssh:"):
        from rinnsal.compute.provisioner import AutoProvisioner
        from rinnsal.compute.ssh import PersistentSSHExecutor

        host_specs = name[5:]
        if not host_specs:
            raise ValueError(
                "Persistent SSH executor requires at least one host: "
                "--executor pssh:[user@]host[:port]"
            )

        hosts = [_parse_ssh_host(s.strip()) for s in host_specs.split(",")]
        return PersistentSSHExecutor(
            hosts=hosts,
            capture=capture,
            provisioner=AutoProvisioner(),
        )
    elif name == "pssh":
        raise ValueError(
            "Persistent SSH executor requires a host: "
            "--executor pssh:[user@]host[:port] "
            "e.g. --executor pssh:rgllab"
        )
    elif name == "slurm":
        from rinnsal.compute.slurm import SlurmExecutor

        return SlurmExecutor(capture=capture)
    else:
        raise ValueError(f"Unknown executor: {name}")
