"""Rinnsal SDK: talk to a remote server from a local viewer script.

Typical use::

    from rinnsal import sdk

    client = sdk.connect("http://fermat:8800", root="/srv/rinnsal")
    run = client.run("20260413_124534")
    loss = run.scalars("loss")
    for s in loss:
        print(s.it, s.value)

    with sdk.with_snapshot(client, run.snapshot_hash) as snap:
        # snap.python is the venv-scoped interpreter; cwd=snap.src_dir
        ...
"""

from __future__ import annotations

from rinnsal.sdk.client import Client, connect
from rinnsal.sdk.references import Artifact, Run, Sample, Series
from rinnsal.sdk.snapshot import (
    SnapshotEnv,
    run_in_snapshot_subprocess,
    with_snapshot,
)
from rinnsal.sdk.uri import Reference, format_uri, parse_uri

__all__ = [
    "Client", "connect",
    "Run", "Series", "Sample", "Artifact",
    "Reference", "parse_uri", "format_uri",
    "SnapshotEnv", "with_snapshot", "run_in_snapshot_subprocess",
    "resolve",
]


def resolve(uri: str, *, root: str, timeout: float = 30.0):
    """Parse ``rinnsal://…`` and return a handle (:class:`Run` or :class:`Series`).

    - ``rinnsal://host``              → :class:`Client`
    - ``rinnsal://host/flow/run``     → :class:`Run`
    - ``rinnsal://host/flow/run/tag`` → :class:`Series`
    """
    ref = parse_uri(uri)
    client = connect(ref.host_url, root=root, timeout=timeout)
    if ref.run is None:
        return client
    # Look up the run's on-disk path from its id.
    info = client.run_info(ref.run)
    run = Run(
        client,
        run_id=ref.run,
        run_path=info["run_dir"],
        flow=info.get("flow"),
        snapshot_hash=info.get("snapshot_hash"),
    )
    if ref.tag is None:
        return run
    # Pick a series whose kind matches the tag.
    for entry in run.tags():
        if entry["tag"] != ref.tag:
            continue
        kind = entry["kind"]
        if kind == "scalar":
            return run.scalars(ref.tag)
        if kind == "text":
            return run.text(ref.tag)
        if kind == "figure":
            return run.figure(ref.tag)
        if kind == "image":
            return run.image(ref.tag)
    raise KeyError(f"tag {ref.tag!r} not found in run {ref.run}")
