"""Materialize a run's code snapshot into a local venv.

:func:`with_snapshot` downloads the deterministic tarball for a given
snapshot hash, extracts it to a cached directory under
``<cwd>/.rinnsal/viewer_cache/<hash>/src/``, then runs
:class:`AutoProvisioner` to create a matching ``.venv/``. A ``.ready``
sentinel marks a fully-prepared cache entry so subsequent calls skip
both download and provisioning.

:func:`run_in_snapshot_subprocess` is a convenience wrapper that
executes a Python module (or inline code) using the cached venv's
interpreter, so a viewer script runs against the *exact same code* the
training run used.
"""

from __future__ import annotations

import subprocess
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Iterator, Sequence

if TYPE_CHECKING:
    from rinnsal.sdk.client import Client


@dataclass(frozen=True, slots=True)
class SnapshotEnv:
    """A materialized snapshot with its provisioned Python interpreter."""

    snapshot_hash: str
    src_dir: Path        # extracted source tree
    venv_dir: Path       # provisioned .venv
    python: str          # shell command to invoke the provisioned python


def _cache_dir(snapshot_hash: str, cache_root: Path | None) -> Path:
    root = cache_root or (Path.cwd() / ".rinnsal" / "viewer_cache")
    return root / snapshot_hash


def _materialize(
    client: "Client",
    snapshot_hash: str,
    *,
    cache_root: Path | None,
    provision: bool,
) -> SnapshotEnv:
    """Download + extract + provision. Idempotent — reuses the cache."""
    from rinnsal.cluster.archive import extract_archive
    from rinnsal.compute.provisioner import AutoProvisioner

    entry = _cache_dir(snapshot_hash, cache_root)
    src = entry / "src"
    venv = entry / ".venv"
    ready = entry / ".ready"

    if ready.exists() and src.is_dir():
        return SnapshotEnv(
            snapshot_hash=snapshot_hash,
            src_dir=src,
            venv_dir=venv,
            python=str(venv / "bin" / "python"),
        )

    entry.mkdir(parents=True, exist_ok=True)
    if not src.is_dir():
        data = client.snapshot_archive(snapshot_hash)
        extract_archive(data, src)

    if provision:
        provisioner = AutoProvisioner(search_dir=src)
        script = provisioner.provision_script(str(src))
        subprocess.run(
            ["bash", "-c", script], check=True,
        )
        python_cmd = provisioner.python_command(str(src))
    else:
        python_cmd = "python"

    ready.write_text(snapshot_hash)
    return SnapshotEnv(
        snapshot_hash=snapshot_hash,
        src_dir=src,
        venv_dir=venv,
        python=python_cmd,
    )


@contextmanager
def with_snapshot(
    client: "Client",
    snapshot_hash: str,
    *,
    cache_root: Path | None = None,
    provision: bool = True,
) -> Iterator[SnapshotEnv]:
    """Context-manager that yields a :class:`SnapshotEnv`.

    The environment is *not* activated inside the current Python process
    — use :func:`run_in_snapshot_subprocess` to actually execute code
    against it. Loading a different version of the same package into a
    live interpreter is not safe; subprocess isolation is the guarantee.
    """
    env = _materialize(
        client, snapshot_hash,
        cache_root=cache_root, provision=provision,
    )
    yield env


def run_in_snapshot_subprocess(
    client: "Client",
    snapshot_hash: str,
    argv: Sequence[str],
    *,
    cache_root: Path | None = None,
    provision: bool = True,
    env: dict[str, str] | None = None,
    check: bool = True,
) -> subprocess.CompletedProcess:
    """Run ``argv`` under the snapshot's provisioned Python.

    Example::

        run_in_snapshot_subprocess(
            client, snap, ["my_viewer.py", "--run", run_id]
        )

    runs ``<.venv>/bin/python my_viewer.py --run <run_id>`` with the
    snapshot's source tree on ``PYTHONPATH``-equivalent access (cwd ==
    ``src_dir``). ``argv`` is expected to start with a script name; the
    python interpreter is prepended automatically.
    """
    with with_snapshot(
        client, snapshot_hash,
        cache_root=cache_root, provision=provision,
    ) as snap:
        cmd = f"{snap.python} " + " ".join(str(a) for a in argv)
        return subprocess.run(
            ["bash", "-c", cmd],
            cwd=str(snap.src_dir),
            env=env,
            check=check,
            capture_output=True,
            text=True,
        )
