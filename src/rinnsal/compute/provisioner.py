"""Provisioners for setting up Python environments on remote hosts."""

from __future__ import annotations

import shutil
import sys
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

_LOCAL_PYTHON_VERSION = f"{sys.version_info.major}.{sys.version_info.minor}"


@runtime_checkable
class Provisioner(Protocol):
    """Protocol for remote environment provisioners."""

    def provision_script(self, work_dir: str) -> str:
        """Return a single shell script to provision the remote environment."""
        ...

    def python_command(self, work_dir: str) -> str:
        """Return the command to invoke python in the provisioned environment."""
        ...


class UvProvisioner:
    """Provision using uv — bootstraps uv, creates venv, installs deps."""

    def __init__(
        self,
        extra_packages: list[str] | None = None,
        python_version: str = _LOCAL_PYTHON_VERSION,
    ) -> None:
        self._extra_packages = extra_packages or []
        self._python_version = python_version

    def provision_script(self, work_dir: str) -> str:
        extras = " ".join(self._extra_packages)
        # When the archive has a pyproject.toml we install the full project
        # (and its dependencies) so remote tasks can import project modules.
        # `uv sync` if a lock file is present for reproducibility, else
        # `uv pip install -e .` for a best-effort editable install.
        return "\n".join([
            "set -e",
            f"mkdir -p {work_dir}",
            'export PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH"',
            "command -v uv >/dev/null 2>&1 || curl -LsSf https://astral.sh/uv/install.sh | sh",
            'export PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH"',
            f"cd {work_dir}",
            f"uv venv --quiet --clear --python {self._python_version} .venv",
            # Install project + deps if a pyproject.toml was shipped.
            'if [ -f pyproject.toml ]; then',
            '  if [ -f uv.lock ]; then',
            f'    VIRTUAL_ENV={work_dir}/.venv uv sync --quiet --frozen --active 2>/dev/null || '
            f'      VIRTUAL_ENV={work_dir}/.venv uv sync --quiet --active',
            '  else',
            f'    VIRTUAL_ENV={work_dir}/.venv uv pip install --quiet -e .',
            '  fi',
            'fi',
            # Always ensure cloudpickle + any extras the caller asked for.
            f"VIRTUAL_ENV={work_dir}/.venv uv pip install --quiet cloudpickle {extras}",
        ])

    def python_command(self, work_dir: str) -> str:
        return f"{work_dir}/.venv/bin/python"


class PipProvisioner:
    """Provision using pip — creates venv with python3 -m venv."""

    def __init__(self, extra_packages: list[str] | None = None) -> None:
        self._extra_packages = extra_packages or []

    def provision_script(self, work_dir: str) -> str:
        packages = ["cloudpickle", *self._extra_packages]
        pkg_str = " ".join(packages)
        return "\n".join([
            "set -e",
            f"mkdir -p {work_dir}",
            f"python3 -m venv {work_dir}/.venv",
            f"{work_dir}/.venv/bin/pip install --quiet {pkg_str}",
        ])

    def python_command(self, work_dir: str) -> str:
        return f"{work_dir}/.venv/bin/python"


class PixiProvisioner:
    """Provision using pixi — syncs entire project to remote, runs pixi install.

    The local project directory (containing pixi.toml) is rsynced to the
    remote work_dir. Then pixi is bootstrapped and pixi install is run,
    which resolves all dependencies including local path-based ones.
    """

    def __init__(self, project_dir: str | Path | None = None) -> None:
        if project_dir is None:
            project_dir = Path.cwd()
        self._project_dir = Path(project_dir).resolve()

    @property
    def project_dir(self) -> Path:
        return self._project_dir

    def provision_script(self, work_dir: str) -> str:
        # Pin pixi to the extracted archive's manifest. Without
        # --manifest-path on each subcommand, pixi walks upward and can
        # latch onto an unrelated pixi.toml (e.g. the submitter's dev
        # checkout when the worker scratch dir is under $HOME), leaving
        # no local .pixi/ env and silently "succeeding". The flag
        # belongs to the subcommand, not the root pixi command.
        manifest = f"{work_dir}/pixi.toml"
        mp = f"--manifest-path {manifest}"
        lines = [
            "set -e",
            f"mkdir -p {work_dir}",
            'export PATH="$HOME/.pixi/bin:$PATH"',
            "command -v pixi >/dev/null 2>&1 || curl -fsSL https://pixi.sh/install.sh | sh",
            'export PATH="$HOME/.pixi/bin:$PATH"',
            f"cd {work_dir}",
            f"pixi install {mp} --quiet",
        ]
        # pixi manages everything — including path-based pypi deps and
        # cloudpickle if declared. Don't shell pip into the env; it
        # either fights PEP 668 or resolves to the wrong interpreter.
        # If the project needs cloudpickle, declare it in pixi.toml.
        # Run custom provision script if present (for builds like cmake/mitsuba).
        # Uses "pixi run bash" so the script sees the pixi-managed Python
        # and all dependencies on PATH.
        # We also set Python_ROOT_DIR so cmake's find_package(Python) picks
        # up pixi's Python instead of the system one, and invalidate any
        # stale cmake cache that points at a different Python.
        provision_sh = self._project_dir / ".rinnsal-provision.sh"
        if provision_sh.exists():
            lines.append(
                # Resolve pixi's Python and export hints for cmake
                f'PIXI_PYTHON="$(pixi run {mp} which python)" && '
                f'PIXI_PYTHON_DIR="$(dirname "$(dirname "$PIXI_PYTHON")")" && '
                f'export Python_ROOT_DIR="$PIXI_PYTHON_DIR" && '
                f'export Python3_ROOT_DIR="$PIXI_PYTHON_DIR" && '
                # Invalidate cmake caches that hardcode a different Python
                f'find {work_dir} -name CMakeCache.txt -exec '
                f'grep -l "Python.*EXECUTABLE" {{}} \\; | while read cache; do '
                f'if ! grep -q "$PIXI_PYTHON" "$cache" 2>/dev/null; then '
                f'echo "[rinnsal] Clearing stale cmake cache: $cache"; '
                f'rm -f "$cache"; fi; done'
            )
            lines.append(f"pixi run {mp} bash {work_dir}/.rinnsal-provision.sh")
        return "\n".join(lines)

    def python_command(self, work_dir: str) -> str:
        # Pin --manifest-path (belongs to the subcommand) so pixi doesn't
        # walk upward and end up running python from an unrelated parent
        # project.
        return (
            f'export PATH="$HOME/.pixi/bin:$PATH" && '
            f'cd {work_dir} && '
            f'pixi run --manifest-path {work_dir}/pixi.toml python'
        )


def _detect_provisioner(search_dir: str | Path | None = None) -> Provisioner:
    """Auto-detect the best provisioner based on local project files.

    Detection order (pixi wins over uv when both are present — pixi
    projects often carry uv.lock as a transitive artifact but the
    authoritative environment is pixi's):

    1. pixi.lock or pixi.toml → PixiProvisioner
    2. uv.lock → UvProvisioner
    3. requirements.txt → PipProvisioner
    4. pyproject.toml → UvProvisioner
    5. fallback → PipProvisioner
    """
    if search_dir is None:
        search_dir = Path.cwd()
    else:
        search_dir = Path(search_dir)

    if (search_dir / "pixi.lock").exists() or (search_dir / "pixi.toml").exists():
        return PixiProvisioner(project_dir=search_dir)
    if (search_dir / "uv.lock").exists():
        return UvProvisioner()
    if (search_dir / "requirements.txt").exists():
        return PipProvisioner()
    if (search_dir / "pyproject.toml").exists():
        return UvProvisioner()

    # Check if uv is available locally
    if shutil.which("uv"):
        return UvProvisioner()

    return PipProvisioner()


class AutoProvisioner:
    """Auto-detects the appropriate provisioner from local project files."""

    def __init__(self, search_dir: str | Path | None = None) -> None:
        self._inner = _detect_provisioner(search_dir)

    @property
    def inner(self) -> Provisioner:
        return self._inner

    @property
    def project_dir(self) -> Path | None:
        """Delegate to inner provisioner's project_dir if it has one."""
        return getattr(self._inner, "project_dir", None)

    def provision_script(self, work_dir: str) -> str:
        return self._inner.provision_script(work_dir)

    def python_command(self, work_dir: str) -> str:
        return self._inner.python_command(work_dir)
