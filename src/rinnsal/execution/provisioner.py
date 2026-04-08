"""Provisioners for setting up Python environments on remote hosts."""

from __future__ import annotations

import shutil
import sys
from pathlib import Path
from typing import Protocol, runtime_checkable

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
        packages = ["cloudpickle", *self._extra_packages]
        pkg_str = " ".join(packages)
        return "\n".join([
            "set -e",
            f"mkdir -p {work_dir}",
            'export PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH"',
            "command -v uv >/dev/null 2>&1 || curl -LsSf https://astral.sh/uv/install.sh | sh",
            'export PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH"',
            f"cd {work_dir}",
            f"uv venv --quiet --clear --python {self._python_version} .venv",
            f"uv pip install --quiet --python {work_dir}/.venv/bin/python {pkg_str}",
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
    """Provision using pixi — syncs local pixi project to remote.

    Copies the local pixi.toml and pixi.lock to the remote work_dir,
    installs pixi if needed, and runs pixi install to mirror the
    local environment. Also ensures cloudpickle is available.
    """

    def __init__(
        self,
        extra_packages: list[str] | None = None,
        project_dir: str | Path | None = None,
    ) -> None:
        self._extra_packages = extra_packages or []
        # Find the pixi project root
        if project_dir is None:
            project_dir = Path.cwd()
        self._project_dir = Path(project_dir)
        self._pixi_toml = self._project_dir / "pixi.toml"
        self._pixi_lock = self._project_dir / "pixi.lock"

    def get_sync_files(self) -> list[tuple[Path, str]]:
        """Return list of (local_path, remote_relative_path) to sync."""
        files = []
        if self._pixi_toml.exists():
            files.append((self._pixi_toml, "pixi.toml"))
        if self._pixi_lock.exists():
            files.append((self._pixi_lock, "pixi.lock"))
        return files

    def provision_script(self, work_dir: str) -> str:
        lines = [
            "set -e",
            f"mkdir -p {work_dir}",
            'export PATH="$HOME/.pixi/bin:$PATH"',
            "command -v pixi >/dev/null 2>&1 || curl -fsSL https://pixi.sh/install.sh | sh",
            'export PATH="$HOME/.pixi/bin:$PATH"',
            f"cd {work_dir}",
        ]
        if self._pixi_toml.exists():
            # Project files are synced separately by the SSH executor
            # Just run pixi install to set up the environment
            lines.append("pixi install --quiet")
        else:
            # No local pixi project — create a minimal one
            lines.append("pixi init --quiet 2>/dev/null || true")
            packages = ["cloudpickle", *self._extra_packages]
            for pkg in packages:
                lines.append(f"pixi add --quiet {pkg}")
            lines.append("pixi install --quiet")

        # Ensure cloudpickle is available even if not in the project deps
        lines.append("pixi add --quiet cloudpickle 2>/dev/null || true")
        return "\n".join(lines)

    def python_command(self, work_dir: str) -> str:
        return f'export PATH="$HOME/.pixi/bin:$PATH" && cd {work_dir} && pixi run python'


def _detect_provisioner(search_dir: str | Path | None = None) -> Provisioner:
    """Auto-detect the best provisioner based on local project files.

    Detection order:
    1. uv.lock → UvProvisioner
    2. pixi.lock or pixi.toml → PixiProvisioner
    3. requirements.txt → PipProvisioner
    4. pyproject.toml → UvProvisioner
    5. fallback → PipProvisioner
    """
    if search_dir is None:
        search_dir = Path.cwd()
    else:
        search_dir = Path(search_dir)

    if (search_dir / "uv.lock").exists():
        return UvProvisioner()
    if (search_dir / "pixi.lock").exists() or (search_dir / "pixi.toml").exists():
        return PixiProvisioner()
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

    def provision_script(self, work_dir: str) -> str:
        return self._inner.provision_script(work_dir)

    def python_command(self, work_dir: str) -> str:
        return self._inner.python_command(work_dir)
