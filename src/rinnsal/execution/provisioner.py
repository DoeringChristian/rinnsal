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

    def _find_path_deps(self) -> list[tuple[str, bool]]:
        """Find path-based pypi dependencies in pixi.toml. Returns [(path, editable)]."""
        pixi_toml = self._project_dir / "pixi.toml"
        if not pixi_toml.exists():
            return []
        try:
            import tomllib
        except ImportError:
            try:
                import tomli as tomllib  # type: ignore
            except ImportError:
                return []
        try:
            with open(pixi_toml, "rb") as f:
                data = tomllib.load(f)
        except Exception:
            return []

        deps = []
        for section in ["pypi-dependencies"]:
            for _name, spec in data.get(section, {}).items():
                if isinstance(spec, dict) and "path" in spec:
                    deps.append((spec["path"], spec.get("editable", False)))
        return deps

    def provision_script(self, work_dir: str) -> str:
        lines = [
            "set -e",
            f"mkdir -p {work_dir}",
            'export PATH="$HOME/.pixi/bin:$PATH"',
            "command -v pixi >/dev/null 2>&1 || curl -fsSL https://pixi.sh/install.sh | sh",
            'export PATH="$HOME/.pixi/bin:$PATH"',
            f"cd {work_dir}",
            "pixi install --quiet",
        ]
        # Explicitly install path-based deps (pixi install may not handle them on a fresh clone)
        path_deps = self._find_path_deps()
        if path_deps:
            lines.append("pixi run pip install --quiet uv-build hatchling setuptools 2>/dev/null || true")
            for path, _editable in path_deps:
                lines.append(f"pixi run pip install --quiet {work_dir}/{path}")
        # Ensure cloudpickle is available
        lines.append("pixi run pip install --quiet cloudpickle 2>/dev/null || true")
        # Run custom provision script if present (for builds like cmake/mitsuba).
        # Uses "pixi run bash" so the script sees the pixi-managed Python
        # and all dependencies on PATH.
        provision_sh = self._project_dir / ".rinnsal-provision.sh"
        if provision_sh.exists():
            lines.append(f"pixi run bash {work_dir}/.rinnsal-provision.sh")
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


def build_ray_runtime_env(
    provisioner: Provisioner | None = None,
    extra_packages: list[str] | None = None,
    user_runtime_env: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a Ray ``runtime_env`` dict from a provisioner.

    Auto-detects the provisioner if none is given.  Merges the result
    with *user_runtime_env* (user values take precedence for scalars;
    list values like ``pip`` are concatenated).
    """
    if provisioner is None:
        provisioner = _detect_provisioner()

    # Unwrap AutoProvisioner
    inner = getattr(provisioner, "inner", provisioner)

    env: dict[str, Any] = {}

    # working_dir — ship project source to workers
    project_dir = getattr(inner, "_project_dir", None) or getattr(
        inner, "project_dir", None
    )
    if project_dir is not None:
        env["working_dir"] = str(project_dir)

    # pip — always include cloudpickle; add provisioner extras
    pip_packages: list[str] = ["cloudpickle"]
    prov_extras = getattr(inner, "_extra_packages", None)
    if prov_extras:
        pip_packages.extend(prov_extras)
    if extra_packages:
        pip_packages.extend(extra_packages)

    # Deduplicate while preserving order
    seen: set[str] = set()
    deduped: list[str] = []
    for p in pip_packages:
        if p not in seen:
            seen.add(p)
            deduped.append(p)
    env["pip"] = deduped

    # Merge with user-provided runtime_env
    if user_runtime_env:
        for key, val in user_runtime_env.items():
            if key == "pip" and isinstance(val, list):
                # Append user pip packages, deduplicate
                for p in val:
                    if p not in seen:
                        seen.add(p)
                        env["pip"].append(p)
            elif key == "env_vars" and isinstance(val, dict):
                env.setdefault("env_vars", {}).update(val)
            elif key == "py_modules" and isinstance(val, list):
                env.setdefault("py_modules", []).extend(val)
            else:
                # Scalar keys: user wins
                env[key] = val

    return env


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
