"""Code snapshot management for reproducible execution."""

from __future__ import annotations

import hashlib
import inspect
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Callable, Any


def find_git_root(start: Path | None = None) -> Path | None:
    """Find the git root directory starting from a path."""
    if start is None:
        start = Path.cwd()

    try:
        result = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            capture_output=True,
            text=True,
            cwd=start,
        )
        if result.returncode == 0:
            return Path(result.stdout.strip())
    except (subprocess.SubprocessError, FileNotFoundError):
        pass

    # Fallback: walk up looking for .git
    current = start.resolve()
    while current != current.parent:
        if (current / ".git").exists():
            return current
        current = current.parent
    return None


_ENV_MARKERS = (
    "site-packages",
    ".pixi",
    ".venv",
    "venv",
    ".conda",
    "conda-meta",
    "node_modules",
)


def _is_env_path(relative: str) -> bool:
    """Return True if a repo-relative path points inside an environment.

    These paths contain compiled extensions (.so/.pyd) that cannot be
    relocated, so they must not be remapped into a snapshot.
    """
    parts = Path(relative).parts
    return any(marker in parts for marker in _ENV_MARKERS)


def build_pythonpath(snapshot_path: Path | None = None) -> str:
    """Build PYTHONPATH for a subprocess.

    When snapshot_path is set, remap repo-local sys.path entries to their
    snapshot equivalents so that the subprocess imports from the snapshot.
    Paths that point inside virtual environments or package managers are
    kept as-is because they contain compiled extensions that cannot be
    relocated.
    """
    if snapshot_path is not None:
        git_root = find_git_root()
        if git_root:
            git_root_str = str(git_root.resolve())
            snapshot_str = str(snapshot_path.resolve())

            remapped = []
            for p in sys.path:
                resolved = str(Path(p).resolve()) if p else ""
                if resolved.startswith(git_root_str):
                    relative = resolved[len(git_root_str):]
                    if _is_env_path(relative):
                        remapped.append(p)
                    else:
                        remapped.append(snapshot_str + relative)
                else:
                    remapped.append(p)
            pythonpath = os.pathsep.join(remapped)
        else:
            # No git root found, just prepend snapshot
            pythonpath = (
                str(snapshot_path) + os.pathsep + os.pathsep.join(sys.path)
            )
    else:
        pythonpath = os.pathsep.join(sys.path)

    existing = os.environ.get("PYTHONPATH")
    if existing:
        pythonpath = pythonpath + os.pathsep + existing
    return pythonpath


class SnapshotManager:
    """Manages code snapshots for task execution.

    Creates snapshots of source files so that tasks execute against
    a fixed version of the code, even if files change during execution.
    """

    def __init__(self, snapshot_dir: Path | None = None) -> None:
        self._snapshot_dir = snapshot_dir
        self._snapshots: dict[str, Path] = {}  # hash -> snapshot path
        self._temp_dirs: list[Path] = []

    def create_snapshot(self, func: Callable[..., Any]) -> tuple[str, Path]:
        """Create a snapshot of the source files for a function.

        Args:
            func: The function to snapshot

        Returns:
            Tuple of (snapshot_hash, snapshot_path)
        """
        # Get the source file of the function
        try:
            source_file = Path(inspect.getfile(func)).resolve()
        except (TypeError, OSError):
            # Built-in or C extension - no snapshot needed
            return "", Path()

        if not source_file.exists():
            return "", Path()

        # Find the project root (git root or package root)
        project_root = find_git_root(source_file.parent)
        if project_root is None:
            # Fallback: walk up to find package root
            project_root = source_file.parent
            while (project_root.parent / "__init__.py").exists():
                project_root = project_root.parent
            if (project_root / "__init__.py").exists():
                project_root = project_root.parent

        # Compute hash of all Python files in the project
        snapshot_hash = self._compute_hash(project_root)

        # Check if we already have this snapshot
        if snapshot_hash in self._snapshots:
            return snapshot_hash, self._snapshots[snapshot_hash]

        # Create snapshot directory
        if self._snapshot_dir:
            snapshot_path = self._snapshot_dir / snapshot_hash
            snapshot_path.mkdir(parents=True, exist_ok=True)
        else:
            snapshot_path = Path(tempfile.mkdtemp(prefix="rinnsal_snapshot_"))
            self._temp_dirs.append(snapshot_path)

        # Copy Python files to snapshot
        self._copy_python_files(project_root, snapshot_path)

        self._snapshots[snapshot_hash] = snapshot_path
        return snapshot_hash, snapshot_path

    def _compute_hash(self, root: Path) -> str:
        """Compute a hash of all Python files in the directory."""
        hasher = hashlib.sha256()

        # Sort files for deterministic ordering
        py_files = sorted(root.rglob("*.py"))

        for py_file in py_files:
            # Skip __pycache__, venvs, and .rinnsal artifacts
            parts = py_file.relative_to(root).parts
            if any(
                p in ("__pycache__", ".venv", "venv", ".pixi", ".conda", ".rinnsal")
                for p in parts
            ):
                continue

            rel_path = py_file.relative_to(root)
            hasher.update(str(rel_path).encode())
            hasher.update(py_file.read_bytes())

        return hasher.hexdigest()[:16]

    def _copy_python_files(self, src: Path, dst: Path) -> None:
        """Copy Python files from src to dst, preserving structure."""
        for py_file in src.rglob("*.py"):
            # Skip __pycache__, venvs, and .rinnsal artifacts
            parts = py_file.relative_to(src).parts
            if any(
                p in ("__pycache__", ".venv", "venv", ".pixi", ".conda", ".rinnsal")
                for p in parts
            ):
                continue

            rel_path = py_file.relative_to(src)
            dst_file = dst / rel_path
            dst_file.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(py_file, dst_file)

    def get_snapshot_path(self, snapshot_hash: str) -> Path | None:
        """Get the path to a snapshot by its hash."""
        return self._snapshots.get(snapshot_hash)

    def cleanup(self) -> None:
        """Clean up temporary snapshot directories."""
        for temp_dir in self._temp_dirs:
            if temp_dir.exists():
                shutil.rmtree(temp_dir, ignore_errors=True)
        self._temp_dirs.clear()

    def __del__(self) -> None:
        self.cleanup()


# Global snapshot manager
_snapshot_manager: SnapshotManager | None = None


def get_snapshot_manager() -> SnapshotManager:
    """Get or create the global snapshot manager."""
    global _snapshot_manager
    if _snapshot_manager is None:
        _snapshot_manager = SnapshotManager()
    return _snapshot_manager


def set_snapshot_manager(manager: SnapshotManager) -> None:
    """Set the global snapshot manager."""
    global _snapshot_manager
    _snapshot_manager = manager
