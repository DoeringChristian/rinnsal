"""Code snapshot management for reproducible execution."""

from __future__ import annotations

import hashlib
import inspect
import os
import shutil
import subprocess as _subprocess
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Generator


def find_git_root(start: Path | None = None) -> Path | None:
    """Find the git root directory starting from a path."""
    if start is None:
        start = Path.cwd()

    try:
        result = _subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            capture_output=True,
            text=True,
            cwd=start,
        )
        if result.returncode == 0:
            return Path(result.stdout.strip())
    except (_subprocess.SubprocessError, FileNotFoundError):
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


_SKIP_DIRS = frozenset(
    {
        "__pycache__",
        ".venv",
        "venv",
        ".pixi",
        ".conda",
        ".rinnsal",
        ".git",
        "node_modules",
    }
)

# Binary extensions that must NOT be copied into snapshots.
# They are ABI-specific and must be loaded from the original install.
_BINARY_EXTENSIONS = frozenset(
    {".so", ".pyd", ".dylib", ".dll", ".a", ".lib"}
)


class SnapshotManager:
    """Manages code snapshots for task execution.

    Creates snapshots of source files so that tasks execute against
    a fixed version of the code, even if files change during execution.

    By default, uses ``git ls-files`` to list tracked files (respects
    ``.gitignore``, includes configs/templates/etc.). Falls back to
    ``rglob`` with ``include_patterns`` for non-git projects.

    Args:
        snapshot_dir: Directory to store persistent snapshots.
            If None, uses temporary directories that are cleaned up.
        include_patterns: Glob patterns for files to include.
            If None (default), uses ``git ls-files`` when in a git repo,
            or ``["*.py"]`` as fallback.
            Set to ``["*.py"]`` for the old behavior.
        max_snapshots: Maximum number of snapshots to keep on disk.
            When set, the oldest snapshots are pruned after creating a
            new one. Default None (no garbage collection).
    """

    def __init__(
        self,
        snapshot_dir: Path | None = None,
        include_patterns: list[str] | None = None,
        max_snapshots: int | None = None,
    ) -> None:
        self._snapshot_dir = snapshot_dir
        self._include_patterns = include_patterns
        self._max_snapshots = max_snapshots
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

        # Compute hash of all tracked files in the project
        snapshot_hash = self._compute_hash(project_root)

        # Check if we already have this snapshot
        if snapshot_hash in self._snapshots:
            return snapshot_hash, self._snapshots[snapshot_hash]

        # Check if snapshot exists on disk from a previous run
        if self._snapshot_dir:
            snapshot_path = self._snapshot_dir / snapshot_hash
            if snapshot_path.exists():
                self._snapshots[snapshot_hash] = snapshot_path
                return snapshot_hash, snapshot_path
            snapshot_path.mkdir(parents=True, exist_ok=True)
        else:
            snapshot_path = Path(tempfile.mkdtemp(prefix="rinnsal_snapshot_"))
            self._temp_dirs.append(snapshot_path)

        # Copy files to snapshot
        self._copy_files(project_root, snapshot_path)

        self._snapshots[snapshot_hash] = snapshot_path

        # Garbage collect old snapshots if configured
        if self._max_snapshots is not None and self._snapshot_dir:
            self._prune_snapshots()

        return snapshot_hash, snapshot_path

    def _get_file_list(self, root: Path) -> list[Path]:
        """Get list of files to include in snapshot.

        Uses ``git ls-files`` by default (respects .gitignore, includes
        all tracked file types). Falls back to rglob with patterns for
        non-git projects or when include_patterns is set explicitly.
        """
        git_root = find_git_root(root)

        if (
            git_root
            and git_root.resolve() == root.resolve()
            and self._include_patterns is None
        ):
            # Use git ls-files — respects .gitignore, includes all tracked files
            try:
                result = _subprocess.run(
                    ["git", "ls-files", "-z"],
                    capture_output=True,
                    cwd=root,
                )
                if result.returncode == 0 and result.stdout:
                    raw = result.stdout.decode("utf-8", errors="replace")
                    files = [
                        root / f
                        for f in raw.split("\0")
                        if f and not self._should_skip(Path(f))
                    ]
                    return sorted(f for f in files if f.is_file())
            except (_subprocess.SubprocessError, FileNotFoundError):
                pass

        # Fallback: rglob with patterns
        patterns = self._include_patterns or ["*.py"]
        files: list[Path] = []
        for pattern in patterns:
            files.extend(root.rglob(pattern))

        return sorted(
            f
            for f in files
            if f.is_file() and not self._should_skip(f.relative_to(root))
        )

    @staticmethod
    def _should_skip(rel_path: Path) -> bool:
        """Check if a relative path should be excluded from snapshots.

        Excludes files in skip directories and binary extensions (.so,
        .pyd, .dylib, etc.) which are ABI-specific and must be loaded
        from the original install.
        """
        if any(p in _SKIP_DIRS for p in rel_path.parts):
            return True
        # Check all suffixes — handles .cpython-312-x86_64-linux-gnu.so
        if any(s in _BINARY_EXTENSIONS for s in rel_path.suffixes):
            return True
        return False

    def _compute_hash(self, root: Path) -> str:
        """Compute a hash of all tracked files in the directory."""
        hasher = hashlib.sha256()

        for f in self._get_file_list(root):
            rel_path = f.relative_to(root)
            hasher.update(str(rel_path).encode())
            try:
                hasher.update(f.read_bytes())
            except (PermissionError, OSError):
                # Skip files we can't read
                continue

        return hasher.hexdigest()[:32]

    def _copy_files(self, src: Path, dst: Path) -> None:
        """Copy tracked files from src to dst, preserving structure."""
        for f in self._get_file_list(src):
            rel_path = f.relative_to(src)
            dst_file = dst / rel_path
            if dst_file.exists():
                continue
            dst_file.parent.mkdir(parents=True, exist_ok=True)
            try:
                shutil.copy2(f, dst_file)
            except PermissionError:
                continue

    def _prune_snapshots(self) -> None:
        """Remove oldest snapshots beyond max_snapshots limit."""
        if (
            not self._snapshot_dir
            or not self._snapshot_dir.exists()
            or self._max_snapshots is None
        ):
            return

        snapshots = sorted(
            (
                d
                for d in self._snapshot_dir.iterdir()
                if d.is_dir()
            ),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )

        for old in snapshots[self._max_snapshots :]:
            # Don't prune snapshots that are in active use
            if old.name not in self._snapshots:
                shutil.rmtree(old, ignore_errors=True)

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


def run_in_snapshot(
    func: Callable[..., Any],
    *args: Any,
    hash: str | None = None,
    flow: str | None = None,
    db_path: str = ".rinnsal",
    **kwargs: Any,
) -> Any:
    """Run a function in a subprocess using snapshot code.

    Unlike use_snapshot(), this spawns a fresh Python process,
    avoiding conflicts with already-loaded native extensions (.so/.pyd).
    Use this when your code depends on native extensions that may have
    been imported before the snapshot context.

    Args:
        func: Function to execute (must be picklable via cloudpickle/dill)
        *args: Positional arguments (must be picklable)
        hash: Snapshot hash to use directly
        flow: Flow name — uses the snapshot from its latest run
        db_path: Path to the .rinnsal database directory
        **kwargs: Keyword arguments (must be picklable)

    Returns:
        The function's return value (must be picklable)

    Raises:
        ValueError: If snapshot not found
        RuntimeError: If subprocess execution fails

    Examples:
        def load_model(checkpoint_path):
            from my_module import Model
            return Model.load(checkpoint_path)

        model = run_in_snapshot(load_model, "model.pt", flow="training")
    """
    import cloudpickle

    _, snapshot_path = _resolve_snapshot_hash(hash, flow, db_path)

    # Build environment with remapped PYTHONPATH
    env = os.environ.copy()
    env["PYTHONPATH"] = build_pythonpath(snapshot_path)

    # Serialize function and arguments using cloudpickle
    payload = cloudpickle.dumps((func, args, kwargs))

    # Python code to execute in subprocess
    worker_code = """
import sys
import cloudpickle

# Read payload from stdin
payload = sys.stdin.buffer.read()
func, args, kwargs = cloudpickle.loads(payload)

# Execute and write result to stdout
try:
    result = func(*args, **kwargs)
    sys.stdout.buffer.write(cloudpickle.dumps(("ok", result)))
except Exception as e:
    import traceback
    sys.stdout.buffer.write(cloudpickle.dumps(("error", str(e), traceback.format_exc())))
"""

    # Run in subprocess
    result = _subprocess.run(
        [sys.executable, "-c", worker_code],
        input=payload,
        capture_output=True,
        env=env,
    )

    if result.returncode != 0:
        stderr = result.stderr.decode("utf-8", errors="replace")
        raise RuntimeError(f"Subprocess failed:\n{stderr}")

    # Deserialize result using cloudpickle
    status, *data = cloudpickle.loads(result.stdout)
    if status == "ok":
        return data[0]
    else:
        error_msg, tb = data
        raise RuntimeError(f"Function raised exception:\n{tb}")


def _invalidate_project_modules(snapshot_path: Path | None) -> None:
    """Remove project-local modules from sys.modules.

    This forces Python to re-import them from the (possibly remapped)
    sys.path. Only removes modules whose file is under the git root
    or the snapshot path — stdlib, third-party, and rinnsal's own
    modules are untouched.
    """
    git_root = find_git_root()
    prefixes = []
    if git_root:
        prefixes.append(str(git_root.resolve()))
    if snapshot_path:
        prefixes.append(str(snapshot_path.resolve()))

    if not prefixes:
        return

    # Never invalidate rinnsal itself — it's framework code
    protected = ("rinnsal.",)

    to_remove = []
    for name, mod in sys.modules.items():
        if mod is None:
            continue
        if any(name.startswith(p) for p in protected):
            continue
        mod_file = getattr(mod, "__file__", None)
        if mod_file is None:
            continue
        # Skip binary extensions - they aren't copied to snapshots
        # and can't be safely reimported once loaded
        if any(mod_file.endswith(ext) for ext in _BINARY_EXTENSIONS):
            continue
        resolved = str(Path(mod_file).resolve())
        if any(resolved.startswith(p) for p in prefixes):
            to_remove.append(name)

    for name in to_remove:
        del sys.modules[name]


def _resolve_snapshot_hash(
    hash: str | None = None,
    flow: str | None = None,
    db_path: str = ".rinnsal",
) -> tuple[str, Path]:
    """Resolve a snapshot hash and path from either a direct hash or flow name.

    Returns:
        Tuple of (snapshot_hash, snapshot_path)
    """
    if hash is None and flow is None:
        raise ValueError("Either 'hash' or 'flow' must be provided")

    if flow is not None:
        from rinnsal.persistence.file_store import FileDatabase

        db = FileDatabase(root=db_path)
        runs = db.fetch_flow_runs(flow, limit=1)
        if not runs:
            raise ValueError(f"No runs found for flow '{flow}'")
        hash = runs[0].get("metadata", {}).get("snapshot")
        if not hash:
            raise ValueError(
                f"Latest run of '{flow}' has no snapshot recorded. "
                "Re-run the flow to create a snapshot."
            )

    snapshot_path = Path(db_path) / "snapshots" / hash
    if not snapshot_path.exists():
        raise ValueError(f"Snapshot '{hash}' not found at {snapshot_path}")

    return hash, snapshot_path


@contextmanager
def use_snapshot(
    hash: str | None = None,
    flow: str | None = None,
    db_path: str = ".rinnsal",
) -> Generator[Path, None, None]:
    """Remap sys.path to use a previous code snapshot.

    All imports inside the context manager resolve against the snapshot,
    allowing you to run code using the exact module versions from a
    previous execution.

    Warning:
        This context manager manipulates sys.path and sys.modules in-place.
        It does NOT work correctly if native extensions (.so/.pyd) from the
        project tree have already been imported before entering the context.
        Native extensions cannot be reloaded in a running Python process.

        If your code uses native extensions (e.g., DrJit, Mitsuba, PyTorch
        C++ extensions built locally), use ``run_in_snapshot()`` instead,
        which executes in a fresh subprocess.

    Args:
        hash: Snapshot hash to use directly
        flow: Flow name — uses the snapshot from its latest run
        db_path: Path to the .rinnsal database directory

    Yields:
        The snapshot directory path

    Examples:
        with use_snapshot(flow="my_training_flow"):
            from my_module import viewer
            viewer.show(result)

        with use_snapshot(hash="abc123def456"):
            import my_module
            my_module.inspect(data)

    See Also:
        run_in_snapshot: Subprocess-based alternative for native extensions.
    """
    _, snapshot_path = _resolve_snapshot_hash(hash, flow, db_path)

    # Remap sys.path so new imports pick up snapshot versions.
    # We do NOT invalidate sys.modules — in-process module swapping
    # breaks pickle/cloudpickle identity checks. Like Metaflow/MLflow/Ray,
    # snapshot replay should run in a separate process for full isolation.
    original_path = sys.path.copy()
    remapped = build_pythonpath(snapshot_path)
    sys.path = remapped.split(os.pathsep)

    try:
        yield snapshot_path
    finally:
        sys.path = original_path
