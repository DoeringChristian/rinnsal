"""Deterministic project tarball builder for cluster mode.

The submitter packages the current project into a tarball that gets
shipped to the coordinator and unpacked on workers. Determinism matters
because the same project bytes must hash to the same value across runs
(so caching works).

Algorithm:
* List files via ``git ls-files --recurse-submodules`` (when in a git
  repo); fall back to a simple walk that excludes obvious junk.
* Sort the list.
* Build a tar with file mtimes/uid/gid normalized.
* Hash the resulting tar bytes via sha256.
"""

from __future__ import annotations

import hashlib
import io
import logging
import subprocess
import tarfile
from pathlib import Path

log = logging.getLogger("rinnsal.cluster.archive")

# Junk paths skipped by the non-git fallback.
_DEFAULT_EXCLUDES = {
    ".git",
    ".rinnsal",
    "runs",
    ".venv",
    "venv",
    "__pycache__",
    "node_modules",
    ".pixi",
    ".cache",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    "dist",
    "build",
}


def _list_files_git(root: Path) -> list[Path] | None:
    """List files the way a deterministic packaging tool would see the tree.

    We combine two git lists:
    * ``ls-files --recurse-submodules`` — tracked files, including
      submodule contents (this is what pulls in vendored subprojects
      like ``ext/rinnsal``).
    * ``ls-files --others --exclude-standard`` — untracked files that
      aren't gitignored (your uncommitted ``pixi.toml`` / ``test.py``).

    Without the second list, an uncommitted-but-not-ignored project
    would ship an almost-empty archive, which silently falls through to
    the wrong provisioner on the worker.
    """
    def _run(args: list[str]) -> bytes | None:
        try:
            result = subprocess.run(
                ["git", *args],
                capture_output=True,
                cwd=str(root),
                timeout=30,
            )
        except (FileNotFoundError, subprocess.SubprocessError):
            return None
        if result.returncode != 0:
            return None
        return result.stdout

    tracked = _run(["ls-files", "--recurse-submodules", "-z"])
    if tracked is None:
        return None
    # Untracked (not gitignored) files at the top level. We don't need
    # --recurse-submodules here: submodules' own tracked files are
    # already covered above.
    untracked = _run(["ls-files", "--others", "--exclude-standard", "-z"])
    raw = tracked + (untracked or b"")
    if not raw:
        return []
    seen: set[str] = set()
    files: list[Path] = []
    for entry in raw.split(b"\0"):
        if not entry:
            continue
        rel = entry.decode("utf-8", errors="replace")
        if rel in seen:
            continue
        seen.add(rel)
        p = root / rel
        if p.is_file():
            files.append(p)
    return files


def _list_files_walk(root: Path) -> list[Path]:
    files: list[Path] = []
    for p in root.rglob("*"):
        # Skip excluded directories (any path component).
        if any(part in _DEFAULT_EXCLUDES for part in p.parts):
            continue
        if p.is_file():
            files.append(p)
    return files


def build_project_archive(root: Path) -> tuple[bytes, str]:
    """Return ``(tarball_bytes, sha256_hex)``.

    Project paths inside the tar are relative to *root*. mtime/uid/gid
    are zeroed to keep the hash stable across machines and over time.
    """
    root = Path(root).resolve()
    if not root.is_dir():
        raise ValueError(f"project root is not a directory: {root}")

    files = _list_files_git(root)
    if files is None:
        files = _list_files_walk(root)

    files.sort()
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tar:
        for path in files:
            try:
                rel = path.relative_to(root)
            except ValueError:
                continue
            try:
                info = tar.gettarinfo(str(path), arcname=str(rel))
            except OSError:
                continue
            # Normalize for determinism.
            info.mtime = 0
            info.uid = 0
            info.gid = 0
            info.uname = ""
            info.gname = ""
            try:
                with open(path, "rb") as f:
                    tar.addfile(info, f)
            except OSError:
                # Skip unreadable files rather than failing the build.
                continue

    data = buf.getvalue()
    return data, hashlib.sha256(data).hexdigest()


def extract_archive(data: bytes, dest: Path) -> None:
    """Extract a tar previously built by :func:`build_project_archive`."""
    dest.mkdir(parents=True, exist_ok=True)
    buf = io.BytesIO(data)
    with tarfile.open(fileobj=buf, mode="r") as tar:
        # Defense in depth: filter out absolute / parent-traversal members.
        safe_members = []
        for m in tar.getmembers():
            if m.name.startswith("/") or ".." in Path(m.name).parts:
                log.warning("skipping unsafe tar member: %s", m.name)
                continue
            safe_members.append(m)
        tar.extractall(path=str(dest), members=safe_members)
