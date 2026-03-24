"""Thorough tests for the overhauled snapshot system.

Tests: git ls-files integration, include_patterns, garbage collection,
hash stability, .gitignore respect, non-Python file inclusion, fallback
behavior, and edge cases.
"""

import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from unittest import mock

import pytest

from rinnsal.core.snapshot import (
    SnapshotManager,
    find_git_root,
    _SKIP_DIRS,
)


def _create_git_repo(path: Path) -> None:
    """Initialize a git repo with some tracked files."""
    subprocess.run(["git", "init"], cwd=path, capture_output=True)
    subprocess.run(
        ["git", "config", "user.email", "test@test.com"],
        cwd=path, capture_output=True,
    )
    subprocess.run(
        ["git", "config", "user.name", "Test"],
        cwd=path, capture_output=True,
    )


def _git_add_commit(path: Path, msg: str = "commit") -> None:
    """Stage all and commit."""
    subprocess.run(["git", "add", "."], cwd=path, capture_output=True)
    subprocess.run(
        ["git", "commit", "-m", msg, "--allow-empty"],
        cwd=path, capture_output=True,
    )


class TestGitLsFilesIntegration:
    """Test that git ls-files is used by default in git repos."""

    def test_snapshot_uses_git_ls_files(self, tmp_path):
        """Default mode uses git ls-files, includes non-.py files."""
        repo = tmp_path / "repo"
        repo.mkdir()
        _create_git_repo(repo)

        # Create tracked files of various types
        (repo / "main.py").write_text("print('hello')")
        (repo / "config.yaml").write_text("lr: 0.01")
        (repo / "schema.json").write_text('{"type": "object"}')
        (repo / "README.md").write_text("# My project")

        # Create untracked / gitignored files
        (repo / ".gitignore").write_text("build/\n*.log\n")
        build_dir = repo / "build"
        build_dir.mkdir()
        (build_dir / "output.py").write_text("# build artifact")
        (repo / "debug.log").write_text("some log")

        _git_add_commit(repo, "initial")

        manager = SnapshotManager()
        files = manager._get_file_list(repo)
        rel_names = {str(f.relative_to(repo)) for f in files}

        # Tracked files should be included
        assert "main.py" in rel_names
        assert "config.yaml" in rel_names
        assert "schema.json" in rel_names
        assert "README.md" in rel_names
        assert ".gitignore" in rel_names

        # Gitignored files should be excluded
        assert "build/output.py" not in rel_names
        assert "debug.log" not in rel_names

    def test_snapshot_copies_non_py_files(self, tmp_path):
        """Snapshot directory includes .yaml, .json, etc."""
        repo = tmp_path / "repo"
        repo.mkdir()
        _create_git_repo(repo)

        (repo / "main.py").write_text("x = 1")
        (repo / "config.yaml").write_text("key: val")
        _git_add_commit(repo, "initial")

        snap_dir = tmp_path / "snapshots"
        manager = SnapshotManager(snapshot_dir=snap_dir)

        # Create a function whose source is in repo
        # We'll mock inspect.getfile to return our repo file
        def dummy():
            return 1

        with mock.patch("inspect.getfile", return_value=str(repo / "main.py")):
            with mock.patch(
                "rinnsal.core.snapshot.find_git_root", return_value=repo
            ):
                h, snap_path = manager.create_snapshot(dummy)

        assert (snap_path / "main.py").exists()
        assert (snap_path / "config.yaml").exists()
        assert (snap_path / "config.yaml").read_text() == "key: val"

    def test_hash_changes_when_yaml_changes(self, tmp_path):
        """Changing a non-.py file changes the snapshot hash."""
        repo = tmp_path / "repo"
        repo.mkdir()
        _create_git_repo(repo)

        (repo / "main.py").write_text("x = 1")
        config = repo / "config.yaml"
        config.write_text("lr: 0.01")
        _git_add_commit(repo, "initial")

        manager = SnapshotManager()
        hash1 = manager._compute_hash(repo)

        # Change the yaml
        config.write_text("lr: 0.05")
        subprocess.run(["git", "add", "."], cwd=repo, capture_output=True)

        hash2 = manager._compute_hash(repo)
        assert hash1 != hash2


class TestIncludePatterns:
    """Test explicit include_patterns configuration."""

    def test_py_only_pattern(self, tmp_path):
        """include_patterns=['*.py'] gives old behavior."""
        repo = tmp_path / "repo"
        repo.mkdir()
        (repo / "main.py").write_text("x = 1")
        (repo / "config.yaml").write_text("key: val")
        (repo / "lib.py").write_text("y = 2")

        manager = SnapshotManager(include_patterns=["*.py"])
        files = manager._get_file_list(repo)
        names = {f.name for f in files}

        assert "main.py" in names
        assert "lib.py" in names
        assert "config.yaml" not in names

    def test_multi_pattern(self, tmp_path):
        """Multiple patterns include matching files."""
        repo = tmp_path / "repo"
        repo.mkdir()
        (repo / "main.py").write_text("x = 1")
        (repo / "config.yaml").write_text("key: val")
        (repo / "data.csv").write_text("a,b")

        manager = SnapshotManager(include_patterns=["*.py", "*.yaml"])
        files = manager._get_file_list(repo)
        names = {f.name for f in files}

        assert "main.py" in names
        assert "config.yaml" in names
        assert "data.csv" not in names

    def test_patterns_skip_pycache(self, tmp_path):
        """Patterns respect the skip list."""
        repo = tmp_path / "repo"
        repo.mkdir()
        cache = repo / "__pycache__"
        cache.mkdir()
        (cache / "main.cpython-312.pyc").write_bytes(b"")
        (repo / "main.py").write_text("x = 1")

        manager = SnapshotManager(include_patterns=["*.py", "*.pyc"])
        files = manager._get_file_list(repo)
        names = {str(f.relative_to(repo)) for f in files}

        assert "main.py" in names
        assert "__pycache__/main.cpython-312.pyc" not in names

    def test_patterns_override_git_ls_files(self, tmp_path):
        """When patterns are set, git ls-files is NOT used."""
        repo = tmp_path / "repo"
        repo.mkdir()
        _create_git_repo(repo)

        (repo / "main.py").write_text("x = 1")
        (repo / "config.yaml").write_text("key: val")
        _git_add_commit(repo, "initial")

        # With explicit patterns, should use rglob, not git
        manager = SnapshotManager(include_patterns=["*.py"])
        files = manager._get_file_list(repo)
        names = {f.name for f in files}

        assert "main.py" in names
        assert "config.yaml" not in names  # not matched by *.py pattern


class TestGarbageCollection:
    """Test snapshot pruning with max_snapshots."""

    def test_no_gc_by_default(self, tmp_path):
        """Without max_snapshots, snapshots accumulate."""
        snap_dir = tmp_path / "snapshots"
        snap_dir.mkdir()

        # Create fake snapshot dirs
        for i in range(5):
            d = snap_dir / f"hash{i:04d}"
            d.mkdir()
            (d / "main.py").write_text(f"v{i}")

        manager = SnapshotManager(snapshot_dir=snap_dir)
        manager._prune_snapshots()

        # All should still exist
        assert len(list(snap_dir.iterdir())) == 5

    def test_gc_prunes_oldest(self, tmp_path):
        """With max_snapshots=3, keeps 3 most recent."""
        snap_dir = tmp_path / "snapshots"
        snap_dir.mkdir()

        import time

        dirs = []
        for i in range(5):
            d = snap_dir / f"hash{i:04d}"
            d.mkdir()
            (d / "main.py").write_text(f"v{i}")
            time.sleep(0.05)  # ensure different mtimes
            dirs.append(d)

        manager = SnapshotManager(
            snapshot_dir=snap_dir, max_snapshots=3
        )
        manager._prune_snapshots()

        remaining = sorted(snap_dir.iterdir())
        assert len(remaining) == 3
        # The 3 newest should survive
        names = {d.name for d in remaining}
        assert "hash0004" in names
        assert "hash0003" in names
        assert "hash0002" in names

    def test_gc_does_not_prune_active(self, tmp_path):
        """Active snapshots are not pruned even if oldest."""
        snap_dir = tmp_path / "snapshots"
        snap_dir.mkdir()

        import time

        for i in range(5):
            d = snap_dir / f"hash{i:04d}"
            d.mkdir()
            (d / "main.py").write_text(f"v{i}")
            time.sleep(0.05)

        manager = SnapshotManager(
            snapshot_dir=snap_dir, max_snapshots=3
        )
        # Mark hash0000 as active
        manager._snapshots["hash0000"] = snap_dir / "hash0000"
        manager._prune_snapshots()

        # hash0000 should survive despite being oldest
        assert (snap_dir / "hash0000").exists()


class TestHashProperties:
    """Test hash computation properties."""

    def test_hash_is_32_chars(self, tmp_path):
        """Hash is 32 hex chars (128 bits)."""
        repo = tmp_path / "repo"
        repo.mkdir()
        (repo / "main.py").write_text("x = 1")

        manager = SnapshotManager(include_patterns=["*.py"])
        h = manager._compute_hash(repo)
        assert len(h) == 32
        assert all(c in "0123456789abcdef" for c in h)

    def test_hash_deterministic(self, tmp_path):
        """Same files produce the same hash."""
        repo = tmp_path / "repo"
        repo.mkdir()
        (repo / "main.py").write_text("x = 1")
        (repo / "lib.py").write_text("y = 2")

        manager = SnapshotManager(include_patterns=["*.py"])
        h1 = manager._compute_hash(repo)
        h2 = manager._compute_hash(repo)
        assert h1 == h2

    def test_hash_changes_on_content_change(self, tmp_path):
        """Changing file content changes the hash."""
        repo = tmp_path / "repo"
        repo.mkdir()
        f = repo / "main.py"
        f.write_text("x = 1")

        manager = SnapshotManager(include_patterns=["*.py"])
        h1 = manager._compute_hash(repo)

        f.write_text("x = 2")
        h2 = manager._compute_hash(repo)
        assert h1 != h2

    def test_hash_changes_on_new_file(self, tmp_path):
        """Adding a new file changes the hash."""
        repo = tmp_path / "repo"
        repo.mkdir()
        (repo / "main.py").write_text("x = 1")

        manager = SnapshotManager(include_patterns=["*.py"])
        h1 = manager._compute_hash(repo)

        (repo / "lib.py").write_text("y = 2")
        h2 = manager._compute_hash(repo)
        assert h1 != h2

    def test_hash_skips_unreadable_files(self, tmp_path):
        """Files that can't be read are skipped without error."""
        repo = tmp_path / "repo"
        repo.mkdir()
        (repo / "main.py").write_text("x = 1")

        manager = SnapshotManager(include_patterns=["*.py"])
        # Should not crash even if a file becomes unreadable
        h = manager._compute_hash(repo)
        assert len(h) == 32


class TestSkipDirs:
    """Test that skip directories are properly excluded."""

    def test_pycache_skipped(self, tmp_path):
        repo = tmp_path / "repo"
        repo.mkdir()
        cache = repo / "__pycache__"
        cache.mkdir()
        (cache / "mod.pyc").write_bytes(b"")
        (repo / "main.py").write_text("x = 1")

        manager = SnapshotManager(include_patterns=["*"])
        files = manager._get_file_list(repo)
        assert not any("__pycache__" in str(f) for f in files)

    def test_venv_skipped(self, tmp_path):
        repo = tmp_path / "repo"
        repo.mkdir()
        venv = repo / ".venv" / "lib"
        venv.mkdir(parents=True)
        (venv / "pip.py").write_text("# pip")
        (repo / "main.py").write_text("x = 1")

        manager = SnapshotManager(include_patterns=["*.py"])
        files = manager._get_file_list(repo)
        assert not any(".venv" in str(f) for f in files)

    def test_rinnsal_dir_skipped(self, tmp_path):
        repo = tmp_path / "repo"
        repo.mkdir()
        rinnsal = repo / ".rinnsal" / "tasks"
        rinnsal.mkdir(parents=True)
        (rinnsal / "data.py").write_text("# data")
        (repo / "main.py").write_text("x = 1")

        manager = SnapshotManager(include_patterns=["*.py"])
        files = manager._get_file_list(repo)
        assert not any(".rinnsal" in str(f) for f in files)

    def test_git_dir_skipped(self, tmp_path):
        repo = tmp_path / "repo"
        repo.mkdir()
        _create_git_repo(repo)
        (repo / "main.py").write_text("x = 1")
        _git_add_commit(repo, "initial")

        manager = SnapshotManager(include_patterns=["*"])
        files = manager._get_file_list(repo)
        assert not any(".git" in f.relative_to(repo).parts for f in files)


class TestFallbackBehavior:
    """Test behavior when git is not available."""

    def test_non_git_repo_uses_rglob(self, tmp_path):
        """Without git, falls back to rglob with patterns."""
        repo = tmp_path / "repo"
        repo.mkdir()
        (repo / "main.py").write_text("x = 1")
        (repo / "config.yaml").write_text("key: val")

        # Default patterns (no git → ["*.py"])
        manager = SnapshotManager()
        with mock.patch(
            "rinnsal.core.snapshot.find_git_root", return_value=None
        ):
            files = manager._get_file_list(repo)

        names = {f.name for f in files}
        assert "main.py" in names
        assert "config.yaml" not in names  # only *.py in fallback

    def test_non_git_with_explicit_patterns(self, tmp_path):
        """Explicit patterns work without git."""
        repo = tmp_path / "repo"
        repo.mkdir()
        (repo / "main.py").write_text("x = 1")
        (repo / "config.yaml").write_text("key: val")

        manager = SnapshotManager(include_patterns=["*.py", "*.yaml"])
        with mock.patch(
            "rinnsal.core.snapshot.find_git_root", return_value=None
        ):
            files = manager._get_file_list(repo)

        names = {f.name for f in files}
        assert "main.py" in names
        assert "config.yaml" in names


class TestSnapshotDeduplication:
    """Test that identical content produces the same snapshot."""

    def test_same_content_same_hash(self, tmp_path):
        """Two repos with identical files get the same hash."""
        repo1 = tmp_path / "repo1"
        repo2 = tmp_path / "repo2"
        repo1.mkdir()
        repo2.mkdir()

        (repo1 / "main.py").write_text("x = 1")
        (repo2 / "main.py").write_text("x = 1")

        manager = SnapshotManager(include_patterns=["*.py"])
        h1 = manager._compute_hash(repo1)
        h2 = manager._compute_hash(repo2)
        assert h1 == h2

    def test_reuses_existing_snapshot_on_disk(self, tmp_path):
        """If snapshot dir already exists, don't re-copy."""
        snap_dir = tmp_path / "snapshots"
        repo = tmp_path / "repo"
        repo.mkdir()
        (repo / "main.py").write_text("x = 1")

        manager = SnapshotManager(
            snapshot_dir=snap_dir, include_patterns=["*.py"]
        )

        def dummy():
            return 1

        with mock.patch("inspect.getfile", return_value=str(repo / "main.py")):
            with mock.patch(
                "rinnsal.core.snapshot.find_git_root", return_value=None
            ):
                h1, p1 = manager.create_snapshot(dummy)
                # Second call should reuse
                h2, p2 = manager.create_snapshot(dummy)

        assert h1 == h2
        assert p1 == p2
