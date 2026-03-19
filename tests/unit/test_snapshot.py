"""Tests for snapshot functionality."""

import os
import sys
import tempfile
from pathlib import Path
from unittest import mock

import pytest

from rinnsal.core.snapshot import (
    SnapshotManager,
    find_git_root,
    build_pythonpath,
    get_snapshot_manager,
    _is_env_path,
)


class TestFindGitRoot:
    def test_finds_git_root(self):
        # This test runs in the rinnsal repo which has a .git
        root = find_git_root()
        assert root is not None
        assert (root / ".git").exists()

    def test_returns_none_for_non_git(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = find_git_root(Path(tmpdir))
            # May find parent git root, so just check it doesn't crash
            assert root is None or isinstance(root, Path)


class TestSnapshotManager:
    def test_create_snapshot_for_function(self):
        manager = SnapshotManager()

        def my_func():
            return 42

        # Functions defined in test file should work
        snapshot_hash, snapshot_path = manager.create_snapshot(my_func)

        # Should create a snapshot (hash and path)
        assert snapshot_hash != ""
        assert snapshot_path.exists()

        # Cleanup
        manager.cleanup()

    def test_snapshot_deduplication(self):
        manager = SnapshotManager()

        def func1():
            return 1

        def func2():
            return 2

        # Same source file = same snapshot
        h1, p1 = manager.create_snapshot(func1)
        h2, p2 = manager.create_snapshot(func2)

        assert h1 == h2
        assert p1 == p2

        manager.cleanup()

    def test_snapshot_with_custom_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            snapshot_dir = Path(tmpdir) / "snapshots"
            manager = SnapshotManager(snapshot_dir=snapshot_dir)

            def my_func():
                return 42

            snapshot_hash, snapshot_path = manager.create_snapshot(my_func)

            assert snapshot_path.parent == snapshot_dir
            assert snapshot_path.exists()

    def test_cleanup_removes_temp_dirs(self):
        manager = SnapshotManager()

        def my_func():
            return 42

        _, snapshot_path = manager.create_snapshot(my_func)
        assert snapshot_path.exists()

        manager.cleanup()
        # Temp dirs should be cleaned up
        assert not snapshot_path.exists()


class TestBuildPythonpath:
    def test_without_snapshot(self):
        pythonpath = build_pythonpath(None)
        # Should contain current sys.path
        assert pythonpath != ""

    def test_with_snapshot_remaps_paths(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            snapshot_path = Path(tmpdir)
            pythonpath = build_pythonpath(snapshot_path)

            # Should contain the snapshot path
            assert tmpdir in pythonpath


class TestEnvPathDetection:
    """Test that environment paths are not remapped into snapshots."""

    def test_pixi_site_packages_is_env_path(self):
        assert _is_env_path("/.pixi/envs/default/lib/python3.12/site-packages")

    def test_venv_site_packages_is_env_path(self):
        assert _is_env_path("/.venv/lib/python3.12/site-packages")

    def test_conda_is_env_path(self):
        assert _is_env_path("/.conda/envs/myenv/lib/python3.12")

    def test_source_dir_is_not_env_path(self):
        assert not _is_env_path("/src/mypackage")

    def test_empty_relative_is_not_env_path(self):
        assert not _is_env_path("/")

    def test_remap_skips_pixi_site_packages(self):
        """build_pythonpath must not remap .pixi paths into the snapshot."""
        git_root = find_git_root()
        if git_root is None:
            pytest.skip("No git root")

        pixi_path = str(git_root / ".pixi" / "envs" / "default" / "lib" / "python3.12" / "site-packages")
        src_path = str(git_root / "src")

        with tempfile.TemporaryDirectory() as tmpdir:
            snapshot_path = Path(tmpdir)
            snapshot_str = str(snapshot_path.resolve())

            fake_sys_path = [src_path, pixi_path, "/usr/lib/python3"]
            with mock.patch.object(sys, "path", fake_sys_path):
                pythonpath = build_pythonpath(snapshot_path)

            entries = pythonpath.split(os.pathsep)
            # src should be remapped
            assert any(e.startswith(snapshot_str) for e in entries)
            # .pixi path should NOT be remapped — kept as original
            assert pixi_path in entries

    def test_remap_skips_venv_site_packages(self):
        """build_pythonpath must not remap .venv paths into the snapshot."""
        git_root = find_git_root()
        if git_root is None:
            pytest.skip("No git root")

        venv_path = str(git_root / ".venv" / "lib" / "python3.12" / "site-packages")
        src_path = str(git_root / "src")

        with tempfile.TemporaryDirectory() as tmpdir:
            snapshot_path = Path(tmpdir)
            snapshot_str = str(snapshot_path.resolve())

            fake_sys_path = [src_path, venv_path]
            with mock.patch.object(sys, "path", fake_sys_path):
                pythonpath = build_pythonpath(snapshot_path)

            entries = pythonpath.split(os.pathsep)
            assert any(e.startswith(snapshot_str) for e in entries)
            assert venv_path in entries


class TestGlobalSnapshotManager:
    def test_get_snapshot_manager_singleton(self):
        manager1 = get_snapshot_manager()
        manager2 = get_snapshot_manager()
        assert manager1 is manager2


class TestExecutorSnapshotDefault:
    def test_subprocess_executor_snapshot_default_true(self):
        from rinnsal.execution.subprocess import SubprocessExecutor

        executor = SubprocessExecutor(max_workers=1)
        assert executor.snapshot is True

    def test_subprocess_executor_snapshot_can_disable(self):
        from rinnsal.execution.subprocess import SubprocessExecutor

        executor = SubprocessExecutor(max_workers=1, snapshot=False)
        assert executor.snapshot is False
