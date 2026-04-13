"""File locking for concurrent access."""

from __future__ import annotations

import os
import sys
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


class FileLock:
    """Cross-platform file lock using OS-level locking.

    Uses fcntl on Unix and msvcrt on Windows.
    """

    def __init__(self, path: Path, timeout: float = 10.0) -> None:
        self._path = Path(path)
        self._timeout = timeout
        self._lock_path = self._path.with_suffix(self._path.suffix + ".lock")
        self._fd: int | None = None

    @property
    def path(self) -> Path:
        return self._path

    @property
    def is_locked(self) -> bool:
        return self._fd is not None

    def acquire(self) -> None:
        """Acquire the file lock."""
        if self._fd is not None:
            return  # Already locked

        # Ensure parent directory exists
        self._lock_path.parent.mkdir(parents=True, exist_ok=True)

        start_time = time.monotonic()

        while True:
            try:
                # Open or create lock file
                self._fd = os.open(
                    str(self._lock_path),
                    os.O_CREAT | os.O_RDWR,
                )

                # Try to acquire lock
                self._lock_fd(self._fd)
                return

            except (OSError, BlockingIOError):
                if self._fd is not None:
                    os.close(self._fd)
                    self._fd = None

                if time.monotonic() - start_time > self._timeout:
                    raise TimeoutError(
                        f"Could not acquire lock on {self._lock_path} "
                        f"within {self._timeout} seconds"
                    )

                time.sleep(0.01)

    def release(self) -> None:
        """Release the file lock."""
        if self._fd is None:
            return

        try:
            self._unlock_fd(self._fd)
        finally:
            os.close(self._fd)
            self._fd = None

        # Try to remove lock file (best effort)
        try:
            self._lock_path.unlink(missing_ok=True)
        except OSError:
            pass

    def _lock_fd(self, fd: int) -> None:
        """Lock a file descriptor."""
        if sys.platform == "win32":
            import msvcrt

            msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
        else:
            import fcntl

            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def _unlock_fd(self, fd: int) -> None:
        """Unlock a file descriptor."""
        if sys.platform == "win32":
            import msvcrt

            try:
                msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
            except OSError:
                pass
        else:
            import fcntl

            fcntl.flock(fd, fcntl.LOCK_UN)

    def __enter__(self) -> FileLock:
        self.acquire()
        return self

    def __exit__(
        self, exc_type: type | None, exc_val: Exception | None, exc_tb: object
    ) -> None:
        self.release()


@contextmanager
def file_lock(path: Path, timeout: float = 10.0) -> Iterator[FileLock]:
    """Context manager for file locking."""
    lock = FileLock(path, timeout)
    lock.acquire()
    try:
        yield lock
    finally:
        lock.release()
