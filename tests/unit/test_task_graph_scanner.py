"""TaskGraphCache must skip heavy event payloads instead of reading them.

Without the wire-format header peek, /api/flows reads every byte of
every events.pb in the project — a multi-GB I/O storm on a slow
filesystem. This test pins the I/O behavior: the scanner only reads
record headers when the payload isn't task_node/task_edge.
"""

from __future__ import annotations

from pathlib import Path

from rinnsal.data.file_store import FileDatabase
from rinnsal.data.logger.logger import Logger
from rinnsal.viewer._data import TaskGraphCache, invalidate_caches


HEAVY_PNG = b"\x89PNG\r\n\x1a\n" + b"X" * (1 * 1024 * 1024)


def _make_run_with_heavy_payloads(tmp_path: Path, n_images: int = 20) -> Path:
    db = FileDatabase(root=tmp_path / ".rinnsal")
    rd = tmp_path / ".rinnsal/flows/f/runs/r1"
    rd.mkdir(parents=True)
    lg = Logger(rd, database=db)
    lg.add_task_node("first", "h1", "success", duration=0.1, params='{"x": 1}')
    lg.add_task_edge("first", "second")
    lg.add_task_node("second", "h2", "success", duration=0.2, params='{"y": 2}')
    # Heavy payloads — should be skipped by the scanner.
    for i in range(n_images):
        lg.add_image(f"fig{i}", HEAVY_PNG, it=i)
    lg.close()
    return rd


class TestScanner:
    def test_extracts_nodes_and_edges(self, tmp_path):
        invalidate_caches()
        rd = _make_run_with_heavy_payloads(tmp_path)
        cache = TaskGraphCache()
        cache.load(rd / "events.pb")
        names = {n[0] for n in cache.task_nodes}
        assert names == {"first", "second"}
        assert cache.task_edges == [("first", "second")]

    def test_does_not_read_heavy_payloads(self, tmp_path, monkeypatch):
        """Spy on file.read() to assert we never read a multi-MB chunk."""
        invalidate_caches()
        rd = _make_run_with_heavy_payloads(tmp_path, n_images=10)

        original_open = open
        big_reads: list[int] = []

        class CountingFile:
            def __init__(self, real):
                self._real = real

            def read(self, n=-1):
                data = self._real.read(n)
                if len(data) > 64 * 1024:  # >64KB is "heavy"
                    big_reads.append(len(data))
                return data

            def seek(self, *a, **kw):
                return self._real.seek(*a, **kw)

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                self._real.close()

        def fake_open(path, mode="r", *a, **kw):
            real = original_open(path, mode, *a, **kw)
            if "b" in mode and str(path).endswith("events.pb"):
                return CountingFile(real)
            return real

        monkeypatch.setattr("builtins.open", fake_open)

        cache = TaskGraphCache()
        cache.load(rd / "events.pb")

        assert big_reads == [], (
            f"scanner read heavy chunks ({big_reads}); should have skipped "
            "all image payloads via f.seek"
        )
        assert len(cache.task_nodes) == 2
