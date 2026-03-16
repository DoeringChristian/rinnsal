"""Event file reader and writer for protobuf-based logging.

File format:
    Each record is stored as:
    - 4 bytes: message length (little endian uint32)
    - N bytes: serialized protobuf Event message

This format allows efficient append-only writes and streaming reads.
"""

from __future__ import annotations

import struct
import threading
from pathlib import Path
from typing import TYPE_CHECKING, Iterator

if TYPE_CHECKING:
    from rinnsal.logger.events_pb2 import Event


class EventFileWriter:
    """Writes Event messages to a file in length-prefixed format.

    Thread-safe for concurrent writes from multiple threads.
    """

    def __init__(self, path: Path | str):
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._file = open(self._path, "ab")
        self._lock = threading.Lock()

    def write(self, event: Event) -> None:
        """Write an event to the file."""
        data = event.SerializeToString()
        length = struct.pack("<I", len(data))
        with self._lock:
            self._file.write(length)
            self._file.write(data)

    def flush(self) -> None:
        """Flush buffered data to disk."""
        with self._lock:
            self._file.flush()

    def close(self) -> None:
        """Close the file."""
        with self._lock:
            self._file.close()

    def __enter__(self) -> EventFileWriter:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()


class EventFileReader:
    """Reads Event messages from a file in length-prefixed format.

    Supports reading while the file is being written to.
    """

    def __init__(self, path: Path | str):
        self._path = Path(path)
        if not self._path.exists():
            raise FileNotFoundError(f"Event file not found: {path}")

    def __iter__(self) -> Iterator[Event]:
        """Iterate over all events in the file."""
        from rinnsal.logger.events_pb2 import Event

        with open(self._path, "rb") as f:
            while True:
                length_bytes = f.read(4)
                if len(length_bytes) < 4:
                    break

                length = struct.unpack("<I", length_bytes)[0]

                data = f.read(length)
                if len(data) < length:
                    break

                event = Event()
                event.ParseFromString(data)
                yield event

    def read_all(self) -> list[Event]:
        """Read all events from the file."""
        return list(self)

