"""Reader for exploring and loading rinnsal logs."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import cloudpickle

from rinnsal.logger.logger import EVENTS_FILE, MARKER_FILE


class LazyFigure:
    """A lazy proxy for a pickled figure.

    The actual figure is only loaded when you access any attribute,
    call a method, or pass it to a function that inspects it.
    """

    def __init__(self, loader):
        object.__setattr__(self, "_loader", loader)
        object.__setattr__(self, "_obj", None)
        object.__setattr__(self, "_loaded", False)

    def _resolve(self):
        if not object.__getattribute__(self, "_loaded"):
            obj = object.__getattribute__(self, "_loader")()
            object.__setattr__(self, "_obj", obj)
            object.__setattr__(self, "_loaded", True)
        return object.__getattribute__(self, "_obj")

    def __getattr__(self, name):
        return getattr(self._resolve(), name)

    def __setattr__(self, name, value):
        setattr(self._resolve(), name, value)

    def __repr__(self):
        return repr(self._resolve())

    def __str__(self):
        return str(self._resolve())

    def __iter__(self):
        return iter(self._resolve())

    def __call__(self, *args, **kwargs):
        return self._resolve()(*args, **kwargs)

    def __getitem__(self, key):
        return self._resolve()[key]

    def __len__(self):
        return len(self._resolve())

    def __bool__(self):
        return bool(self._resolve())


class LogReader:
    """Reader for exploring and loading rinnsal logs.

    Args:
        log_dir: Path to a log directory (single run or parent of
                 multiple runs).

    Example:
        reader = LogReader("/path/to/logs/run1")
        print(reader.iterations)
        print(reader.scalar_tags)

        loss = reader.load_scalars("loss")
        fig = reader.load_figure("plot", iteration=100)

        reader = LogReader("/path/to/logs")
        print(reader.runs)
        run1 = reader.get_run("run1")
    """

    def __init__(self, log_dir: str | Path):
        self._log_dir = Path(log_dir)
        if not self._log_dir.exists():
            raise FileNotFoundError(
                f"Log directory not found: {log_dir}"
            )
        # Caches
        self._scalars_cache: dict[str, list[tuple[int, float]]] | None = (
            None
        )
        self._text_cache: dict[str, list[tuple[int, str]]] | None = None
        self._figures_cache: (
            dict[str, list[tuple[int, bytes]]] | None
        ) = None
        self._checkpoints_cache: (
            dict[str, list[tuple[int, bytes]]] | None
        ) = None
        self._loaded = False

    def _load_events(self) -> None:
        """Load all events from protobuf file into caches."""
        if self._loaded:
            return

        from rinnsal.logger.event_file import EventFileReader

        self._scalars_cache = {}
        self._text_cache = {}
        self._figures_cache = {}
        self._checkpoints_cache = {}

        events_path = self._log_dir / EVENTS_FILE
        if not events_path.exists():
            self._loaded = True
            return

        reader = EventFileReader(events_path)
        for event in reader:
            it = event.iteration
            data_type = event.WhichOneof("data")

            if data_type == "scalar":
                tag = event.scalar.tag
                if tag not in self._scalars_cache:
                    self._scalars_cache[tag] = []
                self._scalars_cache[tag].append(
                    (it, event.scalar.value)
                )

            elif data_type == "text":
                tag = event.text.tag
                if tag not in self._text_cache:
                    self._text_cache[tag] = []
                self._text_cache[tag].append(
                    (it, event.text.value)
                )

            elif data_type == "figure":
                tag = event.figure.tag
                if tag not in self._figures_cache:
                    self._figures_cache[tag] = []
                self._figures_cache[tag].append(
                    (it, event.figure.data)
                )

            elif data_type == "checkpoint":
                tag = event.checkpoint.tag
                if tag not in self._checkpoints_cache:
                    self._checkpoints_cache[tag] = []
                self._checkpoints_cache[tag].append(
                    (it, event.checkpoint.data)
                )

        # Sort by iteration
        for cache in [
            self._scalars_cache,
            self._text_cache,
            self._figures_cache,
            self._checkpoints_cache,
        ]:
            for tag in cache:
                cache[tag].sort(key=lambda x: x[0])

        self._loaded = True

    @property
    def path(self) -> Path:
        return self._log_dir

    @property
    def is_run(self) -> bool:
        return (self._log_dir / MARKER_FILE).exists()

    @property
    def runs(self) -> list[str]:
        if self.is_run:
            return ["."]
        runs = []
        for item in self._log_dir.rglob(MARKER_FILE):
            run_path = item.parent
            runs.append(str(run_path.relative_to(self._log_dir)))
        return sorted(runs)

    def get_run(self, name: str) -> LogReader:
        if name == ".":
            return self
        return LogReader(self._log_dir / name)

    @property
    def iterations(self) -> list[int]:
        if not self.is_run:
            raise ValueError(
                "Not a run directory. Use get_run() first."
            )
        self._load_events()
        iterations: set[int] = set()
        for cache in [
            self._scalars_cache,
            self._text_cache,
            self._figures_cache,
            self._checkpoints_cache,
        ]:
            if cache:
                for values in cache.values():
                    for it, _ in values:
                        iterations.add(it)
        return sorted(iterations)

    @property
    def scalar_tags(self) -> set[str]:
        if not self.is_run:
            raise ValueError(
                "Not a run directory. Use get_run() first."
            )
        self._load_events()
        return set(self._scalars_cache.keys()) if self._scalars_cache else set()

    @property
    def text_tags(self) -> set[str]:
        if not self.is_run:
            raise ValueError(
                "Not a run directory. Use get_run() first."
            )
        self._load_events()
        return set(self._text_cache.keys()) if self._text_cache else set()

    @property
    def figure_tags(self) -> set[str]:
        if not self.is_run:
            raise ValueError(
                "Not a run directory. Use get_run() first."
            )
        self._load_events()
        return (
            set(self._figures_cache.keys())
            if self._figures_cache
            else set()
        )

    @property
    def checkpoint_tags(self) -> set[str]:
        if not self.is_run:
            raise ValueError(
                "Not a run directory. Use get_run() first."
            )
        self._load_events()
        return (
            set(self._checkpoints_cache.keys())
            if self._checkpoints_cache
            else set()
        )

    def load_scalars(self, tag: str) -> list[tuple[int, float]]:
        if not self.is_run:
            raise ValueError(
                "Not a run directory. Use get_run() first."
            )
        self._load_events()
        return (self._scalars_cache or {}).get(tag, [])

    def scalars(self, tag: str) -> tuple[list[int], list[float]]:
        data = self.load_scalars(tag)
        if not data:
            return [], []
        its, vals = zip(*data)
        return list(its), list(vals)

    def load_text(self, tag: str) -> list[tuple[int, str]]:
        if not self.is_run:
            raise ValueError(
                "Not a run directory. Use get_run() first."
            )
        self._load_events()
        return (self._text_cache or {}).get(tag, [])

    def load_figure(self, tag: str, iteration: int) -> Any:
        if not self.is_run:
            raise ValueError(
                "Not a run directory. Use get_run() first."
            )
        self._load_events()
        if self._figures_cache and tag in self._figures_cache:
            for it, data in self._figures_cache[tag]:
                if it == iteration:
                    return cloudpickle.loads(data)
        raise FileNotFoundError(
            f"Figure not found: {tag} at iteration {iteration}"
        )

    def figure_iterations(self, tag: str) -> list[int]:
        if not self.is_run:
            raise ValueError(
                "Not a run directory. Use get_run() first."
            )
        self._load_events()
        if self._figures_cache and tag in self._figures_cache:
            return [it for it, _ in self._figures_cache[tag]]
        return []

    def figures(
        self, tag: str
    ) -> tuple[list[int], list[LazyFigure]]:
        iters = self.figure_iterations(tag)
        figs = [
            LazyFigure(lambda it=it: self.load_figure(tag, it))
            for it in iters
        ]
        return iters, figs

    def load_checkpoint(self, tag: str, iteration: int) -> Any:
        if not self.is_run:
            raise ValueError(
                "Not a run directory. Use get_run() first."
            )
        self._load_events()
        if (
            self._checkpoints_cache
            and tag in self._checkpoints_cache
        ):
            for it, data in self._checkpoints_cache[tag]:
                if it == iteration:
                    return cloudpickle.loads(data)
        raise FileNotFoundError(
            f"Checkpoint not found: {tag} at iteration {iteration}"
        )

    def checkpoint_iterations(self, tag: str) -> list[int]:
        if not self.is_run:
            raise ValueError(
                "Not a run directory. Use get_run() first."
            )
        self._load_events()
        if (
            self._checkpoints_cache
            and tag in self._checkpoints_cache
        ):
            return [
                it for it, _ in self._checkpoints_cache[tag]
            ]
        return []

    def load_checkpoints(
        self, tag: str
    ) -> list[tuple[int, Any]]:
        if not self.is_run:
            raise ValueError(
                "Not a run directory. Use get_run() first."
            )
        self._load_events()
        if (
            self._checkpoints_cache
            and tag in self._checkpoints_cache
        ):
            return [
                (it, cloudpickle.loads(data))
                for it, data in self._checkpoints_cache[tag]
            ]
        return []

    def __getitem__(self, tag: str) -> tuple[int, Any]:
        if not self.is_run:
            raise ValueError(
                "Not a run directory. Use get_run() first."
            )

        if tag in self.scalar_tags:
            data = self.load_scalars(tag)
            if data:
                return data[-1]

        if tag in self.text_tags:
            data = self.load_text(tag)
            if data:
                return data[-1]

        if tag in self.figure_tags:
            iterations = self.figure_iterations(tag)
            if iterations:
                last_it = iterations[-1]
                return (last_it, self.load_figure(tag, last_it))

        if tag in self.checkpoint_tags:
            data = self.load_checkpoints(tag)
            if data:
                return data[-1]

        raise KeyError(f"Tag not found: {tag}")

    def __repr__(self) -> str:
        if self.is_run:
            return f"LogReader('{self._log_dir}', iterations={len(self.iterations)})"
        return f"LogReader('{self._log_dir}', runs={self.runs})"
