"""High-level handles over the low-level :class:`Client`.

A :class:`Run` wraps one run on the server and exposes typed accessors
for scalars, text, figures, cards, and raw blobs. :class:`Series` is a
lazy, iteration-indexable view over one tag within a run — the piece a
custom viewer will iterate over to scrub through training history.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Iterator

if TYPE_CHECKING:
    from rinnsal.sdk.client import Client


# ── Series ─────────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class Sample:
    """One iteration of a :class:`Series`."""

    it: int
    value: Any


class Series:
    """Iteration-indexable view over one tag's emissions.

    Built by :meth:`Run.scalars`, :meth:`Run.text`, :meth:`Run.figures`,
    :meth:`Run.images`. Entries are *not* fetched eagerly — indexing by
    iteration (``series[10]``) returns a :class:`Sample` whose ``value``
    is realized lazily (a scalar is just a float; a figure/image is
    fetched on ``.load()``).
    """

    def __init__(
        self,
        *,
        kind: str,
        tag: str,
        iterations: list[int],
        loader,
    ) -> None:
        self.kind = kind   # "scalar" | "text" | "figure" | "image"
        self.tag = tag
        self._its = iterations
        self._loader = loader  # (it: int) -> Any

    # ── dunder ─────────────────────────────────────────────────────

    def __len__(self) -> int:
        return len(self._its)

    def __iter__(self) -> Iterator[Sample]:
        for it in self._its:
            yield Sample(it=it, value=self._loader(it))

    def __getitem__(self, it: int) -> Sample:
        if it not in self._its:
            raise KeyError(
                f"iteration {it} not in series {self.tag!r} "
                f"(available: {self._its[:5]}...)"
            )
        return Sample(it=it, value=self._loader(it))

    @property
    def iterations(self) -> list[int]:
        """Every iteration at which this tag was emitted."""
        return list(self._its)

    @property
    def latest(self) -> Sample:
        """The newest emission."""
        if not self._its:
            raise ValueError(f"series {self.tag!r} is empty")
        it = self._its[-1]
        return Sample(it=it, value=self._loader(it))


# ── Run ────────────────────────────────────────────────────────────


class Run:
    """Handle to one run on a remote server.

    Construct via :meth:`Client.run` (see :mod:`rinnsal.sdk.__init__`).
    """

    def __init__(
        self,
        client: "Client",
        *,
        run_id: str,
        run_path: str,
        flow: str | None = None,
        snapshot_hash: str | None = None,
    ) -> None:
        self._client = client
        self.run_id = run_id
        self.run_path = run_path
        self.flow = flow
        self._snapshot_hash = snapshot_hash

    # ── identity ───────────────────────────────────────────────────

    @property
    def snapshot_hash(self) -> str | None:
        """The code snapshot hash for this run, if the engine captured one."""
        if self._snapshot_hash is None:
            info = self._client.run_info(self.run_id)
            self._snapshot_hash = info.get("snapshot_hash")
            if self.flow is None:
                self.flow = info.get("flow")
        return self._snapshot_hash

    # ── scalars / text ─────────────────────────────────────────────

    def scalars(self, tag: str) -> Series:
        data = self._client.scalars(self.run_path).get(tag, [])
        its = [int(e["it"]) for e in data]
        by_it = {int(e["it"]): float(e["value"]) for e in data}
        return Series(
            kind="scalar", tag=tag, iterations=its,
            loader=lambda it: by_it[it],
        )

    def text(self, tag: str) -> Series:
        data = self._client.text(self.run_path).get(tag, [])
        its = [int(e["it"]) for e in data]
        by_it = {int(e["it"]): str(e["value"]) for e in data}
        return Series(
            kind="text", tag=tag, iterations=its,
            loader=lambda it: by_it[it],
        )

    # ── figures / images ───────────────────────────────────────────

    def figure(self, tag: str) -> Series:
        """Lazy handle: images are fetched on demand via ``.load()``."""
        meta = self._client.figures_meta(self.run_path).get(tag, [])
        its = [int(e["it"]) for e in meta]
        return Series(
            kind="figure", tag=tag, iterations=its,
            loader=lambda it: Artifact(
                client=self._client,
                run_path=self.run_path,
                kind="figure",
                tag=tag,
                it=it,
            ),
        )

    def image(self, tag: str) -> Series:
        meta = self._client.images_meta(self.run_path).get(tag, [])
        its = [int(e["it"]) for e in meta]
        return Series(
            kind="image", tag=tag, iterations=its,
            loader=lambda it: Artifact(
                client=self._client,
                run_path=self.run_path,
                kind="image",
                tag=tag,
                it=it,
            ),
        )

    # ── tags / cards / blobs ───────────────────────────────────────

    def tags(self) -> list[dict]:
        """Every ``(tag, kind)`` pair emitted by this run."""
        return self._client.tags(self.run_path)

    def cards(self) -> list[dict]:
        return self._client.cards(self.run_path)

    def card(
        self, name: str, task: str = "", it: int | None = None
    ) -> dict:
        return self._client.card(self.run_path, name, task=task, it=it)

    def blob(self, blob_hash: str) -> bytes:
        """Fetch one content-addressed blob attached to this run."""
        return self._client.blob(self.run_path, blob_hash)


# ── Artifact ───────────────────────────────────────────────────────


class Artifact:
    """Lazy handle to bytes attached to a run (figure PNG, image PNG)."""

    def __init__(
        self,
        *,
        client: "Client",
        run_path: str,
        kind: str,
        tag: str,
        it: int,
    ) -> None:
        self._client = client
        self._run_path = run_path
        self.kind = kind
        self.tag = tag
        self.it = it

    def bytes(self) -> bytes:
        """Return the raw bytes (fetches once)."""
        if self.kind == "figure":
            return self._client.figure_png(
                self._run_path, self.tag, self.it
            )
        if self.kind == "image":
            return self._client.image_png(
                self._run_path, self.tag, self.it
            )
        raise ValueError(f"unknown artifact kind {self.kind!r}")

    def load(self) -> bytes:
        """Alias for :meth:`bytes` — symmetric with scalar ``.value``."""
        return self.bytes()
