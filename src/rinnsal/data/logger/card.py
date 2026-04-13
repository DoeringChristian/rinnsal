"""Card builder — user-composed grouping of components.

A card is an addressable, named collection of Components. Cards can be
re-emitted at multiple iterations; the viewer shows a slider across
emissions sharing the same ``(task, name)`` identity.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from rinnsal.data.logger.components import Component, autodetect

if TYPE_CHECKING:
    from rinnsal.data.logger.logger import Logger
    from rinnsal.data.logger.proxy import LoggerProxy


# Components use a "checkpoint" oneof slot at the top-level Event; inside
# CardComponent the same proto message lives under "artifact". This map
# translates the field name the component returns from to_payload() to
# the CardComponent slot name.
_CARD_FIELD_MAP: dict[str, str] = {
    "checkpoint": "artifact",
}


class Card:
    """Builder for a composed card.

    Typical usage inside a task::

        card = logger.card("training_progress")
        card.append(Markdown("# Epoch 5"))
        card.append(Plotly(fig))
        card.append(Scalar(loss))
        # implicit commit on context-manager exit or logger shutdown

    Or with a context manager::

        with logger.card("eda") as c:
            c.append(Markdown("# Distribution"))
            c.append(Table(df))

    ``logger.card(name)`` captures the current task name (if any) so the
    card is keyed by ``(task, name)`` in storage.
    """

    def __init__(
        self,
        logger: "Logger | LoggerProxy",
        name: str,
        task: str = "",
    ) -> None:
        self._logger = logger
        self._name = name
        self._task = task
        self._components: list[tuple[str, Component]] = []
        self._committed = False

    @property
    def name(self) -> str:
        return self._name

    @property
    def task(self) -> str:
        return self._task

    def append(self, component: Any, tag: str = "") -> "Card":
        """Add a component to the card.

        Raw values (plotly/matplotlib figures, strings, bytes, numpy
        arrays) are auto-detected via :func:`components.autodetect`.
        """
        if not isinstance(component, Component):
            component = autodetect(component)
        self._components.append((tag, component))
        return self

    def commit(self, it: int | None = None) -> None:
        """Write the card as a single CardEvent at iteration *it*.

        No-op if the card has no components yet. Clears the pending
        component list so the same builder can be reused for the next
        iteration.
        """
        if not self._components:
            return
        self._logger._enqueue_card(
            self._name, self._task, list(self._components), it
        )
        self._components.clear()
        self._committed = True

    def __enter__(self) -> "Card":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_type is None and self._components and not self._committed:
            self.commit()

    def __repr__(self) -> str:
        return (
            f"Card(name={self._name!r}, task={self._task!r}, "
            f"components={len(self._components)}, "
            f"committed={self._committed})"
        )


def build_card_event(
    name: str,
    task: str,
    components: list[tuple[str, Component]],
    logger: "Logger | LoggerProxy | None" = None,
) -> Any:
    """Materialize a list of (tag, Component) into a CardEvent proto.

    *logger* is passed to each component's ``to_payload`` so heavy
    payloads can offload to a blob store if one is configured.
    """
    from rinnsal.data.logger.events_pb2 import CardComponent, CardEvent

    card_event = CardEvent(name=name, task=task)
    for tag, component in components:
        field, msg = component.to_payload(logger)
        if hasattr(msg, "tag"):
            msg.tag = tag
        slot = _CARD_FIELD_MAP.get(field, field)
        cc = CardComponent(tag=tag)
        getattr(cc, slot).CopyFrom(msg)
        card_event.components.append(cc)
    return card_event
