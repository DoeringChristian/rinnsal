"""Aim integration for rinnsal.

Usage::

    from rinnsal import task
    from rinnsal.aim import AimLogger

    @task
    def train(lr: float, epochs: int):
        logger = AimLogger(experiment="training")
        for it in range(epochs):
            logger.track(step(), name="loss", step=it)

:class:`AimLogger` is a thin subclass of :class:`aim.Run` that
auto-populates from :mod:`rinnsal.context`:

* ``repo`` defaults to the cluster coordinator's aim server when the
  active executor is a :class:`ClusterExecutor` (queried via
  ``GET /api/cluster/aim``); otherwise falls back to a local repo at
  ``<current.db_path>/.aim``.
* ``experiment`` defaults to the current flow name.
* ``run["rinnsal"]`` is populated with flow name, run id, task name,
  task hash, snapshot hash, and executor kind.
* ``run["hparams"]`` is populated from :data:`rinnsal.context.current.task_args`
  — i.e. the resolved positional + keyword arguments the task was
  invoked with, so you don't have to re-list them.

Any kwargs the user passes in win over the defaults.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

from rinnsal.context import current

if TYPE_CHECKING:
    import aim

_log = logging.getLogger("rinnsal.aim")


# ── repo resolution ────────────────────────────────────────────────


def _executor_kind(executor: Any) -> str:
    """Short tag for the executor class: ``inline`` / ``subprocess`` /
    ``cluster`` / ``ssh`` / ``pssh`` / ``slurm`` / ``fork`` / ``unknown``.
    """
    if executor is None:
        return "unknown"
    name = type(executor).__name__.lower()
    for tag in (
        "cluster", "persistentssh", "ssh", "slurm",
        "subprocess", "fork", "inline",
    ):
        if tag in name:
            return "pssh" if tag == "persistentssh" else tag
    return type(executor).__name__


def _cluster_repo_url(executor: Any) -> str | None:
    """Query the cluster coordinator for its advertised aim repo.

    Returns ``None`` when the executor isn't a ClusterExecutor, when
    the coordinator isn't reachable, or when it was started without
    ``--aim``.
    """
    if executor is None:
        return None
    host = getattr(executor, "_host_url", None) or getattr(
        executor, "host_url", None
    )
    if not host:
        return None
    try:
        import httpx
    except ImportError:
        return None
    try:
        with httpx.Client(timeout=3.0) as client:
            r = client.get(f"{host.rstrip('/')}/api/cluster/aim")
        if r.status_code != 200:
            return None
        data = r.json()
    except Exception as e:
        _log.debug("cluster aim lookup failed: %s", e)
        return None
    return data.get("repo") or None


def _resolve_repo() -> str:
    """Pick a default aim repo based on the ambient flow context.

    Remote executor with a coordinator advertising an aim server →
    that server. Everything else → ``<db_path>/.aim`` (falling back to
    ``./.aim`` when we're not inside a flow).
    """
    remote = _cluster_repo_url(current.executor)
    if remote:
        return remote
    db_path = current.db_path
    if db_path is not None:
        return str(Path(db_path) / ".aim")
    return str(Path.cwd() / ".aim")


# ── serialization helpers ──────────────────────────────────────────


def _to_hparam(v: Any, max_len: int = 200) -> Any:
    """Convert an arbitrary task argument to something aim can store.

    aim's ``run[key] = value`` accepts JSON-compatible scalars, lists,
    and dicts. Non-serializable objects (tensors, user classes without
    a dict form) get replaced with ``{"_type": …, "_repr": …}`` so aim
    doesn't choke, but the user can still see what was there.
    """
    if v is None or isinstance(v, (bool, int, float)):
        return v
    if isinstance(v, str):
        return v if len(v) <= max_len else v[:max_len] + "..."
    if isinstance(v, dict):
        return {str(k): _to_hparam(val, max_len) for k, val in v.items()}
    if hasattr(v, "__dataclass_fields__"):
        return {
            k: _to_hparam(getattr(v, k), max_len)
            for k in v.__dataclass_fields__
        }
    if hasattr(v, "to_dict"):
        try:
            return _to_hparam(v.to_dict(), max_len)
        except Exception:
            pass
    if isinstance(v, (list, tuple)):
        return [_to_hparam(x, max_len) for x in v]
    r = repr(v)
    if len(r) > max_len:
        r = r[:max_len] + "..."
    return {"_type": type(v).__name__, "_repr": r}


# ── AimLogger ──────────────────────────────────────────────────────


def _aim_run_cls():
    """Lazy import of aim.Run with an actionable error message."""
    try:
        import aim
    except ImportError as e:
        raise ImportError(
            "rinnsal.aim requires aim. Install with: "
            "pip install 'rinnsal[aim]' (or `pip install 'aim>=3.25,<4'`)"
        ) from e
    return aim.Run


# aim subclasses must inherit from aim.Run, which we can only resolve
# at import time if aim is available. Build the class lazily.
_CACHED_AIMLOGGER: type | None = None


def _build_aimlogger_cls() -> type:
    global _CACHED_AIMLOGGER
    if _CACHED_AIMLOGGER is not None:
        return _CACHED_AIMLOGGER

    RunBase = _aim_run_cls()

    class AimLogger(RunBase):  # type: ignore[misc, valid-type]
        """aim.Run pre-wired to the rinnsal flow/task/run context.

        Parameters
        ----------
        repo:
            Override the aim repo URL or path. By default picks the
            cluster coordinator's aim server (when running under
            ``ClusterExecutor``) or ``<db_path>/.aim`` locally.
        experiment:
            Override the aim experiment name. Defaults to the current
            flow name.
        populate_hparams:
            When true (default), ``current.task_args`` is copied into
            ``run["hparams"]``. Turn off if you want to build hparams
            manually.
        **params:
            Extra hparams-style values; each is stored under ``run[k]``.
        """

        def __init__(
            self,
            *,
            repo: str | Path | None = None,
            experiment: str | None = None,
            system_tracking_interval: int | None = 10,
            log_system_params: bool = True,
            capture_terminal_logs: bool = False,
            populate_hparams: bool = True,
            **params: Any,
        ) -> None:
            repo_resolved = (
                str(repo) if repo is not None else _resolve_repo()
            )
            exp = experiment or current.flow_name or "default"

            super().__init__(
                repo=repo_resolved,
                experiment=exp,
                system_tracking_interval=system_tracking_interval,
                log_system_params=log_system_params,
                capture_terminal_logs=capture_terminal_logs,
            )

            # Stamp rinnsal metadata under a namespace (standard aim
            # convention for hparam groups).
            self["rinnsal"] = {
                "flow": current.flow_name or None,
                "run_id": current.run_id or None,
                "task": current.task_name or None,
                "task_hash": current.task_hash or None,
                "snapshot_hash": current.snapshot_hash or None,
                "executor": _executor_kind(current.executor),
            }

            # Auto-populate hparams from the task's resolved arguments.
            if populate_hparams and current.task_args:
                self["hparams"] = {
                    k: _to_hparam(v) for k, v in current.task_args.items()
                }

            # User-supplied extras get their own top-level keys so they
            # don't collide with rinnsal/hparams.
            for k, v in params.items():
                self[k] = _to_hparam(v)

    _CACHED_AIMLOGGER = AimLogger
    return AimLogger


def __getattr__(name: str) -> Any:
    """Expose ``AimLogger`` lazily so importing ``rinnsal.aim`` doesn't
    require aim to be installed at import time.
    """
    if name == "AimLogger":
        return _build_aimlogger_cls()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = ["AimLogger"]
