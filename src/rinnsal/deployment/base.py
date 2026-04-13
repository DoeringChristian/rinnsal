"""Deployment protocol — package a flow and trigger remote runs.

This layer is the seam where rinnsal would integrate with Argo, Step Functions,
Airflow, etc. The default implementation (``LocalDeployment``) is a no-op that
runs the flow in-process.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from rinnsal.modeling.flow import FlowDef


@dataclass
class FlowArtifact:
    """An opaque, deploy-ready bundle of a flow."""

    flow_name: str
    payload: Any  # backend-specific (e.g. zip path, container ref, dict)


@dataclass
class DeploymentHandle:
    """Reference to a deployed flow that can be triggered."""

    flow_name: str
    payload: Any


@runtime_checkable
class Deployment(Protocol):
    def package(self, flow: FlowDef) -> FlowArtifact: ...

    def deploy(self, artifact: FlowArtifact) -> DeploymentHandle: ...

    def trigger(
        self, handle: DeploymentHandle, params: dict[str, Any] | None = None
    ) -> Any: ...
