"""LocalDeployment — in-process no-op deployment."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from rinnsal.deployment.base import DeploymentHandle, FlowArtifact

if TYPE_CHECKING:
    from rinnsal.modeling.flow import FlowDef


class LocalDeployment:
    """No-op deployment: 'deploys' by holding a reference to the flow,
    'triggers' by calling it in-process.
    """

    def package(self, flow: FlowDef) -> FlowArtifact:
        return FlowArtifact(flow_name=flow.name, payload=flow)

    def deploy(self, artifact: FlowArtifact) -> DeploymentHandle:
        return DeploymentHandle(flow_name=artifact.flow_name, payload=artifact.payload)

    def trigger(
        self,
        handle: DeploymentHandle,
        params: dict[str, Any] | None = None,
    ) -> Any:
        flow = handle.payload
        return flow(**(params or {})).run()
