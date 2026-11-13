"""Domain repository over the atomic JSON state file."""

from __future__ import annotations

from typing import Any

from ..errors import (
    AssetNotFound,
    InvalidStateTransition,
    RenditionNotFound,
)
from ..models import (
    IntegrityAudit,
    MediaAsset,
    ProvenanceEdge,
    Rendition,
    TERMINAL_CHECK_STATES,
    TERMINAL_RENDITION_STATES,
)
from ..serialization import deep_copy
from .state_file import JsonStateFile


class ProvenanceRepository:
    def __init__(self, state_file: JsonStateFile) -> None:
        self.state_file = state_file

    def create_asset(self, asset: MediaAsset) -> MediaAsset:
        def mutate(state: dict[str, Any]) -> MediaAsset:
            assets = state["assets"]
            if asset.id in assets:
                raise InvalidStateTransition("asset", "duplicate")
            assets[asset.id] = asset.to_dict()
            return asset

        return self.state_file.mutate(mutate)

    def get_asset(self, asset_id: str) -> MediaAsset:
        state = self.state_file.read()
        raw = state["assets"].get(asset_id)
        if not isinstance(raw, dict):
            raise AssetNotFound(asset_id)
        return MediaAsset.from_dict(deep_copy(raw))

    def create_rendition(self, rendition: Rendition) -> Rendition:
        def mutate(state: dict[str, Any]) -> Rendition:
            renditions = state["renditions"]
            if rendition.id in renditions:
                raise InvalidStateTransition("rendition", "duplicate")
            renditions[rendition.id] = rendition.to_dict()
            return rendition

        return self.state_file.mutate(mutate)

    def get_rendition(self, rendition_id: str) -> Rendition:
        state = self.state_file.read()
        raw = state["renditions"].get(rendition_id)
        if not isinstance(raw, dict):
            raise RenditionNotFound(rendition_id)
        return Rendition.from_dict(deep_copy(raw))

    def transition_rendition(self, rendition_id: str, target: str) -> Rendition:
        def mutate(state: dict[str, Any]) -> Rendition:
            raw = state["renditions"].get(rendition_id)
            if not isinstance(raw, dict):
                raise RenditionNotFound(rendition_id)
            rendition = Rendition.from_dict(deep_copy(raw))
            rendition.transition_to(target)
            state["renditions"][rendition_id] = rendition.to_dict()
            return rendition

        return self.state_file.mutate(mutate)

    def fail_rendition(
        self,
        rendition_id: str,
        error_code: str,
        error_message: str,
    ) -> Rendition:
        def mutate(state: dict[str, Any]) -> Rendition:
            raw = state["renditions"].get(rendition_id)
            if not isinstance(raw, dict):
                raise RenditionNotFound(rendition_id)
            rendition = Rendition.from_dict(deep_copy(raw))
            rendition.error_code = error_code
            rendition.error_message = error_message
            rendition.transition_to("failed")
            state["renditions"][rendition_id] = rendition.to_dict()
            return rendition

        return self.state_file.mutate(mutate)

    def complete_rendition(
        self,
        rendition_id: str,
        output_asset: MediaAsset,
        edge: ProvenanceEdge,
    ) -> Rendition:
        def mutate(state: dict[str, Any]) -> Rendition:
            raw = state["renditions"].get(rendition_id)
            if not isinstance(raw, dict):
                raise RenditionNotFound(rendition_id)
            rendition = Rendition.from_dict(deep_copy(raw))
            if output_asset.id in state["assets"]:
                raise InvalidStateTransition("asset", "duplicate")
            state["assets"][output_asset.id] = output_asset.to_dict()
            state["provenance_edges"].append(edge.to_dict())
            rendition.output_asset_id = output_asset.id
            rendition.error_code = None
            rendition.error_message = None
            rendition.transition_to("completed")
            state["renditions"][rendition_id] = rendition.to_dict()
            return rendition

        return self.state_file.mutate(mutate)

    def get_edge(self, rendition_id: str) -> ProvenanceEdge:
        state = self.state_file.read()
        for raw in state["provenance_edges"]:
            if isinstance(raw, dict) and raw.get("rendition_id") == rendition_id:
                return ProvenanceEdge.from_dict(deep_copy(raw))
        raise RenditionNotFound(rendition_id)

    def lineage_for(self, rendition_id: str) -> dict[str, Any]:
        rendition = self.get_rendition(rendition_id)
        if rendition.status != "completed":
            return {
                "rendition_id": rendition_id,
                "status": rendition.status,
                "parents": [],
                "output": None,
                "edge": None,
            }
        edge = self.get_edge(rendition_id)
        parents = [self.get_asset(asset_id).to_public_dict() for asset_id in edge.parent_asset_ids]
        output = self.get_asset(edge.output_asset_id).to_public_dict()
        return {
            "rendition_id": rendition_id,
            "status": rendition.status,
            "parents": parents,
            "output": output,
            "edge": edge.to_dict(),
        }

    def interrupt_rendition(
        self,
        rendition_id: str,
        reason: str,
        detail: str,
    ) -> Rendition | None:
        """Mark admitted-but-unfinished work as interrupted.

        Returns ``None`` when the rendition already reached a terminal state,
        so a late stop can never overwrite a completed result.
        """

        def mutate(state: dict[str, Any]) -> Rendition | None:
            raw = state["renditions"].get(rendition_id)
            if not isinstance(raw, dict):
                return None
            rendition = Rendition.from_dict(deep_copy(raw))
            if rendition.status in TERMINAL_RENDITION_STATES:
                return None
            rendition.error_code = reason
            rendition.error_message = detail
            rendition.transition_to("interrupted")
            state["renditions"][rendition_id] = rendition.to_dict()
            return rendition

        return self.state_file.mutate(mutate)

    def find_pending_renditions(self) -> list[str]:
        state = self.state_file.read()
        return sorted(
            identifier
            for identifier, raw in state["renditions"].items()
            if isinstance(raw, dict)
            and raw.get("status") in {"queued", "running"}
        )

    def find_pending_checks(self) -> list[str]:
        state = self.state_file.read()
        return sorted(
            identifier
            for identifier, raw in state["integrity_checks"].items()
            if isinstance(raw, dict) and raw.get("status") == "pending"
        )

    def abandon_check(
        self,
        check_id: str,
        reason: str,
    ) -> IntegrityAudit | None:
        def mutate(state: dict[str, Any]) -> IntegrityAudit | None:
            raw = state["integrity_checks"].get(check_id)
            if not isinstance(raw, dict):
                return None
            audit = IntegrityAudit.from_dict(deep_copy(raw))
            if audit.status in TERMINAL_CHECK_STATES:
                return None
            audit.finish(
                status="abandoned",
                expected_digest=audit.expected_digest,
                reproduced_digest=None,
                expected_descriptor=audit.expected_descriptor,
                reproduced_descriptor=None,
                reason=reason,
            )
            state["integrity_checks"][check_id] = audit.to_dict()
            return audit

        return self.state_file.mutate(mutate)

    def recover_incomplete_work(self) -> dict[str, int]:
        """Settle work that a previous process left behind before it could finish.

        A process that was stopped mid-operation leaves ``running`` or
        ``pending`` records behind. On the next start those records are
        converted to explicit incomplete facts so they are never mistaken for
        completed output.
        """

        pending_renditions = self.find_pending_renditions()
        for rendition_id in pending_renditions:
            self.interrupt_rendition(
                rendition_id,
                "RENDER_INTERRUPTED",
                "The rendition did not finish before the service stopped.",
            )
        pending_checks = self.find_pending_checks()
        for check_id in pending_checks:
            self.abandon_check(check_id, "check_interrupted")
        return {
            "renditions": len(pending_renditions),
            "integrity_checks": len(pending_checks),
        }

    def insert_integrity_audit(self, audit: IntegrityAudit) -> IntegrityAudit:
        def mutate(state: dict[str, Any]) -> IntegrityAudit:
            audits = state["integrity_checks"]
            if audit.id in audits:
                raise InvalidStateTransition("integrity_check", "duplicate")
            audits[audit.id] = audit.to_dict()
            return audit

        return self.state_file.mutate(mutate)

    def settle_integrity_audit(
        self,
        check_id: str,
        *,
        status: str,
        expected_digest: str | None,
        reproduced_digest: str | None,
        expected_descriptor: dict[str, Any] | None,
        reproduced_descriptor: dict[str, Any] | None,
        reason: str | None,
    ) -> IntegrityAudit:
        def mutate(state: dict[str, Any]) -> IntegrityAudit:
            raw = state["integrity_checks"].get(check_id)
            if not isinstance(raw, dict):
                raise RenditionNotFound(check_id)
            audit = IntegrityAudit.from_dict(deep_copy(raw))
            audit.finish(
                status=status,
                expected_digest=expected_digest,
                reproduced_digest=reproduced_digest,
                expected_descriptor=expected_descriptor,
                reproduced_descriptor=reproduced_descriptor,
                reason=reason,
            )
            state["integrity_checks"][check_id] = audit.to_dict()
            return audit

        return self.state_file.mutate(mutate)

    def get_integrity_audit(self, check_id: str) -> IntegrityAudit:
        state = self.state_file.read()
        raw = state["integrity_checks"].get(check_id)
        if not isinstance(raw, dict):
            raise RenditionNotFound(check_id)
        return IntegrityAudit.from_dict(deep_copy(raw))
