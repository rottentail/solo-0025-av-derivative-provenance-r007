"""Admission gate and bounded-drain coordination for the HTTP service.

A normal stop closes the admission gate first: no new derivative rendition may
start once the signal has arrived. Work that was already admitted keeps its
lease until the operation has fully finished (or has been rejected), so a
partial buffer can never be published as a completed rendition.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass

from .errors import ServiceShuttingDown


@dataclass
class WorkLease:
    """One admitted unit of CPU-bound derivative work."""

    _lifecycle: "ServiceLifecycle"
    _slot: int
    kind: str
    rendition_id: str | None = None
    check_id: str | None = None
    _released: bool = False

    def attach_rendition(self, rendition_id: str) -> None:
        self._lifecycle.attach_rendition(self, rendition_id)

    def attach_check(self, check_id: str) -> None:
        self._lifecycle.attach_check(self, check_id)

    def release(self) -> None:
        self._lifecycle.release_lease(self)


class ServiceLifecycle:
    """Tracks admission state, admitted work, and in-flight requests."""

    def __init__(self) -> None:
        self._condition = threading.Condition(threading.RLock())
        self._draining = False
        self._stop_reason: str | None = None
        self._requested_at: float | None = None
        self._active_work: dict[int, WorkLease] = {}
        self._active_requests = 0
        self._next_slot = 0

    # -- admission gate -------------------------------------------------

    @property
    def draining(self) -> bool:
        with self._condition:
            return self._draining

    @property
    def stop_reason(self) -> str | None:
        with self._condition:
            return self._stop_reason

    def acquire_work(self, kind: str) -> WorkLease:
        """Admit new work, or reject it once a stop has been requested."""

        with self._condition:
            if self._draining:
                raise ServiceShuttingDown()
            lease = WorkLease(self, self._next_slot, kind)
            self._next_slot += 1
            self._active_work[lease._slot] = lease
            return lease

    def attach_rendition(self, lease: WorkLease, rendition_id: str) -> None:
        with self._condition:
            if lease._slot in self._active_work:
                lease.rendition_id = rendition_id

    def attach_check(self, lease: WorkLease, check_id: str) -> None:
        with self._condition:
            if lease._slot in self._active_work:
                lease.check_id = check_id

    def release_lease(self, lease: WorkLease) -> None:
        with self._condition:
            if lease._released:
                return
            lease._released = True
            self._active_work.pop(lease._slot, None)
            self._condition.notify_all()

    def begin_shutdown(self, reason: str = "signal") -> None:
        with self._condition:
            if not self._draining:
                self._draining = True
                self._stop_reason = reason
                self._requested_at = time.monotonic()
            self._condition.notify_all()

    def shutdown_deadline(self, budget_seconds: float) -> float:
        """Deadline for the bounded drain that began at the first stop signal."""

        with self._condition:
            started = self._requested_at if self._requested_at is not None else time.monotonic()
            return started + budget_seconds

    # -- request accounting --------------------------------------------

    def note_request_started(self) -> None:
        with self._condition:
            self._active_requests += 1

    def note_request_finished(self) -> None:
        with self._condition:
            self._active_requests -= 1
            self._condition.notify_all()

    # -- draining -------------------------------------------------------

    def wait_until_idle(self, deadline: float) -> bool:
        """Wait for admitted work and in-flight requests, bounded by deadline."""

        with self._condition:
            while self._active_work or self._active_requests > 0:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._condition.wait(timeout=remaining)
            return True

    def active_rendition_ids(self) -> list[str]:
        with self._condition:
            return [
                lease.rendition_id
                for lease in self._active_work.values()
                if lease.kind == "rendition" and lease.rendition_id is not None
            ]

    def active_check_ids(self) -> list[str]:
        with self._condition:
            return [
                lease.check_id
                for lease in self._active_work.values()
                if lease.kind == "integrity_check" and lease.check_id is not None
            ]
