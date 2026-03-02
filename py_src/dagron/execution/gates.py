"""Approval gates for human-in-the-loop DAG execution."""

from __future__ import annotations

import asyncio
import threading
from enum import Enum


class GateRejectedError(Exception):
    """Raised when an approval gate is rejected."""

    def __init__(self, gate_name: str, reason: str = "") -> None:
        self.gate_name = gate_name
        self.reason = reason
        msg = f"Gate '{gate_name}' was rejected"
        if reason:
            msg += f": {reason}"
        super().__init__(msg)


class GateTimeoutError(Exception):
    """Raised when an approval gate times out."""

    def __init__(self, gate_name: str, timeout: float) -> None:
        self.gate_name = gate_name
        self.timeout = timeout
        super().__init__(f"Gate '{gate_name}' timed out after {timeout}s")


class GateStatus(Enum):
    """Status of an approval gate."""

    PENDING = "pending"
    WAITING = "waiting"
    APPROVED = "approved"
    REJECTED = "rejected"
    TIMED_OUT = "timed_out"


class ApprovalGate:
    """A single approval gate that blocks execution until approved or rejected.

    Args:
        timeout: Maximum seconds to wait for approval. None = wait forever.
        auto_approve: If True, the gate auto-approves immediately (useful for testing).
    """

    def __init__(
        self,
        timeout: float | None = None,
        auto_approve: bool = False,
    ) -> None:
        self.timeout = timeout
        self._auto_approve = auto_approve
        self._status = GateStatus.PENDING
        self._reason: str = ""
        self._sync_event = threading.Event()
        self._async_event: asyncio.Event | None = None
        self._lock = threading.Lock()

        if auto_approve:
            self._status = GateStatus.APPROVED
            self._sync_event.set()

    @property
    def status(self) -> GateStatus:
        return self._status

    @property
    def reason(self) -> str:
        return self._reason

    def approve(self) -> None:
        """Approve this gate, unblocking the waiting executor."""
        with self._lock:
            if self._status in (GateStatus.PENDING, GateStatus.WAITING):
                self._status = GateStatus.APPROVED
                self._sync_event.set()
                if self._async_event is not None:
                    self._async_event.set()

    def reject(self, reason: str = "") -> None:
        """Reject this gate, causing the waiting executor to fail the node."""
        with self._lock:
            if self._status in (GateStatus.PENDING, GateStatus.WAITING):
                self._status = GateStatus.REJECTED
                self._reason = reason
                self._sync_event.set()
                if self._async_event is not None:
                    self._async_event.set()

    def wait_sync(self) -> None:
        """Block the current thread until the gate is resolved.

        Raises:
            GateRejectedError: If the gate is rejected.
            GateTimeoutError: If the gate times out.
        """
        with self._lock:
            if self._status == GateStatus.APPROVED:
                return
            if self._status == GateStatus.REJECTED:
                raise GateRejectedError(gate_name="", reason=self._reason)
            self._status = GateStatus.WAITING

        resolved = self._sync_event.wait(timeout=self.timeout)

        if not resolved:
            with self._lock:
                self._status = GateStatus.TIMED_OUT
            raise GateTimeoutError(gate_name="", timeout=self.timeout or 0.0)

        if self._status == GateStatus.REJECTED:
            raise GateRejectedError(gate_name="", reason=self._reason)

    async def wait_async(self) -> None:
        """Await until the gate is resolved.

        Raises:
            GateRejectedError: If the gate is rejected.
            GateTimeoutError: If the gate times out.
        """
        with self._lock:
            if self._status == GateStatus.APPROVED:
                return
            if self._status == GateStatus.REJECTED:
                raise GateRejectedError(gate_name="", reason=self._reason)
            self._status = GateStatus.WAITING
            if self._async_event is None:
                self._async_event = asyncio.Event()
                if self._sync_event.is_set():
                    self._async_event.set()

        try:
            if self.timeout is not None:
                await asyncio.wait_for(self._async_event.wait(), timeout=self.timeout)
            else:
                await self._async_event.wait()
        except TimeoutError:
            with self._lock:
                self._status = GateStatus.TIMED_OUT
            raise GateTimeoutError(gate_name="", timeout=self.timeout or 0.0) from None

        if self._status == GateStatus.REJECTED:
            raise GateRejectedError(gate_name="", reason=self._reason)

    def reset(self) -> None:
        """Reset the gate to PENDING state for reuse."""
        with self._lock:
            self._status = GateStatus.PENDING
            self._reason = ""
            self._sync_event.clear()
            if self._async_event is not None:
                self._async_event.clear()


class GateController:
    """Thread-safe controller for managing multiple approval gates.

    Gates are execution-time concerns, not graph structure. The DAG stays pure.

    Example::

        controller = GateController({
            "deploy": ApprovalGate(timeout=300),
            "notify": ApprovalGate(),
        })
        executor = DAGExecutor(dag, gates=controller)

        # From another thread:
        controller.approve("deploy")
        controller.reject("notify", reason="Not ready")
    """

    def __init__(self, gates: dict[str, ApprovalGate] | None = None) -> None:
        self._gates: dict[str, ApprovalGate] = dict(gates) if gates else {}
        self._lock = threading.Lock()

    def add_gate(self, name: str, gate: ApprovalGate) -> None:
        """Add a gate to the controller."""
        with self._lock:
            self._gates[name] = gate

    def approve(self, name: str) -> None:
        """Approve a named gate."""
        gate = self._gates.get(name)
        if gate is None:
            raise KeyError(f"No gate named '{name}'")
        gate.approve()

    def reject(self, name: str, reason: str = "") -> None:
        """Reject a named gate."""
        gate = self._gates.get(name)
        if gate is None:
            raise KeyError(f"No gate named '{name}'")
        gate.reject(reason)

    def status(self, name: str) -> GateStatus:
        """Get the status of a named gate."""
        gate = self._gates.get(name)
        if gate is None:
            raise KeyError(f"No gate named '{name}'")
        return gate.status

    def waiting_gates(self) -> list[str]:
        """Return names of all gates currently in WAITING state."""
        return [
            name for name, gate in self._gates.items()
            if gate.status == GateStatus.WAITING
        ]

    def get_gate(self, name: str) -> ApprovalGate | None:
        """Get a gate by name, or None if not found."""
        return self._gates.get(name)

    def has_gate(self, name: str) -> bool:
        """Check if a gate exists."""
        return name in self._gates

    def wait_sync(self, name: str) -> None:
        """Wait for a named gate synchronously.

        Raises:
            KeyError: If the gate does not exist.
            GateRejectedError: If the gate is rejected.
            GateTimeoutError: If the gate times out.
        """
        gate = self._gates.get(name)
        if gate is None:
            raise KeyError(f"No gate named '{name}'")
        try:
            gate.wait_sync()
        except GateRejectedError:
            raise GateRejectedError(name, gate.reason) from None
        except GateTimeoutError:
            raise GateTimeoutError(name, gate.timeout or 0.0) from None

    async def wait_async(self, name: str) -> None:
        """Wait for a named gate asynchronously.

        Raises:
            KeyError: If the gate does not exist.
            GateRejectedError: If the gate is rejected.
            GateTimeoutError: If the gate times out.
        """
        gate = self._gates.get(name)
        if gate is None:
            raise KeyError(f"No gate named '{name}'")
        try:
            await gate.wait_async()
        except GateRejectedError:
            raise GateRejectedError(name, gate.reason) from None
        except GateTimeoutError:
            raise GateTimeoutError(name, gate.timeout or 0.0) from None

    def reset_all(self) -> None:
        """Reset all gates to PENDING state."""
        for gate in self._gates.values():
            gate.reset()
