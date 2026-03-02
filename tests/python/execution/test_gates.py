"""Tests for Approval Gates / Human-in-the-Loop."""

import asyncio
import threading
import time

import pytest

from dagron import DAG, DAGExecutor
from dagron.execution.gates import (
    ApprovalGate,
    GateController,
    GateRejectedError,
    GateStatus,
    GateTimeoutError,
)


class TestApprovalGate:
    def test_auto_approve(self):
        gate = ApprovalGate(auto_approve=True)
        assert gate.status == GateStatus.APPROVED
        gate.wait_sync()  # should not block

    def test_approve_unblocks(self):
        gate = ApprovalGate()
        assert gate.status == GateStatus.PENDING

        def approve_later():
            time.sleep(0.05)
            gate.approve()

        t = threading.Thread(target=approve_later)
        t.start()
        gate.wait_sync()
        t.join()
        assert gate.status == GateStatus.APPROVED  # type: ignore[comparison-overlap]

    def test_reject_raises(self):
        gate = ApprovalGate()

        def reject_later():
            time.sleep(0.05)
            gate.reject("not ready")

        t = threading.Thread(target=reject_later)
        t.start()
        with pytest.raises(GateRejectedError):
            gate.wait_sync()
        t.join()
        assert gate.status == GateStatus.REJECTED
        assert gate.reason == "not ready"

    def test_timeout(self):
        gate = ApprovalGate(timeout=0.1)
        with pytest.raises(GateTimeoutError):
            gate.wait_sync()
        assert gate.status == GateStatus.TIMED_OUT

    def test_reset(self):
        gate = ApprovalGate()
        gate.approve()
        assert gate.status == GateStatus.APPROVED
        gate.reset()
        assert gate.status == GateStatus.PENDING  # type: ignore[comparison-overlap]

    def test_approve_before_wait(self):
        gate = ApprovalGate()
        gate.approve()
        gate.wait_sync()  # should return immediately
        assert gate.status == GateStatus.APPROVED


class TestApprovalGateAsync:
    @pytest.mark.asyncio
    async def test_async_approve(self):
        gate = ApprovalGate()

        async def approve_later():
            await asyncio.sleep(0.05)
            gate.approve()

        task = asyncio.create_task(approve_later())
        await gate.wait_async()
        await task
        assert gate.status == GateStatus.APPROVED

    @pytest.mark.asyncio
    async def test_async_reject(self):
        gate = ApprovalGate()

        async def reject_later():
            await asyncio.sleep(0.05)
            gate.reject("nope")

        task = asyncio.create_task(reject_later())
        with pytest.raises(GateRejectedError):
            await gate.wait_async()
        await task
        assert gate.status == GateStatus.REJECTED

    @pytest.mark.asyncio
    async def test_async_timeout(self):
        gate = ApprovalGate(timeout=0.1)
        with pytest.raises(GateTimeoutError):
            await gate.wait_async()

    @pytest.mark.asyncio
    async def test_async_auto_approve(self):
        gate = ApprovalGate(auto_approve=True)
        await gate.wait_async()
        assert gate.status == GateStatus.APPROVED


class TestGateController:
    def test_approve_and_status(self):
        controller = GateController({
            "deploy": ApprovalGate(),
            "notify": ApprovalGate(),
        })
        assert controller.status("deploy") == GateStatus.PENDING
        controller.approve("deploy")
        assert controller.status("deploy") == GateStatus.APPROVED

    def test_reject_with_reason(self):
        controller = GateController({"gate1": ApprovalGate()})
        controller.reject("gate1", reason="cancelled by user")
        assert controller.status("gate1") == GateStatus.REJECTED

    def test_unknown_gate_raises(self):
        controller = GateController()
        with pytest.raises(KeyError, match="No gate named"):
            controller.approve("nonexistent")

    def test_waiting_gates(self):
        g1 = ApprovalGate()
        g2 = ApprovalGate()
        controller = GateController({"g1": g1, "g2": g2})

        # Start waiting on g1 in a thread
        def wait_g1():
            import contextlib

            with contextlib.suppress(Exception):
                controller.wait_sync("g1")

        t = threading.Thread(target=wait_g1)
        t.start()
        time.sleep(0.05)  # let it enter WAITING state
        waiting = controller.waiting_gates()
        assert "g1" in waiting
        assert "g2" not in waiting
        controller.approve("g1")
        t.join()

    def test_add_gate(self):
        controller = GateController()
        controller.add_gate("new", ApprovalGate(auto_approve=True))
        assert controller.has_gate("new")
        controller.wait_sync("new")

    def test_reset_all(self):
        controller = GateController({
            "a": ApprovalGate(auto_approve=True),
            "b": ApprovalGate(auto_approve=True),
        })
        assert controller.status("a") == GateStatus.APPROVED
        controller.reset_all()
        assert controller.status("a") == GateStatus.PENDING


class TestGateWithExecutor:
    def test_gated_node_waits(self):
        dag = DAG()
        dag.add_node("setup")
        dag.add_node("deploy")
        dag.add_edge("setup", "deploy")

        controller = GateController({"deploy": ApprovalGate()})
        order = []

        def setup_task():
            order.append("setup")
            return "ready"

        def deploy_task():
            controller.wait_sync("deploy")
            order.append("deploy")
            return "deployed"

        # Approve from another thread
        def approve_later():
            time.sleep(0.1)
            controller.approve("deploy")

        t = threading.Thread(target=approve_later)
        t.start()

        executor = DAGExecutor(dag)
        result = executor.execute({"setup": setup_task, "deploy": deploy_task})
        t.join()

        assert result.succeeded == 2
        assert order == ["setup", "deploy"]

    def test_rejected_gate_fails_node(self):
        dag = DAG()
        dag.add_node("gate_node")

        controller = GateController({"gate_node": ApprovalGate()})

        def gated_task():
            controller.wait_sync("gate_node")
            return "ok"

        # Reject immediately from another thread
        def reject_now():
            time.sleep(0.05)
            controller.reject("gate_node", "denied")

        t = threading.Thread(target=reject_now)
        t.start()

        executor = DAGExecutor(dag)
        result = executor.execute({"gate_node": gated_task})
        t.join()

        assert result.failed == 1
        nr = result.node_results["gate_node"]
        assert isinstance(nr.error, GateRejectedError)
