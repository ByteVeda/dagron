"""Tests for Resource Declarations."""

import threading
import time

import pytest

from dagron import DAG
from dagron.execution.resources import (
    AsyncResourceAwareExecutor,
    ResourceAwareExecutor,
    ResourcePool,
    ResourceRequirements,
    ResourceTimeline,
)


class TestResourceRequirements:
    def test_basic_requirements(self):
        req = ResourceRequirements(resources={"gpu": 2, "memory_mb": 4096})
        assert req.resources["gpu"] == 2
        assert req.resources["memory_mb"] == 4096

    def test_shorthand_gpu(self):
        req = ResourceRequirements.gpu(2)
        assert req.resources == {"gpu": 2}

    def test_shorthand_cpu(self):
        req = ResourceRequirements.cpu(4)
        assert req.resources == {"cpu_slots": 4}

    def test_shorthand_memory(self):
        req = ResourceRequirements.memory(8192)
        assert req.resources == {"memory_mb": 8192}

    def test_fits(self):
        req = ResourceRequirements(resources={"gpu": 2})
        assert req.fits({"gpu": 4})
        assert req.fits({"gpu": 2})
        assert not req.fits({"gpu": 1})
        assert not req.fits({})

    def test_empty_requirements_always_fit(self):
        req = ResourceRequirements()
        assert req.fits({"gpu": 0})
        assert req.fits({})

    def test_frozen(self):
        req = ResourceRequirements(resources={"gpu": 1})
        with pytest.raises(AttributeError):
            req.resources = {"gpu": 2}  # type: ignore[misc]


class TestResourcePool:
    def test_basic_acquire_release(self):
        pool = ResourcePool({"gpu": 4})
        req = ResourceRequirements.gpu(2)

        assert pool.try_acquire(req)
        assert pool.available == {"gpu": 2}
        assert pool.allocated == {"gpu": 2}

        pool.release(req)
        assert pool.available == {"gpu": 4}
        assert pool.allocated == {"gpu": 0}

    def test_acquire_fails_when_insufficient(self):
        pool = ResourcePool({"gpu": 2})
        req = ResourceRequirements.gpu(3)
        assert not pool.try_acquire(req)

    def test_blocking_acquire(self):
        pool = ResourcePool({"gpu": 2})
        req = ResourceRequirements.gpu(2)

        pool.try_acquire(req)  # takes all GPUs

        released = []

        def release_later():
            time.sleep(0.05)
            pool.release(req)
            released.append(True)

        t = threading.Thread(target=release_later)
        t.start()

        # This should block until resources are released
        assert pool.acquire(req, timeout=2.0)
        t.join()
        assert released == [True]

    def test_acquire_timeout(self):
        pool = ResourcePool({"gpu": 1})
        pool.try_acquire(ResourceRequirements.gpu(1))
        assert not pool.acquire(ResourceRequirements.gpu(1), timeout=0.1)

    def test_can_satisfy(self):
        pool = ResourcePool({"gpu": 4, "memory_mb": 8192})
        assert pool.can_satisfy(ResourceRequirements(resources={"gpu": 4}))
        assert not pool.can_satisfy(ResourceRequirements(resources={"gpu": 5}))
        assert not pool.can_satisfy(ResourceRequirements(resources={"cpu": 1}))

    def test_timeline_recorded(self):
        pool = ResourcePool({"gpu": 4})
        req = ResourceRequirements.gpu(2)
        pool.try_acquire(req, node_name="step1")
        pool.release(req, node_name="step1")
        snaps = pool.timeline.snapshots
        assert len(snaps) == 2
        assert snaps[0].event == "acquired"
        assert snaps[1].event == "released"


class TestResourceTimeline:
    def test_peak_utilization(self):
        timeline = ResourceTimeline()
        timeline.record({"gpu": 2}, {"gpu": 2})
        timeline.record({"gpu": 3}, {"gpu": 1})
        timeline.record({"gpu": 1}, {"gpu": 3})
        assert timeline.peak_utilization() == {"gpu": 3}


class TestResourceAwareExecutor:
    def test_simple_execution(self):
        dag = DAG()
        dag.add_node("a")
        dag.add_node("b")
        dag.add_edge("a", "b")

        pool = ResourcePool({"gpu": 2})
        requirements = {
            "a": ResourceRequirements.gpu(1),
            "b": ResourceRequirements.gpu(1),
        }

        executor = ResourceAwareExecutor(dag, pool, requirements)
        result = executor.execute({"a": lambda: 1, "b": lambda: 2})

        assert result.succeeded == 2

    def test_resource_constrained(self):
        """Tasks needing more resources wait for others to finish."""
        dag = DAG()
        dag.add_node("a")
        dag.add_node("b")
        # a and b are independent but share resources

        pool = ResourcePool({"gpu": 1})
        requirements = {
            "a": ResourceRequirements.gpu(1),
            "b": ResourceRequirements.gpu(1),
        }

        order = []
        executor = ResourceAwareExecutor(dag, pool, requirements, costs={"a": 2.0, "b": 1.0})
        result = executor.execute({
            "a": lambda: order.append("a") or "a",  # type: ignore[func-returns-value]
            "b": lambda: order.append("b") or "b",  # type: ignore[func-returns-value]
        })

        assert result.succeeded == 2
        # Both should complete (order depends on scheduling)
        assert set(order) == {"a", "b"}

    def test_pre_validation_fails(self):
        dag = DAG()
        dag.add_node("a")

        pool = ResourcePool({"gpu": 1})
        requirements = {"a": ResourceRequirements.gpu(2)}

        executor = ResourceAwareExecutor(dag, pool, requirements)
        with pytest.raises(ValueError, match="pool capacity"):
            executor.execute({"a": lambda: 1})

    def test_fail_fast(self):
        dag = DAG()
        dag.add_node("a")
        dag.add_node("b")
        dag.add_edge("a", "b")

        pool = ResourcePool({"cpu_slots": 4})

        executor = ResourceAwareExecutor(dag, pool, fail_fast=True)
        result = executor.execute({
            "a": lambda: (_ for _ in ()).throw(ValueError("boom")),
            "b": lambda: "ok",
        })

        assert result.failed == 1
        assert result.skipped == 1

    def test_no_requirements_defaults(self):
        """Nodes without requirements get empty requirements (always fit)."""
        dag = DAG()
        dag.add_node("a")

        pool = ResourcePool({"gpu": 1})
        executor = ResourceAwareExecutor(dag, pool)
        result = executor.execute({"a": lambda: 42})
        assert result.succeeded == 1

    def test_diamond_with_resources(self):
        dag = DAG()
        dag.add_node("a")
        dag.add_node("b")
        dag.add_node("c")
        dag.add_node("d")
        dag.add_edge("a", "b")
        dag.add_edge("a", "c")
        dag.add_edge("b", "d")
        dag.add_edge("c", "d")

        pool = ResourcePool({"gpu": 2})
        requirements = {
            "a": ResourceRequirements.gpu(1),
            "b": ResourceRequirements.gpu(1),
            "c": ResourceRequirements.gpu(1),
            "d": ResourceRequirements.gpu(2),
        }

        executor = ResourceAwareExecutor(dag, pool, requirements)
        result = executor.execute({
            "a": lambda: "a",
            "b": lambda: "b",
            "c": lambda: "c",
            "d": lambda: "d",
        })

        assert result.succeeded == 4

    def test_tracing(self):
        dag = DAG()
        dag.add_node("a")

        pool = ResourcePool({"gpu": 1})
        requirements = {"a": ResourceRequirements.gpu(1)}

        executor = ResourceAwareExecutor(
            dag, pool, requirements, enable_tracing=True
        )
        result = executor.execute({"a": lambda: 1})

        assert result.trace is not None
        event_types = [e.event_type.value for e in result.trace.events]
        assert "resource_acquired" in event_types
        assert "resource_released" in event_types


class TestAsyncResourceAwareExecutor:
    @pytest.mark.asyncio
    async def test_simple_async(self):

        dag = DAG()
        dag.add_node("a")
        dag.add_node("b")
        dag.add_edge("a", "b")

        pool = ResourcePool({"gpu": 2})
        requirements = {
            "a": ResourceRequirements.gpu(1),
            "b": ResourceRequirements.gpu(1),
        }

        async def task_a():
            return 1

        async def task_b():
            return 2

        executor = AsyncResourceAwareExecutor(dag, pool, requirements)
        result = await executor.execute({
            "a": task_a,
            "b": task_b,
        })

        assert result.succeeded == 2
