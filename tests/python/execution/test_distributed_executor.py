"""Tests for distributed execution (Feature 3)."""

import pytest

from dagron import (
    DAG,
    ExecutionCallbacks,
    NodeStatus,
)
from dagron.execution.backends.base import DistributedBackend
from dagron.execution.backends.multiprocessing import MultiprocessingBackend
from dagron.execution.backends.thread import ThreadBackend
from dagron.execution.distributed_executor import (
    DistributedExecutionResult,
    DistributedExecutor,
)


@pytest.fixture
def simple_dag():
    dag = DAG()
    dag.add_nodes(["a", "b", "c"])
    dag.add_edge("a", "b")
    dag.add_edge("b", "c")
    return dag


class TestThreadBackend:
    def test_implements_protocol(self):
        backend = ThreadBackend(max_workers=2)
        assert isinstance(backend, DistributedBackend)
        assert backend.name == "thread"
        backend.shutdown()

    def test_submit_and_result(self):
        backend = ThreadBackend(max_workers=2)
        future = backend.submit(lambda: 42)
        assert backend.result(future) == 42
        backend.shutdown()

    def test_shutdown_is_idempotent(self):
        backend = ThreadBackend(max_workers=1)
        backend.submit(lambda: 1)
        backend.shutdown()
        backend.shutdown()  # should not raise


class TestMultiprocessingBackend:
    def test_implements_protocol(self):
        backend = MultiprocessingBackend(max_workers=1)
        assert isinstance(backend, DistributedBackend)
        assert backend.name == "multiprocessing"
        backend.shutdown()

    def test_submit_and_result(self):
        backend = MultiprocessingBackend(max_workers=1)
        # Use a module-level picklable function
        future = backend.submit(pow, 2, 10)
        assert backend.result(future) == 1024
        backend.shutdown()


class TestRayBackend:
    def test_import_error_without_ray(self):
        pytest.importorskip("ray")
        # If ray is installed, this should not raise
        from dagron.execution.backends.ray import RayBackend

        backend = RayBackend(num_cpus=1)
        assert backend.name == "ray"
        backend.shutdown()


class TestCeleryBackend:
    def test_import_error_without_celery(self):
        celery_mod = pytest.importorskip("celery")
        from dagron.execution.backends.celery import CeleryBackend

        app = celery_mod.Celery("test")
        backend = CeleryBackend(app=app)
        assert backend.name == "celery"


class TestDistributedExecutor:
    def test_basic_execution_thread(self, simple_dag):
        backend = ThreadBackend(max_workers=2)
        tasks = {
            "a": lambda: "result_a",
            "b": lambda: "result_b",
            "c": lambda: "result_c",
        }
        with DistributedExecutor(simple_dag, backend) as executor:
            dist_result = executor.execute(tasks)

        assert isinstance(dist_result, DistributedExecutionResult)
        result = dist_result.execution_result
        assert result.succeeded == 3
        assert result.failed == 0
        assert result.skipped == 0
        assert dist_result.backend_name == "thread"

    def test_node_results(self, simple_dag):
        backend = ThreadBackend(max_workers=2)
        tasks = {
            "a": lambda: 42,
            "b": lambda: "hello",
            "c": lambda: [1, 2, 3],
        }
        with DistributedExecutor(simple_dag, backend) as executor:
            dist_result = executor.execute(tasks)

        result = dist_result.execution_result
        assert result.node_results["a"].result == 42
        assert result.node_results["b"].result == "hello"
        assert result.node_results["c"].result == [1, 2, 3]

    def test_failure_handling(self, simple_dag):
        backend = ThreadBackend(max_workers=2)

        def fail():
            raise ValueError("boom")

        tasks = {"a": fail, "b": lambda: "b", "c": lambda: "c"}
        executor = DistributedExecutor(simple_dag, backend, fail_fast=False)
        dist_result = executor.execute(tasks)
        backend.shutdown()

        result = dist_result.execution_result
        assert result.node_results["a"].status == NodeStatus.FAILED
        assert isinstance(result.node_results["a"].error, ValueError)
        assert result.failed == 1

    def test_fail_fast_skips_downstream(self, simple_dag):
        backend = ThreadBackend(max_workers=2)

        def fail():
            raise ValueError("boom")

        tasks = {"a": fail, "b": lambda: "b", "c": lambda: "c"}
        with DistributedExecutor(simple_dag, backend, fail_fast=True) as executor:
            dist_result = executor.execute(tasks)

        result = dist_result.execution_result
        assert result.node_results["a"].status == NodeStatus.FAILED
        assert result.node_results["b"].status == NodeStatus.SKIPPED
        assert result.node_results["c"].status == NodeStatus.SKIPPED
        assert result.failed == 1
        assert result.skipped == 2

    def test_missing_task_skipped(self, simple_dag):
        backend = ThreadBackend(max_workers=2)
        tasks = {"a": lambda: "a"}
        with DistributedExecutor(simple_dag, backend, fail_fast=False) as executor:
            dist_result = executor.execute(tasks)

        result = dist_result.execution_result
        assert result.node_results["a"].status == NodeStatus.COMPLETED
        assert result.node_results["b"].status == NodeStatus.SKIPPED
        assert result.node_results["c"].status == NodeStatus.SKIPPED

    def test_callbacks(self, simple_dag):
        started = []
        completed = []
        tasks = {"a": lambda: "a", "b": lambda: "b", "c": lambda: "c"}
        callbacks = ExecutionCallbacks(
            on_start=lambda n: started.append(n),
            on_complete=lambda n, v: completed.append((n, v)),
        )
        backend = ThreadBackend(max_workers=1)
        with DistributedExecutor(simple_dag, backend, callbacks=callbacks) as executor:
            executor.execute(tasks)

        assert set(started) == {"a", "b", "c"}
        assert set(completed) == {("a", "a"), ("b", "b"), ("c", "c")}

    def test_tracing(self, simple_dag):
        backend = ThreadBackend(max_workers=1)
        tasks = {"a": lambda: "a", "b": lambda: "b", "c": lambda: "c"}
        with DistributedExecutor(simple_dag, backend, enable_tracing=True) as executor:
            dist_result = executor.execute(tasks)

        result = dist_result.execution_result
        assert result.trace is not None
        events = result.trace.events
        assert len(events) > 0
        event_types = {e.event_type.value for e in events}
        assert "execution_started" in event_types
        assert "execution_completed" in event_types
        assert "node_completed" in event_types

    def test_dispatch_info(self, simple_dag):
        backend = ThreadBackend(max_workers=1)
        tasks = {"a": lambda: "a", "b": lambda: "b", "c": lambda: "c"}
        with DistributedExecutor(simple_dag, backend) as executor:
            dist_result = executor.execute(tasks)

        assert "a" in dist_result.dispatch_info
        assert dist_result.dispatch_info["a"]["backend"] == "thread"

    def test_context_manager(self, simple_dag):
        backend = ThreadBackend(max_workers=1)
        with DistributedExecutor(simple_dag, backend) as executor:
            executor.execute({"a": lambda: 1, "b": lambda: 2, "c": lambda: 3})
        # After context manager, pool should be shut down
        assert backend._pool is None

    def test_diamond_dag_parallel(self, diamond_dag):
        import time

        backend = ThreadBackend(max_workers=4)
        start = time.monotonic()
        tasks = {
            "a": lambda: time.sleep(0.05),
            "b": lambda: time.sleep(0.1),
            "c": lambda: time.sleep(0.1),
            "d": lambda: time.sleep(0.05),
        }
        with DistributedExecutor(diamond_dag, backend) as executor:
            dist_result = executor.execute(tasks)

        elapsed = time.monotonic() - start
        assert dist_result.execution_result.succeeded == 4
        # b and c run in parallel, so total should be ~0.2s, not ~0.3s
        assert elapsed < 0.28


class TestDistributedExecutionResult:
    def test_dataclass_defaults(self):
        r = DistributedExecutionResult()
        assert r.backend_name == ""
        assert r.dispatch_info == {}
        assert r.execution_result.succeeded == 0
