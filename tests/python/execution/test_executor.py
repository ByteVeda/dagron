import pytest

from dagron import (
    DAG,
    AsyncDAGExecutor,
    DAGExecutor,
    ExecutionCallbacks,
    ExecutionResult,
    NodeResult,
    NodeStatus,
)


@pytest.fixture
def simple_dag():
    dag = DAG()
    dag.add_nodes(["a", "b", "c"])
    dag.add_edge("a", "b")
    dag.add_edge("b", "c")
    return dag


class TestDAGExecutor:
    def test_basic_execution(self, simple_dag):
        tasks = {
            "a": lambda: "result_a",
            "b": lambda: "result_b",
            "c": lambda: "result_c",
        }
        executor = DAGExecutor(simple_dag)
        result = executor.execute(tasks)

        assert isinstance(result, ExecutionResult)
        assert result.succeeded == 3
        assert result.failed == 0
        assert result.skipped == 0
        assert result.total_duration_seconds >= 0

    def test_node_results(self, simple_dag):
        tasks = {
            "a": lambda: 42,
            "b": lambda: "hello",
            "c": lambda: [1, 2, 3],
        }
        executor = DAGExecutor(simple_dag)
        result = executor.execute(tasks)

        for name in ["a", "b", "c"]:
            nr = result.node_results[name]
            assert isinstance(nr, NodeResult)
            assert nr.status == NodeStatus.COMPLETED
            assert nr.duration_seconds >= 0

        assert result.node_results["a"].result == 42
        assert result.node_results["b"].result == "hello"
        assert result.node_results["c"].result == [1, 2, 3]

    def test_failure_handling(self, simple_dag):
        def fail():
            raise ValueError("boom")

        tasks = {"a": fail, "b": lambda: "b", "c": lambda: "c"}
        executor = DAGExecutor(simple_dag, fail_fast=False)
        result = executor.execute(tasks)

        assert result.node_results["a"].status == NodeStatus.FAILED
        assert isinstance(result.node_results["a"].error, ValueError)
        assert result.failed == 1

    def test_fail_fast_skips_downstream(self, simple_dag):
        def fail():
            raise ValueError("boom")

        tasks = {"a": fail, "b": lambda: "b", "c": lambda: "c"}
        executor = DAGExecutor(simple_dag, fail_fast=True)
        result = executor.execute(tasks)

        assert result.node_results["a"].status == NodeStatus.FAILED
        assert result.node_results["b"].status == NodeStatus.SKIPPED
        assert result.node_results["c"].status == NodeStatus.SKIPPED
        assert result.failed == 1
        assert result.skipped == 2

    def test_missing_task_skipped(self, simple_dag):
        tasks = {"a": lambda: "a"}  # b and c have no tasks
        executor = DAGExecutor(simple_dag, fail_fast=False)
        result = executor.execute(tasks)

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
        executor = DAGExecutor(simple_dag, callbacks=callbacks)
        executor.execute(tasks)

        assert set(started) == {"a", "b", "c"}
        assert set(completed) == {("a", "a"), ("b", "b"), ("c", "c")}

    def test_parallel_execution(self, diamond_dag):
        """Verify that independent tasks actually run in parallel."""
        import time

        start = time.monotonic()
        tasks = {
            "a": lambda: time.sleep(0.05),
            "b": lambda: time.sleep(0.1),
            "c": lambda: time.sleep(0.1),
            "d": lambda: time.sleep(0.05),
        }
        executor = DAGExecutor(diamond_dag, max_workers=2)
        result = executor.execute(tasks)

        elapsed = time.monotonic() - start
        assert result.succeeded == 4
        # Sequential would be ~0.3s, parallel b+c should be ~0.2s
        assert elapsed < 0.8

    def test_with_costs(self, diamond_dag):
        tasks = {
            "a": lambda: "a",
            "b": lambda: "b",
            "c": lambda: "c",
            "d": lambda: "d",
        }
        costs = {"a": 1.0, "b": 2.0, "c": 1.0, "d": 1.0}
        executor = DAGExecutor(diamond_dag, costs=costs)
        result = executor.execute(tasks)
        assert result.succeeded == 4


class TestAsyncDAGExecutor:
    @pytest.mark.asyncio
    async def test_basic_execution(self, simple_dag):
        async def make_task(v):
            return v

        tasks = {
            "a": lambda: make_task("a"),
            "b": lambda: make_task("b"),
            "c": lambda: make_task("c"),
        }
        executor = AsyncDAGExecutor(simple_dag)
        result = await executor.execute(tasks)

        assert result.succeeded == 3
        assert result.failed == 0
        assert result.node_results["a"].result == "a"

    @pytest.mark.asyncio
    async def test_fail_fast(self, simple_dag):
        async def fail():
            raise ValueError("async boom")

        async def ok(v):
            return v

        tasks = {
            "a": lambda: fail(),
            "b": lambda: ok("b"),
            "c": lambda: ok("c"),
        }
        executor = AsyncDAGExecutor(simple_dag, fail_fast=True)
        result = await executor.execute(tasks)

        assert result.node_results["a"].status == NodeStatus.FAILED
        assert result.node_results["b"].status == NodeStatus.SKIPPED
        assert result.node_results["c"].status == NodeStatus.SKIPPED
