"""Tests for @dagron.task decorator and Pipeline."""

import asyncio

import pytest

from dagron import DAG, Pipeline, task


@task
def fetch_users() -> list:
    return [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]


@task
def fetch_orders() -> list:
    return [{"user_id": 1, "amount": 100}, {"user_id": 2, "amount": 200}]


@task
def merge(fetch_users: list, fetch_orders: list) -> dict:
    return {"users": fetch_users, "orders": fetch_orders}


@task
def summarize(merge: dict) -> str:
    return f"{len(merge['users'])} users, {len(merge['orders'])} orders"


class TestTaskDecorator:
    def test_decorator_preserves_function(self):
        assert fetch_users.__name__ == "fetch_users"
        assert callable(fetch_users)

    def test_decorator_adds_spec(self):
        spec = fetch_users._dagron_task
        assert spec.name == "fetch_users"
        assert spec.dependencies == []
        assert spec.is_async is False

    def test_decorator_infers_dependencies(self):
        spec = merge._dagron_task
        assert spec.name == "merge"
        assert set(spec.dependencies) == {"fetch_users", "fetch_orders"}

    def test_non_decorated_raises(self):
        with pytest.raises(TypeError, match="not a @dagron.task"):
            Pipeline([lambda: None])


class TestPipeline:
    def test_basic_pipeline(self):
        pipeline = Pipeline([fetch_users, fetch_orders, merge])
        assert isinstance(pipeline.dag, DAG)
        assert pipeline.dag.node_count() == 3
        assert pipeline.dag.edge_count() == 2

    def test_dag_has_correct_edges(self):
        pipeline = Pipeline([fetch_users, fetch_orders, merge])
        dag = pipeline.dag
        assert dag.has_edge("fetch_users", "merge")
        assert dag.has_edge("fetch_orders", "merge")

    def test_task_names(self):
        pipeline = Pipeline([fetch_users, fetch_orders, merge])
        assert set(pipeline.task_names) == {"fetch_users", "fetch_orders", "merge"}

    def test_dag_analysis(self):
        pipeline = Pipeline([fetch_users, fetch_orders, merge, summarize])
        dag = pipeline.dag
        stats = dag.stats()
        assert stats.node_count == 4
        assert stats.edge_count == 3
        assert stats.root_count == 2
        assert stats.leaf_count == 1

    def test_duplicate_task_name_raises(self):
        @task
        def dup() -> int:
            return 1

        @task
        def dup() -> int:  # noqa: F811
            return 2

        with pytest.raises(ValueError, match="Duplicate task name"):
            Pipeline([dup, dup])


class TestPipelineExecution:
    def test_sync_execution(self):
        pipeline = Pipeline([fetch_users, fetch_orders, merge])
        result = pipeline.execute()
        assert result.succeeded == 3
        assert result.failed == 0

        merge_result = result.node_results["merge"].result
        assert len(merge_result["users"]) == 2
        assert len(merge_result["orders"]) == 2

    def test_chained_execution(self):
        pipeline = Pipeline([fetch_users, fetch_orders, merge, summarize])
        result = pipeline.execute()
        assert result.succeeded == 4
        assert "2 users, 2 orders" in result.node_results["summarize"].result

    def test_execution_with_overrides(self):
        pipeline = Pipeline([fetch_users, fetch_orders, merge])
        result = pipeline.execute(overrides={"fetch_users": [{"id": 99}]})
        assert result.succeeded == 3
        merge_result = result.node_results["merge"].result
        assert merge_result["users"] == [{"id": 99}]

    def test_execution_with_tracing(self):
        pipeline = Pipeline([fetch_users, fetch_orders, merge])
        result = pipeline.execute(enable_tracing=True)
        assert result.trace is not None
        assert len(result.trace.events) > 0

    def test_task_with_defaults(self):
        @task
        def greet(name: str = "world") -> str:
            return f"hello {name}"

        pipeline = Pipeline([greet])
        result = pipeline.execute()
        assert result.succeeded == 1
        assert result.node_results["greet"].result == "hello world"


class TestPipelineAsync:
    @pytest.mark.asyncio
    async def test_async_execution(self):
        @task
        async def async_fetch() -> list:
            return [1, 2, 3]

        @task
        async def async_process(async_fetch: list) -> int:
            return sum(async_fetch)

        pipeline = Pipeline([async_fetch, async_process])
        result = await pipeline.execute_async()
        assert result.succeeded == 2
        assert result.node_results["async_process"].result == 6

    @pytest.mark.asyncio
    async def test_mixed_sync_async(self):
        @task
        def sync_data() -> list:
            return [1, 2, 3]

        @task
        async def async_sum(sync_data: list) -> int:
            return sum(sync_data)

        pipeline = Pipeline([sync_data, async_sum])
        result = await pipeline.execute_async()
        assert result.succeeded == 2
        assert result.node_results["async_sum"].result == 6
