"""Tests for Graph Partitioning."""
# mypy: disable-error-code="attr-defined"

import pytest

from dagron import DAG
from dagron.execution.distributed import PartitionedDAGExecutor


class TestPartitionLevelBased:
    def test_diamond(self):
        dag = DAG()
        dag.add_node("a")
        dag.add_node("b")
        dag.add_node("c")
        dag.add_node("d")
        dag.add_edge("a", "b")
        dag.add_edge("a", "c")
        dag.add_edge("b", "d")
        dag.add_edge("c", "d")

        result = dag.partition_level_based(2)
        assert len(result.partitions) <= 2
        all_nodes = set()
        for p in result.partitions:
            all_nodes.update(p.node_names)
        assert all_nodes == {"a", "b", "c", "d"}

    def test_linear(self):
        dag = DAG()
        dag.add_node("a")
        dag.add_node("b")
        dag.add_node("c")
        dag.add_node("d")
        dag.add_edge("a", "b")
        dag.add_edge("b", "c")
        dag.add_edge("c", "d")

        result = dag.partition_level_based(2)
        assert len(result.partitions) == 2
        assert result.cross_edge_count > 0

    def test_single_partition(self):
        dag = DAG()
        dag.add_node("a")
        dag.add_node("b")
        dag.add_edge("a", "b")

        result = dag.partition_level_based(1)
        assert len(result.partitions) == 1

    def test_more_partitions_than_levels(self):
        dag = DAG()
        dag.add_node("a")
        dag.add_node("b")
        dag.add_edge("a", "b")

        result = dag.partition_level_based(10)
        assert len(result.partitions) <= 2

    def test_empty_dag(self):
        dag = DAG()
        result = dag.partition_level_based(2)
        assert len(result.partitions) == 0

    def test_partition_order_exists(self):
        dag = DAG()
        dag.add_node("a")
        dag.add_node("b")
        dag.add_node("c")
        dag.add_edge("a", "b")
        dag.add_edge("b", "c")

        result = dag.partition_level_based(3)
        assert isinstance(result.partition_order, list)

    def test_with_costs(self):
        dag = DAG()
        dag.add_node("a")
        dag.add_node("b")
        dag.add_node("c")
        dag.add_edge("a", "b")
        dag.add_edge("b", "c")

        result = dag.partition_level_based(2, {"a": 10.0, "b": 1.0, "c": 1.0})
        assert len(result.partitions) <= 2


class TestPartitionBalanced:
    def test_balanced_cost(self):
        dag = DAG()
        dag.add_node("a")
        dag.add_node("b")
        dag.add_node("c")
        dag.add_node("d")
        dag.add_edge("a", "b")
        dag.add_edge("b", "c")
        dag.add_edge("c", "d")

        costs = {"a": 10.0, "b": 1.0, "c": 1.0, "d": 10.0}
        result = dag.partition_balanced(2, costs)
        assert len(result.partitions) == 2
        # Costs should be relatively balanced
        costs_per_partition = [p.total_cost for p in result.partitions]
        assert max(costs_per_partition) < sum(costs_per_partition)

    def test_empty(self):
        dag = DAG()
        result = dag.partition_balanced(2)
        assert len(result.partitions) == 0


class TestPartitionCommunicationMin:
    def test_reduces_cross_edges(self):
        dag = DAG()
        for i in range(6):
            dag.add_node(f"n{i}")
        dag.add_edge("n0", "n1")
        dag.add_edge("n0", "n2")
        dag.add_edge("n1", "n3")
        dag.add_edge("n2", "n4")
        dag.add_edge("n3", "n5")
        dag.add_edge("n4", "n5")

        result = dag.partition_communication_min(2)
        assert len(result.partitions) <= 2
        all_nodes = set()
        for p in result.partitions:
            all_nodes.update(p.node_names)
        assert len(all_nodes) == 6

    def test_with_custom_params(self):
        dag = DAG()
        dag.add_node("a")
        dag.add_node("b")
        dag.add_node("c")
        dag.add_edge("a", "b")
        dag.add_edge("b", "c")

        result = dag.partition_communication_min(2, max_iterations=5, max_imbalance=0.5)
        assert len(result.partitions) <= 2


class TestPartitionedDAGExecutor:
    def test_basic_execution(self):
        dag = DAG()
        dag.add_node("a")
        dag.add_node("b")
        dag.add_node("c")
        dag.add_edge("a", "b")
        dag.add_edge("b", "c")

        executor = PartitionedDAGExecutor(dag, k=2, strategy="level_based")
        result = executor.execute(
            {
                "a": lambda: 1,
                "b": lambda: 2,
                "c": lambda: 3,
            }
        )

        assert result.succeeded == 3
        assert result.node_results["a"].result == 1
        assert result.node_results["b"].result == 2
        assert result.node_results["c"].result == 3

    def test_diamond_partitioned(self):
        dag = DAG()
        dag.add_node("a")
        dag.add_node("b")
        dag.add_node("c")
        dag.add_node("d")
        dag.add_edge("a", "b")
        dag.add_edge("a", "c")
        dag.add_edge("b", "d")
        dag.add_edge("c", "d")

        executor = PartitionedDAGExecutor(dag, k=2, strategy="balanced")
        result = executor.execute(
            {
                "a": lambda: "a",
                "b": lambda: "b",
                "c": lambda: "c",
                "d": lambda: "d",
            }
        )

        assert result.succeeded == 4

    def test_fail_fast_across_partitions(self):
        dag = DAG()
        dag.add_node("a")
        dag.add_node("b")
        dag.add_node("c")
        dag.add_edge("a", "b")
        dag.add_edge("b", "c")

        executor = PartitionedDAGExecutor(dag, k=3, fail_fast=True)

        def fail():
            raise ValueError("boom")

        result = executor.execute(
            {
                "a": fail,
                "b": lambda: 2,
                "c": lambda: 3,
            }
        )

        assert result.failed >= 1

    def test_unknown_strategy(self):
        dag = DAG()
        dag.add_node("a")
        executor = PartitionedDAGExecutor(dag, k=2, strategy="unknown")
        with pytest.raises(ValueError, match="Unknown partitioning"):
            executor.execute({"a": lambda: 1})

    def test_communication_min_strategy(self):
        dag = DAG()
        dag.add_node("a")
        dag.add_node("b")
        dag.add_node("c")
        dag.add_edge("a", "b")
        dag.add_edge("b", "c")

        executor = PartitionedDAGExecutor(
            dag,
            k=2,
            strategy="communication_min",
            max_iterations=5,
            max_imbalance=0.5,
        )
        result = executor.execute(
            {
                "a": lambda: 1,
                "b": lambda: 2,
                "c": lambda: 3,
            }
        )

        assert result.succeeded == 3
