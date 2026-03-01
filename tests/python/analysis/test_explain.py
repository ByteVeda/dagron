"""Tests for Explain mode and What-If analysis."""

import pytest

from dagron import DAGBuilder, NodeExplanation, WhatIfResult


@pytest.fixture
def diamond_dag():
    return (
        DAGBuilder()
        .add_node("a")
        .add_node("b")
        .add_node("c")
        .add_node("d")
        .add_edge("a", "b")
        .add_edge("a", "c")
        .add_edge("b", "d")
        .add_edge("c", "d")
        .build()
    )


@pytest.fixture
def chain_dag():
    return (
        DAGBuilder()
        .add_node("a")
        .add_node("b")
        .add_node("c")
        .add_node("d")
        .add_edge("a", "b")
        .add_edge("b", "c")
        .add_edge("c", "d")
        .build()
    )


class TestExplain:
    def test_root_node(self, diamond_dag):
        info = diamond_dag.explain("a")
        assert isinstance(info, NodeExplanation)
        assert info.name == "a"
        assert info.depth_from_root == 0
        assert info.is_root is True
        assert info.is_leaf is False
        assert info.in_degree == 0
        assert info.out_degree == 2

    def test_leaf_node(self, diamond_dag):
        info = diamond_dag.explain("d")
        assert info.is_leaf is True
        assert info.is_root is False
        assert info.in_degree == 2

    def test_depth(self, chain_dag):
        assert chain_dag.explain("a").depth_from_root == 0
        assert chain_dag.explain("b").depth_from_root == 1
        assert chain_dag.explain("c").depth_from_root == 2
        assert chain_dag.explain("d").depth_from_root == 3

    def test_critical_path_membership(self, chain_dag):
        info = chain_dag.explain("b")
        assert info.on_critical_path is True

    def test_ancestor_descendant_count(self, chain_dag):
        info = chain_dag.explain("b")
        assert info.ancestor_count == 1  # just "a"
        assert info.descendant_count == 2  # "c" and "d"

    def test_blocked_by_and_blocks(self, diamond_dag):
        info = diamond_dag.explain("d")
        assert set(info.blocked_by) == {"b", "c"}
        assert info.blocks == []

    def test_bottleneck_score(self, chain_dag):
        # Root has most descendants
        root_info = chain_dag.explain("a")
        leaf_info = chain_dag.explain("d")
        assert root_info.bottleneck_score > leaf_info.bottleneck_score

    def test_summary(self, diamond_dag):
        info = diamond_dag.explain("a")
        summary = info.summary()
        assert "Node: a" in summary
        assert "Depth from root: 0" in summary

    def test_dominates(self, chain_dag):
        info = chain_dag.explain("a")
        # a dominates b, c, d in a chain
        assert "b" in info.dominates
        assert "c" in info.dominates
        assert "d" in info.dominates


class TestWhatIf:
    def test_remove_node(self, diamond_dag):
        result = diamond_dag.what_if(remove_nodes=["b"])
        assert isinstance(result, WhatIfResult)
        assert result.would_create_cycle is False
        assert result.new_node_count == 3

    def test_remove_node_orphans(self, chain_dag):
        # Removing "b" from a->b->c->d orphans c (becomes new root)
        result = chain_dag.what_if(remove_nodes=["b"])
        assert result.new_node_count == 3
        # c becomes a new root since its only predecessor was b
        assert "c" in result.orphaned_nodes

    def test_add_edge_creates_cycle(self, chain_dag):
        result = chain_dag.what_if(add_edges=[("d", "a")])
        assert result.would_create_cycle is True
        assert len(result.cycle_path) > 0

    def test_add_edge_no_cycle(self, diamond_dag):
        dag = DAGBuilder().add_node("a").add_node("b").add_node("c").build()
        result = dag.what_if(add_edges=[("a", "b")])
        assert result.would_create_cycle is False

    def test_parallelism_change(self, chain_dag):
        # Adding parallel paths should increase parallelism
        result = chain_dag.what_if(
            add_nodes=["e"],
            add_edges=[("a", "e"), ("e", "d")],
        )
        assert result.parallelism_change >= 0

    def test_remove_edge(self, diamond_dag):
        result = diamond_dag.what_if(remove_edges=[("a", "b")])
        assert result.new_edge_count == 3  # was 4, removed 1

    def test_summary(self, chain_dag):
        result = chain_dag.what_if(remove_nodes=["b"])
        summary = result.summary()
        assert "What-If Analysis" in summary
