"""Tests for the typed NodeRef handle.

NodeRef is dagron's stable, persistent handle to a node returned by
`add_node`. Every public method that accepts a `str` node name should also
accept a `NodeRef`. This file exercises the cross-cutting NodeRef behavior
that the older string-based test suite doesn't cover.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from dagron import (
    DAG,
    DAGBuilder,
    DAGExecutor,
    NodeNotFoundError,
    NodeRef,
    StaleNodeRefError,
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import Any

# ---------------------------------------------------------------------------
# Construction / identity
# ---------------------------------------------------------------------------


class TestAddNodeReturnsRef:
    def test_returns_node_ref(self):
        dag = DAG()
        ref = dag.add_node("alpha")
        assert isinstance(ref, NodeRef)
        assert ref.name == "alpha"
        assert isinstance(ref.epoch, int)

    def test_each_node_has_distinct_epoch(self):
        dag = DAG()
        a = dag.add_node("a")
        b = dag.add_node("b")
        assert a.epoch != b.epoch

    def test_add_nodes_returns_refs(self):
        dag = DAG()
        refs = dag.add_nodes(["a", "b", "c"])
        assert all(isinstance(r, NodeRef) for r in refs)
        assert [r.name for r in refs] == ["a", "b", "c"]

    def test_node_ref_lookup(self):
        dag = DAG()
        original = dag.add_node("foo")
        looked_up = dag.node_ref("foo")
        assert looked_up == original
        assert dag.node_ref("missing") is None

    def test_node_ref_equality_and_hash(self):
        dag = DAG()
        a = dag.add_node("a")
        same = dag.node_ref("a")
        assert a == same
        assert hash(a) == hash(same)
        # NodeRef should work as a dict key
        bag = {a: "stored"}
        assert bag[same] == "stored"


# ---------------------------------------------------------------------------
# Backwards-compat: every method that takes str should accept NodeRef
# ---------------------------------------------------------------------------


class TestNodeRefAcceptedEverywhere:
    @pytest.fixture
    def dag(self):
        d = DAG()
        d.add_node("a")
        d.add_node("b")
        d.add_node("c")
        d.add_node("d")
        d.add_edge("a", "b")
        d.add_edge("a", "c")
        d.add_edge("b", "d")
        d.add_edge("c", "d")
        return d

    def test_add_edge_accepts_mixed(self, dag):
        # Build a fresh DAG to test edge addition explicitly
        d = DAG()
        a = d.add_node("a")
        b = d.add_node("b")
        c = d.add_node("c")
        # All three combinations
        d.add_edge(a, b)  # ref, ref
        d.add_edge(b, "c")  # ref, str
        d.add_edge("a", c)  # str, ref
        assert d.edge_count() == 3

    def test_add_edges_batch_accepts_refs(self):
        d = DAG()
        a = d.add_node("a")
        b = d.add_node("b")
        c = d.add_node("c")
        d.add_edges([(a, b), (b, c, 2.5)])
        assert d.edge_count() == 2

    def test_has_node_accepts_ref(self, dag):
        a = dag.node_ref("a")
        assert dag.has_node(a) is True
        assert dag.has_node("a") is True

    def test_has_edge_accepts_ref(self, dag):
        a = dag.node_ref("a")
        b = dag.node_ref("b")
        assert dag.has_edge(a, b) is True

    def test_predecessors_successors_ancestors_descendants(self, dag):
        d = dag.node_ref("d")
        b = dag.node_ref("b")
        a = dag.node_ref("a")
        assert {n.name for n in dag.predecessors(d)} == {"b", "c"}
        assert {n.name for n in dag.successors(a)} == {"b", "c"}
        assert {n.name for n in dag.ancestors(d)} == {"a", "b", "c"}
        assert {n.name for n in dag.descendants(b)} == {"d"}

    def test_in_out_degree_accept_ref(self, dag):
        d = dag.node_ref("d")
        a = dag.node_ref("a")
        assert dag.in_degree(d) == 2
        assert dag.out_degree(a) == 2

    def test_get_set_payload_accepts_ref(self):
        d = DAG()
        a = d.add_node("a", payload="hello")
        assert d.get_payload(a) == "hello"
        d.set_payload(a, "world")
        assert d.get_payload(a) == "world"

    def test_get_set_metadata_accepts_ref(self):
        d = DAG()
        a = d.add_node("a", metadata={"v": 1})
        assert d.get_metadata(a) == {"v": 1}
        d.set_metadata(a, {"v": 2})
        assert d.get_metadata(a) == {"v": 2}

    def test_remove_node_accepts_ref(self, dag):
        b = dag.node_ref("b")
        dag.remove_node(b)
        assert not dag.has_node("b")

    def test_remove_edge_accepts_ref(self, dag):
        a = dag.node_ref("a")
        b = dag.node_ref("b")
        dag.remove_edge(a, b)
        assert not dag.has_edge("a", "b")

    def test_subgraph_accepts_refs(self, dag):
        a = dag.node_ref("a")
        b = dag.node_ref("b")
        sub = dag.subgraph([a, b])
        assert sub.node_count() == 2
        assert sub.has_edge("a", "b")

    def test_subgraph_by_depth_accepts_ref(self, dag):
        a = dag.node_ref("a")
        sub = dag.subgraph_by_depth(a, depth=1, direction="forward")
        assert sub.node_count() == 3  # a + b + c

    def test_collapse_accepts_refs(self, dag):
        b = dag.node_ref("b")
        c = dag.node_ref("c")
        collapsed = dag.collapse([b, c], "bc")
        assert collapsed.has_node("bc")
        assert not collapsed.has_node("b")
        assert not collapsed.has_node("c")

    def test_paths_accept_refs(self, dag):
        a = dag.node_ref("a")
        d = dag.node_ref("d")
        paths = dag.all_paths(a, d)
        assert len(paths) == 2
        sp = dag.shortest_path(a, d)
        assert sp is not None
        assert sp[0].name == "a"
        assert sp[-1].name == "d"

    def test_dominator_tree_accepts_ref(self, dag):
        a = dag.node_ref("a")
        tree = dag.dominator_tree(a)
        assert isinstance(tree, list)

    def test_iter_ancestors_descendants_accept_ref(self, dag):
        d = dag.node_ref("d")
        a = dag.node_ref("a")
        anc = list(dag.iter_ancestors(d))
        desc = list(dag.iter_descendants(a))
        assert {n.name for n in anc} == {"a", "b", "c"}
        assert {n.name for n in desc} == {"b", "c", "d"}

    def test_dirty_set_accepts_refs(self, dag):
        a = dag.node_ref("a")
        dirty = dag.dirty_set([a])
        assert "a" in dirty
        assert "d" in dirty

    def test_is_ancestor_accepts_refs(self, dag):
        a = dag.node_ref("a")
        d = dag.node_ref("d")
        assert dag.is_ancestor(a, d) is True
        assert dag.is_ancestor(d, a) is False


# ---------------------------------------------------------------------------
# Stale-ref detection
# ---------------------------------------------------------------------------


class TestStaleNodeRef:
    def test_node_ref_survives_unrelated_mutations(self):
        d = DAG()
        a = d.add_node("a")
        d.add_node("b")
        d.add_edge("a", "b")
        # Add another node, remove an unrelated one — `a` should stay valid.
        d.add_node("c")
        d.remove_node("b")
        assert d.has_node(a) is True
        assert d.predecessors(a) == []  # still resolvable

    def test_ref_to_removed_node_raises(self):
        d = DAG()
        a = d.add_node("a")
        d.remove_node("a")
        with pytest.raises(NodeNotFoundError):
            d.add_edge(a, "missing_target")

    def test_ref_after_name_reuse_is_stale(self):
        d = DAG()
        a1 = d.add_node("a")
        d.remove_node("a")
        d.add_node("a")
        # a1 points to the OLD `a` (different epoch)
        with pytest.raises(StaleNodeRefError):
            d.has_edge(a1, a1)

    def test_fresh_ref_from_node_ref_method(self):
        d = DAG()
        a1 = d.add_node("a")
        d.remove_node("a")
        d.add_node("a")
        # node_ref() returns the live ref
        a2 = d.node_ref("a")
        assert a2 is not None
        assert a1 != a2
        assert d.has_node(a2) is True


# ---------------------------------------------------------------------------
# Builder + Executor + ExecutionResult lookup
# ---------------------------------------------------------------------------


class TestBuilderAcceptsRefs:
    def test_builder_add_edge_accepts_mixed(self):
        # Builder is deferred-construction: it doesn't itself produce refs,
        # but should accept refs from a separately-built DAG.
        helper = DAG()
        a = helper.add_node("a")
        b = helper.add_node("b")
        # NodeRefs from `helper` carry the names; the builder uses the names.
        dag = (
            DAGBuilder()
            .add_node("a")
            .add_node("b")
            .add_edge(a, b)  # NodeRef passed through
            .build()
        )
        assert dag.has_edge("a", "b")


class TestExecutorAcceptsRefs:
    def test_tasks_dict_accepts_node_ref_keys(self):
        d = DAG()
        a = d.add_node("a")
        b = d.add_node("b")
        d.add_edge(a, b)

        results: dict[str, int] = {}

        def fn_a():
            results["a"] = 1
            return 42

        def fn_b():
            results["b"] = 2
            return 99

        # Mix str and NodeRef keys in tasks dict
        tasks: dict[str | NodeRef, Callable[[], Any]] = {a: fn_a, "b": fn_b}
        executor = DAGExecutor(d)
        result = executor.execute(tasks)

        assert result.succeeded == 2
        assert results == {"a": 1, "b": 2}

    def test_execution_result_getitem_accepts_ref(self):
        d = DAG()
        a = d.add_node("a")
        executor = DAGExecutor(d)
        result = executor.execute({a: lambda: "value"})
        # __getitem__ accepts both str and NodeRef
        assert result[a].result == "value"
        assert result["a"].result == "value"
        assert a in result
        assert "a" in result
