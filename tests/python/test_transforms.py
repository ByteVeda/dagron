import pytest

from dagron import DAG, CycleError, DuplicateNodeError

# --- Transitive Reduction ---


class TestTransitiveReduction:
    def test_removes_redundant_edge(self):
        dag = DAG()
        dag.add_nodes(["a", "b", "c", "d"])
        dag.add_edges([("a", "b"), ("a", "c"), ("b", "d"), ("c", "d"), ("a", "d")])
        assert dag.edge_count() == 5

        reduced = dag.transitive_reduction()
        assert reduced.node_count() == 4
        assert reduced.edge_count() == 4
        assert reduced.has_edge("a", "b")
        assert reduced.has_edge("a", "c")
        assert reduced.has_edge("b", "d")
        assert reduced.has_edge("c", "d")
        assert not reduced.has_edge("a", "d")

    def test_preserves_minimal_diamond(self, diamond_dag):
        reduced = diamond_dag.transitive_reduction()
        assert reduced.edge_count() == 4

    def test_linear_chain_with_shortcut(self):
        dag = DAG()
        dag.add_nodes(["a", "b", "c"])
        dag.add_edges([("a", "b"), ("b", "c"), ("a", "c")])

        reduced = dag.transitive_reduction()
        assert reduced.edge_count() == 2
        assert reduced.has_edge("a", "b")
        assert reduced.has_edge("b", "c")
        assert not reduced.has_edge("a", "c")

    def test_empty(self, empty_dag):
        reduced = empty_dag.transitive_reduction()
        assert reduced.node_count() == 0
        assert reduced.edge_count() == 0

    def test_preserves_payloads(self):
        dag = DAG()
        dag.add_node("a", payload={"key": "val_a"})
        dag.add_node("b", payload=42)
        dag.add_node("c", payload="hello")
        dag.add_edges([("a", "b"), ("b", "c"), ("a", "c")])

        reduced = dag.transitive_reduction()
        assert reduced.get_payload("a") == {"key": "val_a"}
        assert reduced.get_payload("b") == 42
        assert reduced.get_payload("c") == "hello"

    def test_preserves_edge_weights(self):
        dag = DAG()
        dag.add_nodes(["a", "b", "c"])
        dag.add_edge("a", "b", weight=5.0, label="dep")
        dag.add_edge("b", "c", weight=3.0)
        dag.add_edge("a", "c", weight=99.0)  # shortcut, removed

        reduced = dag.transitive_reduction()
        assert reduced.edge_count() == 2

    def test_original_unmodified(self):
        dag = DAG()
        dag.add_nodes(["a", "b", "c"])
        dag.add_edges([("a", "b"), ("b", "c"), ("a", "c")])
        original_edges = dag.edge_count()

        _ = dag.transitive_reduction()
        assert dag.edge_count() == original_edges


# --- Transitive Closure ---


class TestTransitiveClosure:
    def test_adds_missing_edges(self):
        dag = DAG()
        dag.add_nodes(["a", "b", "c"])
        dag.add_edges([("a", "b"), ("b", "c")])

        closed = dag.transitive_closure()
        assert closed.edge_count() == 3
        assert closed.has_edge("a", "c")

    def test_already_complete(self):
        dag = DAG()
        dag.add_nodes(["a", "b", "c"])
        dag.add_edges([("a", "b"), ("b", "c"), ("a", "c")])

        closed = dag.transitive_closure()
        assert closed.edge_count() == 3

    def test_diamond(self, diamond_dag):
        closed = diamond_dag.transitive_closure()
        assert closed.has_edge("a", "d")
        assert closed.edge_count() == 5

    def test_empty(self, empty_dag):
        closed = empty_dag.transitive_closure()
        assert closed.node_count() == 0


# --- Filter ---


class TestFilter:
    def test_keeps_matching(self, diamond_dag):
        filtered = diamond_dag.filter(lambda name, _: name in ("a", "b"))
        assert filtered.node_count() == 2
        assert filtered.has_node("a")
        assert filtered.has_node("b")
        assert filtered.has_edge("a", "b")

    def test_removes_dangling_edges(self, diamond_dag):
        filtered = diamond_dag.filter(lambda name, _: name in ("a", "d"))
        assert filtered.node_count() == 2
        assert filtered.edge_count() == 0

    def test_empty_result(self, diamond_dag):
        filtered = diamond_dag.filter(lambda name, _: False)
        assert filtered.node_count() == 0
        assert filtered.edge_count() == 0

    def test_preserves_payloads(self):
        dag = DAG()
        dag.add_node("a", payload=10)
        dag.add_node("b", payload=20)
        dag.add_node("c", payload=30)
        dag.add_edge("a", "b")

        filtered = dag.filter(lambda name, _: name != "c")
        assert filtered.get_payload("a") == 10
        assert filtered.get_payload("b") == 20
        assert not filtered.has_node("c")

    def test_filter_by_payload(self):
        dag = DAG()
        dag.add_node("a", payload={"priority": 1})
        dag.add_node("b", payload={"priority": 5})
        dag.add_node("c", payload={"priority": 3})
        dag.add_edges([("a", "b"), ("b", "c")])

        filtered = dag.filter(lambda name, p: p["priority"] >= 3)
        assert filtered.node_count() == 2
        assert filtered.has_node("b")
        assert filtered.has_node("c")
        assert filtered.has_edge("b", "c")


# --- Merge ---


class TestMerge:
    def test_disjoint(self):
        dag1 = DAG()
        dag1.add_nodes(["a", "b"])
        dag1.add_edge("a", "b")

        dag2 = DAG()
        dag2.add_nodes(["c", "d"])
        dag2.add_edge("c", "d")

        merged = dag1.merge(dag2)
        assert merged.node_count() == 4
        assert merged.edge_count() == 2

    def test_keep_first(self):
        dag1 = DAG()
        dag1.add_node("a", payload=10)

        dag2 = DAG()
        dag2.add_node("a", payload=20)

        merged = dag1.merge(dag2, conflict="keep_first")
        assert merged.get_payload("a") == 10

    def test_keep_second(self):
        dag1 = DAG()
        dag1.add_node("a", payload=10)

        dag2 = DAG()
        dag2.add_node("a", payload=20)

        merged = dag1.merge(dag2, conflict="keep_second")
        assert merged.get_payload("a") == 20

    def test_error_on_conflict(self):
        dag1 = DAG()
        dag1.add_node("a", payload=10)

        dag2 = DAG()
        dag2.add_node("a", payload=20)

        with pytest.raises(DuplicateNodeError):
            dag1.merge(dag2, conflict="error")

    def test_cycle_fails(self):
        dag1 = DAG()
        dag1.add_nodes(["a", "b"])
        dag1.add_edge("a", "b")

        dag2 = DAG()
        dag2.add_nodes(["a", "b"])
        dag2.add_edge("b", "a")

        with pytest.raises(CycleError):
            dag1.merge(dag2, conflict="keep_first")

    def test_custom_resolver(self):
        dag1 = DAG()
        dag1.add_node("a", payload=10)
        dag1.add_node("b", payload=20)

        dag2 = DAG()
        dag2.add_node("a", payload=5)
        dag2.add_node("c", payload=30)

        merged = dag1.merge(
            dag2,
            conflict_resolver=lambda name, p1, p2: p1 + p2,
        )
        assert merged.get_payload("a") == 15  # 10 + 5
        assert merged.get_payload("b") == 20
        assert merged.get_payload("c") == 30

    def test_invalid_conflict_strategy(self):
        dag1 = DAG()
        dag2 = DAG()

        with pytest.raises(ValueError, match="Invalid conflict strategy"):
            dag1.merge(dag2, conflict="invalid")
