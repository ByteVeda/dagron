"""Tests for Phase 7: reverse, collapse, dominator tree, binary serialization, incremental."""

import pytest

import dagron
from dagron import DAG, IncrementalExecutor


def diamond_dag():
    dag = DAG()
    dag.add_node("a")
    dag.add_node("b")
    dag.add_node("c")
    dag.add_node("d")
    dag.add_edge("a", "b")
    dag.add_edge("a", "c")
    dag.add_edge("b", "d")
    dag.add_edge("c", "d")
    return dag


# --- Reverse ---


def test_reverse_diamond():
    dag = diamond_dag()
    rev = dag.reverse()
    assert rev.node_count() == 4
    assert rev.edge_count() == 4
    assert rev.has_edge("b", "a")
    assert rev.has_edge("c", "a")
    assert rev.has_edge("d", "b")
    assert rev.has_edge("d", "c")
    assert not rev.has_edge("a", "b")


def test_reverse_preserves_payloads():
    dag = DAG()
    dag.add_node("a", payload=42)
    dag.add_node("b", payload="hello")
    dag.add_edge("a", "b")

    rev = dag.reverse()
    assert rev.get_payload("a") == 42
    assert rev.get_payload("b") == "hello"
    assert rev.has_edge("b", "a")


def test_reverse_preserves_edges():
    dag = DAG()
    dag.add_node("x")
    dag.add_node("y")
    dag.add_edge("x", "y", weight=3.5, label="dep")

    rev = dag.reverse()
    edges = rev.edges()
    assert len(edges) == 1
    assert edges[0] == ("y", "x")


def test_reverse_empty():
    dag = DAG()
    rev = dag.reverse()
    assert rev.node_count() == 0
    assert rev.edge_count() == 0


# --- Collapse ---


def test_collapse_pair_in_diamond():
    dag = diamond_dag()
    collapsed = dag.collapse(["b", "c"], "bc")
    assert collapsed.node_count() == 3
    assert collapsed.has_node("a")
    assert collapsed.has_node("bc")
    assert collapsed.has_node("d")
    assert not collapsed.has_node("b")
    assert not collapsed.has_node("c")
    assert collapsed.has_edge("a", "bc")
    assert collapsed.has_edge("bc", "d")


def test_collapse_preserves_payloads():
    dag = DAG()
    dag.add_node("a", payload=10)
    dag.add_node("b", payload=20)
    dag.add_node("c", payload=30)
    dag.add_edge("a", "b")
    dag.add_edge("b", "c")

    collapsed = dag.collapse(["b"], "merged", payload=99)
    assert collapsed.get_payload("a") == 10
    assert collapsed.get_payload("merged") == 99
    assert collapsed.get_payload("c") == 30


def test_collapse_error_on_missing_node():
    dag = diamond_dag()
    with pytest.raises(dagron.NodeNotFoundError):
        dag.collapse(["b", "nonexistent"], "bc")


def test_collapse_all():
    dag = DAG()
    dag.add_node("a")
    dag.add_node("b")
    dag.add_edge("a", "b")

    collapsed = dag.collapse(["a", "b"], "all")
    assert collapsed.node_count() == 1
    assert collapsed.has_node("all")
    assert collapsed.edge_count() == 0


# --- Dominator tree ---


def test_dominator_tree_diamond():
    dag = diamond_dag()
    dom = dag.dominator_tree("a")
    dom_map = dict(dom)
    assert dom_map["a"] == "a"
    assert dom_map["b"] == "a"
    assert dom_map["c"] == "a"
    assert dom_map["d"] == "a"


def test_dominator_tree_linear():
    dag = DAG()
    dag.add_node("a")
    dag.add_node("b")
    dag.add_node("c")
    dag.add_edge("a", "b")
    dag.add_edge("b", "c")

    dom = dag.dominator_tree("a")
    dom_map = dict(dom)
    assert dom_map["a"] == "a"
    assert dom_map["b"] == "a"
    assert dom_map["c"] == "b"


def test_dominator_tree_converging():
    dag = DAG()
    dag.add_node("a")
    dag.add_node("b")
    dag.add_node("c")
    dag.add_node("d")
    dag.add_node("e")
    dag.add_edge("a", "b")
    dag.add_edge("a", "c")
    dag.add_edge("b", "d")
    dag.add_edge("c", "d")
    dag.add_edge("b", "e")
    dag.add_edge("d", "e")

    dom = dag.dominator_tree("a")
    dom_map = dict(dom)
    assert dom_map["d"] == "a"
    assert dom_map["e"] == "a"


# --- Binary serialization ---


def test_to_bytes_from_bytes_empty():
    dag = DAG()
    data = dag.to_bytes()
    dag2 = DAG.from_bytes(data)
    assert dag2.node_count() == 0
    assert dag2.edge_count() == 0


def test_to_bytes_from_bytes_diamond():
    dag = diamond_dag()
    data = dag.to_bytes()
    dag2 = DAG.from_bytes(data)
    assert dag2.node_count() == 4
    assert dag2.edge_count() == 4
    assert dag2.has_edge("a", "b")
    assert dag2.has_edge("c", "d")


def test_to_bytes_from_bytes_with_payloads():
    dag = DAG()
    dag.add_node("a", payload={"x": 1})
    dag.add_node("b", payload={"y": 2})
    dag.add_edge("a", "b")

    data = dag.to_bytes(payload_serializer=lambda p: p)
    dag2 = DAG.from_bytes(data, payload_deserializer=lambda p: p)
    assert dag2.get_payload("a") == {"x": 1}
    assert dag2.get_payload("b") == {"y": 2}


# --- Incremental executor ---


def test_incremental_first_run_executes_all():
    dag = DAG()
    dag.add_node("a")
    dag.add_node("b")
    dag.add_edge("a", "b")

    executor = IncrementalExecutor(dag)
    result = executor.execute({"a": lambda: 1, "b": lambda: 2})

    assert set(result.recomputed) == {"a", "b"}
    assert result.reused == []
    assert result.node_results["a"].result == 1
    assert result.node_results["b"].result == 2


def test_incremental_changed_nodes_re_executes_dirty():
    dag = DAG()
    dag.add_node("a")
    dag.add_node("b")
    dag.add_node("c")
    dag.add_edge("a", "b")
    dag.add_edge("b", "c")

    call_count = {"a": 0, "b": 0, "c": 0}

    def make_task(name, value):
        def task():
            call_count[name] += 1
            return value

        return task

    executor = IncrementalExecutor(dag)
    tasks = {
        "a": make_task("a", 10),
        "b": make_task("b", 20),
        "c": make_task("c", 30),
    }

    # First run
    executor.execute(tasks)
    assert call_count == {"a": 1, "b": 1, "c": 1}

    # Second run with a changed
    result = executor.execute(tasks, changed_nodes=["a"])
    # a re-executes (changed), same result -> early_cutoff
    # b re-executes (a was in propagation_set as changed node), same result -> early_cutoff
    # c is dirty but b got early_cutoff (not in propagation_set) -> reused
    assert "a" in result.recomputed
    assert "b" in result.recomputed
    assert "c" in result.reused
    assert "a" in result.early_cutoff
    assert "b" in result.early_cutoff


def test_incremental_early_cutoff():
    dag = DAG()
    dag.add_node("a")
    dag.add_node("b")
    dag.add_node("c")
    dag.add_edge("a", "b")
    dag.add_edge("b", "c")

    counter = [0]

    def task_a():
        counter[0] += 1
        return 10  # always returns same value

    def task_b():
        counter[0] += 1
        return 20

    def task_c():
        counter[0] += 1
        return 30

    executor = IncrementalExecutor(dag)
    tasks = {"a": task_a, "b": task_b, "c": task_c}

    # First run
    executor.execute(tasks)
    assert counter[0] == 3

    # Second run: a changed but produces same result
    result = executor.execute(tasks, changed_nodes=["a"])
    # a re-executes, produces same result -> early cutoff
    # b should still re-execute since a is in propagation_set (it's a changed node)
    assert "a" in result.recomputed
    assert "a" in result.early_cutoff


def test_incremental_provenance():
    dag = DAG()
    dag.add_node("a")
    dag.add_node("b")
    dag.add_node("c")
    dag.add_edge("a", "b")
    dag.add_edge("b", "c")

    executor = IncrementalExecutor(dag)
    tasks = {"a": lambda: 1, "b": lambda: 2, "c": lambda: 3}

    # First run
    executor.execute(tasks)

    # Second run with changed
    result = executor.execute(tasks, changed_nodes=["a"])
    assert "a" in result.provenance
    assert "b" in result.provenance
    assert "c" in result.provenance
    assert "a" in result.provenance["a"]
    assert "a" in result.provenance["b"]
    assert "a" in result.provenance["c"]


def test_incremental_reused_nodes():
    # a -> b, c -> d (c is independent of a)
    dag = DAG()
    dag.add_node("a")
    dag.add_node("b")
    dag.add_node("c")
    dag.add_node("d")
    dag.add_edge("a", "b")
    dag.add_edge("c", "d")

    call_count = {"a": 0, "b": 0, "c": 0, "d": 0}

    def make_task(name, value):
        def task():
            call_count[name] += 1
            return value

        return task

    executor = IncrementalExecutor(dag)
    tasks = {
        "a": make_task("a", 10),
        "b": make_task("b", 20),
        "c": make_task("c", 30),
        "d": make_task("d", 40),
    }

    # First run
    executor.execute(tasks)
    assert call_count == {"a": 1, "b": 1, "c": 1, "d": 1}

    # Change only a — c and d should be reused
    result = executor.execute(tasks, changed_nodes=["a"])
    assert "c" in result.reused
    assert "d" in result.reused
    # c and d should not have been called again
    assert call_count["c"] == 1
    assert call_count["d"] == 1
