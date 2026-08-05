"""Tests for IncrementalExecutor."""

from dagron import DAG, IncrementalExecutor


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
