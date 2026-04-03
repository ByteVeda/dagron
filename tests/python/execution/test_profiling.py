"""Tests for post-execution profiling."""

import time

from dagron import DAG, DAGExecutor, NodeProfile, ProfileReport, profile_execution


def _delayed(value: str, seconds: float = 0.01) -> str:
    time.sleep(seconds)
    return value


def test_profile_linear_dag():
    dag = DAG()
    dag.add_nodes(["a", "b", "c"])
    dag.add_edge("a", "b")
    dag.add_edge("b", "c")

    executor = DAGExecutor(dag)
    result = executor.execute(
        {
            "a": lambda: _delayed("a"),
            "b": lambda: _delayed("b"),
            "c": lambda: _delayed("c"),
        }
    )

    report = profile_execution(dag, result)
    assert isinstance(report, ProfileReport)
    assert len(report.node_profiles) == 3
    assert len(report.critical_path) == 3
    assert report.critical_path_duration > 0


def test_profile_diamond_dag():
    dag = DAG()
    dag.add_nodes(["a", "b", "c", "d"])
    dag.add_edge("a", "b")
    dag.add_edge("a", "c")
    dag.add_edge("b", "d")
    dag.add_edge("c", "d")

    executor = DAGExecutor(dag)
    result = executor.execute(
        {
            "a": lambda: "a",
            "b": lambda: "b",
            "c": lambda: "c",
            "d": lambda: "d",
        }
    )

    report = profile_execution(dag, result)
    assert len(report.node_profiles) == 4
    assert "a" in report.node_profiles
    assert "d" in report.node_profiles


def test_profile_slack():
    dag = DAG()
    dag.add_nodes(["a", "b", "c", "d"])
    dag.add_edge("a", "b")
    dag.add_edge("a", "c")
    dag.add_edge("b", "d")
    dag.add_edge("c", "d")

    executor = DAGExecutor(dag)
    result = executor.execute(
        {
            "a": lambda: _delayed("a"),
            "b": lambda: _delayed("b", 0.05),
            "c": lambda: _delayed("c"),
            "d": lambda: _delayed("d"),
        }
    )

    report = profile_execution(dag, result)
    # The longer path (a->b->d) should have nodes with less slack
    a_profile = report.node_profiles["a"]
    assert a_profile.slack >= 0


def test_profile_node_profile_fields():
    dag = DAG()
    dag.add_node("a")

    executor = DAGExecutor(dag)
    result = executor.execute({"a": lambda: 42})

    report = profile_execution(dag, result)
    p = report.node_profiles["a"]
    assert isinstance(p, NodeProfile)
    assert p.name == "a"
    assert p.duration >= 0
    assert p.earliest_start >= 0
    assert p.latest_start >= 0
    assert p.slack >= 0
    assert isinstance(p.on_critical_path, bool)
    assert isinstance(p.blocked_descendants, int)


def test_profile_parallelism_efficiency():
    dag = DAG()
    dag.add_nodes(["a", "b", "c"])
    dag.add_edge("a", "b")
    dag.add_edge("a", "c")

    executor = DAGExecutor(dag)
    result = executor.execute(
        {
            "a": lambda: "a",
            "b": lambda: "b",
            "c": lambda: "c",
        }
    )

    report = profile_execution(dag, result)
    assert report.parallelism_efficiency >= 0


def test_profile_bottlenecks():
    dag = DAG()
    dag.add_nodes(["a", "b", "c", "d", "e"])
    dag.add_edge("a", "b")
    dag.add_edge("a", "c")
    dag.add_edge("b", "d")
    dag.add_edge("c", "d")
    dag.add_edge("d", "e")

    executor = DAGExecutor(dag)
    result = executor.execute(
        {
            "a": lambda: _delayed("a"),
            "b": lambda: _delayed("b", 0.05),
            "c": lambda: _delayed("c"),
            "d": lambda: _delayed("d"),
            "e": lambda: _delayed("e"),
        }
    )

    report = profile_execution(dag, result)
    assert len(report.bottlenecks) > 0


def test_profile_summary():
    dag = DAG()
    dag.add_nodes(["a", "b"])
    dag.add_edge("a", "b")

    executor = DAGExecutor(dag)
    result = executor.execute({"a": lambda: 1, "b": lambda: 2})

    report = profile_execution(dag, result)
    summary = report.summary()
    assert "Profile Report" in summary
    assert "Critical path" in summary


def test_profile_to_dict():
    dag = DAG()
    dag.add_nodes(["a", "b"])
    dag.add_edge("a", "b")

    executor = DAGExecutor(dag)
    result = executor.execute({"a": lambda: 1, "b": lambda: 2})

    report = profile_execution(dag, result)
    d = report.to_dict()
    assert "node_profiles" in d
    assert "critical_path" in d
    assert "bottlenecks" in d
    assert "parallelism_efficiency" in d


def test_profile_empty_result():
    dag = DAG()
    from dagron.execution import ExecutionResult

    result = ExecutionResult()
    report = profile_execution(dag, result)
    assert len(report.node_profiles) == 0
