"""Tests for data lineage tracking (Feature 2)."""

from dagron import DAG, DAGExecutor, ExecutionResult, NodeResult, NodeStatus
from dagron.analysis.lineage import (
    ImpactRecord,
    LineageRecord,
    LineageReport,
    track_lineage,
)


def _make_result(completed=(), failed=(), skipped=()):
    """Helper to build an ExecutionResult from node name lists."""
    result = ExecutionResult()
    for name in completed:
        result.node_results[name] = NodeResult(
            name=name, status=NodeStatus.COMPLETED, result=f"{name}_value"
        )
        result.succeeded += 1
    for name in failed:
        result.node_results[name] = NodeResult(
            name=name, status=NodeStatus.FAILED, error=ValueError(f"{name} failed")
        )
        result.failed += 1
    for name in skipped:
        result.node_results[name] = NodeResult(name=name, status=NodeStatus.SKIPPED)
        result.skipped += 1
    return result


class TestLineageRecord:
    def test_frozen_dataclass(self):
        r = LineageRecord(
            direct_inputs=["a"],
            upstream_chain=["a"],
            contributing_nodes=frozenset({"a"}),
            depth=1,
        )
        assert r.direct_inputs == ["a"]
        assert r.depth == 1


class TestImpactRecord:
    def test_frozen_dataclass(self):
        r = ImpactRecord(
            directly_affects=["b"],
            transitively_affects=["b", "c"],
            affected_leaves=["c"],
        )
        assert r.directly_affects == ["b"]
        assert r.affected_leaves == ["c"]


class TestLineageReport:
    def test_lineage_linear_dag(self, linear_dag):
        result = _make_result(completed=["a", "b", "c"])
        report = LineageReport(linear_dag, result)

        rec = report.lineage("c")
        assert rec.direct_inputs == ["b"]
        assert set(rec.upstream_chain) == {"a", "b"}
        assert rec.contributing_nodes == frozenset({"a", "b"})
        assert rec.depth >= 2

    def test_lineage_root_node(self, linear_dag):
        result = _make_result(completed=["a", "b", "c"])
        report = LineageReport(linear_dag, result)

        rec = report.lineage("a")
        assert rec.direct_inputs == []
        assert rec.upstream_chain == []
        assert rec.contributing_nodes == frozenset()
        assert rec.depth == 0

    def test_lineage_diamond_dag(self, diamond_dag):
        result = _make_result(completed=["a", "b", "c", "d"])
        report = LineageReport(diamond_dag, result)

        rec = report.lineage("d")
        assert sorted(rec.direct_inputs) == ["b", "c"]
        assert set(rec.upstream_chain) == {"a", "b", "c"}
        assert rec.depth >= 2

    def test_lineage_excludes_failed_nodes(self, linear_dag):
        result = _make_result(completed=["a", "c"], failed=["b"])
        report = LineageReport(linear_dag, result)

        rec = report.lineage("c")
        # b failed, so only a is in completed ancestors
        assert "b" not in rec.contributing_nodes
        assert "a" in rec.contributing_nodes

    def test_impact_linear_dag(self, linear_dag):
        result = _make_result(completed=["a", "b", "c"])
        report = LineageReport(linear_dag, result)

        impact = report.impact("a")
        assert impact.directly_affects == ["b"]
        assert set(impact.transitively_affects) == {"b", "c"}
        assert impact.affected_leaves == ["c"]

    def test_impact_leaf_node(self, linear_dag):
        result = _make_result(completed=["a", "b", "c"])
        report = LineageReport(linear_dag, result)

        impact = report.impact("c")
        assert impact.directly_affects == []
        assert impact.transitively_affects == []
        assert impact.affected_leaves == []

    def test_impact_diamond_dag(self, diamond_dag):
        result = _make_result(completed=["a", "b", "c", "d"])
        report = LineageReport(diamond_dag, result)

        impact = report.impact("a")
        assert sorted(impact.directly_affects) == ["b", "c"]
        assert set(impact.transitively_affects) == {"b", "c", "d"}
        assert impact.affected_leaves == ["d"]

    def test_data_flow_path(self, diamond_dag):
        result = _make_result(completed=["a", "b", "c", "d"])
        report = LineageReport(diamond_dag, result)

        path = report.data_flow_path("a", "d")
        assert path is not None
        assert path[0] == "a"
        assert path[-1] == "d"
        assert len(path) == 3  # a -> b/c -> d

    def test_data_flow_path_no_completed_route(self, linear_dag):
        # b failed, so there's no completed path from a to c
        result = _make_result(completed=["a", "c"], failed=["b"])
        report = LineageReport(linear_dag, result)

        path = report.data_flow_path("a", "c")
        # The only path a->b->c has b failed (intermediate), so None
        assert path is None

    def test_broken_lineage(self, linear_dag):
        # a failed, but b somehow completed (e.g. fail_fast=False with no dep)
        result = _make_result(completed=["b", "c"], failed=["a"])
        report = LineageReport(linear_dag, result)

        broken = report.broken_lineage()
        assert ("a", "b") in broken

    def test_no_broken_lineage(self, linear_dag):
        result = _make_result(completed=["a", "b", "c"])
        report = LineageReport(linear_dag, result)

        broken = report.broken_lineage()
        assert broken == []

    def test_full_lineage(self, linear_dag):
        result = _make_result(completed=["a", "b", "c"])
        report = LineageReport(linear_dag, result)

        full = report.full_lineage()
        assert set(full.keys()) == {"a", "b", "c"}
        assert isinstance(full["a"], LineageRecord)
        assert isinstance(full["c"], LineageRecord)

    def test_summary(self, linear_dag):
        result = _make_result(completed=["a", "b", "c"])
        report = LineageReport(linear_dag, result)

        text = report.summary()
        assert "Lineage Report" in text
        assert "Completed: 3" in text

    def test_empty_result(self, linear_dag):
        result = ExecutionResult()
        report = LineageReport(linear_dag, result)

        full = report.full_lineage()
        assert full == {}


class TestTrackLineage:
    def test_convenience_function(self, linear_dag):
        result = _make_result(completed=["a", "b", "c"])
        report = track_lineage(linear_dag, result)
        assert isinstance(report, LineageReport)

    def test_monkey_patch(self, linear_dag):
        result = _make_result(completed=["a", "b", "c"])
        report = linear_dag.track_lineage(result)
        assert isinstance(report, LineageReport)


class TestWithRealExecution:
    def test_lineage_after_execution(self):
        dag = DAG()
        dag.add_nodes(["a", "b", "c"])
        dag.add_edge("a", "b")
        dag.add_edge("b", "c")

        tasks = {
            "a": lambda: 1,
            "b": lambda: 2,
            "c": lambda: 3,
        }
        executor = DAGExecutor(dag)
        result = executor.execute(tasks)

        report = track_lineage(dag, result)
        rec = report.lineage("c")
        assert set(rec.upstream_chain) == {"a", "b"}
        assert rec.depth >= 2
