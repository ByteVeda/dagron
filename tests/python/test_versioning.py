"""Tests for DAG Time-Travel / Structural Versioning."""

import pytest

from dagron import MutationType, VersionedDAG


class TestVersionedDAG:
    def test_initial_version(self):
        vdag = VersionedDAG()
        assert vdag.version == 0
        assert vdag.dag.node_count() == 0

    def test_version_increments(self):
        vdag = VersionedDAG()
        vdag.add_node("a")
        assert vdag.version == 1
        vdag.add_node("b")
        assert vdag.version == 2
        vdag.add_edge("a", "b")
        assert vdag.version == 3

    def test_current_state(self):
        vdag = VersionedDAG()
        vdag.add_node("a")
        vdag.add_node("b")
        vdag.add_edge("a", "b")
        assert vdag.dag.node_count() == 2
        assert vdag.dag.edge_count() == 1

    def test_at_version(self):
        vdag = VersionedDAG()
        vdag.add_node("a")
        vdag.add_node("b")
        vdag.add_edge("a", "b")

        v1 = vdag.at_version(1)
        assert v1.node_count() == 1
        assert v1.has_node("a")
        assert not v1.has_node("b")

        v2 = vdag.at_version(2)
        assert v2.node_count() == 2
        assert v2.edge_count() == 0

        v3 = vdag.at_version(3)
        assert v3.node_count() == 2
        assert v3.edge_count() == 1

    def test_at_version_zero(self):
        vdag = VersionedDAG()
        vdag.add_node("a")
        v0 = vdag.at_version(0)
        assert v0.node_count() == 0

    def test_at_version_out_of_range(self):
        vdag = VersionedDAG()
        vdag.add_node("a")
        with pytest.raises(ValueError, match="out of range"):
            vdag.at_version(5)

    def test_diff_versions(self):
        vdag = VersionedDAG()
        vdag.add_node("a")
        vdag.add_node("b")
        vdag.add_edge("a", "b")

        diff = vdag.diff_versions(1, 3)
        assert "b" in diff.added_nodes
        assert ("a", "b") in diff.added_edges

    def test_history(self):
        vdag = VersionedDAG()
        vdag.add_node("a")
        vdag.add_node("b")
        vdag.add_edge("a", "b")

        history = vdag.history()
        assert len(history) == 3
        assert history[0].mutation_type == MutationType.ADD_NODE
        assert history[0].args["name"] == "a"
        assert history[2].mutation_type == MutationType.ADD_EDGE

    def test_history_since(self):
        vdag = VersionedDAG()
        vdag.add_node("a")
        vdag.add_node("b")
        vdag.add_edge("a", "b")

        since = vdag.history_since(1)
        assert len(since) == 2
        assert since[0].version == 2

    def test_remove_node(self):
        vdag = VersionedDAG()
        vdag.add_node("a")
        vdag.add_node("b")
        vdag.remove_node("b")

        assert vdag.version == 3
        assert not vdag.dag.has_node("b")

        v2 = vdag.at_version(2)
        assert v2.has_node("b")

    def test_fork(self):
        vdag = VersionedDAG()
        vdag.add_node("a")
        vdag.add_node("b")
        vdag.add_edge("a", "b")

        forked = vdag.fork(at_version=2)
        assert forked.version == 2
        assert forked.dag.node_count() == 2
        assert forked.dag.edge_count() == 0

        # Modifying fork doesn't affect original
        forked.add_node("c")
        assert forked.version == 3
        assert vdag.version == 3
        assert not vdag.dag.has_node("c")

    def test_fork_current(self):
        vdag = VersionedDAG()
        vdag.add_node("a")
        vdag.add_node("b")

        forked = vdag.fork()
        assert forked.version == 2
        forked.add_node("c")
        assert not vdag.dag.has_node("c")

    def test_set_payload(self):
        vdag = VersionedDAG()
        vdag.add_node("a")
        vdag.set_payload("a", {"key": "value"})
        assert vdag.version == 2
        assert vdag.dag.get_payload("a") == {"key": "value"}

    def test_set_metadata(self):
        vdag = VersionedDAG()
        vdag.add_node("a")
        vdag.set_metadata("a", {"env": "prod"})
        assert vdag.version == 2

    def test_mutation_timestamps(self):
        vdag = VersionedDAG()
        vdag.add_node("a")
        vdag.add_node("b")

        history = vdag.history()
        assert history[0].timestamp <= history[1].timestamp

    def test_wrap_existing_dag(self):
        from dagron import DAGBuilder

        dag = (
            DAGBuilder()
            .add_node("x")
            .add_node("y")
            .add_edge("x", "y")
            .build()
        )
        vdag = VersionedDAG(dag)
        assert vdag.version == 0
        assert vdag.dag.node_count() == 2

        vdag.add_node("z")
        assert vdag.version == 1
