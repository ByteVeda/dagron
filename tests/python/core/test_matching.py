"""Tests for regex/glob node matching."""

import pytest

from dagron import DAG, GraphError


def task_dag():
    dag = DAG()
    dag.add_node("task_build")
    dag.add_node("task_test")
    dag.add_node("task_deploy")
    dag.add_node("setup_env")
    dag.add_node("cleanup")
    dag.add_edge("task_build", "task_test")
    dag.add_edge("task_test", "task_deploy")
    dag.add_edge("setup_env", "task_build")
    return dag


class TestNodesMatchingRegex:
    def test_prefix(self):
        dag = task_dag()
        nodes = dag.nodes_matching_regex("^task_")
        assert len(nodes) == 3
        names = {n.name for n in nodes}
        assert names == {"task_build", "task_test", "task_deploy"}

    def test_suffix(self):
        dag = task_dag()
        nodes = dag.nodes_matching_regex("_env$")
        assert len(nodes) == 1
        assert nodes[0].name == "setup_env"

    def test_exact(self):
        dag = task_dag()
        nodes = dag.nodes_matching_regex("^cleanup$")
        assert len(nodes) == 1

    def test_no_matches(self):
        dag = task_dag()
        nodes = dag.nodes_matching_regex("^nonexistent")
        assert len(nodes) == 0

    def test_invalid_regex(self):
        dag = task_dag()
        with pytest.raises(GraphError):
            dag.nodes_matching_regex("[invalid")

    def test_all(self):
        dag = task_dag()
        nodes = dag.nodes_matching_regex(".*")
        assert len(nodes) == 5


class TestNodesMatchingGlob:
    def test_star(self):
        dag = task_dag()
        nodes = dag.nodes_matching_glob("task_*")
        assert len(nodes) == 3

    def test_question_mark(self):
        dag = task_dag()
        # task_test is 9 chars: "task_" + "test" (4 chars)
        nodes = dag.nodes_matching_glob("task_????")
        assert len(nodes) == 1
        assert nodes[0].name == "task_test"

    def test_all(self):
        dag = task_dag()
        nodes = dag.nodes_matching_glob("*")
        assert len(nodes) == 5

    def test_no_matches(self):
        dag = task_dag()
        nodes = dag.nodes_matching_glob("xyz_*")
        assert len(nodes) == 0

    def test_exact_match(self):
        dag = task_dag()
        nodes = dag.nodes_matching_glob("cleanup")
        assert len(nodes) == 1
        assert nodes[0].name == "cleanup"
