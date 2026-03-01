"""Tests for from_records integration helper."""

import pytest
from dataclasses import dataclass

from dagron import DAG, from_records, NodeNotFoundError


class TestFromRecordsDicts:
    def test_basic(self):
        records = [
            {"name": "a", "value": 1},
            {"name": "b", "value": 2},
            {"name": "c", "value": 3},
        ]
        dag = from_records(records)
        assert dag.node_count() == 3
        assert dag.edge_count() == 0

    def test_with_edges(self):
        records = [
            {"name": "a", "deps": []},
            {"name": "b", "deps": ["a"]},
            {"name": "c", "deps": ["a", "b"]},
        ]
        dag = from_records(records, edge_fn=lambda r: r["deps"])
        assert dag.node_count() == 3
        assert dag.edge_count() == 3
        assert dag.has_edge("a", "b")
        assert dag.has_edge("a", "c")
        assert dag.has_edge("b", "c")

    def test_payload_stored(self):
        records = [
            {"name": "a", "value": 42},
        ]
        dag = from_records(records)
        payload = dag.get_payload("a")
        assert payload == {"name": "a", "value": 42}

    def test_custom_payload_fn(self):
        records = [
            {"name": "a", "value": 42},
        ]
        dag = from_records(records, payload_fn=lambda r: r["value"])
        assert dag.get_payload("a") == 42

    def test_custom_name_field(self):
        records = [
            {"id": "node1", "data": "x"},
            {"id": "node2", "data": "y"},
        ]
        dag = from_records(records, name_field="id")
        assert dag.has_node("node1")
        assert dag.has_node("node2")

    def test_empty(self):
        dag = from_records([])
        assert dag.node_count() == 0


class TestFromRecordsDataclasses:
    def test_basic(self):
        @dataclass
        class Task:
            name: str
            deps: list

        tasks = [
            Task(name="build", deps=[]),
            Task(name="test", deps=["build"]),
            Task(name="deploy", deps=["test"]),
        ]
        dag = from_records(tasks, edge_fn=lambda t: t.deps)
        assert dag.node_count() == 3
        assert dag.has_edge("build", "test")
        assert dag.has_edge("test", "deploy")

    def test_custom_name_field(self):
        @dataclass
        class Step:
            step_name: str

        steps = [Step(step_name="s1"), Step(step_name="s2")]
        dag = from_records(steps, name_field="step_name")
        assert dag.has_node("s1")
        assert dag.has_node("s2")


class TestFromRecordsMonkeyPatch:
    def test_dag_from_records(self):
        records = [{"name": "a"}, {"name": "b"}]
        dag = DAG.from_records(records)
        assert dag.node_count() == 2


class TestFromRecordsAllTopo:
    def test_all_topological_orderings(self):
        dag = DAG()
        dag.add_node("a")
        dag.add_node("b")
        dag.add_node("c")
        dag.add_edge("a", "b")
        dag.add_edge("a", "c")

        orderings = dag.all_topological_orderings()
        assert len(orderings) == 2
        for order in orderings:
            assert order[0].name == "a"

    def test_all_topological_orderings_with_limit(self):
        dag = DAG()
        dag.add_node("a")
        dag.add_node("b")
        dag.add_node("c")
        orderings = dag.all_topological_orderings(limit=2)
        assert len(orderings) == 2

    def test_all_topological_orderings_linear(self):
        dag = DAG()
        dag.add_node("a")
        dag.add_node("b")
        dag.add_node("c")
        dag.add_edge("a", "b")
        dag.add_edge("b", "c")
        orderings = dag.all_topological_orderings()
        assert len(orderings) == 1
