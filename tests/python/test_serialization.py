import json

import pytest
from dagron import DAG, DagronError


class TestToJson:
    def test_to_json_empty(self, empty_dag):
        result = empty_dag.to_json()
        data = json.loads(result)
        assert data["nodes"] == []
        assert data["edges"] == []

    def test_json_round_trip_diamond(self, diamond_dag):
        json_str = diamond_dag.to_json()
        dag2 = DAG.from_json(json_str)

        assert dag2.node_count() == 4
        assert dag2.edge_count() == 4
        assert dag2.has_node("a")
        assert dag2.has_node("b")
        assert dag2.has_node("c")
        assert dag2.has_node("d")
        assert dag2.has_edge("a", "b")
        assert dag2.has_edge("a", "c")
        assert dag2.has_edge("b", "d")
        assert dag2.has_edge("c", "d")

    def test_json_preserves_edge_weights(self):
        dag = DAG()
        dag.add_node("x")
        dag.add_node("y")
        dag.add_edge("x", "y", weight=3.5)

        json_str = dag.to_json()
        dag2 = DAG.from_json(json_str)

        data = json.loads(dag2.to_json())
        assert len(data["edges"]) == 1
        assert data["edges"][0]["weight"] == pytest.approx(3.5)

    def test_json_preserves_edge_labels(self):
        dag = DAG()
        dag.add_node("x")
        dag.add_node("y")
        dag.add_edge("x", "y", label="depends_on")

        json_str = dag.to_json()
        dag2 = DAG.from_json(json_str)

        data = json.loads(dag2.to_json())
        assert data["edges"][0]["label"] == "depends_on"


class TestJsonPayloads:
    def test_json_with_payload_serializer(self):
        dag = DAG()
        dag.add_node("a", payload={"key": "value", "num": 42})
        dag.add_node("b", payload=[1, 2, 3])

        json_str = dag.to_json(payload_serializer=lambda p: p)
        data = json.loads(json_str)

        payloads = {n["name"]: n["payload"] for n in data["nodes"]}
        assert payloads["a"] == {"key": "value", "num": 42}
        assert payloads["b"] == [1, 2, 3]

    def test_json_with_payload_deserializer(self):
        dag = DAG()
        dag.add_node("a", payload={"key": "value"})
        json_str = dag.to_json(payload_serializer=lambda p: p)

        dag2 = DAG.from_json(json_str, payload_deserializer=lambda v: v)
        assert dag2.get_payload("a") == {"key": "value"}

    def test_json_payload_round_trip(self):
        dag = DAG()
        dag.add_node("a", payload={"x": 1, "y": "hello"})
        dag.add_node("b", payload=[True, None, 3.14])
        dag.add_edge("a", "b")

        json_str = dag.to_json(payload_serializer=lambda p: p)
        dag2 = DAG.from_json(json_str, payload_deserializer=lambda v: v)

        assert dag2.get_payload("a") == {"x": 1, "y": "hello"}
        assert dag2.get_payload("b")[0] is True
        assert dag2.get_payload("b")[1] is None
        assert dag2.get_payload("b")[2] == pytest.approx(3.14)
        assert dag2.has_edge("a", "b")

    def test_json_no_payload(self):
        dag = DAG()
        dag.add_node("a", payload={"ignored": True})

        json_str = dag.to_json()  # no serializer
        data = json.loads(json_str)

        # payload field should be absent (skipped when None)
        assert "payload" not in data["nodes"][0]


class TestFromJsonInvalid:
    def test_from_json_invalid(self):
        with pytest.raises(DagronError, match="JSON deserialization failed"):
            DAG.from_json("not valid json {{{")


class TestToDot:
    def test_to_dot_diamond(self, diamond_dag):
        dot = diamond_dag.to_dot()
        assert dot.startswith("digraph {")
        assert dot.endswith("}")
        assert '"a"' in dot
        assert '"b"' in dot
        assert '"a" -> "b"' in dot
        assert '"b" -> "d"' in dot

    def test_to_dot_with_labels(self):
        dag = DAG()
        dag.add_node("x")
        dag.add_node("y")
        dag.add_edge("x", "y", label="dep")

        dot = dag.to_dot()
        assert 'label="dep"' in dot

    def test_to_dot_node_attrs(self, diamond_dag):
        def attrs(name, payload):
            if name == "a":
                return "shape=box, color=red"
            return None

        dot = diamond_dag.to_dot(node_attrs=attrs)
        assert '"a" [shape=box, color=red]' in dot
        assert '"b";' in dot  # no attrs for b


class TestToMermaid:
    def test_to_mermaid_diamond(self, diamond_dag):
        mermaid = diamond_dag.to_mermaid()
        assert mermaid.startswith("graph TD\n")
        assert 'a["a"]' in mermaid
        assert 'b["b"]' in mermaid
        assert "a --> b" in mermaid
        assert "b --> d" in mermaid

    def test_to_mermaid_with_labels(self):
        dag = DAG()
        dag.add_node("x")
        dag.add_node("y")
        dag.add_edge("x", "y", label="depends")

        mermaid = dag.to_mermaid()
        assert 'x -->|"depends"| y' in mermaid
