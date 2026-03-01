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


class TestBinaryRoundTrip:
    def test_binary_round_trip(self, diamond_dag):
        data = diamond_dag.to_bytes()
        dag2 = DAG.from_bytes(data)
        assert dag2.node_count() == 4
        assert dag2.edge_count() == 4
        assert dag2.has_edge("a", "b")
        assert dag2.has_edge("c", "d")

    def test_binary_with_payloads(self):
        dag = DAG()
        dag.add_node("a", payload=42)
        dag.add_node("b", payload="hello")
        dag.add_edge("a", "b")

        data = dag.to_bytes(payload_serializer=lambda p: p)
        dag2 = DAG.from_bytes(data, payload_deserializer=lambda v: v)
        assert dag2.get_payload("a") == 42
        assert dag2.get_payload("b") == "hello"

    def test_binary_round_trip_large(self):
        dag = DAG()
        n = 1000
        for i in range(n):
            dag.add_node(f"node_{i}", payload={"index": i, "label": f"item_{i}"})
        for i in range(n - 1):
            dag.add_edge(f"node_{i}", f"node_{i + 1}", weight=i * 0.1)

        data = dag.to_bytes(payload_serializer=lambda p: p)
        dag2 = DAG.from_bytes(data, payload_deserializer=lambda v: v)

        assert dag2.node_count() == n
        assert dag2.edge_count() == n - 1
        assert dag2.get_payload("node_0") == {"index": 0, "label": "item_0"}
        assert dag2.get_payload("node_999") == {"index": 999, "label": "item_999"}
        assert dag2.has_edge("node_0", "node_1")
        assert dag2.has_edge("node_998", "node_999")


class TestSaveLoad:
    def test_save_load_round_trip(self, tmp_path, diamond_dag):
        path = str(tmp_path / "test.dag")
        diamond_dag.save(path)
        dag2 = DAG.load(path)
        assert dag2.node_count() == 4
        assert dag2.edge_count() == 4
        assert dag2.has_edge("a", "b")
        assert dag2.has_edge("c", "d")

    def test_save_load_with_payloads(self, tmp_path):
        dag = DAG()
        dag.add_node("a", payload={"key": "value"})
        dag.add_node("b", payload=[1, 2, 3])
        dag.add_edge("a", "b", weight=2.5, label="dep")

        path = str(tmp_path / "payloads.dag")
        dag.save(path, payload_serializer=lambda p: p)
        dag2 = DAG.load(path, payload_deserializer=lambda v: v)

        assert dag2.get_payload("a") == {"key": "value"}
        assert dag2.get_payload("b") == [1, 2, 3]
        assert dag2.has_edge("a", "b")

    def test_save_load_edge_weights(self, tmp_path):
        dag = DAG()
        dag.add_nodes(["x", "y"])
        dag.add_edge("x", "y", weight=3.5, label="test")

        path = str(tmp_path / "edges.dag")
        dag.save(path)
        dag2 = DAG.load(path)

        json_str = dag2.to_json()
        import json

        data = json.loads(json_str)
        assert data["edges"][0]["weight"] == pytest.approx(3.5)
        assert data["edges"][0]["label"] == "test"

    def test_load_nonexistent_file(self):
        with pytest.raises(DagronError, match="Failed to open file"):
            DAG.load("/nonexistent/path/to/file.dag")

    def test_save_load_large_graph(self, tmp_path):
        dag = DAG()
        for i in range(100):
            dag.add_node(f"n{i}")
        for i in range(99):
            dag.add_edge(f"n{i}", f"n{i + 1}")

        path = str(tmp_path / "large.dag")
        dag.save(path)
        dag2 = DAG.load(path)
        assert dag2.node_count() == 100
        assert dag2.edge_count() == 99

    def test_save_load_empty_graph(self, tmp_path):
        dag = DAG()
        path = str(tmp_path / "empty.dag")
        dag.save(path)
        dag2 = DAG.load(path)
        assert dag2.node_count() == 0
        assert dag2.edge_count() == 0

    def test_save_load_large_streaming(self, tmp_path):
        dag = DAG()
        n = 5000
        for i in range(n):
            dag.add_node(f"node_{i}")
        for i in range(n - 1):
            dag.add_edge(f"node_{i}", f"node_{i + 1}")

        path = str(tmp_path / "large_streaming.dag")
        dag.save(path)
        dag2 = DAG.load(path)
        assert dag2.node_count() == n
        assert dag2.edge_count() == n - 1


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
