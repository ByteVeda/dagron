import pytest
from dagron import NodeNotFoundError


class TestLen:
    def test_empty(self, empty_dag):
        assert len(empty_dag) == 0

    def test_with_nodes(self, diamond_dag):
        assert len(diamond_dag) == 4


class TestContains:
    def test_existing_node(self, linear_dag):
        assert "a" in linear_dag
        assert "b" in linear_dag

    def test_missing_node(self, linear_dag):
        assert "z" not in linear_dag


class TestGetItem:
    def test_get_payload(self, empty_dag):
        empty_dag.add_node("a", payload=42)
        assert empty_dag["a"] == 42

    def test_get_none_payload(self, empty_dag):
        empty_dag.add_node("a")
        assert empty_dag["a"] is None

    def test_missing_node_raises(self, empty_dag):
        with pytest.raises(NodeNotFoundError):
            _ = empty_dag["z"]


class TestSetItem:
    def test_set_payload(self, empty_dag):
        empty_dag.add_node("a")
        empty_dag["a"] = 99
        assert empty_dag["a"] == 99


class TestIter:
    def test_iter_names(self, diamond_dag):
        names = set(diamond_dag)
        assert names == {"a", "b", "c", "d"}

    def test_iter_empty(self, empty_dag):
        assert list(empty_dag) == []


class TestRepr:
    def test_repr_empty(self, empty_dag):
        assert repr(empty_dag) == "DAG(nodes=0, edges=0)"

    def test_repr_with_data(self, diamond_dag):
        assert repr(diamond_dag) == "DAG(nodes=4, edges=4)"


class TestBool:
    def test_empty_is_falsy(self, empty_dag):
        assert not empty_dag

    def test_nonempty_is_truthy(self, linear_dag):
        assert linear_dag
