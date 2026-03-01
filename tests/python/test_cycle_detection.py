import pytest
from dagron import DAG, CycleError


class TestCycleDetection:
    def test_direct_cycle(self, empty_dag):
        empty_dag.add_nodes(["a", "b"])
        empty_dag.add_edge("a", "b")
        with pytest.raises(CycleError):
            empty_dag.add_edge("b", "a")

    def test_indirect_cycle(self, linear_dag):
        with pytest.raises(CycleError):
            linear_dag.add_edge("c", "a")

    def test_self_loop(self, empty_dag):
        empty_dag.add_node("a")
        with pytest.raises(CycleError):
            empty_dag.add_edge("a", "a")

    def test_no_false_positive(self, diamond_dag):
        # Diamond shape should not falsely detect a cycle
        diamond_dag.validate()

    def test_cycle_error_message_contains_path(self, empty_dag):
        empty_dag.add_nodes(["a", "b", "c"])
        empty_dag.add_edge("a", "b")
        empty_dag.add_edge("b", "c")
        with pytest.raises(CycleError, match="cycle"):
            empty_dag.add_edge("c", "a")


class TestValidate:
    def test_validate_valid_dag(self, diamond_dag):
        assert diamond_dag.validate() is True

    def test_validate_empty_dag(self, empty_dag):
        assert empty_dag.validate() is True

    def test_validate_single_node(self, empty_dag):
        empty_dag.add_node("a")
        assert empty_dag.validate() is True
