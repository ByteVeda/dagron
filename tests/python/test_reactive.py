"""Tests for Reactive/Observable DAG."""

import pytest

from dagron import DAG, DAGBuilder, ReactiveDAG


@pytest.fixture
def calc_dag():
    """A simple calculation DAG: a, b -> sum -> double."""
    return (
        DAGBuilder()
        .add_node("a")
        .add_node("b")
        .add_node("sum")
        .add_node("double")
        .add_edge("a", "sum")
        .add_edge("b", "sum")
        .add_edge("sum", "double")
        .build()
    )


@pytest.fixture
def calc_tasks():
    return {
        "a": lambda: 1,
        "b": lambda: 2,
        "sum": lambda a=0, b=0: a + b,
        "double": lambda sum=0: sum * 2,
    }


class TestReactiveDAG:
    def test_initialize(self, calc_dag, calc_tasks):
        reactive = ReactiveDAG(calc_dag, calc_tasks)
        values = reactive.initialize()
        assert values["a"] == 1
        assert values["b"] == 2
        assert values["sum"] == 3
        assert values["double"] == 6

    def test_set_input(self, calc_dag, calc_tasks):
        reactive = ReactiveDAG(calc_dag, calc_tasks)
        reactive.initialize()

        changed = reactive.set_input("a", 10)
        assert "sum" in changed
        assert "double" in changed
        assert reactive.get("sum") == 12  # 10 + 2
        assert reactive.get("double") == 24  # 12 * 2

    def test_set_input_no_change(self, calc_dag, calc_tasks):
        reactive = ReactiveDAG(calc_dag, calc_tasks)
        reactive.initialize()

        changed = reactive.set_input("a", 1)  # same value
        assert changed == {}

    def test_early_cutoff(self):
        """If a node produces the same value, downstream shouldn't update."""
        dag = (
            DAGBuilder()
            .add_node("input")
            .add_node("clamp")
            .add_node("output")
            .add_edge("input", "clamp")
            .add_edge("clamp", "output")
            .build()
        )
        call_count = {"output": 0}

        def output_fn(clamp=0):
            call_count["output"] += 1
            return clamp * 10

        tasks = {
            "input": lambda: 5,
            "clamp": lambda input=0: min(input, 100),  # clamps to 100
            "output": output_fn,
        }

        reactive = ReactiveDAG(dag, tasks)
        reactive.initialize()
        assert reactive.get("clamp") == 5
        assert call_count["output"] == 1

        # Change input but clamp still returns same value (both below 100)
        reactive.set_input("input", 5)  # Same value, no propagation
        assert call_count["output"] == 1

    def test_subscribe(self, calc_dag, calc_tasks):
        reactive = ReactiveDAG(calc_dag, calc_tasks)
        updates = []

        reactive.subscribe("double", lambda name, val: updates.append((name, val)))
        reactive.initialize()

        assert ("double", 6) in updates

        reactive.set_input("a", 10)
        assert ("double", 24) in updates

    def test_subscribe_all(self, calc_dag, calc_tasks):
        reactive = ReactiveDAG(calc_dag, calc_tasks)
        updates = []

        reactive.subscribe_all(lambda name, val: updates.append(name))
        reactive.initialize()

        assert set(updates) == {"a", "b", "sum", "double"}

    def test_unsubscribe(self, calc_dag, calc_tasks):
        reactive = ReactiveDAG(calc_dag, calc_tasks)
        updates = []

        unsub = reactive.subscribe("double", lambda name, val: updates.append(val))
        reactive.initialize()
        assert len(updates) == 1  # initial value

        unsub()
        reactive.set_input("a", 100)
        assert len(updates) == 1  # no new update after unsubscribe

    def test_set_inputs_batch(self, calc_dag, calc_tasks):
        reactive = ReactiveDAG(calc_dag, calc_tasks)
        reactive.initialize()

        changed = reactive.set_inputs({"a": 10, "b": 20})
        assert reactive.get("sum") == 30
        assert reactive.get("double") == 60
        assert "sum" in changed
        assert "double" in changed

    def test_get_value(self, calc_dag, calc_tasks):
        reactive = ReactiveDAG(calc_dag, calc_tasks)
        assert reactive.get("a") is None  # before initialization
        reactive.initialize()
        assert reactive.get("a") == 1

    def test_values_property(self, calc_dag, calc_tasks):
        reactive = ReactiveDAG(calc_dag, calc_tasks)
        reactive.initialize()
        values = reactive.values
        assert isinstance(values, dict)
        assert len(values) == 4

    def test_dag_property(self, calc_dag, calc_tasks):
        reactive = ReactiveDAG(calc_dag, calc_tasks)
        assert reactive.dag is calc_dag

    def test_auto_initialize_on_set_input(self, calc_dag, calc_tasks):
        reactive = ReactiveDAG(calc_dag, calc_tasks)
        # set_input before initialize should auto-initialize
        changed = reactive.set_input("a", 10)
        assert reactive.get("sum") == 12
