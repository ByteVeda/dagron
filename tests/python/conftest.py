import pytest
from dagron import DAG


@pytest.fixture
def empty_dag():
    """An empty DAG."""
    return DAG()


@pytest.fixture
def linear_dag():
    """A linear DAG: a -> b -> c."""
    dag = DAG()
    dag.add_node("a")
    dag.add_node("b")
    dag.add_node("c")
    dag.add_edge("a", "b")
    dag.add_edge("b", "c")
    return dag


@pytest.fixture
def diamond_dag():
    """A diamond DAG: a -> b, a -> c, b -> d, c -> d."""
    dag = DAG()
    dag.add_node("a")
    dag.add_node("b")
    dag.add_node("c")
    dag.add_node("d")
    dag.add_edge("a", "b")
    dag.add_edge("a", "c")
    dag.add_edge("b", "d")
    dag.add_edge("c", "d")
    return dag


@pytest.fixture
def complex_dag():
    """A complex DAG with 6 nodes and multiple paths.

    Structure:
        a -> b -> d -> f
        a -> c -> e -> f
        b -> e
    """
    dag = DAG()
    dag.add_nodes(["a", "b", "c", "d", "e", "f"])
    dag.add_edges([
        ("a", "b"),
        ("a", "c"),
        ("b", "d"),
        ("b", "e"),
        ("c", "e"),
        ("d", "f"),
        ("e", "f"),
    ])
    return dag
