"""Tests for graph statistics."""

from dagron import DAG, GraphStats


def test_empty_graph_stats():
    dag = DAG()
    stats = dag.stats()
    assert isinstance(stats, GraphStats)
    assert stats.node_count == 0
    assert stats.edge_count == 0
    assert stats.depth == 0
    assert stats.width == 0
    assert stats.density == 0.0
    assert stats.longest_path_length == 0
    assert stats.root_count == 0
    assert stats.leaf_count == 0
    assert stats.is_weakly_connected is True
    assert stats.component_count == 0


def test_single_node_stats():
    dag = DAG()
    dag.add_node("a")
    stats = dag.stats()
    assert stats.node_count == 1
    assert stats.edge_count == 0
    assert stats.depth == 1
    assert stats.width == 1
    assert stats.root_count == 1
    assert stats.leaf_count == 1
    assert stats.is_weakly_connected is True
    assert stats.component_count == 1


def test_linear_dag_stats():
    dag = DAG()
    dag.add_nodes(["a", "b", "c"])
    dag.add_edge("a", "b")
    dag.add_edge("b", "c")
    stats = dag.stats()
    assert stats.node_count == 3
    assert stats.edge_count == 2
    assert stats.depth == 3
    assert stats.width == 1
    assert stats.longest_path_length == 2
    assert stats.root_count == 1
    assert stats.leaf_count == 1
    assert stats.max_in_degree == 1
    assert stats.max_out_degree == 1
    assert stats.avg_in_degree == stats.avg_out_degree


def test_diamond_dag_stats(diamond_dag):
    stats = diamond_dag.stats()
    assert stats.node_count == 4
    assert stats.edge_count == 4
    assert stats.depth == 3
    assert stats.width == 2
    assert stats.longest_path_length == 2
    assert stats.root_count == 1
    assert stats.leaf_count == 1
    assert stats.max_in_degree == 2
    assert stats.max_out_degree == 2
    assert stats.is_weakly_connected is True


def test_disconnected_graph_stats():
    dag = DAG()
    dag.add_nodes(["a", "b", "c"])
    stats = dag.stats()
    assert stats.component_count == 3
    assert stats.is_weakly_connected is False


def test_stats_repr():
    dag = DAG()
    dag.add_node("a")
    stats = dag.stats()
    r = repr(stats)
    assert "GraphStats" in r
    assert "nodes=1" in r


def test_stats_to_dict():
    dag = DAG()
    dag.add_nodes(["a", "b"])
    dag.add_edge("a", "b")
    stats = dag.stats()
    d = stats.to_dict()
    assert d["node_count"] == 2
    assert d["edge_count"] == 1
    assert d["depth"] == 2
    assert d["is_weakly_connected"] is True


def test_complex_dag_stats(complex_dag):
    stats = complex_dag.stats()
    assert stats.node_count == 6
    assert stats.edge_count == 7
    assert stats.root_count == 1
    assert stats.leaf_count == 1
    assert stats.is_weakly_connected is True
    assert stats.component_count == 1
    assert stats.depth >= 3
    assert stats.width >= 2


def test_density():
    dag = DAG()
    dag.add_nodes(["a", "b", "c"])
    dag.add_edge("a", "b")
    dag.add_edge("a", "c")
    dag.add_edge("b", "c")
    stats = dag.stats()
    assert abs(stats.density - 0.5) < 1e-10
