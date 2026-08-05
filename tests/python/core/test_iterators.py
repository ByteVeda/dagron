"""Tests for lazy iterators."""

from dagron import DAG


def test_iter_nodes():
    dag = DAG()
    dag.add_nodes(["a", "b", "c"])
    it = dag.iter_nodes()
    assert len(it) == 3
    nodes = list(it)
    assert len(nodes) == 3
    names = {n.name for n in nodes}
    assert names == {"a", "b", "c"}


def test_iter_roots():
    dag = DAG()
    dag.add_nodes(["a", "b", "c"])
    dag.add_edge("a", "b")
    dag.add_edge("a", "c")
    it = dag.iter_roots()
    roots = list(it)
    assert len(roots) == 1
    assert roots[0].name == "a"


def test_iter_leaves():
    dag = DAG()
    dag.add_nodes(["a", "b", "c"])
    dag.add_edge("a", "b")
    dag.add_edge("a", "c")
    it = dag.iter_leaves()
    leaves = list(it)
    assert len(leaves) == 2
    names = {n.name for n in leaves}
    assert names == {"b", "c"}


def test_iter_ancestors():
    dag = DAG()
    dag.add_nodes(["a", "b", "c"])
    dag.add_edge("a", "b")
    dag.add_edge("b", "c")
    it = dag.iter_ancestors("c")
    ancestors = list(it)
    names = {n.name for n in ancestors}
    assert names == {"a", "b"}


def test_iter_descendants():
    dag = DAG()
    dag.add_nodes(["a", "b", "c"])
    dag.add_edge("a", "b")
    dag.add_edge("b", "c")
    it = dag.iter_descendants("a")
    descendants = list(it)
    names = {n.name for n in descendants}
    assert names == {"b", "c"}


def test_iter_topological_sort():
    dag = DAG()
    dag.add_nodes(["a", "b", "c"])
    dag.add_edge("a", "b")
    dag.add_edge("b", "c")
    it = dag.iter_topological_sort()
    assert len(it) == 3
    nodes = list(it)
    names = [n.name for n in nodes]
    assert names == ["a", "b", "c"]


def test_iter_topological_levels():
    dag = DAG()
    dag.add_nodes(["a", "b", "c", "d"])
    dag.add_edge("a", "b")
    dag.add_edge("a", "c")
    dag.add_edge("b", "d")
    dag.add_edge("c", "d")
    it = dag.iter_topological_levels()
    assert len(it) == 3
    levels = list(it)
    assert len(levels) == 3
    assert {n.name for n in levels[0]} == {"a"}
    assert {n.name for n in levels[1]} == {"b", "c"}
    assert {n.name for n in levels[2]} == {"d"}


def test_iterator_collect():
    dag = DAG()
    dag.add_nodes(["a", "b", "c"])
    dag.add_edge("a", "b")
    dag.add_edge("b", "c")

    it = dag.iter_topological_sort()
    # Consume one item
    first = next(it)
    assert first.name == "a"
    # Collect remaining
    remaining = it.collect()
    assert len(remaining) == 2
    assert remaining[0].name == "b"
    assert remaining[1].name == "c"


def test_iterator_repr():
    dag = DAG()
    dag.add_nodes(["a", "b"])
    it = dag.iter_nodes()
    r = repr(it)
    assert "NodeIterator" in r
    assert "total=2" in r


def test_level_iterator_collect():
    dag = DAG()
    dag.add_nodes(["a", "b", "c"])
    dag.add_edge("a", "b")
    dag.add_edge("a", "c")
    it = dag.iter_topological_levels()
    # Consume one level
    first = next(it)
    assert len(first) == 1
    # Collect remaining
    remaining = it.collect()
    assert len(remaining) == 1


def test_iterator_exhaustion():
    dag = DAG()
    dag.add_node("a")
    it = dag.iter_nodes()
    first = next(it)
    assert first.name == "a"
    # Should be exhausted
    remaining = list(it)
    assert remaining == []


def test_empty_iterators():
    dag = DAG()
    assert len(dag.iter_nodes()) == 0
    assert list(dag.iter_nodes()) == []
    assert list(dag.iter_roots()) == []
    assert list(dag.iter_leaves()) == []
