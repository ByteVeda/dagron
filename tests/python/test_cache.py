"""Tests for generation-based caching and dirty tracking (Phase 3)."""

import dagron


def make_diamond():
    dag = dagron.DAG()
    dag.add_node("a")
    dag.add_node("b")
    dag.add_node("c")
    dag.add_node("d")
    dag.add_edge("a", "b")
    dag.add_edge("a", "c")
    dag.add_edge("b", "d")
    dag.add_edge("c", "d")
    return dag


class TestCacheInfo:
    def test_cache_info_initial(self):
        dag = dagron.DAG()
        info = dag.cache_info()
        assert info["hits"] == 0
        assert info["misses"] == 0
        assert info["size"] == 0

    def test_cache_hit_roots(self):
        dag = make_diamond()
        dag.roots()
        dag.roots()
        info = dag.cache_info()
        assert info["hits"] == 1
        assert info["misses"] == 1

    def test_cache_hit_leaves(self):
        dag = make_diamond()
        dag.leaves()
        dag.leaves()
        info = dag.cache_info()
        assert info["hits"] == 1
        assert info["misses"] == 1

    def test_cache_hit_topo_sort(self):
        dag = make_diamond()
        dag.topological_sort()
        dag.topological_sort()
        info = dag.cache_info()
        assert info["hits"] == 1
        assert info["misses"] == 1

    def test_cache_hit_topo_sort_dfs(self):
        dag = make_diamond()
        dag.topological_sort_dfs()
        dag.topological_sort_dfs()
        info = dag.cache_info()
        assert info["hits"] == 1
        assert info["misses"] == 1

    def test_cache_hit_topo_levels(self):
        dag = make_diamond()
        dag.topological_levels()
        dag.topological_levels()
        info = dag.cache_info()
        assert info["hits"] == 1
        assert info["misses"] == 1


class TestCacheInvalidation:
    def test_invalidation_on_add_node(self):
        dag = make_diamond()
        dag.roots()
        assert dag.cache_info()["misses"] == 1
        dag.add_node("e")
        dag.roots()
        assert dag.cache_info()["misses"] == 2
        assert dag.cache_info()["hits"] == 0

    def test_invalidation_on_add_edge(self):
        dag = make_diamond()
        dag.topological_sort()
        assert dag.cache_info()["misses"] == 1
        dag.add_node("e")
        dag.add_edge("d", "e")
        dag.topological_sort()
        assert dag.cache_info()["misses"] == 2
        assert dag.cache_info()["hits"] == 0

    def test_invalidation_on_remove_node(self):
        dag = make_diamond()
        dag.leaves()
        assert dag.cache_info()["misses"] == 1
        dag.remove_node("d")
        dag.leaves()
        assert dag.cache_info()["misses"] == 2
        assert dag.cache_info()["hits"] == 0

    def test_invalidation_on_remove_edge(self):
        dag = make_diamond()
        dag.topological_levels()
        assert dag.cache_info()["misses"] == 1
        dag.remove_edge("b", "d")
        dag.topological_levels()
        assert dag.cache_info()["misses"] == 2
        assert dag.cache_info()["hits"] == 0


class TestClearCache:
    def test_clear_cache(self):
        dag = make_diamond()
        dag.roots()
        dag.roots()
        assert dag.cache_info()["hits"] == 1
        dag.clear_cache()
        dag.roots()
        assert dag.cache_info()["misses"] == 2


class TestGeneration:
    def test_generation_property(self):
        dag = dagron.DAG()
        assert dag.generation == 0
        dag.add_node("a")
        assert dag.generation == 1
        dag.add_node("b")
        assert dag.generation == 2
        dag.add_edge("a", "b")
        assert dag.generation == 3
        dag.remove_edge("a", "b")
        assert dag.generation == 4
        dag.remove_node("b")
        assert dag.generation == 5


class TestCacheCorrectness:
    def test_cached_result_is_correct(self):
        dag = make_diamond()
        t1 = dag.topological_sort()
        names1 = [n.name for n in t1]
        assert "e" not in names1

        dag.add_node("e")
        dag.add_edge("d", "e")
        t2 = dag.topological_sort()
        names2 = [n.name for n in t2]
        assert "e" in names2
        assert names2.index("d") < names2.index("e")

    def test_cache_with_executor(self):
        """Executor uses execution_plan (uncached), but roots/leaves cache normally."""
        dag = make_diamond()
        dag.roots()
        dag.roots()
        assert dag.cache_info()["hits"] == 1
        # execution_plan is not cached — doesn't affect cache stats for roots
        dag.execution_plan()
        dag.roots()
        assert dag.cache_info()["hits"] == 2
