"""Tests for Content-Addressable Caching."""

import tempfile

import pytest

from dagron import DAG
from dagron.execution.cached_executor import CachedDAGExecutor
from dagron.execution.content_cache import (
    CacheKeyBuilder,
    CachePolicy,
    CacheStats,
    ContentAddressableCache,
    FileSystemCacheBackend,
)


class TestCacheKeyBuilder:
    def test_hash_task_from_lambda(self):
        h1 = CacheKeyBuilder.hash_task(lambda: 42)
        assert isinstance(h1, str)
        assert len(h1) == 16

    def test_hash_task_from_function(self):
        def my_func():
            return 123

        h = CacheKeyBuilder.hash_task(my_func)
        assert isinstance(h, str)

    def test_hash_value_primitive(self):
        h1 = CacheKeyBuilder.hash_value(42)
        h2 = CacheKeyBuilder.hash_value(42)
        assert h1 == h2
        assert h1 != CacheKeyBuilder.hash_value(43)

    def test_hash_value_with_protocol(self):
        class HasCacheKey:
            def __dagron_cache_key__(self) -> str:
                return "custom_key"

        h = CacheKeyBuilder.hash_value(HasCacheKey())
        assert isinstance(h, str)

    def test_build_key_deterministic(self):
        k1 = CacheKeyBuilder.build_key("node1", "abc123", {"pred1": "hash1"})
        k2 = CacheKeyBuilder.build_key("node1", "abc123", {"pred1": "hash1"})
        assert k1 == k2

    def test_build_key_changes_with_pred(self):
        k1 = CacheKeyBuilder.build_key("node1", "abc123", {"pred1": "hash1"})
        k2 = CacheKeyBuilder.build_key("node1", "abc123", {"pred1": "hash2"})
        assert k1 != k2

    def test_build_key_changes_with_task(self):
        k1 = CacheKeyBuilder.build_key("node1", "task_v1", {})
        k2 = CacheKeyBuilder.build_key("node1", "task_v2", {})
        assert k1 != k2

    def test_build_key_sorted_preds(self):
        k1 = CacheKeyBuilder.build_key("n", "t", {"a": "1", "b": "2"})
        k2 = CacheKeyBuilder.build_key("n", "t", {"b": "2", "a": "1"})
        assert k1 == k2  # Order-independent


class TestFileSystemCacheBackend:
    def test_put_and_get(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            backend = FileSystemCacheBackend(tmpdir)
            from dagron.execution.content_cache import CacheEntryMetadata

            meta = CacheEntryMetadata(node_name="test", cache_key="key1")
            backend.put("key1", {"result": 42}, meta)

            value, found = backend.get("key1")
            assert found
            assert value == {"result": 42}

    def test_get_missing(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            backend = FileSystemCacheBackend(tmpdir)
            value, found = backend.get("nonexistent")
            assert not found
            assert value is None

    def test_has(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            backend = FileSystemCacheBackend(tmpdir)
            from dagron.execution.content_cache import CacheEntryMetadata

            assert not backend.has("key1")
            meta = CacheEntryMetadata(node_name="test", cache_key="key1")
            backend.put("key1", 42, meta)
            assert backend.has("key1")

    def test_delete(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            backend = FileSystemCacheBackend(tmpdir)
            from dagron.execution.content_cache import CacheEntryMetadata

            meta = CacheEntryMetadata(node_name="test", cache_key="key1")
            backend.put("key1", 42, meta)
            backend.delete("key1")
            assert not backend.has("key1")

    def test_clear(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            backend = FileSystemCacheBackend(tmpdir)
            from dagron.execution.content_cache import CacheEntryMetadata

            for i in range(3):
                meta = CacheEntryMetadata(node_name=f"n{i}", cache_key=f"k{i}")
                backend.put(f"k{i}", i, meta)
            backend.clear()
            for i in range(3):
                assert not backend.has(f"k{i}")

    def test_stats(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            backend = FileSystemCacheBackend(tmpdir)
            from dagron.execution.content_cache import CacheEntryMetadata

            meta = CacheEntryMetadata(node_name="test", cache_key="key1")
            backend.put("key1", 42, meta)
            backend.get("key1")  # hit
            backend.get("missing")  # miss

            stats = backend.stats()
            assert stats.hits == 1
            assert stats.misses == 1
            assert stats.total_entries == 1

    def test_ttl_eviction(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            policy = CachePolicy(ttl_seconds=0.1)
            backend = FileSystemCacheBackend(tmpdir, policy)
            import time

            from dagron.execution.content_cache import CacheEntryMetadata

            meta = CacheEntryMetadata(node_name="test", cache_key="key1")
            backend.put("key1", 42, meta)
            assert backend.has("key1")

            time.sleep(0.2)
            assert not backend.has("key1")

    def test_max_entries_eviction(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            policy = CachePolicy(max_entries=2)
            backend = FileSystemCacheBackend(tmpdir, policy)
            from dagron.execution.content_cache import CacheEntryMetadata

            for i in range(3):
                meta = CacheEntryMetadata(node_name=f"n{i}", cache_key=f"k{i}")
                backend.put(f"k{i}", i, meta)

            stats = backend.stats()
            assert stats.total_entries <= 2

    def test_persistence_across_instances(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            from dagron.execution.content_cache import CacheEntryMetadata

            backend1 = FileSystemCacheBackend(tmpdir)
            meta = CacheEntryMetadata(node_name="test", cache_key="key1")
            backend1.put("key1", 42, meta)

            backend2 = FileSystemCacheBackend(tmpdir)
            value, found = backend2.get("key1")
            assert found
            assert value == 42


class TestContentAddressableCache:
    def test_compute_key_and_store(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            backend = FileSystemCacheBackend(tmpdir)
            cache = ContentAddressableCache(backend)

            key = cache.compute_key("node1", lambda: 42, {})
            assert isinstance(key, str)
            assert len(key) == 64  # SHA256 hex

            cache.put(key, 42, "node1")
            value, found = cache.get(key)
            assert found
            assert value == 42


class TestCachedDAGExecutor:
    def test_first_run_all_misses(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            dag = DAG()
            dag.add_node("a")
            dag.add_node("b")
            dag.add_edge("a", "b")

            backend = FileSystemCacheBackend(tmpdir)
            cache = ContentAddressableCache(backend)

            executor = CachedDAGExecutor(dag, cache)
            result = executor.execute({"a": lambda: 1, "b": lambda: 2})

            assert result.execution_result.succeeded == 2
            assert result.cache_hits == 0
            assert result.cache_misses == 2
            assert len(result.nodes_executed) == 2

    def test_second_run_all_hits(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            dag = DAG()
            dag.add_node("a")
            dag.add_node("b")
            dag.add_edge("a", "b")

            backend = FileSystemCacheBackend(tmpdir)
            cache = ContentAddressableCache(backend)

            def task_a():
                return 1

            def task_b():
                return 2

            tasks = {"a": task_a, "b": task_b}

            # First run
            executor = CachedDAGExecutor(dag, cache)
            result1 = executor.execute(tasks)
            assert result1.cache_misses == 2

            # Second run — same tasks, should hit cache
            result2 = executor.execute(tasks)
            assert result2.cache_hits == 2
            assert result2.cache_misses == 0
            assert len(result2.nodes_cached) == 2

    def test_upstream_change_invalidates_downstream(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            dag = DAG()
            dag.add_node("a")
            dag.add_node("b")
            dag.add_edge("a", "b")

            backend = FileSystemCacheBackend(tmpdir)
            cache = ContentAddressableCache(backend)

            # First run
            executor = CachedDAGExecutor(dag, cache)
            executor.execute({"a": lambda: 1, "b": lambda: 2})

            # Second run with different task for "a" — b should also miss
            executor.execute({"a": lambda: 99, "b": lambda: 2})
            # The key for b changes because a's result hash changed

    def test_fail_fast(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            dag = DAG()
            dag.add_node("a")
            dag.add_node("b")
            dag.add_edge("a", "b")

            backend = FileSystemCacheBackend(tmpdir)
            cache = ContentAddressableCache(backend)

            def fail():
                raise ValueError("boom")

            executor = CachedDAGExecutor(dag, cache, fail_fast=True)
            result = executor.execute({"a": fail, "b": lambda: 2})

            assert result.execution_result.failed == 1
            assert result.execution_result.skipped == 1

    def test_missing_task_skipped(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            dag = DAG()
            dag.add_node("a")

            backend = FileSystemCacheBackend(tmpdir)
            cache = ContentAddressableCache(backend)

            executor = CachedDAGExecutor(dag, cache)
            result = executor.execute({})
            assert result.execution_result.skipped == 1

    def test_with_tracing(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            dag = DAG()
            dag.add_node("a")

            backend = FileSystemCacheBackend(tmpdir)
            cache = ContentAddressableCache(backend)

            def task_a():
                return 42

            executor = CachedDAGExecutor(dag, cache, enable_tracing=True)
            result = executor.execute({"a": task_a})

            trace = result.execution_result.trace
            assert trace is not None
            event_types = [e.event_type.value for e in trace.events]
            assert "node_cache_miss" in event_types

            # Second run should have cache hit (same function reference)
            result2 = executor.execute({"a": task_a})
            trace2 = result2.execution_result.trace
            assert trace2 is not None
            event_types2 = [e.event_type.value for e in trace2.events]
            assert "node_cache_hit" in event_types2

    def test_cache_stats(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            dag = DAG()
            dag.add_node("a")

            backend = FileSystemCacheBackend(tmpdir)
            cache = ContentAddressableCache(backend)

            def task_a():
                return 1

            executor = CachedDAGExecutor(dag, cache)
            executor.execute({"a": task_a})
            executor.execute({"a": task_a})

            stats = cache.stats()
            assert stats.hits >= 1
            assert stats.hit_rate > 0


class TestCacheStats:
    def test_hit_rate(self):
        stats = CacheStats(hits=3, misses=7)
        assert stats.hit_rate == pytest.approx(0.3)

    def test_hit_rate_zero(self):
        stats = CacheStats()
        assert stats.hit_rate == 0.0
