"""Tests for `dagron.contentcache` — cross-process content-addressed cache.

Different from the in-process `ContentAddressableCache` in
`dagron.execution.content_cache`: this module uses the filesystem as its
index, so independent Python processes share intermediates transparently.
"""

from __future__ import annotations

import hashlib
import pickle
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from dagron import Effect
from dagron.contentcache import (
    ContentCache,
    default_cache_dir,
    default_hash,
    fingerprint_function,
    fingerprint_node,
    numpy_hash,
)

# ---------------------------------------------------------------------------
# Hashers
# ---------------------------------------------------------------------------


class TestDefaultHash:
    def test_deterministic(self):
        assert default_hash("hello") == default_hash("hello")
        assert default_hash([1, 2, 3]) == default_hash([1, 2, 3])

    def test_different_values_differ(self):
        assert default_hash("hello") != default_hash("world")
        assert default_hash([1, 2, 3]) != default_hash([1, 2, 4])

    def test_returns_32_bytes(self):
        assert len(default_hash("anything")) == 32

    def test_unpickleable_falls_back_to_repr(self):
        class _Weird:
            __slots__ = ()

            def __reduce__(self):
                raise TypeError("not pickleable")

        # Must not raise
        h = default_hash(_Weird())
        assert len(h) == 32


class TestNumpyHash:
    def test_falls_back_for_non_array(self):
        # Without numpy installed, behaves like default_hash; with numpy
        # but a non-array input, also falls back.
        assert numpy_hash("hello") == default_hash("hello")
        assert numpy_hash([1, 2, 3]) == default_hash([1, 2, 3])

    def test_array_uses_tobytes(self):
        np = pytest.importorskip("numpy")
        a = np.array([1, 2, 3, 4], dtype=np.int32)
        b = np.array([1, 2, 3, 4], dtype=np.int32)
        c = np.array([1, 2, 3, 5], dtype=np.int32)
        d = np.array([1.0, 2.0, 3.0, 4.0], dtype=np.float64)  # different dtype

        assert numpy_hash(a) == numpy_hash(b)
        assert numpy_hash(a) != numpy_hash(c)
        assert numpy_hash(a) != numpy_hash(d)


# ---------------------------------------------------------------------------
# Function fingerprinting
# ---------------------------------------------------------------------------


class TestFingerprintFunction:
    def test_same_function_same_fingerprint(self):
        def add(a, b):
            return a + b

        assert fingerprint_function(add) == fingerprint_function(add)

    def test_different_function_body_differs(self):
        def add(a, b):
            return a + b

        def sub(a, b):
            return a - b

        assert fingerprint_function(add) != fingerprint_function(sub)

    def test_different_constants_differ(self):
        def f1():
            return 42

        def f2():
            return 43

        assert fingerprint_function(f1) != fingerprint_function(f2)


class TestFingerprintNode:
    def test_combines_function_effect_inputs(self):
        def f(a):
            return a + 1

        # Different inputs → different fingerprints
        fp1 = fingerprint_node(f, Effect.PURE, [b"input-a"])
        fp2 = fingerprint_node(f, Effect.PURE, [b"input-b"])
        assert fp1 != fp2

        # Different effect tag → different fingerprint
        fp3 = fingerprint_node(f, Effect.READ, [b"input-a"])
        assert fp1 != fp3

        # Same everything → same fingerprint
        fp4 = fingerprint_node(f, Effect.PURE, [b"input-a"])
        assert fp1 == fp4


# ---------------------------------------------------------------------------
# ContentCache low-level API
# ---------------------------------------------------------------------------


class TestContentCacheLowLevel:
    def test_get_miss_returns_false(self, tmp_path):
        cache = ContentCache(cache_dir=tmp_path)
        val, hit = cache.get(b"missing-fingerprint" + b"\x00" * 14)  # 32-byte
        assert val is None
        assert hit is False

    def test_put_then_get_returns_value(self, tmp_path):
        cache = ContentCache(cache_dir=tmp_path)
        fp = b"\x01" * 32
        cache.put(fp, [1, 2, 3])
        val, hit = cache.get(fp)
        assert hit is True
        assert val == [1, 2, 3]

    def test_has(self, tmp_path):
        cache = ContentCache(cache_dir=tmp_path)
        fp = b"\x02" * 32
        assert not cache.has(fp)
        cache.put(fp, "x")
        assert cache.has(fp)

    def test_delete(self, tmp_path):
        cache = ContentCache(cache_dir=tmp_path)
        fp = b"\x03" * 32
        cache.put(fp, "x")
        cache.delete(fp)
        assert not cache.has(fp)

    def test_clear(self, tmp_path):
        cache = ContentCache(cache_dir=tmp_path)
        cache.put(b"\x04" * 32, "a")
        cache.put(b"\x05" * 32, "b")
        cache.clear()
        assert not cache.has(b"\x04" * 32)
        assert not cache.has(b"\x05" * 32)

    def test_corrupted_file_treated_as_miss(self, tmp_path):
        cache = ContentCache(cache_dir=tmp_path)
        fp = b"\x06" * 32
        cache.put(fp, "ok")
        # Corrupt the on-disk file by overwriting it with garbage.
        path = cache._path_for(fp)
        path.write_bytes(b"GARBAGE NOT MAGIC")
        _, hit = cache.get(fp)
        assert hit is False

    def test_path_sharded(self, tmp_path):
        cache = ContentCache(cache_dir=tmp_path)
        fp = bytes.fromhex("a1b2c3d4" + "e5" * 28)  # 32 bytes
        path = cache._path_for(fp)
        # Shards: a1/b2/c3d4e5...
        assert "a1" in str(path)
        assert "b2" in str(path)


# ---------------------------------------------------------------------------
# ContentCache.compute_or_cached — high-level API
# ---------------------------------------------------------------------------


class TestComputeOrCached:
    def test_first_miss_then_hit(self, tmp_path):
        cache = ContentCache(cache_dir=tmp_path)
        call_count = [0]

        def f(x: int) -> int:
            call_count[0] += 1
            return x * 100

        v1, hit1 = cache.compute_or_cached(f, args=(7,), effect=Effect.PURE)
        v2, hit2 = cache.compute_or_cached(f, args=(7,), effect=Effect.PURE)
        assert v1 == 700
        assert v2 == 700
        assert hit1 is False
        assert hit2 is True
        # f should have been called exactly once (second was a cache hit).
        assert call_count[0] == 1

    def test_different_args_recompute(self, tmp_path):
        cache = ContentCache(cache_dir=tmp_path)
        call_count = [0]

        def f(x: int) -> int:
            call_count[0] += 1
            return x

        cache.compute_or_cached(f, args=(1,), effect=Effect.PURE)
        cache.compute_or_cached(f, args=(2,), effect=Effect.PURE)
        cache.compute_or_cached(f, args=(3,), effect=Effect.PURE)
        assert call_count[0] == 3

    def test_kwargs_factor_into_key(self, tmp_path):
        cache = ContentCache(cache_dir=tmp_path)
        call_count = [0]

        def f(*, x: int) -> int:
            call_count[0] += 1
            return x

        cache.compute_or_cached(f, kwargs={"x": 1}, effect=Effect.PURE)
        cache.compute_or_cached(f, kwargs={"x": 1}, effect=Effect.PURE)  # hit
        cache.compute_or_cached(f, kwargs={"x": 2}, effect=Effect.PURE)  # miss
        assert call_count[0] == 2

    def test_uncacheable_effect_skips_cache(self, tmp_path):
        cache = ContentCache(cache_dir=tmp_path)
        call_count = [0]

        def fetch(url: str) -> str:
            call_count[0] += 1
            return url * 2

        for _ in range(3):
            v, hit = cache.compute_or_cached(fetch, args=("x",), effect=Effect.NETWORK)
            assert v == "xx"
            assert hit is False
        # All 3 calls actually invoked fetch.
        assert call_count[0] == 3

    def test_no_effect_defaults_to_caching(self, tmp_path):
        # When effect=None, behaves like PURE (cacheable).
        cache = ContentCache(cache_dir=tmp_path)
        call_count = [0]

        def f(x: int) -> int:
            call_count[0] += 1
            return x

        cache.compute_or_cached(f, args=(1,))
        cache.compute_or_cached(f, args=(1,))
        assert call_count[0] == 1


# ---------------------------------------------------------------------------
# Cross-process sharing — the headline claim of Phase 6
# ---------------------------------------------------------------------------


_CHILD_SCRIPT = textwrap.dedent(
    """
    import sys, pickle
    sys.path.insert(0, {dagron_path!r})
    from dagron import Effect
    from dagron.contentcache import ContentCache

    cache = ContentCache(cache_dir={cache_dir!r})
    call_count = [0]

    def slow_fn(x):
        call_count[0] += 1
        return x * 1000

    val, hit = cache.compute_or_cached(slow_fn, args=({arg},), effect=Effect.PURE)
    print(pickle.dumps((val, hit, call_count[0])).hex())
    """
)


def _run_child(cache_dir: Path, arg: int) -> tuple[object, bool, int]:
    """Run a fresh Python process that uses our ContentCache."""
    # Pass our py_src/ on the child's PYTHONPATH so it imports the same dagron.
    dagron_path = str(Path(__file__).parent.parent.parent / "py_src")
    code = _CHILD_SCRIPT.format(
        dagron_path=dagron_path,
        cache_dir=str(cache_dir),
        arg=arg,
    )
    proc = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"child failed: stderr={proc.stderr}")
    return pickle.loads(bytes.fromhex(proc.stdout.strip()))


class TestCrossProcessSharing:
    def test_second_process_hits_cache(self, tmp_path):
        # First process: compute and cache.
        v1, hit1, calls1 = _run_child(tmp_path, 42)
        assert v1 == 42_000
        assert hit1 is False
        assert calls1 == 1

        # Second process (fresh interpreter): should hit the cache.
        v2, hit2, calls2 = _run_child(tmp_path, 42)
        assert v2 == 42_000
        assert hit2 is True
        assert calls2 == 0, "child should not have invoked slow_fn"

    def test_different_inputs_independent_cache_entries(self, tmp_path):
        v1, hit1, _ = _run_child(tmp_path, 1)
        v2, hit2, _ = _run_child(tmp_path, 2)
        v3, hit3, _ = _run_child(tmp_path, 1)

        assert v1 == 1000
        assert v2 == 2000
        assert v3 == 1000
        assert hit1 is False
        assert hit2 is False
        assert hit3 is True


# ---------------------------------------------------------------------------
# default_cache_dir
# ---------------------------------------------------------------------------


class TestDefaultCacheDir:
    def test_uses_env_override(self, monkeypatch):
        monkeypatch.setenv("DAGRON_CACHE_DIR", "/tmp/dagron-test-cache-xyz")
        assert default_cache_dir() == Path("/tmp/dagron-test-cache-xyz")

    def test_default_is_xdg_style(self, monkeypatch):
        monkeypatch.delenv("DAGRON_CACHE_DIR", raising=False)
        d = default_cache_dir()
        assert "dagron" in str(d)
        assert "cas" in str(d)


# ---------------------------------------------------------------------------
# Function source mutation invalidates cache
# ---------------------------------------------------------------------------


class TestSourceMutationInvalidates:
    def test_two_distinct_functions_have_distinct_keys(self, tmp_path):
        # Simulate "user changed the source" by defining two functions
        # with different bytecode but same name & arity.
        cache = ContentCache(cache_dir=tmp_path)

        def make_v1():
            def f(x):
                return x + 1

            return f

        def make_v2():
            def f(x):
                return x + 2  # different body

            return f

        v1, _ = cache.compute_or_cached(make_v1(), args=(10,), effect=Effect.PURE)
        v2, hit2 = cache.compute_or_cached(make_v2(), args=(10,), effect=Effect.PURE)
        assert v1 == 11
        assert v2 == 12
        assert hit2 is False, "source-mutated function should miss the cache"


# ---------------------------------------------------------------------------
# magic-byte sanity
# ---------------------------------------------------------------------------


def test_cache_file_starts_with_magic(tmp_path):
    cache = ContentCache(cache_dir=tmp_path)
    fp = hashlib.blake2b(b"x", digest_size=32).digest()
    cache.put(fp, "hello")
    on_disk = cache._path_for(fp).read_bytes()
    assert on_disk.startswith(b"DAGRON")
