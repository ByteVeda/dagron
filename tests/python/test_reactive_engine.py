"""Tests for `dagron.reactive` — Signal / Computed / Watcher.

Different from `tests/python/execution/test_reactive.py` which exercises
the older push-based `ReactiveDAG` over an existing DAG. This file tests
the new auto-tracking primitives.
"""

from __future__ import annotations

import gc

import pytest

import dagron.reactive as dr

# ---------------------------------------------------------------------------
# Signal — leaf value mutation
# ---------------------------------------------------------------------------


class TestSignal:
    def test_initial_value(self):
        s = dr.signal(42)
        assert s() == 42

    def test_set_and_read(self):
        s = dr.signal(0)
        s.set(100)
        assert s() == 100

    def test_set_same_value_is_noop(self):
        # Same-value sets should not trigger downstream recomputes.
        s = dr.signal(7)
        recomputes = [0]

        def f() -> int:
            recomputes[0] += 1
            return s() * 2

        c = dr.computed(f)
        c()
        assert recomputes[0] == 1
        s.set(7)  # same value — no invalidation
        c()
        assert recomputes[0] == 1

    def test_peek_does_not_track(self):
        # peek() reads the value without registering a dependency.
        s = dr.signal(1)
        recomputes = [0]

        def f() -> int:
            recomputes[0] += 1
            return s.peek() + 100  # peek, no tracking

        c = dr.computed(f)
        c()
        assert recomputes[0] == 1
        s.set(999)
        c()  # NOT invalidated — peek didn't register a dep
        assert recomputes[0] == 1


# ---------------------------------------------------------------------------
# Computed — lazy memoised derivation
# ---------------------------------------------------------------------------


class TestComputed:
    def test_basic_derivation(self):
        a = dr.signal(2)
        b = dr.signal(3)
        s = dr.computed(lambda: a() + b())
        assert s() == 5

    def test_lazy_recompute_on_read(self):
        a = dr.signal(1)
        recomputes = [0]

        def f() -> int:
            recomputes[0] += 1
            return a() * 10

        c = dr.computed(f)
        c()
        c()
        c()
        assert recomputes[0] == 1, "cached after first read"

    def test_invalidation_recomputes_on_next_read(self):
        a = dr.signal(1)
        recomputes = [0]

        def f() -> int:
            recomputes[0] += 1
            return a() * 10

        c = dr.computed(f)
        c()
        a.set(2)
        # Invalidation alone does NOT recompute; reading does.
        assert recomputes[0] == 1
        c()
        assert recomputes[0] == 2

    def test_nested_computed(self):
        a = dr.signal(1)
        b = dr.computed(lambda: a() + 1)
        c = dr.computed(lambda: b() * 2)
        assert c() == 4
        a.set(10)
        assert c() == 22

    def test_narrow_recompute_skips_unrelated(self):
        # Only the upstream-affected branch should recompute.
        a = dr.signal(1)
        b = dr.signal(100)
        recomputes_a = [0]
        recomputes_b = [0]

        def fa() -> int:
            recomputes_a[0] += 1
            return a() + 1

        def fb() -> int:
            recomputes_b[0] += 1
            return b() + 1

        ca = dr.computed(fa)
        cb = dr.computed(fb)

        ca()
        cb()
        assert recomputes_a[0] == 1
        assert recomputes_b[0] == 1

        a.set(2)
        # Read both; only `ca` should have recomputed.
        ca()
        cb()
        assert recomputes_a[0] == 2
        assert recomputes_b[0] == 1, "cb should NOT have recomputed"


# ---------------------------------------------------------------------------
# Watcher — side-effecting subscriber
# ---------------------------------------------------------------------------


class TestWatcher:
    def test_initial_fire(self):
        log: list[int] = []
        s = dr.signal(7)

        @dr.watch
        def w():
            log.append(s())

        assert log == [7], "watcher fires once at construction"

    def test_fires_on_dep_change(self):
        log: list[int] = []
        s = dr.signal(1)

        @dr.watch
        def w():
            log.append(s())

        s.set(2)
        s.set(3)
        assert log == [1, 2, 3]

    def test_does_not_fire_on_unrelated_change(self):
        log: list[int] = []
        a = dr.signal(1)
        b = dr.signal(100)

        @dr.watch
        def w():
            log.append(a())  # depends only on a

        log.clear()
        b.set(200)
        b.set(300)
        assert log == [], "watcher should not fire when only b changes"

    def test_dispose_stops_firing(self):
        log: list[int] = []
        s = dr.signal(1)

        @dr.watch
        def w():
            log.append(s())

        log.clear()
        w.dispose()
        s.set(2)
        s.set(3)
        assert log == []


# ---------------------------------------------------------------------------
# Batching — glitch-free updates
# ---------------------------------------------------------------------------


class TestBatching:
    def test_diamond_glitch_free(self):
        # Classic glitch test: a watcher sees a CONSISTENT view across
        # multiple signal mutations.
        a = dr.signal(1)
        b = dr.signal(2)
        s = dr.computed(lambda: a() + b())
        log: list[int] = []

        @dr.watch
        def w():
            log.append(s())

        log.clear()
        with dr.batch():
            a.set(10)
            b.set(20)
        # In a glitch-free system, w sees s=30, fired exactly once.
        assert log == [30], f"expected one fire to 30, got {log}"

    def test_nested_batches_only_fire_outermost(self):
        s = dr.signal(0)
        log: list[int] = []

        @dr.watch
        def w():
            log.append(s())

        log.clear()
        with dr.batch():
            s.set(1)
            with dr.batch():
                s.set(2)
                s.set(3)
            s.set(4)
        # All five sets coalesced into one fire of value 4.
        assert log == [4]

    def test_batch_with_no_changes_does_not_fire(self):
        s = dr.signal(5)
        log: list[int] = []

        @dr.watch
        def w():
            log.append(s())

        log.clear()
        with dr.batch():
            pass
        assert log == []


# ---------------------------------------------------------------------------
# Memory: dropped Computed / Watcher don't leak
# ---------------------------------------------------------------------------


class TestMemory:
    def test_dropped_computed_collected(self):
        s = dr.signal(0)
        c = dr.computed(lambda: s() * 2)
        c()  # establishes obs link from s -> c
        # observers is a WeakSet, so dropping c should let it be collected.
        del c
        gc.collect()
        # Setting s shouldn't crash even if c is gone.
        s.set(99)

    def test_disposed_watcher_does_not_keep_self_alive(self):
        s = dr.signal(0)

        @dr.watch
        def w():
            s()

        w.dispose()
        # No assertion — just that no exceptions fire on subsequent sets.
        s.set(1)


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_self_referential_computed_does_not_recurse_forever(self):
        # If a Computed reads itself in its body, we shouldn't add it as
        # its own dep / fall into a recompute loop.
        s = dr.signal(1)
        recomputes = [0]

        def fn() -> int:
            recomputes[0] += 1
            return s() + 1

        c = dr.computed(fn)
        # Read c() inside another computed body.
        outer = dr.computed(lambda: c() * 10)
        assert outer() == 20

        # Re-read does not re-fire either.
        outer()
        assert recomputes[0] == 1

    def test_same_signal_read_twice_in_one_compute_one_dep(self):
        # Reading the same signal twice inside a body should count as one dep.
        s = dr.signal(7)
        recomputes = [0]

        def fn() -> int:
            recomputes[0] += 1
            return s() + s() + s()  # three reads

        c = dr.computed(fn)
        c()
        s.set(8)
        c()
        # Only one extra recompute despite three internal reads.
        assert recomputes[0] == 2

    def test_computed_chain_of_ten_recomputes_all(self):
        # Linear chain a -> c0 -> c1 -> ... -> c9. Mutating `a` should
        # mark all of them dirty; each gets recomputed once when read.
        a = dr.signal(0)
        recomputes = [0]

        def make_step(prev):
            def f():
                recomputes[0] += 1
                return prev() + 1

            return dr.computed(f)

        nodes: list[dr.Computed[int]] = [a]  # type: ignore[list-item]
        for _ in range(10):
            nodes.append(make_step(nodes[-1]))

        last = nodes[-1]
        assert last() == 10

        recomputes[0] = 0
        a.set(100)
        assert last() == 110
        # Each of the 10 Computed nodes should have recomputed exactly once.
        assert recomputes[0] == 10


# ---------------------------------------------------------------------------
# Type alias smoke
# ---------------------------------------------------------------------------


def test_module_exports():
    assert hasattr(dr, "Signal")
    assert hasattr(dr, "Computed")
    assert hasattr(dr, "Watcher")
    assert hasattr(dr, "signal")
    assert hasattr(dr, "computed")
    assert hasattr(dr, "watch")
    assert hasattr(dr, "batch")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
