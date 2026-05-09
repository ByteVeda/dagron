"""Tests for Phase 4 — effect-typed nodes.

Effect tags classify each `@task` by its purity / side-effect class. They
are queryable on the TaskSpec, mirrored onto DAG node metadata, and (when
opted in) gate parallelism in the executor: NONDETERMINISTIC tasks
serialize amongst themselves; PURE/READ/WRITE/NETWORK parallelize freely.
"""

from __future__ import annotations

import threading
import time
import warnings
from typing import TYPE_CHECKING

from dagron import (
    DAG,
    DAGExecutor,
    Effect,
    NodeRef,
    effects_of,
    flow,
    task,
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import Any

# ---------------------------------------------------------------------------
# Effect enum properties
# ---------------------------------------------------------------------------


class TestEffectEnum:
    def test_default_classes(self):
        assert Effect.PURE.value == "pure"
        assert Effect.NONDETERMINISTIC.value == "nondeterministic"

    def test_is_cacheable(self):
        assert Effect.PURE.is_cacheable
        assert Effect.READ.is_cacheable
        assert not Effect.WRITE.is_cacheable
        assert not Effect.NETWORK.is_cacheable
        assert not Effect.NONDETERMINISTIC.is_cacheable

    def test_is_deterministic(self):
        assert Effect.PURE.is_deterministic
        assert Effect.READ.is_deterministic
        assert not Effect.NONDETERMINISTIC.is_deterministic

    def test_is_isolated(self):
        assert Effect.NONDETERMINISTIC.is_isolated
        assert not Effect.PURE.is_isolated


# ---------------------------------------------------------------------------
# @task(effect=...) parameter
# ---------------------------------------------------------------------------


class TestTaskEffectParameter:
    def test_bare_task_defaults_to_pure(self):
        @task
        def f() -> int:
            return 1

        assert f._dagron_task.effect == Effect.PURE  # type: ignore[attr-defined]

    def test_explicit_effect(self):
        @task(effect=Effect.NETWORK)
        def fetch_url(url: str) -> str:
            return url

        assert fetch_url._dagron_task.effect == Effect.NETWORK  # type: ignore[attr-defined]

    def test_each_effect_class(self):
        for e in Effect:

            @task(effect=e)
            def f(_: Effect = e) -> int:  # default arg captures e per closure
                return 0

            assert f._dagron_task.effect == e  # type: ignore[attr-defined]

    def test_decorated_function_still_callable(self):
        @task(effect=Effect.READ)
        def add(a: int, b: int) -> int:
            return a + b

        assert add(2, 3) == 5


# ---------------------------------------------------------------------------
# AST scan heuristic
# ---------------------------------------------------------------------------


class TestAstScan:
    def test_pure_task_with_time_call_warns(self):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")

            @task
            def impure_pure() -> float:
                return time.time()

            assert any(
                "impure" in str(w.message) and "time.time" in str(w.message) for w in caught
            ), [str(w.message) for w in caught]

    def test_pure_task_without_impure_calls_does_not_warn(self):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")

            @task
            def clean(a: int, b: int) -> int:
                return a + b

            assert not any("impure" in str(w.message) for w in caught), [
                str(w.message) for w in caught
            ]

    def test_nondeterministic_task_does_not_warn_about_time(self):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")

            @task(effect=Effect.NONDETERMINISTIC)
            def now() -> float:
                return time.time()

            assert not any("impure" in str(w.message) for w in caught), [
                str(w.message) for w in caught
            ]

    def test_pure_task_with_random_warns(self):
        import random

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")

            @task
            def roll_dice() -> int:
                return random.randint(1, 6)

            assert any("impure" in str(w.message) for w in caught), [
                str(w.message) for w in caught
            ]


# ---------------------------------------------------------------------------
# Effect → DAG metadata mirroring
# ---------------------------------------------------------------------------


class TestDagMetadataMirror:
    def test_flow_writes_effect_to_metadata(self):
        @task(effect=Effect.READ)
        def read_db() -> list[int]:
            return [1, 2, 3]

        @task(effect=Effect.NETWORK)
        def push_to_api(rows: list[int]) -> str:
            return f"sent {len(rows)} rows"

        @flow
        def pipeline():
            return push_to_api(read_db())

        dag = pipeline.dag()
        meta_read = dag.get_metadata("read_db")
        meta_push = dag.get_metadata("push_to_api")
        assert meta_read == {"effect": "read"}
        assert meta_push == {"effect": "network"}

    def test_effects_of_helper(self):
        @task
        def a() -> int:
            return 1

        @task(effect=Effect.WRITE)
        def b(x: int) -> None:
            return None

        @flow
        def pipeline():
            return b(a())

        eff = effects_of(pipeline.dag())
        assert eff == {"a": Effect.PURE, "b": Effect.WRITE}

    def test_effects_of_untagged_dag_defaults_to_pure(self):
        d = DAG()
        d.add_node("x")
        assert effects_of(d) == {"x": Effect.PURE}


# ---------------------------------------------------------------------------
# Executor effect isolation
# ---------------------------------------------------------------------------


class TestExecutorIsolation:
    def test_two_pure_nodes_run_in_parallel(self):
        """When isolation is enforced, PURE tasks should still parallelize.

        Use a concurrency counter (max_active) instead of wall-clock timing
        — wall-clock thresholds are flaky on slow CI runners.
        """

        @task
        def pure_a() -> None:
            return None

        @task
        def pure_b() -> None:
            return None

        @flow
        def pipeline():
            pure_a()
            return pure_b()

        dag = pipeline.dag()

        active = 0
        max_active = 0
        lock = threading.Lock()
        # Both tasks must be inside the critical section at the same instant
        # for max_active to reach 2; the barrier guarantees they overlap.
        barrier = threading.Barrier(2, timeout=2.0)

        def make_fn() -> Callable[[], None]:
            def fn() -> None:
                nonlocal active, max_active
                barrier.wait()
                with lock:
                    active += 1
                    max_active = max(max_active, active)
                time.sleep(0.02)
                with lock:
                    active -= 1

            return fn

        tasks_dict: dict[str | NodeRef, Callable[[], Any]] = {
            "pure_a": make_fn(),
            "pure_b": make_fn(),
        }
        executor = DAGExecutor(
            dag,
            max_workers=2,
            enforce_effect_isolation=True,
        )
        result = executor.execute(tasks_dict)

        assert result.succeeded == 2
        assert max_active == 2, (
            f"PURE tasks did not run concurrently (max_active={max_active}); "
            "isolation incorrectly serialised non-ND tasks."
        )

    def test_two_nondeterministic_nodes_serialize(self):
        """Under isolation, two NONDETERMINISTIC tasks must NOT overlap."""
        # Track concurrent-execution count via a shared counter.
        active = 0
        max_active = 0
        lock = threading.Lock()

        def make_nd_fn():
            def fn():
                nonlocal active, max_active
                with lock:
                    active += 1
                    max_active = max(max_active, active)
                time.sleep(0.05)
                with lock:
                    active -= 1
                return active

            return fn

        @task(effect=Effect.NONDETERMINISTIC)
        def nd_a():
            return None

        @task(effect=Effect.NONDETERMINISTIC)
        def nd_b():
            return None

        @flow
        def pipeline():
            nd_a()
            return nd_b()

        dag = pipeline.dag()
        nd_tasks: dict[str | NodeRef, Callable[[], Any]] = {
            "nd_a": make_nd_fn(),
            "nd_b": make_nd_fn(),
        }
        executor = DAGExecutor(
            dag,
            max_workers=2,
            enforce_effect_isolation=True,
        )
        result = executor.execute(nd_tasks)

        assert result.succeeded == 2
        assert max_active == 1, (
            f"NONDETERMINISTIC tasks ran concurrently (max_active={max_active}); "
            "isolation lock failed."
        )

    def test_isolation_off_lets_nondeterministic_overlap(self):
        """Without isolation, NONDETERMINISTIC tasks can overlap (no enforcement)."""
        active = 0
        max_active = 0
        lock = threading.Lock()

        def make_fn():
            def fn():
                nonlocal active, max_active
                with lock:
                    active += 1
                    max_active = max(max_active, active)
                time.sleep(0.05)
                with lock:
                    active -= 1
                return None

            return fn

        @task(effect=Effect.NONDETERMINISTIC)
        def x():
            return None

        @task(effect=Effect.NONDETERMINISTIC)
        def y():
            return None

        @flow
        def pipeline():
            x()
            return y()

        dag = pipeline.dag()
        executor = DAGExecutor(
            dag,
            max_workers=2,
            enforce_effect_isolation=False,  # opt out
        )
        loose_tasks: dict[str | NodeRef, Callable[[], Any]] = {
            "x": make_fn(),
            "y": make_fn(),
        }
        executor.execute(loose_tasks)
        assert max_active == 2, "without isolation, ND tasks should overlap"
