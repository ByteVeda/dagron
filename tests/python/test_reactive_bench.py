"""Benchmarks for `dagron.reactive` — narrow recompute paths on large graphs.

The headline claim for Phase 5 is: in a graph of 10k Computed nodes, mutating
one upstream signal and re-reading a downstream node should recompute only
the affected subgraph (not the whole graph) and finish in well under 1 ms.

These benchmarks use `pytest-benchmark`. Run with::

    uv run pytest tests/python/test_reactive_bench.py --benchmark-only
"""

from __future__ import annotations

import sys
from typing import Any

import pytest

import dagron.reactive as dr

# Each level in a Computed chain consumes ~5 stack frames during evaluation.
# Bump the limit so 1000-deep chains evaluate without RecursionError.
sys.setrecursionlimit(20_000)


# ---------------------------------------------------------------------------
# Topology builders
# ---------------------------------------------------------------------------


def _build_linear_chain(depth: int) -> tuple[dr.Signal[int], dr.Computed[int]]:
    """root signal → c0 → c1 → ... → c{depth-1} (returned as the tip)."""
    root: dr.Signal[int] = dr.signal(0)
    prev: dr.Computed[int] | dr.Signal[int] = root

    nodes: list[dr.Computed[int]] = []
    for _ in range(depth):
        # Capture prev in default arg to bind by-value, not by closure.
        def step(p=prev) -> int:  # type: ignore[no-untyped-def]
            return p() + 1

        prev = dr.computed(step)
        nodes.append(prev)

    # Return the root signal and the tail computed.
    return root, nodes[-1]


def _build_wide_diamond_set(
    n_branches: int,
) -> tuple[dr.Signal[int], list[dr.Computed[int]]]:
    """One root signal feeds N independent Computed branches.

    Mutating root invalidates all N — but recomputing only one branch
    should be cheap.
    """
    root: dr.Signal[int] = dr.signal(0)
    branches: list[dr.Computed[int]] = []
    for i in range(n_branches):

        def branch(i=i) -> int:  # type: ignore[no-untyped-def]
            return root() + i

        branches.append(dr.computed(branch))
    return root, branches


# ---------------------------------------------------------------------------
# Benchmarks — narrow recompute on a 10k-node linear chain
# ---------------------------------------------------------------------------


@pytest.mark.benchmark(group="reactive-narrow-recompute")
def test_narrow_recompute_chain_100(benchmark: Any) -> None:
    """100-node chain, single mutation, full re-read.

    Includes Python interpreter overhead; useful as a sanity baseline.
    """
    root, tip = _build_linear_chain(100)
    tip()  # initial compute

    counter = [0]

    def cycle() -> int:
        counter[0] += 1
        root.set(counter[0])
        return tip()

    result = benchmark(cycle)
    assert result == counter[0] + 100


@pytest.mark.benchmark(group="reactive-narrow-recompute")
def test_narrow_recompute_chain_1000(benchmark: Any) -> None:
    """1000-node chain — single linear path, expected linear cost.

    Each level adds ~5 Python stack frames; this file bumps
    `sys.setrecursionlimit(20_000)` at import time. Real DAGs rarely
    chain more than a few dozen deep — this is a stress upper bound.
    """
    root, tip = _build_linear_chain(1000)
    tip()

    counter = [0]

    def cycle() -> int:
        counter[0] += 1
        root.set(counter[0])
        return tip()

    result = benchmark(cycle)
    assert result == counter[0] + 1000


@pytest.mark.benchmark(group="reactive-narrow-recompute")
def test_one_branch_in_10k_wide_fanout(benchmark: Any) -> None:
    """10k branches off one root.

    Headline scenario: mutating the root invalidates all 10k branches, but
    we read just *one* of them. The reactive engine should recompute only
    that one — `pytest-benchmark` will report the cost.
    """
    root, branches = _build_wide_diamond_set(10_000)
    # Initial compute of the one branch we'll keep reading.
    target = branches[1234]
    target()

    counter = [0]

    def cycle() -> int:
        counter[0] += 1
        root.set(counter[0])
        return target()

    result = benchmark(cycle)
    assert result == counter[0] + 1234


# ---------------------------------------------------------------------------
# Benchmarks — initial-build cost
# ---------------------------------------------------------------------------


@pytest.mark.benchmark(group="reactive-construction")
def test_build_chain_1000(benchmark: Any) -> None:
    """Cost of constructing a 1000-node Computed chain (no eval).

    Construction does not recurse — only evaluation does — so a 1000-deep
    chain builds fine, only evaluation is bounded by recursion limit.
    """

    def build() -> dr.Computed[int]:
        _, tip = _build_linear_chain(1000)
        return tip

    benchmark(build)


@pytest.mark.benchmark(group="reactive-construction")
def test_build_and_evaluate_chain_1000(benchmark: Any) -> None:
    """Cost of constructing AND evaluating a 1000-node chain top-to-bottom."""

    def build_and_eval() -> int:
        _, tip = _build_linear_chain(1000)
        return tip()

    result = benchmark(build_and_eval)
    assert result == 1000


# ---------------------------------------------------------------------------
# Benchmarks — batched updates
# ---------------------------------------------------------------------------


@pytest.mark.benchmark(group="reactive-batching")
def test_batched_100_signal_writes(benchmark: Any) -> None:
    """One Computed reads 100 signals; batch 100 sets → one recompute."""
    signals = [dr.signal(0) for _ in range(100)]

    def aggregate() -> int:
        return sum(s() for s in signals)

    c = dr.computed(aggregate)
    c()  # initial

    counter = [0]

    def cycle() -> int:
        counter[0] += 1
        with dr.batch():
            for i, s in enumerate(signals):
                s.set(counter[0] + i)
        return c()

    result = benchmark(cycle)
    assert result == sum(counter[0] + i for i in range(100))
