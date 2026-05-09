"""Reactive incremental computation — `Signal` / `Computed` / `Watcher`.

dagron's reactive engine: mutate a leaf value (`Signal`) and only the
affected downstream `Computed` nodes recompute on next read; subscribed
`Watcher`s re-fire automatically.

Differences from the existing `dagron.execution.reactive.ReactiveDAG`:

* That class wraps an *existing* `dagron.DAG` and exposes a push-based
  `subscribe()` / `set_input()` API.
* This module provides Solid.js / Jane-Street-`Incremental` style
  primitives where the dependency graph is *implicit* — building a
  `Computed` records its read dependencies as side-effects of evaluating
  its function. No DAG construction step required.

Example::

    import dagron.reactive as dr

    a = dr.signal(1)
    b = dr.signal(2)
    s = dr.computed(lambda: a() + b())
    p = dr.computed(lambda: s() * 10)

    p()                  # 30 — initial compute, builds dep graph
    a.set(5)             # invalidates s and p; b untouched
    p()                  # 70 — recomputes only s and p

    @dr.watch
    def watch_p():
        print("p =", p())  # fires whenever p's value changes

    with dr.batch():
        a.set(0)
        b.set(0)
    # watch_p fires exactly once after the batch — glitch-free.
"""

from __future__ import annotations

import threading
import weakref
from contextlib import contextmanager
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator


# ---------------------------------------------------------------------------
# Tracking machinery
# ---------------------------------------------------------------------------


@runtime_checkable
class _Tracker(Protocol):
    """Anything that observes signals/computed and gets invalidated when they
    change. Both `Computed` and `Watcher` implement it structurally.
    """

    def _add_dep(self, dep: _Observable) -> None: ...
    def _invalidate(self) -> None: ...


@runtime_checkable
class _Observable(Protocol):
    """Anything that can be read inside a tracker and notified of changes."""

    def _attach(self, tracker: _Tracker) -> None: ...
    def _detach(self, tracker: _Tracker) -> None: ...


_local = threading.local()


def _current_tracker() -> _Tracker | None:
    return getattr(_local, "tracker", None)


@contextmanager
def _track(tracker: _Tracker) -> Iterator[None]:
    """Push `tracker` onto the thread-local tracker stack for the duration
    of the with-block. Inner reads of Signals/Computed will attach themselves
    to `tracker` as observers.
    """
    prev = getattr(_local, "tracker", None)
    _local.tracker = tracker
    try:
        yield
    finally:
        _local.tracker = prev


# ---------------------------------------------------------------------------
# Batching — defer Watcher fires until the outermost batch ends.
# ---------------------------------------------------------------------------


def _batch_depth() -> int:
    return getattr(_local, "batch_depth", 0)


def _pending_watchers() -> set[Watcher]:
    pw: set[Watcher] | None = getattr(_local, "pending", None)
    if pw is None:
        pw = set()
        _local.pending = pw
    return pw


@contextmanager
def batch() -> Iterator[None]:
    """Defer Watcher fires until the outermost `batch()` block ends.

    Multiple signal mutations inside a batch produce at most one Watcher
    fire per affected Watcher — guaranteeing glitch-free semantics.

    Usage::

        with dr.batch():
            a.set(1)
            b.set(2)
        # any watcher reading both a and b fires once, not twice.
    """
    _local.batch_depth = _batch_depth() + 1
    try:
        yield
    finally:
        _local.batch_depth -= 1
        if _local.batch_depth == 0:
            _flush_pending_watchers()


def _flush_pending_watchers() -> None:
    pending = _pending_watchers()
    if not pending:
        return
    # Snapshot and clear so newly-scheduled watchers (from inside fires)
    # accumulate for the next flush.
    snapshot = list(pending)
    pending.clear()
    for w in snapshot:
        if not w._disposed:
            w._fire()


# ---------------------------------------------------------------------------
# Signal — settable leaf
# ---------------------------------------------------------------------------


class Signal[T]:
    """A settable leaf in the reactive graph.

    Calling the signal (`s()`) returns its current value. Inside a
    `Computed` body or a `Watcher` body, the read is tracked so the
    consumer is invalidated when `s.set(v)` changes the value.

    Equality-checked: setting the same value (by `==`) is a no-op.
    """

    __slots__ = ("_observers", "_value")

    def __init__(self, value: T) -> None:
        self._value: T = value
        self._observers: weakref.WeakSet[_Tracker] = weakref.WeakSet()

    def __call__(self) -> T:
        """Read the current value, registering this signal as a dependency
        of the current tracker (if any)."""
        tracker = _current_tracker()
        if tracker is not None:
            self._observers.add(tracker)
            tracker._add_dep(self)
        return self._value

    def set(self, value: T) -> None:
        """Update the value and invalidate downstream observers.

        No-op if `value == self._value`.
        """
        try:
            same = self._value == value
        except Exception:
            same = False
        if same:
            return
        self._value = value
        # Snapshot observers; invalidation can mutate the WeakSet.
        for obs in list(self._observers):
            obs._invalidate()
        # If we're not inside a batch, fire pending watchers now.
        if _batch_depth() == 0:
            _flush_pending_watchers()

    def peek(self) -> T:
        """Read the current value WITHOUT registering a dependency."""
        return self._value

    def __repr__(self) -> str:
        return f"Signal({self._value!r})"

    def _attach(self, tracker: _Tracker) -> None:
        self._observers.add(tracker)

    def _detach(self, tracker: _Tracker) -> None:
        self._observers.discard(tracker)


# ---------------------------------------------------------------------------
# Computed — lazy memoised derived value
# ---------------------------------------------------------------------------


class Computed[T]:
    """A lazily-evaluated derived value.

    On first call (or after invalidation), runs `fn()`, recording the
    signals/computed it reads. On subsequent calls, returns the cached
    value as long as no upstream dep has been invalidated.
    """

    __slots__ = ("__weakref__", "_deps", "_dirty", "_fn", "_observers", "_value")

    def __init__(self, fn: Callable[[], T]) -> None:
        self._fn = fn
        self._value: T | None = None
        self._dirty = True
        self._deps: list[_Observable] = []
        self._observers: weakref.WeakSet[_Tracker] = weakref.WeakSet()

    def __call__(self) -> T:
        """Read the current value, recomputing if dirty."""
        if self._dirty:
            self._recompute()
        # Register this Computed as a dep of the current tracker.
        tracker = _current_tracker()
        if tracker is not None and tracker is not self:
            self._observers.add(tracker)
            tracker._add_dep(self)
        return self._value  # type: ignore[return-value]

    def peek(self) -> T:
        """Read without registering a dependency. Still recomputes if dirty."""
        if self._dirty:
            self._recompute()
        return self._value  # type: ignore[return-value]

    def _recompute(self) -> None:
        # Detach from old deps so they don't keep notifying us.
        for d in self._deps:
            d._detach(self)
        self._deps = []
        with _track(self):
            self._value = self._fn()
        self._dirty = False

    # _Tracker protocol
    def _add_dep(self, dep: _Observable) -> None:
        self._deps.append(dep)

    def _invalidate(self) -> None:
        if self._dirty:
            return
        self._dirty = True
        # Cascade invalidation to my observers.
        for obs in list(self._observers):
            obs._invalidate()

    # _Observable protocol
    def _attach(self, tracker: _Tracker) -> None:
        self._observers.add(tracker)

    def _detach(self, tracker: _Tracker) -> None:
        self._observers.discard(tracker)

    def __repr__(self) -> str:
        state = "dirty" if self._dirty else f"={self._value!r}"
        return f"Computed({state})"


# ---------------------------------------------------------------------------
# Watcher — side-effecting subscriber
# ---------------------------------------------------------------------------


class Watcher:
    """A side-effecting subscriber: re-runs whenever a tracked dep changes.

    Built via `dr.watch(fn)` (decorator-style) or `dr.watch_fn(fn)`.
    Construct-time, the body runs once to record initial deps. Subsequent
    invalidations queue the watcher; pending watchers fire at the end of
    the current `batch()` (or immediately if no batch is active).

    Call `.dispose()` to unsubscribe.
    """

    __slots__ = ("__weakref__", "_deps", "_disposed", "_fn")

    def __init__(self, fn: Callable[[], None]) -> None:
        self._fn = fn
        self._deps: list[_Observable] = []
        self._disposed = False
        # Initial fire to establish dependencies.
        self._fire()

    def _fire(self) -> None:
        if self._disposed:
            return
        for d in self._deps:
            d._detach(self)
        self._deps = []
        with _track(self):
            self._fn()

    def dispose(self) -> None:
        """Detach from all observed signals/computed and stop firing."""
        self._disposed = True
        for d in self._deps:
            d._detach(self)
        self._deps = []
        _pending_watchers().discard(self)

    # _Tracker protocol
    def _add_dep(self, dep: _Observable) -> None:
        self._deps.append(dep)

    def _invalidate(self) -> None:
        if self._disposed:
            return
        _pending_watchers().add(self)


# ---------------------------------------------------------------------------
# Public factory aliases — keep call sites concise.
# ---------------------------------------------------------------------------


def signal[T](value: T) -> Signal[T]:
    """Build a `Signal`. Convenience alias for `Signal(value)`."""
    return Signal(value)


def computed[T](fn: Callable[[], T]) -> Computed[T]:
    """Build a `Computed`. Convenience alias for `Computed(fn)`."""
    return Computed(fn)


def watch(fn: Callable[[], None]) -> Watcher:
    """Decorator-style: build a `Watcher` from `fn` and immediately fire once.

    Equivalent to `Watcher(fn)`. Use as a decorator to make intent clearer:

        @dr.watch
        def log_p():
            print(p())
    """
    return Watcher(fn)


__all__ = [
    "Computed",
    "Signal",
    "Watcher",
    "batch",
    "computed",
    "signal",
    "watch",
]
