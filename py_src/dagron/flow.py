"""Pythonic compose API — `@dagron.flow` builds a DAG from Python call structure.

Example::

    @dagron.task
    def fetch() -> list[dict]: ...

    @dagron.task
    def transform(rows: list[dict]) -> pd.DataFrame: ...

    @dagron.flow
    def pipeline():
        raw = fetch()
        return transform(raw)

    result = pipeline()           # ExecutionResult
    df     = result[transform].result
    dag    = pipeline.dag()       # the underlying DAG, for analysis
"""

from __future__ import annotations

import asyncio
import functools
import inspect
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, overload

from dagron._internal import DAG
from dagron.effects import Effect, _warn_if_impure

if TYPE_CHECKING:
    from collections.abc import Callable

    from dagron._internal import NodeRef
    from dagron.execution._types import ExecutionCallbacks, ExecutionResult


# ---------------------------------------------------------------------------
# TaskSpec — metadata attached to every @task function
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TaskSpec:
    """Metadata attached to a `@dagron.task`-decorated function.

    Used by both the legacy `Pipeline` (parameter-name dependency inference)
    and the `@dagron.flow` API (call-structure tracing).
    """

    name: str
    fn: Callable[..., Any]
    dependencies: list[str]
    is_async: bool
    effect: Effect = Effect.PURE


def _get_spec(fn: Any) -> TaskSpec:
    """Extract the TaskSpec from a decorated function. Raises TypeError otherwise."""
    spec: TaskSpec | None = getattr(fn, "_dagron_task", None)
    if spec is None:
        raise TypeError(
            f"{fn!r} is not a @dagron.task-decorated function. "
            "Use @dagron.task to mark functions as pipeline tasks."
        )
    return spec


# ---------------------------------------------------------------------------
# FlowFuture — placeholder returned from a @task call inside a @flow body
# ---------------------------------------------------------------------------


class FlowFuture[T]:
    """Stand-in for a task's eventual return value during flow tracing.

    Returned from a `@dagron.task` call inside a `@dagron.flow` body. Pass
    it as an argument to other task calls to wire dependencies. The type
    parameter `T` carries the wrapped task's return type so downstream
    annotations can be statically checked.
    """

    __slots__ = ("_name",)

    def __init__(self, name: str) -> None:
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    def __repr__(self) -> str:
        return f"FlowFuture({self._name!r})"

    def __hash__(self) -> int:
        return hash(self._name)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, FlowFuture) and self._name == other._name


# ---------------------------------------------------------------------------
# Tracing — recorded calls + active flow context
# ---------------------------------------------------------------------------


@dataclass
class _NodeCall:
    """One recorded call to a `@task` function inside a `@flow` body."""

    name: str
    fn: Callable[..., Any]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    deps: list[str]  # names of upstream FlowFutures referenced in args/kwargs


@dataclass
class _FlowTrace:
    """Records calls made inside a single @flow invocation."""

    calls: list[_NodeCall] = field(default_factory=list)
    counter: dict[str, int] = field(default_factory=dict)

    def fresh_name(self, base: str) -> str:
        n = self.counter.get(base, 0)
        self.counter[base] = n + 1
        return base if n == 0 else f"{base}_{n}"

    def record(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> FlowFuture[Any]:
        deps: list[str] = []
        seen: set[str] = set()

        def collect(value: Any) -> None:
            if isinstance(value, FlowFuture) and value.name not in seen:
                seen.add(value.name)
                deps.append(value.name)

        for a in args:
            collect(a)
        for v in kwargs.values():
            collect(v)

        name = self.fresh_name(fn.__name__)
        self.calls.append(_NodeCall(name=name, fn=fn, args=args, kwargs=kwargs, deps=deps))
        return FlowFuture(name=name)


_current_flow: ContextVar[_FlowTrace | None] = ContextVar("_dagron_flow", default=None)


# ---------------------------------------------------------------------------
# @task decorator — flow-aware
# ---------------------------------------------------------------------------


def _wrap_task[**P, R](fn: Callable[P, R], effect: Effect) -> Callable[P, R]:
    """Internal: wrap `fn` with flow-aware dispatch and attach a TaskSpec."""
    sig = inspect.signature(fn)
    deps = [
        p_name
        for p_name, p in sig.parameters.items()
        if p.kind not in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)
    ]
    spec = TaskSpec(
        name=fn.__name__,
        fn=fn,
        dependencies=deps,
        is_async=asyncio.iscoroutinefunction(fn),
        effect=effect,
    )
    _warn_if_impure(fn, effect)

    @functools.wraps(fn)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        trace = _current_flow.get()
        if trace is not None:
            # Pass the wrapper itself so consumers downstream of the trace
            # (e.g. metadata mirroring) can read its `_dagron_task` spec.
            # The cast is the deliberate "type lie": at trace time we hand
            # back a FlowFuture, but the type system sees R.
            return trace.record(wrapper, args, kwargs)  # type: ignore[return-value]
        return fn(*args, **kwargs)

    wrapper._dagron_task = spec  # type: ignore[attr-defined]
    return wrapper


@overload
def task[**P, R](fn: Callable[P, R], /) -> Callable[P, R]: ...
@overload
def task[**P, R](*, effect: Effect = ...) -> Callable[[Callable[P, R]], Callable[P, R]]: ...
def task(
    fn: Callable[..., Any] | None = None,
    /,
    *,
    effect: Effect = Effect.PURE,
) -> Any:
    """Decorator that marks a function as a dagron task.

    Behavior:

    * **Outside** a `@dagron.flow` context, the decorated function executes
      normally.
    * **Inside** a `@dagron.flow` context, calling it records the call in
      the flow trace and returns a `FlowFuture[R]` placeholder. The
      function body is *not* executed during tracing.

    Typed as a passthrough: `Callable[P, R] -> Callable[P, R]`. The
    runtime values inside a flow context are `FlowFuture[R]` — pass them
    to other `@task` calls to wire dependencies.

    Functions decorated with this are also compatible with the legacy
    `Pipeline` class (parameter-name dependency inference).

    Args:
        fn: The function to decorate. Provided automatically when used as
            a bare `@task` decorator.
        effect: Side-effect classification of this task. Default
            `Effect.PURE`. See `dagron.effects.Effect` for the ladder.

    Example::

        @dagron.task
        def fetch_data() -> list[dict]: ...           # defaults to PURE

        @dagron.task(effect=Effect.NETWORK)
        def fetch_user(uid: int) -> dict: ...

        @dagron.flow
        def my_flow():
            raw = fetch_data()
            return process(raw)
    """
    if fn is not None:
        # Bare @task usage: `@task\ndef foo(): ...`
        return _wrap_task(fn, effect)

    # Parameterised: `@task(effect=...)\ndef foo(): ...`
    def decorator(real_fn: Callable[..., Any]) -> Callable[..., Any]:
        return _wrap_task(real_fn, effect)

    return decorator


# ---------------------------------------------------------------------------
# Building DAG + executor callables from a trace
# ---------------------------------------------------------------------------


def _build_dag(trace: _FlowTrace) -> DAG:
    dag = DAG()
    for call in trace.calls:
        # Mirror the task's effect onto node metadata so downstream
        # consumers (executor isolation, content cache, reactive engine,
        # replay) can read it without re-introspecting the function.
        spec = getattr(call.fn, "_dagron_task", None)
        metadata = {"effect": spec.effect.value} if spec is not None else None
        dag.add_node(call.name, metadata=metadata)
    for call in trace.calls:
        for dep in call.deps:
            dag.add_edge(dep, call.name)
    return dag


def _make_callables(trace: _FlowTrace, results: dict[str, Any]) -> dict[str, Callable[[], Any]]:
    """Build executor callables that resolve FlowFuture args from `results`.

    `results` is shared mutable state; each callable writes its own result
    on completion and reads upstream results on entry.
    """

    def make_callable(call: _NodeCall) -> Callable[[], Any]:
        def run() -> Any:
            resolved_args = tuple(
                results[a._name] if isinstance(a, FlowFuture) else a for a in call.args
            )
            resolved_kwargs = {
                k: (results[v._name] if isinstance(v, FlowFuture) else v)
                for k, v in call.kwargs.items()
            }
            value = call.fn(*resolved_args, **resolved_kwargs)
            results[call.name] = value
            return value

        return run

    return {call.name: make_callable(call) for call in trace.calls}


def _make_async_callables(
    trace: _FlowTrace, results: dict[str, Any]
) -> dict[str, Callable[[], Any]]:
    """Async variant of `_make_callables`."""

    def make_callable(call: _NodeCall) -> Callable[[], Any]:
        async def arun() -> Any:
            resolved_args = tuple(
                results[a._name] if isinstance(a, FlowFuture) else a for a in call.args
            )
            resolved_kwargs = {
                k: (results[v._name] if isinstance(v, FlowFuture) else v)
                for k, v in call.kwargs.items()
            }
            value = call.fn(*resolved_args, **resolved_kwargs)
            if asyncio.iscoroutine(value):
                value = await value
            results[call.name] = value
            return value

        return arun

    return {call.name: make_callable(call) for call in trace.calls}


# ---------------------------------------------------------------------------
# Flow class & @flow decorator
# ---------------------------------------------------------------------------


class Flow:
    """A flow built from a `@dagron.flow`-decorated function.

    Calling a `Flow` instance traces the function, builds the DAG, executes
    it, and returns an `ExecutionResult`. For DAG-only inspection (without
    execution), use `Flow.dag()`.

    Each call retraces the function body, so dynamic branching (`if`/`for`
    on outer parameters) produces a fresh DAG per invocation.
    """

    def __init__(self, fn: Callable[..., Any]) -> None:
        self._fn = fn
        functools.update_wrapper(self, fn)

    def _trace(self, *args: Any, **kwargs: Any) -> tuple[_FlowTrace, FlowFuture[Any] | None]:
        """Run the flow body in trace mode and return (trace, return_value)."""
        if _current_flow.get() is not None:
            raise RuntimeError(
                "Nested @dagron.flow invocations are not supported. "
                "If you need composition, build sub-flows separately."
            )
        trace = _FlowTrace()
        token = _current_flow.set(trace)
        try:
            ret = self._fn(*args, **kwargs)
        finally:
            _current_flow.reset(token)
        if ret is not None and not isinstance(ret, FlowFuture):
            raise TypeError(
                f"@flow {self._fn.__name__!r} must return a FlowFuture or None. "
                f"Got {type(ret).__name__}. Did you forget to call a "
                "@dagron.task function, or call a non-task function inside the flow body?"
            )
        return trace, ret

    def dag(self, *args: Any, **kwargs: Any) -> DAG:
        """Trace the flow and return the resulting DAG without executing it."""
        trace, _ = self._trace(*args, **kwargs)
        return _build_dag(trace)

    # Plan alias.
    build = dag

    def run(
        self,
        *args: Any,
        max_workers: int | None = None,
        callbacks: ExecutionCallbacks | None = None,
        fail_fast: bool = True,
        enable_tracing: bool = False,
        **kwargs: Any,
    ) -> ExecutionResult:
        """Trace, build, and execute the flow synchronously."""
        from dagron.execution.executor import DAGExecutor

        trace, _ = self._trace(*args, **kwargs)
        dag = _build_dag(trace)
        results: dict[str, Any] = {}
        # Widen the dict's static key type so executor.execute (which accepts
        # `Mapping[str | NodeRef, ...]`) type-checks. Runtime keys stay `str`.
        tasks: dict[str | NodeRef, Callable[[], Any]] = {  # noqa: C416
            k: v for k, v in _make_callables(trace, results).items()
        }
        # Sequential execution preserves the FlowFuture wiring contract:
        # downstream tasks need their upstream's results in `results` before
        # they run, which only happens after upstream completes. The Rust
        # executor handles the topo order; max_workers=1 by default to avoid
        # races on the shared `results` dict.
        executor = DAGExecutor(
            dag,
            max_workers=max_workers,
            callbacks=callbacks,
            fail_fast=fail_fast,
            enable_tracing=enable_tracing,
        )
        return executor.execute(tasks)

    async def run_async(
        self,
        *args: Any,
        max_workers: int | None = None,
        callbacks: ExecutionCallbacks | None = None,
        fail_fast: bool = True,
        enable_tracing: bool = False,
        **kwargs: Any,
    ) -> ExecutionResult:
        """Trace, build, and execute the flow asynchronously."""
        from dagron.execution.executor import AsyncDAGExecutor

        trace, _ = self._trace(*args, **kwargs)
        dag = _build_dag(trace)
        results: dict[str, Any] = {}
        tasks: dict[str | NodeRef, Callable[[], Any]] = {  # noqa: C416
            k: v for k, v in _make_async_callables(trace, results).items()
        }
        executor = AsyncDAGExecutor(
            dag,
            max_workers=max_workers or 1,
            callbacks=callbacks,
            fail_fast=fail_fast,
            enable_tracing=enable_tracing,
        )
        return await executor.execute(tasks)

    def __call__(self, *args: Any, **kwargs: Any) -> ExecutionResult:
        """Calling the flow runs it and returns the ExecutionResult."""
        return self.run(*args, **kwargs)


def flow(fn: Callable[..., Any]) -> Flow:
    """Decorator: build a DAG from the call structure of a Python function.

    Inside the decorated function, calls to `@dagron.task` functions are
    recorded as nodes; passing one task's return value to another records
    an edge.

    Example::

        @dagron.task
        def fetch() -> list: ...

        @dagron.task
        def process(rows: list) -> dict: ...

        @dagron.flow
        def pipeline():
            raw = fetch()
            return process(raw)

        result = pipeline()        # ExecutionResult
        dag = pipeline.dag()       # DAG for inspection
    """
    return Flow(fn)
