"""Decorator-based DAG construction and execution."""

from __future__ import annotations

import asyncio
import inspect
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

    from dagron._internal import DAG
    from dagron.execution._types import ExecutionCallbacks, ExecutionResult


@dataclass(frozen=True)
class TaskSpec:
    """Metadata for a decorated task function."""

    name: str
    fn: Callable[..., Any]
    dependencies: list[str]
    is_async: bool


def task[F: Callable[..., Any]](fn: F) -> F:
    """Decorator that marks a function as a DAG task.

    Dependencies are inferred from the function's parameter names.
    Each parameter name must match the name of another ``@task``-decorated
    function whose return value will be passed as that argument.

    Parameters with defaults are treated as optional dependencies:
    if no matching task exists, the default value is used.

    Example::

        @dagron.task
        def fetch_data() -> list[dict]: ...

        @dagron.task
        def process(fetch_data: list[dict]) -> pd.DataFrame: ...

        pipeline = dagron.Pipeline([fetch_data, process])
        result = pipeline.execute()

    """
    sig = inspect.signature(fn)
    deps = []
    for param_name, param in sig.parameters.items():
        # Skip *args, **kwargs
        if param.kind in (
            inspect.Parameter.VAR_POSITIONAL,
            inspect.Parameter.VAR_KEYWORD,
        ):
            continue
        deps.append(param_name)

    spec = TaskSpec(
        name=fn.__name__,
        fn=fn,
        dependencies=deps,
        is_async=asyncio.iscoroutinefunction(fn),
    )
    fn._dagron_task = spec  # type: ignore[attr-defined]
    return fn


def _get_spec(fn: Any) -> TaskSpec:
    """Extract the TaskSpec from a decorated function."""
    spec: TaskSpec | None = getattr(fn, "_dagron_task", None)
    if spec is None:
        raise TypeError(
            f"{fn!r} is not a @dagron.task-decorated function. "
            "Use @dagron.task to mark functions as pipeline tasks."
        )
    return spec


class Pipeline:
    """Build and execute a DAG from ``@task``-decorated functions.

    The pipeline automatically infers the dependency graph from function
    parameter names: if task ``merge`` has a parameter called ``fetch_users``,
    it depends on the task named ``fetch_users``.

    The resulting ``.dag`` attribute is a full dagron ``DAG`` object with
    access to the entire Rust-powered analysis suite (critical path,
    reachability, explain, etc.).

    Args:
        tasks: Sequence of ``@dagron.task``-decorated functions.
        name: Optional pipeline name (used as metadata).

    Example::

        @dagron.task
        def extract() -> list: ...

        @dagron.task
        def transform(extract: list) -> list: ...

        @dagron.task
        def load(transform: list) -> None: ...

        pipeline = Pipeline([extract, transform, load])
        pipeline.dag.stats()           # full dagron analysis
        result = pipeline.execute()    # synchronous execution
    """

    def __init__(
        self,
        tasks: list[Callable[..., Any]],
        *,
        name: str | None = None,
    ) -> None:
        self._specs: dict[str, TaskSpec] = {}
        self._name = name

        for fn in tasks:
            spec = _get_spec(fn)
            if spec.name in self._specs:
                raise ValueError(
                    f"Duplicate task name '{spec.name}'. "
                    "Each task function must have a unique name."
                )
            self._specs[spec.name] = spec

        self._dag = self._build_dag()

    def _build_dag(self) -> DAG:
        from dagron.builder import DAGBuilder

        builder = DAGBuilder()
        task_names = set(self._specs.keys())

        for name in self._specs:
            builder.add_node(name)

        for name, spec in self._specs.items():
            for dep in spec.dependencies:
                if dep in task_names:
                    builder.add_edge(dep, name)

        return builder.build()

    @property
    def dag(self) -> DAG:
        """The underlying dagron DAG object.

        Provides full access to the Rust-powered analysis suite:
        ``critical_path()``, ``stats()``, ``topological_sort()``,
        ``build_reachability_index()``, etc.
        """
        return self._dag

    @property
    def task_names(self) -> list[str]:
        """Names of all tasks in the pipeline."""
        return list(self._specs.keys())

    def validate_contracts(
        self,
        extra_contracts: dict[str, Any] | None = None,
    ) -> list[Any]:
        """Extract type contracts from task annotations and validate them.

        Args:
            extra_contracts: Optional manually-specified contracts that
                override auto-extracted ones.

        Returns:
            List of :class:`ContractViolation` (empty if valid).
        """
        from dagron.contracts import validate_contracts

        return validate_contracts(self, extra_contracts)

    def _make_task_callables(
        self, overrides: dict[str, Any] | None = None
    ) -> dict[str, Callable[[], Any]]:
        """Build the task dict for executors, wiring outputs as inputs."""
        results: dict[str, Any] = {}
        if overrides:
            results.update(overrides)
        task_names = set(self._specs.keys())

        def make_callable(spec: TaskSpec) -> Callable[[], Any]:
            def run() -> Any:
                # If overridden, use the override value directly
                if overrides and spec.name in overrides:
                    value = overrides[spec.name]
                    results[spec.name] = value
                    return value
                kwargs: dict[str, Any] = {}
                sig = inspect.signature(spec.fn)
                for param_name, param in sig.parameters.items():
                    if param.kind in (
                        inspect.Parameter.VAR_POSITIONAL,
                        inspect.Parameter.VAR_KEYWORD,
                    ):
                        continue
                    if param_name in results:
                        kwargs[param_name] = results[param_name]
                    elif param_name in task_names:
                        # Dependency exists but hasn't run yet — shouldn't
                        # happen with correct topo order, but be safe
                        kwargs[param_name] = None
                    elif param.default is not inspect.Parameter.empty:
                        kwargs[param_name] = param.default
                value = spec.fn(**kwargs)
                results[spec.name] = value
                return value

            return run

        return {name: make_callable(spec) for name, spec in self._specs.items()}

    def _make_async_task_callables(
        self, overrides: dict[str, Any] | None = None
    ) -> dict[str, Callable[[], Any]]:
        """Build the async task dict for AsyncDAGExecutor."""
        results: dict[str, Any] = {}
        if overrides:
            results.update(overrides)
        task_names = set(self._specs.keys())

        def make_callable(spec: TaskSpec) -> Callable[[], Any]:
            async def run() -> Any:
                kwargs: dict[str, Any] = {}
                sig = inspect.signature(spec.fn)
                for param_name, param in sig.parameters.items():
                    if param.kind in (
                        inspect.Parameter.VAR_POSITIONAL,
                        inspect.Parameter.VAR_KEYWORD,
                    ):
                        continue
                    if param_name in results:
                        kwargs[param_name] = results[param_name]
                    elif param_name in task_names:
                        kwargs[param_name] = None
                    elif param.default is not inspect.Parameter.empty:
                        kwargs[param_name] = param.default
                if spec.is_async:
                    value = await spec.fn(**kwargs)
                else:
                    value = spec.fn(**kwargs)
                results[spec.name] = value
                return value

            return lambda: run()

        return {name: make_callable(spec) for name, spec in self._specs.items()}

    def execute(
        self,
        *,
        max_workers: int | None = None,
        callbacks: ExecutionCallbacks | None = None,
        fail_fast: bool = True,
        enable_tracing: bool = False,
        overrides: dict[str, Any] | None = None,
    ) -> ExecutionResult:
        """Execute the pipeline synchronously.

        Args:
            max_workers: Maximum concurrent workers.
            callbacks: Optional execution callbacks.
            fail_fast: Skip downstream nodes on failure.
            enable_tracing: Record execution trace events.
            overrides: Pre-set values for specific tasks (skip execution).

        Returns:
            ExecutionResult with per-node results.
        """
        from dagron.execution.executor import DAGExecutor

        # For sync pipelines where tasks depend on each other's results,
        # we must execute sequentially to wire outputs as inputs.
        executor = DAGExecutor(
            self._dag,
            max_workers=1,
            callbacks=callbacks,
            fail_fast=fail_fast,
            enable_tracing=enable_tracing,
        )
        task_callables = self._make_task_callables(overrides)
        return executor.execute(task_callables)

    async def execute_async(
        self,
        *,
        max_workers: int | None = None,
        callbacks: ExecutionCallbacks | None = None,
        fail_fast: bool = True,
        enable_tracing: bool = False,
        overrides: dict[str, Any] | None = None,
    ) -> ExecutionResult:
        """Execute the pipeline asynchronously.

        Async tasks are awaited; sync tasks are called directly.

        Args:
            max_workers: Maximum concurrent tasks.
            callbacks: Optional execution callbacks.
            fail_fast: Skip downstream nodes on failure.
            enable_tracing: Record execution trace events.
            overrides: Pre-set values for specific tasks (skip execution).

        Returns:
            ExecutionResult with per-node results.
        """
        from dagron.execution.executor import AsyncDAGExecutor

        executor = AsyncDAGExecutor(
            self._dag,
            max_workers=max_workers or 1,
            callbacks=callbacks,
            fail_fast=fail_fast,
            enable_tracing=enable_tracing,
        )
        task_callables = self._make_async_task_callables(overrides)
        return await executor.execute(task_callables)
