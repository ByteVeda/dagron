"""dagron — A fast Rust-backed DAG engine for Python."""

from dagron._internal import (
    DAG,
    CycleError,
    DagronError,
    DuplicateNodeError,
    EdgeNotFoundError,
    ExecutionPlan,
    ExecutionStep,
    GraphDiff,
    GraphError,
    GraphStats,
    NodeId,
    NodeIterator,
    NodeLevelIterator,
    NodeNotFoundError,
    ReachabilityIndex,
    ScheduledNode,
)
from dagron.builder import DAGBuilder
from dagron.checkpoint import CheckpointExecutor, CheckpointInfo
from dagron.compose import compose
from dagron.conditions import ConditionalDAGBuilder, ConditionalExecutor
from dagron.dataframe import (
    ColumnSchema,
    DataFramePipeline,
    DataFrameSchema,
    SchemaViolation,
    validate_schema,
)
from dagron.display import _repr_svg_, pretty_print
from dagron.executor import (
    AsyncDAGExecutor,
    DAGExecutor,
    ExecutionCallbacks,
    ExecutionResult,
    IncrementalExecutor,
    IncrementalResult,
    NodeResult,
    NodeStatus,
)
from dagron.explain import NodeExplanation, WhatIfResult
from dagron.explain import explain as _explain_fn
from dagron.explain import what_if as _what_if_fn
from dagron.integration import from_records
from dagron.linting import DAGSchema, LintReport, LintSeverity, LintWarning
from dagron.linting import lint as _lint_fn
from dagron.pipeline import Pipeline, task
from dagron.profiling import NodeProfile, ProfileReport, profile_execution
from dagron.query import query as _query_fn
from dagron.reactive import ReactiveDAG
from dagron.tracing import ExecutionTrace, TraceEvent, TraceEventType
from dagron.versioning import Mutation, MutationType, VersionedDAG

# Monkey-patch for convenience
DAG.from_records = staticmethod(from_records)  # type: ignore[method-assign]
DAG.pretty_print = lambda self, **kw: pretty_print(self, **kw)  # type: ignore[method-assign]
DAG._repr_svg_ = lambda self: _repr_svg_(self)  # type: ignore[method-assign]

# Monkey-patch new features onto DAG
DAG.explain = lambda self, node, costs=None: _explain_fn(self, node, costs)  # type: ignore[attr-defined]
DAG.what_if = lambda self, **kw: _what_if_fn(self, **kw)  # type: ignore[attr-defined]
DAG.lint = lambda self, **kw: _lint_fn(self, **kw)  # type: ignore[attr-defined]
DAG.query = lambda self, expr: _query_fn(self, expr)  # type: ignore[attr-defined]

__version__ = "0.1.0"

__all__ = [
    "DAG",
    "AsyncDAGExecutor",
    "CheckpointExecutor",
    "CheckpointInfo",
    "ColumnSchema",
    "ConditionalDAGBuilder",
    "ConditionalExecutor",
    "CycleError",
    "DAGBuilder",
    "DAGExecutor",
    "DAGSchema",
    "DagronError",
    "DataFramePipeline",
    "DataFrameSchema",
    "DuplicateNodeError",
    "EdgeNotFoundError",
    "ExecutionCallbacks",
    "ExecutionPlan",
    "ExecutionResult",
    "ExecutionStep",
    "ExecutionTrace",
    "GraphDiff",
    "GraphError",
    "GraphStats",
    "IncrementalExecutor",
    "IncrementalResult",
    "LintReport",
    "LintSeverity",
    "LintWarning",
    "Mutation",
    "MutationType",
    "NodeExplanation",
    "NodeId",
    "NodeIterator",
    "NodeLevelIterator",
    "NodeNotFoundError",
    "NodeProfile",
    "NodeResult",
    "NodeStatus",
    "Pipeline",
    "ProfileReport",
    "ReachabilityIndex",
    "ReactiveDAG",
    "ScheduledNode",
    "SchemaViolation",
    "TraceEvent",
    "TraceEventType",
    "VersionedDAG",
    "WhatIfResult",
    "compose",
    "from_records",
    "pretty_print",
    "profile_execution",
    "task",
    "validate_schema",
]
