"""Type stubs for dagron package re-exports."""

from collections.abc import Callable, Sequence
from typing import Any, Literal

from dagron._internal import (
    DAG as DAG,
)
from dagron._internal import (
    CycleError as CycleError,
)
from dagron._internal import (
    DagronError as DagronError,
)
from dagron._internal import (
    DuplicateNodeError as DuplicateNodeError,
)
from dagron._internal import (
    EdgeNotFoundError as EdgeNotFoundError,
)
from dagron._internal import (
    ExecutionPlan as ExecutionPlan,
)
from dagron._internal import (
    ExecutionStep as ExecutionStep,
)
from dagron._internal import (
    GraphDiff as GraphDiff,
)
from dagron._internal import (
    GraphError as GraphError,
)
from dagron._internal import (
    GraphStats as GraphStats,
)
from dagron._internal import (
    NodeId as NodeId,
)
from dagron._internal import (
    NodeIterator as NodeIterator,
)
from dagron._internal import (
    NodeLevelIterator as NodeLevelIterator,
)
from dagron._internal import (
    NodeNotFoundError as NodeNotFoundError,
)
from dagron._internal import (
    ReachabilityIndex as ReachabilityIndex,
)
from dagron._internal import (
    ScheduledNode as ScheduledNode,
)
from dagron.analysis import (
    DAGSchema as DAGSchema,
)
from dagron.analysis import (
    LintReport as LintReport,
)
from dagron.analysis import (
    LintSeverity as LintSeverity,
)
from dagron.analysis import (
    LintWarning as LintWarning,
)
from dagron.analysis import (
    NodeExplanation as NodeExplanation,
)
from dagron.analysis import (
    WhatIfResult as WhatIfResult,
)
from dagron.builder import DAGBuilder as DAGBuilder
from dagron.compose import compose as compose
from dagron.dataframe import (
    ColumnSchema as ColumnSchema,
)
from dagron.dataframe import (
    DataFramePipeline as DataFramePipeline,
)
from dagron.dataframe import (
    DataFrameSchema as DataFrameSchema,
)
from dagron.dataframe import (
    SchemaViolation as SchemaViolation,
)
from dagron.dataframe import (
    validate_schema as validate_schema,
)
from dagron.execution import (
    AsyncDAGExecutor as AsyncDAGExecutor,
)
from dagron.execution import (
    CheckpointExecutor as CheckpointExecutor,
)
from dagron.execution import (
    CheckpointInfo as CheckpointInfo,
)
from dagron.execution import (
    ConditionalDAGBuilder as ConditionalDAGBuilder,
)
from dagron.execution import (
    ConditionalExecutor as ConditionalExecutor,
)
from dagron.execution import (
    DAGExecutor as DAGExecutor,
)
from dagron.execution import (
    ExecutionCallbacks as ExecutionCallbacks,
)
from dagron.execution import (
    ExecutionResult as ExecutionResult,
)
from dagron.execution import (
    ExecutionTrace as ExecutionTrace,
)
from dagron.execution import (
    IncrementalExecutor as IncrementalExecutor,
)
from dagron.execution import (
    IncrementalResult as IncrementalResult,
)
from dagron.execution import (
    NodeProfile as NodeProfile,
)
from dagron.execution import (
    NodeResult as NodeResult,
)
from dagron.execution import (
    NodeStatus as NodeStatus,
)
from dagron.execution import (
    Pipeline as Pipeline,
)
from dagron.execution import (
    ProfileReport as ProfileReport,
)
from dagron.execution import (
    ReactiveDAG as ReactiveDAG,
)
from dagron.execution import (
    TraceEvent as TraceEvent,
)
from dagron.execution import (
    TraceEventType as TraceEventType,
)
from dagron.execution import (
    profile_execution as profile_execution,
)
from dagron.execution import (
    task as task,
)
from dagron.versioning import (
    Mutation as Mutation,
)
from dagron.versioning import (
    MutationType as MutationType,
)
from dagron.versioning import (
    VersionedDAG as VersionedDAG,
)

__version__: str

def from_records(
    records: Sequence[Any],
    *,
    name_field: str = "name",
    edge_fn: Callable[[Any], list[str]] | None = None,
    payload_fn: Callable[[Any], Any] | None = None,
) -> DAG: ...
def pretty_print(
    dag: DAG,
    *,
    layout: Literal["vertical", "horizontal"] = "vertical",
    max_nodes: int = 50,
    show_payloads: bool = False,
    node_formatter: Callable[[str, Any], str] | None = None,
) -> str: ...
def _repr_svg_(dag: DAG, *, max_nodes: int = 100) -> str: ...
