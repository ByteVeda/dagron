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
    NodeRef as NodeRef,
)
from dagron._internal import (
    ReachabilityIndex as ReachabilityIndex,
)
from dagron._internal import (
    ScheduledNode as ScheduledNode,
)
from dagron._internal import (
    StaleNodeRefError as StaleNodeRefError,
)
from dagron.analysis import (
    DAGSchema as DAGSchema,
)
from dagron.analysis import (
    ImpactRecord as ImpactRecord,
)
from dagron.analysis import (
    LineageRecord as LineageRecord,
)
from dagron.analysis import (
    LineageReport as LineageReport,
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
from dagron.analysis import (
    track_lineage as track_lineage,
)
from dagron.builder import DAGBuilder as DAGBuilder
from dagron.compose import compose as compose
from dagron.contracts import (
    ContractValidator as ContractValidator,
)
from dagron.contracts import (
    ContractViolation as ContractViolation,
)
from dagron.contracts import (
    NodeContract as NodeContract,
)
from dagron.contracts import (
    extract_contracts as extract_contracts,
)
from dagron.contracts import (
    validate_contracts as validate_contracts,
)
from dagron.dashboard import (
    DashboardPlugin as DashboardPlugin,
)
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
from dagron.effects import Effect as Effect
from dagron.effects import effects_of as effects_of
from dagron.execution import (
    AsyncDAGExecutor as AsyncDAGExecutor,
)
from dagron.execution import (
    CeleryBackend as CeleryBackend,
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
    DistributedBackend as DistributedBackend,
)
from dagron.execution import (
    DistributedExecutionResult as DistributedExecutionResult,
)
from dagron.execution import (
    DistributedExecutor as DistributedExecutor,
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
    MultiprocessingBackend as MultiprocessingBackend,
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
    RayBackend as RayBackend,
)
from dagron.execution import (
    ReactiveDAG as ReactiveDAG,
)
from dagron.execution import (
    ThreadBackend as ThreadBackend,
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
from dagron.flow import Flow as Flow
from dagron.flow import FlowFuture as FlowFuture
from dagron.flow import flow as flow
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
