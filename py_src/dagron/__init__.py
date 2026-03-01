"""dagron — A fast Rust-backed DAG engine for Python."""

from dagron._internal import (
    DAG,
    CycleError,
    DagronError,
    DuplicateNodeError,
    EdgeNotFoundError,
    GraphError,
    NodeId,
    NodeNotFoundError,
)

__version__ = "0.1.0"

__all__ = [
    "DAG",
    "NodeId",
    "DagronError",
    "CycleError",
    "NodeNotFoundError",
    "DuplicateNodeError",
    "EdgeNotFoundError",
    "GraphError",
]
