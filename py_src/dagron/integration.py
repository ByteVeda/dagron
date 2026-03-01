"""Integration helpers for building DAGs from dataclasses, dicts, and Pydantic models."""

from __future__ import annotations

from typing import Any, Callable, Sequence

from dagron._internal import DAG


def _get_field(record: Any, field: str) -> Any:
    """Get a field from a record (dict, dataclass, or Pydantic model)."""
    if isinstance(record, dict):
        return record[field]
    return getattr(record, field)


def from_records(
    records: Sequence[Any],
    *,
    name_field: str = "name",
    edge_fn: Callable[[Any], list[str]] | None = None,
    payload_fn: Callable[[Any], Any] | None = None,
) -> DAG:
    """Build a DAG from a sequence of records.

    Works with dicts, dataclasses, and Pydantic BaseModel instances.

    Args:
        records: Sequence of records to convert.
        name_field: Field name to use as the node name (default "name").
        edge_fn: Optional callable that takes a record and returns a list
            of node names that this record depends on (edges FROM those
            nodes TO this node). If None, no edges are added.
        payload_fn: Optional callable that takes a record and returns
            the payload to store. If None, the entire record is stored.

    Returns:
        A new DAG built from the records.

    Raises:
        DuplicateNodeError: If any records share the same name.
        NodeNotFoundError: If edge_fn references a nonexistent node.
        CycleError: If the edges would create a cycle.
    """
    dag = DAG()

    # First pass: add all nodes
    for record in records:
        name = _get_field(record, name_field)
        payload = payload_fn(record) if payload_fn else record
        dag.add_node(str(name), payload=payload)

    # Second pass: add edges
    if edge_fn is not None:
        for record in records:
            name = str(_get_field(record, name_field))
            deps = edge_fn(record)
            for dep in deps:
                dag.add_edge(str(dep), name)

    return dag
