"""Multi-DAG composition with namespacing."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dagron._internal import DAG


def compose(
    dags: dict[str, DAG],
    connections: list[tuple[str, str]] | None = None,
    *,
    separator: str = "/",
) -> DAG:
    """Compose multiple DAGs into one with namespace prefixes.

    Each DAG's nodes are prefixed with its namespace key. Cross-namespace
    edges can be specified using the ``connections`` parameter.

    Args:
        dags: Dict mapping namespace names to DAG objects.
        connections: List of (from, to) tuples using namespaced names
            (e.g., ``("etl/load", "ml/train")``).
        separator: Separator between namespace and node name.

    Returns:
        A new DAG containing all nodes and edges with namespaces applied.

    Example::

        combined = compose(
            {"etl": etl_dag, "ml": ml_dag},
            connections=[("etl/load", "ml/train")],
        )
    """
    from dagron._internal import DAG as DagClass

    result = DagClass()

    for namespace, dag in dags.items():
        # Add all nodes with namespace prefix
        for node in dag.topological_sort():
            prefixed = f"{namespace}{separator}{node.name}"
            payload = dag.get_payload(node.name)
            metadata = dag.get_metadata(node.name)
            result.add_node(prefixed, payload=payload, metadata=metadata)

        # Add all edges with namespace prefix
        for node in dag.topological_sort():
            for succ in dag.successors(node.name):
                from_name = f"{namespace}{separator}{node.name}"
                to_name = f"{namespace}{separator}{succ.name}"
                result.add_edge(from_name, to_name)

    # Add cross-namespace connections
    if connections:
        for from_name, to_name in connections:
            result.add_edge(from_name, to_name)

    return result
