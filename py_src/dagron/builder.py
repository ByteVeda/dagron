"""Fluent builder pattern for DAG construction."""

from __future__ import annotations

from dagron._internal import DAG


class DAGBuilder:
    """Fluent builder for constructing DAGs.

    Example::

        dag = (
            DAGBuilder()
            .add_node("a", payload=1)
            .add_node("b", payload=2)
            .add_edge("a", "b")
            .build()
        )
    """

    def __init__(self) -> None:
        self._nodes: list[tuple[str, object, object]] = []
        self._edges: list[tuple[str, str, float | None, str | None]] = []

    def add_node(
        self,
        name: str,
        payload: object = None,
        metadata: object = None,
    ) -> DAGBuilder:
        """Add a node to the builder.

        Args:
            name: Unique name for the node.
            payload: Optional payload object.
            metadata: Optional metadata object.

        Returns:
            self for chaining.
        """
        self._nodes.append((name, payload, metadata))
        return self

    def add_edge(
        self,
        from_node: str,
        to_node: str,
        weight: float | None = None,
        label: str | None = None,
    ) -> DAGBuilder:
        """Add an edge to the builder.

        Args:
            from_node: Source node name.
            to_node: Target node name.
            weight: Optional edge weight.
            label: Optional edge label.

        Returns:
            self for chaining.
        """
        self._edges.append((from_node, to_node, weight, label))
        return self

    def build(self) -> DAG:
        """Build and return the DAG.

        Returns:
            A new DAG with all added nodes and edges.

        Raises:
            DuplicateNodeError: If any node name is duplicated.
            NodeNotFoundError: If an edge references a nonexistent node.
            CycleError: If the edges would create a cycle.
        """
        dag = DAG()
        for name, payload, metadata in self._nodes:
            dag.add_node(name, payload=payload, metadata=metadata)
        for from_node, to_node, weight, label in self._edges:
            dag.add_edge(from_node, to_node, weight=weight, label=label)
        return dag
