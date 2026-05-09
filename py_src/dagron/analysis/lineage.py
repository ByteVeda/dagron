"""Data lineage tracking — trace which upstream nodes contributed to any result."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from dagron._internal import NodeRef

if TYPE_CHECKING:
    from dagron._internal import DAG
    from dagron.execution._types import ExecutionResult


def _name_of(node: str | NodeRef) -> str:
    return node.name if isinstance(node, NodeRef) else node


@dataclass(frozen=True)
class LineageRecord:
    """Lineage information for a single completed node.

    Attributes:
        direct_inputs: Immediate predecessors that completed successfully.
        upstream_chain: All ancestors that completed (transitive closure).
        contributing_nodes: Set of all contributing completed ancestors.
        depth: Maximum depth from any root in the lineage.
    """

    direct_inputs: list[str]
    upstream_chain: list[str]
    contributing_nodes: frozenset[str]
    depth: int


@dataclass(frozen=True)
class ImpactRecord:
    """Downstream impact analysis for a single node.

    Attributes:
        directly_affects: Immediate successors that completed.
        transitively_affects: All descendants that completed.
        affected_leaves: Leaf nodes (no successors) that completed and
            are reachable from the source.
    """

    directly_affects: list[str]
    transitively_affects: list[str]
    affected_leaves: list[str]


class LineageReport:
    """Post-execution lineage analysis over a DAG and its execution result.

    Args:
        dag: The DAG that was executed.
        execution_result: The execution result to analyse.
    """

    def __init__(self, dag: DAG, execution_result: ExecutionResult) -> None:
        self._dag = dag
        self._result = execution_result
        self._completed: set[str] = self._build_completed_set()

    def _build_completed_set(self) -> set[str]:
        from dagron.execution._types import NodeStatus

        return {
            name
            for name, nr in self._result.node_results.items()
            if nr.status == NodeStatus.COMPLETED
        }

    def lineage(self, node: str | NodeRef) -> LineageRecord:
        """Compute lineage for a single node.

        Args:
            node: The node to analyse (str name or NodeRef).

        Returns:
            A :class:`LineageRecord` containing the node's upstream
            provenance filtered to actually-completed nodes.

        Raises:
            KeyError: If the node is not in the DAG.
        """
        node_name = _name_of(node)
        preds = [n.name for n in self._dag.predecessors(node_name)]
        direct_inputs = sorted(n for n in preds if n in self._completed)

        ancestors = {n.name for n in self._dag.ancestors(node_name)}
        completed_ancestors = ancestors & self._completed
        upstream_chain = sorted(completed_ancestors)

        depth = self._compute_depth(node_name, completed_ancestors)

        return LineageRecord(
            direct_inputs=direct_inputs,
            upstream_chain=upstream_chain,
            contributing_nodes=frozenset(completed_ancestors),
            depth=depth,
        )

    def impact(self, node: str | NodeRef) -> ImpactRecord:
        """Compute downstream impact of a single node.

        Args:
            node: The node to analyse (str name or NodeRef).

        Returns:
            An :class:`ImpactRecord` describing downstream completed nodes.

        Raises:
            KeyError: If the node is not in the DAG.
        """
        node_name = _name_of(node)
        succs = [n.name for n in self._dag.successors(node_name)]
        directly_affects = sorted(n for n in succs if n in self._completed)

        descs = {n.name for n in self._dag.descendants(node_name)}
        completed_descs = descs & self._completed
        transitively_affects = sorted(completed_descs)

        dag_leaves = {n.name for n in self._dag.leaves()}
        affected_leaves = sorted(completed_descs & dag_leaves)

        return ImpactRecord(
            directly_affects=directly_affects,
            transitively_affects=transitively_affects,
            affected_leaves=affected_leaves,
        )

    def data_flow_path(self, source: str | NodeRef, target: str | NodeRef) -> list[str] | None:
        """Find the shortest path where all intermediate nodes completed.

        Args:
            source: Start node name.
            target: End node name.

        Returns:
            List of node names forming the shortest completed path,
            or ``None`` if no such path exists.
        """
        source_name = _name_of(source)
        target_name = _name_of(target)
        all_paths_result = self._dag.all_paths(source_name, target_name)
        best: list[str] | None = None
        for path in all_paths_result:
            names = [n.name for n in path]
            # All intermediate nodes (excluding source and target) must be completed
            # Source and target just need to be in the path
            intermediates = names[1:-1]
            if all(n in self._completed for n in intermediates) and (
                best is None or len(names) < len(best)
            ):
                best = names
        return best

    def broken_lineage(self) -> list[tuple[str, str]]:
        """Find edges where upstream failed but downstream still ran.

        Returns:
            List of ``(upstream, downstream)`` tuples where the upstream
            node did not complete but the downstream node did.
        """
        from dagron.execution._types import NodeStatus

        broken: list[tuple[str, str]] = []
        for u, v in self._dag.edges():
            u_result = self._result.node_results.get(u)
            v_result = self._result.node_results.get(v)
            if u_result is None or v_result is None:
                continue
            u_failed = u_result.status != NodeStatus.COMPLETED
            v_completed = v_result.status == NodeStatus.COMPLETED
            if u_failed and v_completed:
                broken.append((u, v))
        return broken

    def full_lineage(self) -> dict[str, LineageRecord]:
        """Compute lineage for all completed nodes.

        Returns:
            Mapping of node name to :class:`LineageRecord` for every
            node that completed successfully.
        """
        return {name: self.lineage(name) for name in sorted(self._completed)}

    def summary(self) -> str:
        """Return a human-readable summary of lineage information."""
        total = len(self._result.node_results)
        completed = len(self._completed)
        broken = self.broken_lineage()

        roots = {n.name for n in self._dag.roots()}
        leaves = {n.name for n in self._dag.leaves()}
        completed_roots = sorted(roots & self._completed)
        completed_leaves = sorted(leaves & self._completed)

        lines = [
            "Lineage Report",
            f"  Total nodes: {total}",
            f"  Completed: {completed}",
            f"  Source nodes: {', '.join(completed_roots) or '(none)'}",
            f"  Leaf nodes: {', '.join(completed_leaves) or '(none)'}",
            f"  Broken lineage edges: {len(broken)}",
        ]
        if broken:
            for u, v in broken:
                lines.append(f"    {u} -> {v}")
        return "\n".join(lines)

    def _compute_depth(self, node: str, completed_ancestors: set[str]) -> int:
        """Compute the maximum depth from any completed root to *node*."""
        if not completed_ancestors:
            return 0

        roots = {n.name for n in self._dag.roots()}
        source_roots = completed_ancestors & roots
        if not source_roots:
            # Ancestors exist but none are roots — count levels of ancestors
            return self._max_chain_length(completed_ancestors)

        max_depth = 0
        for root in source_roots:
            path = self._dag.shortest_path(root, node)
            if path is not None:
                max_depth = max(max_depth, len(path) - 1)
        return max_depth

    def _max_chain_length(self, nodes: set[str]) -> int:
        """Estimate the longest chain within a set of nodes."""
        if not nodes:
            return 0
        # Use topological levels to estimate depth
        levels = self._dag.topological_levels()
        first_level = -1
        last_level = -1
        for i, level in enumerate(levels):
            level_names = {n.name for n in level}
            if level_names & nodes:
                if first_level == -1:
                    first_level = i
                last_level = i
        if first_level == -1:
            return 0
        return last_level - first_level


def track_lineage(dag: DAG, execution_result: ExecutionResult) -> LineageReport:
    """Convenience function to create a :class:`LineageReport`.

    Args:
        dag: The DAG that was executed.
        execution_result: The result of execution.

    Returns:
        A new :class:`LineageReport` instance.
    """
    return LineageReport(dag, execution_result)
