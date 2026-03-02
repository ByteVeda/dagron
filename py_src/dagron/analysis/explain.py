"""Explain mode and what-if analysis for DAG nodes."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dagron._internal import DAG


@dataclass(frozen=True)
class NodeExplanation:
    """Structured diagnostic for a single node in the DAG."""

    name: str
    depth_from_root: int
    on_critical_path: bool
    bottleneck_score: float
    dominates: list[str]
    blocked_by: list[str]
    blocks: list[str]
    in_degree: int
    out_degree: int
    ancestor_count: int
    descendant_count: int
    is_root: bool
    is_leaf: bool

    def summary(self) -> str:
        """Return a human-readable summary."""
        lines = [
            f"Node: {self.name}",
            f"  Depth from root: {self.depth_from_root}",
            f"  On critical path: {self.on_critical_path}",
            f"  Bottleneck score: {self.bottleneck_score:.2f}",
            f"  In-degree: {self.in_degree}, Out-degree: {self.out_degree}",
            f"  Ancestors: {self.ancestor_count}, Descendants: {self.descendant_count}",
            f"  Root: {self.is_root}, Leaf: {self.is_leaf}",
        ]
        if self.blocked_by:
            lines.append(f"  Blocked by: {', '.join(self.blocked_by)}")
        if self.blocks:
            lines.append(f"  Blocks: {', '.join(self.blocks)}")
        if self.dominates:
            lines.append(f"  Dominates: {', '.join(self.dominates)}")
        return "\n".join(lines)


@dataclass(frozen=True)
class WhatIfResult:
    """Result of a hypothetical graph mutation."""

    orphaned_nodes: list[str] = field(default_factory=list)
    parallelism_change: int = 0
    new_critical_path: list[str] = field(default_factory=list)
    new_critical_path_cost: float = 0.0
    would_create_cycle: bool = False
    cycle_path: list[str] = field(default_factory=list)
    new_root_count: int = 0
    new_leaf_count: int = 0
    new_node_count: int = 0
    new_edge_count: int = 0

    def summary(self) -> str:
        """Return a human-readable summary."""
        lines = ["What-If Analysis:"]
        if self.would_create_cycle:
            lines.append(f"  Would create cycle: {' -> '.join(self.cycle_path)}")
            return "\n".join(lines)
        lines.append(f"  Nodes: {self.new_node_count}, Edges: {self.new_edge_count}")
        lines.append(f"  Roots: {self.new_root_count}, Leaves: {self.new_leaf_count}")
        if self.orphaned_nodes:
            lines.append(f"  Orphaned: {', '.join(self.orphaned_nodes)}")
        if self.parallelism_change != 0:
            sign = "+" if self.parallelism_change > 0 else ""
            lines.append(f"  Parallelism change: {sign}{self.parallelism_change}")
        if self.new_critical_path:
            lines.append(f"  New critical path: {' -> '.join(self.new_critical_path)}")
        return "\n".join(lines)


def explain(dag: DAG, node_name: str, costs: dict[str, float] | None = None) -> NodeExplanation:
    """Generate a structured diagnostic for a node.

    Args:
        dag: The DAG to analyze.
        node_name: Name of the node to explain.
        costs: Optional cost mapping for critical path analysis.

    Returns:
        NodeExplanation with depth, critical path membership,
        bottleneck score, dominator set, and dependency chains.
    """
    # Depth: which topological level is this node on?
    levels = dag.topological_levels()
    depth_from_root = 0
    for i, level in enumerate(levels):
        if any(n.name == node_name for n in level):
            depth_from_root = i
            break

    # Critical path membership
    cp_nodes, _cp_cost = dag.critical_path(costs)
    cp_names = [n.name for n in cp_nodes]
    on_critical_path = node_name in cp_names

    # Ancestors and descendants
    ancestors = [n.name for n in dag.ancestors(node_name)]
    descendants = [n.name for n in dag.descendants(node_name)]

    # Direct predecessors and successors
    blocked_by = [n.name for n in dag.predecessors(node_name)]
    blocks = [n.name for n in dag.successors(node_name)]

    # Degrees
    in_deg = dag.in_degree(node_name)
    out_deg = dag.out_degree(node_name)

    # Root/leaf status
    is_root = in_deg == 0
    is_leaf = out_deg == 0

    # Bottleneck score: (descendant_count / total_nodes) * (1 if on critical path else 0.5)
    total_nodes = dag.node_count()
    desc_ratio = len(descendants) / total_nodes if total_nodes > 0 else 0.0
    path_factor = 1.0 if on_critical_path else 0.5
    bottleneck_score = round(desc_ratio * path_factor, 2)

    # Dominates: nodes whose ALL paths from every root go through this node.
    # A node X is dominated by node_name if removing node_name would make X
    # unreachable from all roots.
    dominates: list[str] = []
    descendants_set = set(descendants)
    ancestors_set = set(ancestors)
    if descendants:
        for desc_name in descendants:
            # Get all ancestors of desc_name
            desc_ancestors = {n.name for n in dag.ancestors(desc_name)}
            # Remove node_name from the ancestor set
            desc_ancestors.discard(node_name)
            # If all remaining ancestors are either:
            # 1. Also descendants of node_name (reachable only through it), or
            # 2. Also ancestors of node_name (upstream of it)
            # Then node_name dominates desc_name
            # Simpler check: are there any ancestors of desc_name that are
            # NOT ancestors of node_name and NOT descendants of node_name?
            # If no, then all paths to desc_name go through node_name.
            bypass_ancestors = desc_ancestors - ancestors_set - descendants_set
            if not bypass_ancestors:
                dominates.append(desc_name)

    return NodeExplanation(
        name=node_name,
        depth_from_root=depth_from_root,
        on_critical_path=on_critical_path,
        bottleneck_score=bottleneck_score,
        dominates=sorted(dominates),
        blocked_by=sorted(blocked_by),
        blocks=sorted(blocks),
        in_degree=in_deg,
        out_degree=out_deg,
        ancestor_count=len(ancestors),
        descendant_count=len(descendants),
        is_root=is_root,
        is_leaf=is_leaf,
    )


def what_if(
    dag: DAG,
    *,
    remove_nodes: list[str] | None = None,
    remove_edges: list[tuple[str, str]] | None = None,
    add_nodes: list[str] | None = None,
    add_edges: list[tuple[str, str]] | None = None,
    costs: dict[str, float] | None = None,
) -> WhatIfResult:
    """Analyze the effect of hypothetical mutations without modifying the DAG.

    Args:
        dag: The DAG to analyze.
        remove_nodes: Nodes to hypothetically remove.
        remove_edges: Edges to hypothetically remove.
        add_nodes: Nodes to hypothetically add.
        add_edges: Edges to hypothetically add.
        costs: Optional cost mapping for critical path analysis.

    Returns:
        WhatIfResult with impact analysis.
    """
    from dagron._internal import CycleError

    # Get baseline stats
    original_stats = dag.stats()
    original_max_parallelism = original_stats.width

    # Create a snapshot to mutate
    mutated = dag.snapshot()

    # Check for cycle creation from new edges before making any changes
    if add_edges:
        for from_node, to_node in add_edges:
            # First ensure both nodes exist
            from_exists = mutated.has_node(from_node)
            to_exists = mutated.has_node(to_node)

            if not from_exists:
                mutated.add_node(from_node)
            if not to_exists:
                mutated.add_node(to_node)

            # Try adding the edge, catch cycle error
            try:
                mutated.add_edge(from_node, to_node)
            except CycleError:
                # Find the cycle path
                # The cycle goes: to_node -> ... -> from_node -> to_node
                try:
                    path_nodes = mutated.shortest_path(to_node, from_node)
                    if path_nodes:
                        cycle_path = [n.name for n in path_nodes] + [to_node]
                    else:
                        cycle_path = [from_node, to_node, from_node]
                except Exception:
                    cycle_path = [from_node, to_node, from_node]

                return WhatIfResult(
                    would_create_cycle=True,
                    cycle_path=cycle_path,
                )

    # Add new nodes
    if add_nodes:
        for name in add_nodes:
            if not mutated.has_node(name):
                mutated.add_node(name)

    # Remove edges
    if remove_edges:
        for from_node, to_node in remove_edges:
            if mutated.has_edge(from_node, to_node):
                mutated.remove_edge(from_node, to_node)

    # Remove nodes
    if remove_nodes:
        for name in remove_nodes:
            if mutated.has_node(name):
                mutated.remove_node(name)

    # Analyze the result
    new_stats = mutated.stats()

    # Find orphaned nodes: nodes that were NOT roots in the original DAG
    # but became roots (no predecessors) in the mutated DAG.
    orphaned: list[str] = []
    if remove_nodes or remove_edges:
        original_roots = {n.name for n in dag.roots()}
        removed_set = set(remove_nodes) if remove_nodes else set()
        for node in mutated.topological_sort():
            name = node.name
            if name in removed_set:
                continue
            # Node is orphaned if it's now a root but wasn't before
            if mutated.in_degree(name) == 0 and name not in original_roots:
                orphaned.append(name)

    # New critical path
    new_cp_nodes: list[str] = []
    new_cp_cost = 0.0
    if mutated.node_count() > 0:
        try:
            cp_nodes, cp_cost = mutated.critical_path(costs)
            new_cp_nodes = [n.name for n in cp_nodes]
            new_cp_cost = cp_cost
        except Exception:
            pass

    parallelism_change = new_stats.width - original_max_parallelism

    return WhatIfResult(
        orphaned_nodes=sorted(orphaned),
        parallelism_change=parallelism_change,
        new_critical_path=new_cp_nodes,
        new_critical_path_cost=new_cp_cost,
        would_create_cycle=False,
        new_root_count=new_stats.root_count,
        new_leaf_count=new_stats.leaf_count,
        new_node_count=new_stats.node_count,
        new_edge_count=new_stats.edge_count,
    )
