"""Post-execution profiling and analysis for DAG task execution."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from dagron._internal import DAG
    from dagron.executor import ExecutionResult


@dataclass
class NodeProfile:
    """Profile data for a single node."""

    name: str
    duration: float
    earliest_start: float
    latest_start: float
    slack: float
    on_critical_path: bool
    blocked_descendants: int


@dataclass
class ProfileReport:
    """Complete profiling report for an execution."""

    node_profiles: dict[str, NodeProfile] = field(default_factory=dict)
    critical_path: list[str] = field(default_factory=list)
    critical_path_duration: float = 0.0
    bottlenecks: list[str] = field(default_factory=list)
    parallelism_efficiency: float = 0.0
    actual_max_parallelism: int = 0

    def summary(self) -> str:
        """Return a human-readable summary of the profile."""
        lines = [
            "Profile Report",
            f"  Nodes profiled: {len(self.node_profiles)}",
            f"  Critical path: {' -> '.join(self.critical_path)}",
            f"  Critical path duration: {self.critical_path_duration:.4f}s",
            f"  Parallelism efficiency: {self.parallelism_efficiency:.2f}",
            f"  Max parallelism: {self.actual_max_parallelism}",
            f"  Bottlenecks: {', '.join(self.bottlenecks[:5])}",
        ]
        return "\n".join(lines)

    def to_dict(self) -> dict[str, Any]:
        """Convert report to a dictionary."""
        return {
            "node_profiles": {
                name: {
                    "duration": p.duration,
                    "earliest_start": p.earliest_start,
                    "latest_start": p.latest_start,
                    "slack": p.slack,
                    "on_critical_path": p.on_critical_path,
                    "blocked_descendants": p.blocked_descendants,
                }
                for name, p in self.node_profiles.items()
            },
            "critical_path": self.critical_path,
            "critical_path_duration": self.critical_path_duration,
            "bottlenecks": self.bottlenecks,
            "parallelism_efficiency": self.parallelism_efficiency,
            "actual_max_parallelism": self.actual_max_parallelism,
        }


def profile_execution(dag: DAG, result: ExecutionResult) -> ProfileReport:
    """Analyze an execution result against the DAG structure.

    Computes critical path from actual timings, slack analysis,
    bottleneck detection, and parallelism efficiency.

    Args:
        dag: The DAG that was executed.
        result: The execution result with per-node timings.

    Returns:
        A ProfileReport with detailed analysis.
    """
    from dagron.executor import NodeStatus

    # Extract durations from completed nodes
    durations: dict[str, float] = {}
    for name, nr in result.node_results.items():
        if nr.status == NodeStatus.COMPLETED:
            durations[name] = nr.duration_seconds

    if not durations:
        return ProfileReport()

    # Get topological order
    topo_order = [n.name for n in dag.topological_sort()]
    # Filter to only nodes we have durations for
    topo_order = [n for n in topo_order if n in durations]

    if not topo_order:
        return ProfileReport()

    # Build predecessor/successor maps
    pred_map: dict[str, list[str]] = {}
    succ_map: dict[str, list[str]] = {}
    for name in topo_order:
        pred_map[name] = [n.name for n in dag.predecessors(name) if n.name in durations]
        succ_map[name] = [n.name for n in dag.successors(name) if n.name in durations]

    # Forward pass: earliest start times
    earliest_start: dict[str, float] = {}
    for name in topo_order:
        if not pred_map[name]:
            earliest_start[name] = 0.0
        else:
            earliest_start[name] = max(
                earliest_start[p] + durations[p] for p in pred_map[name]
            )

    # Compute makespan
    makespan = max(earliest_start[n] + durations[n] for n in topo_order)

    # Backward pass: latest start times
    latest_start: dict[str, float] = {}
    for name in reversed(topo_order):
        if not succ_map[name]:
            latest_start[name] = makespan - durations[name]
        else:
            latest_start[name] = (
                min(latest_start[s] for s in succ_map[name]) - durations[name]
            )

    # Compute slack and identify critical path nodes
    slack: dict[str, float] = {}
    for name in topo_order:
        slack[name] = latest_start[name] - earliest_start[name]

    # Get actual critical path using dag.critical_path with real durations
    critical_path_nodes, critical_path_duration = dag.critical_path(durations)
    critical_path = [n.name for n in critical_path_nodes]

    critical_path_set = set(critical_path)

    # Count blocked descendants
    desc_counts: dict[str, int] = {}
    for name in topo_order:
        desc = dag.descendants(name)
        desc_counts[name] = len([d for d in desc if d.name in durations])

    # Build node profiles
    node_profiles: dict[str, NodeProfile] = {}
    for name in topo_order:
        node_profiles[name] = NodeProfile(
            name=name,
            duration=durations[name],
            earliest_start=earliest_start[name],
            latest_start=latest_start[name],
            slack=slack[name],
            on_critical_path=name in critical_path_set,
            blocked_descendants=desc_counts[name],
        )

    # Bottlenecks: sort by duration descending, weighted by descendants
    bottleneck_score = sorted(
        topo_order,
        key=lambda n: durations[n] * (1 + desc_counts[n]),
        reverse=True,
    )
    bottlenecks = bottleneck_score[:5]

    # Parallelism efficiency = sum(durations) / makespan
    total_work = sum(durations.values())
    parallelism_efficiency = total_work / makespan if makespan > 0 else 0.0

    # Actual max parallelism from topological levels
    levels = dag.topological_levels()
    actual_max_parallelism = (
        max(sum(1 for n in level if n.name in durations) for level in levels)
        if levels
        else 0
    )

    return ProfileReport(
        node_profiles=node_profiles,
        critical_path=critical_path,
        critical_path_duration=critical_path_duration,
        bottlenecks=bottlenecks,
        parallelism_efficiency=parallelism_efficiency,
        actual_max_parallelism=actual_max_parallelism,
    )
