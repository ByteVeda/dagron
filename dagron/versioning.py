"""DAG time-travel and structural versioning."""

from __future__ import annotations

import time
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from dagron._internal import DAG, GraphDiff


class MutationType(Enum):
    """Type of DAG mutation."""

    ADD_NODE = "add_node"
    REMOVE_NODE = "remove_node"
    ADD_EDGE = "add_edge"
    REMOVE_EDGE = "remove_edge"
    SET_PAYLOAD = "set_payload"
    SET_METADATA = "set_metadata"


@dataclass(frozen=True)
class Mutation:
    """A single recorded mutation."""

    version: int
    mutation_type: MutationType
    args: dict[str, Any]
    timestamp: float


class VersionedDAG:
    """DAG with full structural versioning and time-travel.

    Every mutation is recorded in an append-only log. You can navigate
    to any historical version, diff between versions, and fork from
    any point.

    Example::

        vdag = VersionedDAG()
        vdag.add_node("a")
        vdag.add_node("b")
        vdag.add_edge("a", "b")
        vdag.version  # 3

        old = vdag.at_version(1)  # DAG with just node "a"
        diff = vdag.diff_versions(1, 3)  # shows added node "b" and edge
        log = vdag.history()  # list of mutations

    Args:
        dag: Optional existing DAG to wrap. If None, starts empty.
    """

    def __init__(self, dag: DAG | None = None, compact_threshold: int | None = None) -> None:
        from dagron._internal import DAG as DagClass

        self._dag = dag if dag is not None else DagClass()
        self._log: list[Mutation] = []
        self._version = 0
        self._snapshots: dict[int, DAG] = {}
        self._base_version: int = 0
        self.compact_threshold = compact_threshold

    @property
    def dag(self) -> DAG:
        """The current DAG object (read-only access recommended)."""
        return self._dag

    @property
    def version(self) -> int:
        """Current version number."""
        return self._version

    def _record(self, mutation_type: MutationType, **kwargs: Any) -> None:
        self._version += 1
        self._log.append(
            Mutation(
                version=self._version,
                mutation_type=mutation_type,
                args=kwargs,
                timestamp=time.time(),
            )
        )
        if self.compact_threshold is not None and len(self._log) >= self.compact_threshold:
            self.compact()

    def add_node(
        self,
        name: str,
        payload: Any = None,
        metadata: Any = None,
    ) -> None:
        """Add a node and record the mutation."""
        self._dag.add_node(name, payload=payload, metadata=metadata)
        self._record(MutationType.ADD_NODE, name=name, payload=payload, metadata=metadata)

    def remove_node(self, name: str) -> None:
        """Remove a node and record the mutation."""
        self._dag.remove_node(name)
        self._record(MutationType.REMOVE_NODE, name=name)

    def add_edge(
        self,
        from_node: str,
        to_node: str,
        weight: float | None = None,
        label: str | None = None,
    ) -> None:
        """Add an edge and record the mutation."""
        self._dag.add_edge(from_node, to_node, weight=weight, label=label)
        self._record(
            MutationType.ADD_EDGE,
            from_node=from_node,
            to_node=to_node,
            weight=weight,
            label=label,
        )

    def remove_edge(self, from_node: str, to_node: str) -> None:
        """Remove an edge and record the mutation."""
        self._dag.remove_edge(from_node, to_node)
        self._record(MutationType.REMOVE_EDGE, from_node=from_node, to_node=to_node)

    def set_payload(self, name: str, payload: Any) -> None:
        """Set a node's payload and record the mutation."""
        self._dag.set_payload(name, payload)
        self._record(MutationType.SET_PAYLOAD, name=name, payload=payload)

    def set_metadata(self, name: str, metadata: Any) -> None:
        """Set a node's metadata and record the mutation."""
        self._dag.set_metadata(name, metadata)
        self._record(MutationType.SET_METADATA, name=name, metadata=metadata)

    def at_version(self, version: int) -> DAG:
        """Reconstruct the DAG at a specific version.

        Args:
            version: The version number to reconstruct (1-based).

        Returns:
            A new DAG representing the state at that version.

        Raises:
            ValueError: If version is out of range or before compaction point
                with no covering snapshot.
        """
        if version < 0 or version > self._version:
            raise ValueError(f"Version {version} out of range [0, {self._version}].")
        # Check if version is before base and no snapshot covers it
        if version < self._base_version and version not in self._snapshots:
            # Check if any snapshot <= version exists
            covering = [v for v in self._snapshots if v <= version]
            if not covering and version > 0:
                raise ValueError(
                    f"Version {version} is before compaction point {self._base_version} "
                    f"and no snapshot covers it."
                )
        return self._replay(version)

    def _replay(self, up_to_version: int) -> DAG:
        """Replay mutations up to a given version, using snapshots when available."""
        return self._replay_from_nearest(up_to_version)

    def _replay_from_nearest(self, up_to_version: int) -> DAG:
        """Find the nearest snapshot <= up_to_version and replay from there."""
        from dagron._internal import DAG as DagClass

        # Find the best snapshot to start from
        best_snap_version = None
        for snap_v in self._snapshots:
            if snap_v <= up_to_version and (
                best_snap_version is None or snap_v > best_snap_version
            ):
                best_snap_version = snap_v

        if best_snap_version is not None:
            dag = self._snapshots[best_snap_version].snapshot()
            start_version = best_snap_version
        else:
            dag = DagClass()
            start_version = 0

        # Replay mutations from start_version to up_to_version
        for mutation in self._log:
            if mutation.version <= start_version:
                continue
            if mutation.version > up_to_version:
                break
            _apply_mutation(dag, mutation)
        return dag

    def compact(self, at_version: int | None = None) -> None:
        """Compact the mutation log by snapshotting at the target version.

        Args:
            at_version: Version to compact at. None means current version.
        """
        if at_version is None:
            at_version = self._version
        if at_version < 0 or at_version > self._version:
            raise ValueError(f"Version {at_version} out of range [0, {self._version}].")

        # Replay to target version and store snapshot
        snapshot = self._replay_from_nearest(at_version)
        self._snapshots[at_version] = snapshot

        # Truncate log: only keep mutations after at_version
        self._log = [m for m in self._log if m.version > at_version]
        self._base_version = at_version

    def diff_versions(self, version_a: int, version_b: int) -> GraphDiff:
        """Diff two versions of the DAG.

        Args:
            version_a: First version number.
            version_b: Second version number.

        Returns:
            GraphDiff showing structural differences.
        """
        dag_a = self.at_version(version_a)
        dag_b = self.at_version(version_b)
        return dag_a.diff(dag_b)

    def history(self) -> list[Mutation]:
        """Get the full mutation history.

        Returns:
            List of all recorded mutations in order.
        """
        return list(self._log)

    def history_since(self, version: int) -> list[Mutation]:
        """Get mutations since a specific version.

        Args:
            version: Starting version (exclusive).

        Returns:
            List of mutations after the given version.
        """
        return [m for m in self._log if m.version > version]

    def fork(self, at_version: int | None = None) -> VersionedDAG:
        """Create an independent fork of this versioned DAG.

        Args:
            at_version: Version to fork from. None means current version.

        Returns:
            A new VersionedDAG forked from the specified version.
        """
        if at_version is None:
            at_version = self._version
        dag = self.at_version(at_version)
        forked = VersionedDAG(dag)
        # Copy log entries up to fork point (only those still in our log)
        forked._log = [m for m in self._log if m.version <= at_version]
        forked._version = at_version
        # Copy relevant snapshots (<= fork version)
        forked._snapshots = {
            v: s.snapshot() for v, s in self._snapshots.items() if v <= at_version
        }
        forked._base_version = min(self._base_version, at_version)
        return forked


def _apply_mutation(dag: DAG, mutation: Mutation) -> None:
    """Apply a single mutation to a DAG."""
    args = mutation.args
    if mutation.mutation_type == MutationType.ADD_NODE:
        dag.add_node(args["name"], payload=args.get("payload"), metadata=args.get("metadata"))
    elif mutation.mutation_type == MutationType.REMOVE_NODE:
        dag.remove_node(args["name"])
    elif mutation.mutation_type == MutationType.ADD_EDGE:
        dag.add_edge(
            args["from_node"],
            args["to_node"],
            weight=args.get("weight"),
            label=args.get("label"),
        )
    elif mutation.mutation_type == MutationType.REMOVE_EDGE:
        dag.remove_edge(args["from_node"], args["to_node"])
    elif mutation.mutation_type == MutationType.SET_PAYLOAD:
        dag.set_payload(args["name"], args.get("payload"))
    elif mutation.mutation_type == MutationType.SET_METADATA:
        dag.set_metadata(args["name"], args.get("metadata"))
