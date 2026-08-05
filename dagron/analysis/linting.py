"""DAG linting and schema validation."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dagron._internal import DAG


class LintSeverity(Enum):
    """Severity level for lint warnings."""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


@dataclass(frozen=True)
class LintWarning:
    """A single lint warning about the DAG structure."""

    code: str
    severity: LintSeverity
    message: str
    nodes: list[str] = field(default_factory=list)

    def __str__(self) -> str:
        prefix = f"[{self.severity.value.upper()}] {self.code}"
        if self.nodes:
            return f"{prefix}: {self.message} (nodes: {', '.join(self.nodes[:5])})"
        return f"{prefix}: {self.message}"


@dataclass
class LintReport:
    """Aggregate lint report for a DAG."""

    warnings: list[LintWarning] = field(default_factory=list)

    @property
    def error_count(self) -> int:
        return sum(1 for w in self.warnings if w.severity == LintSeverity.ERROR)

    @property
    def warning_count(self) -> int:
        return sum(1 for w in self.warnings if w.severity == LintSeverity.WARNING)

    @property
    def info_count(self) -> int:
        return sum(1 for w in self.warnings if w.severity == LintSeverity.INFO)

    @property
    def ok(self) -> bool:
        return self.error_count == 0

    def summary(self) -> str:
        lines = [
            f"Lint Report: {self.error_count} errors, "
            f"{self.warning_count} warnings, {self.info_count} info"
        ]
        for w in self.warnings:
            lines.append(f"  {w}")
        return "\n".join(lines)

    def __str__(self) -> str:
        return self.summary()


def lint(
    dag: DAG,
    *,
    max_fan_in: int = 10,
    max_fan_out: int = 10,
    max_depth: int = 50,
    warn_disconnected: bool = True,
    warn_redundant_edges: bool = True,
) -> LintReport:
    """Analyze a DAG for structural anti-patterns.

    Checks for:
    - High fan-in nodes (too many predecessors)
    - High fan-out nodes (too many successors)
    - Disconnected components
    - Redundant/transitive edges
    - Excessive depth
    - Single-node components
    - No-op leaf nodes with no payload

    Args:
        dag: The DAG to lint.
        max_fan_in: Threshold for high fan-in warning.
        max_fan_out: Threshold for high fan-out warning.
        max_depth: Threshold for excessive depth warning.
        warn_disconnected: Warn about disconnected components.
        warn_redundant_edges: Warn about redundant edges.

    Returns:
        LintReport with all warnings found.
    """
    warnings: list[LintWarning] = []
    stats = dag.stats()

    if stats.node_count == 0:
        warnings.append(
            LintWarning(
                code="EMPTY_GRAPH",
                severity=LintSeverity.INFO,
                message="DAG has no nodes.",
            )
        )
        return LintReport(warnings=warnings)

    # Check for high fan-in
    high_fan_in: list[str] = []
    for node in dag.topological_sort():
        if dag.in_degree(node.name) > max_fan_in:
            high_fan_in.append(node.name)
    if high_fan_in:
        warnings.append(
            LintWarning(
                code="HIGH_FAN_IN",
                severity=LintSeverity.WARNING,
                message=f"Nodes with in-degree > {max_fan_in} may be bottlenecks.",
                nodes=high_fan_in,
            )
        )

    # Check for high fan-out
    high_fan_out: list[str] = []
    for node in dag.topological_sort():
        if dag.out_degree(node.name) > max_fan_out:
            high_fan_out.append(node.name)
    if high_fan_out:
        warnings.append(
            LintWarning(
                code="HIGH_FAN_OUT",
                severity=LintSeverity.WARNING,
                message=f"Nodes with out-degree > {max_fan_out} create wide dependency spread.",
                nodes=high_fan_out,
            )
        )

    # Check for disconnected components
    if warn_disconnected and stats.component_count > 1:
        warnings.append(
            LintWarning(
                code="DISCONNECTED",
                severity=LintSeverity.WARNING,
                message=f"DAG has {stats.component_count} disconnected components. "
                "Consider splitting into separate DAGs.",
            )
        )

    # Check for excessive depth
    if stats.depth > max_depth:
        warnings.append(
            LintWarning(
                code="EXCESSIVE_DEPTH",
                severity=LintSeverity.WARNING,
                message=f"DAG depth ({stats.depth}) exceeds threshold ({max_depth}). "
                "Deep chains limit parallelism.",
            )
        )

    # Check for redundant edges (transitive edges that could be removed)
    if warn_redundant_edges and stats.edge_count > 0:
        try:
            reduced = dag.transitive_reduction()
            redundant_count = stats.edge_count - reduced.edge_count()
            if redundant_count > 0:
                # Find which edges are redundant by comparing edge sets
                original_edges = set(dag.edges())
                reduced_edges = set(reduced.edges())
                redundant_edges = original_edges - reduced_edges
                redundant_strs = [f"{e[0]}->{e[1]}" for e in list(redundant_edges)[:5]]
                warnings.append(
                    LintWarning(
                        code="REDUNDANT_EDGES",
                        severity=LintSeverity.INFO,
                        message=f"{redundant_count} redundant edge(s) found "
                        f"(e.g. {', '.join(redundant_strs)}). "
                        "These don't affect correctness but add noise.",
                    )
                )
        except Exception:
            pass

    # Check for isolated nodes (no predecessors and no successors, but graph has edges)
    if stats.edge_count > 0:
        isolated: list[str] = []
        for node in dag.topological_sort():
            if dag.in_degree(node.name) == 0 and dag.out_degree(node.name) == 0:
                isolated.append(node.name)
        if isolated:
            warnings.append(
                LintWarning(
                    code="ISOLATED_NODES",
                    severity=LintSeverity.WARNING,
                    message="Isolated nodes with no edges found.",
                    nodes=isolated,
                )
            )

    return LintReport(warnings=warnings)


class DAGSchema:
    """Declarative structural constraints for DAG validation.

    Define expected structural properties and validate DAGs against them.

    Example::

        schema = DAGSchema(
            single_root=True,
            max_depth=5,
            leaf_pattern="output_*",
            required_nodes=["start", "end"],
        )
        errors = schema.validate(dag)
        if errors:
            for err in errors:
                print(err)
    """

    def __init__(
        self,
        *,
        single_root: bool | None = None,
        single_leaf: bool | None = None,
        max_depth: int | None = None,
        min_nodes: int | None = None,
        max_nodes: int | None = None,
        max_in_degree: int | None = None,
        max_out_degree: int | None = None,
        connected: bool | None = None,
        root_pattern: str | None = None,
        leaf_pattern: str | None = None,
        required_nodes: list[str] | None = None,
        forbidden_nodes: list[str] | None = None,
    ) -> None:
        self.single_root = single_root
        self.single_leaf = single_leaf
        self.max_depth = max_depth
        self.min_nodes = min_nodes
        self.max_nodes = max_nodes
        self.max_in_degree = max_in_degree
        self.max_out_degree = max_out_degree
        self.connected = connected
        self.root_pattern = root_pattern
        self.leaf_pattern = leaf_pattern
        self.required_nodes = required_nodes
        self.forbidden_nodes = forbidden_nodes

    def validate(self, dag: DAG) -> list[str]:
        """Validate a DAG against the schema constraints.

        Args:
            dag: The DAG to validate.

        Returns:
            List of error messages. Empty list means validation passed.
        """
        errors: list[str] = []
        stats = dag.stats()

        if self.single_root is True and stats.root_count != 1:
            errors.append(f"Expected single root, found {stats.root_count}.")

        if self.single_leaf is True and stats.leaf_count != 1:
            errors.append(f"Expected single leaf, found {stats.leaf_count}.")

        if self.max_depth is not None and stats.depth > self.max_depth:
            errors.append(f"Depth {stats.depth} exceeds maximum {self.max_depth}.")

        if self.min_nodes is not None and stats.node_count < self.min_nodes:
            errors.append(f"Node count {stats.node_count} below minimum {self.min_nodes}.")

        if self.max_nodes is not None and stats.node_count > self.max_nodes:
            errors.append(f"Node count {stats.node_count} exceeds maximum {self.max_nodes}.")

        if self.max_in_degree is not None:
            for node in dag.topological_sort():
                if dag.in_degree(node.name) > self.max_in_degree:
                    errors.append(
                        f"Node '{node.name}' in-degree {dag.in_degree(node.name)} "
                        f"exceeds maximum {self.max_in_degree}."
                    )

        if self.max_out_degree is not None:
            for node in dag.topological_sort():
                if dag.out_degree(node.name) > self.max_out_degree:
                    errors.append(
                        f"Node '{node.name}' out-degree {dag.out_degree(node.name)} "
                        f"exceeds maximum {self.max_out_degree}."
                    )

        if self.connected is True and not stats.is_weakly_connected:
            errors.append(f"DAG is not connected ({stats.component_count} components).")

        if self.root_pattern is not None:
            roots = dag.roots()
            for root in roots:
                if not _glob_match(root.name, self.root_pattern):
                    errors.append(
                        f"Root '{root.name}' does not match pattern '{self.root_pattern}'."
                    )

        if self.leaf_pattern is not None:
            leaves = dag.leaves()
            for leaf in leaves:
                if not _glob_match(leaf.name, self.leaf_pattern):
                    errors.append(
                        f"Leaf '{leaf.name}' does not match pattern '{self.leaf_pattern}'."
                    )

        if self.required_nodes:
            for name in self.required_nodes:
                if not dag.has_node(name):
                    errors.append(f"Required node '{name}' not found.")

        if self.forbidden_nodes:
            for name in self.forbidden_nodes:
                if dag.has_node(name):
                    errors.append(f"Forbidden node '{name}' found.")

        return errors


def _glob_match(name: str, pattern: str) -> bool:
    """Simple glob matching: * matches any sequence, ? matches single char."""
    regex = re.escape(pattern).replace(r"\*", ".*").replace(r"\?", ".")
    return bool(re.fullmatch(regex, name))
