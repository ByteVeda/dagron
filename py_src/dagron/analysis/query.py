"""Graph query language (mini-DSL) for selecting nodes."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dagron._internal import DAG


def query(dag: DAG, expr: str) -> list[str]:
    """Select nodes using a concise query expression.

    Supports the following syntax:

    **Set functions:**
    - ``roots`` — root nodes (no predecessors)
    - ``leaves`` — leaf nodes (no successors)
    - ``critical_path`` — nodes on the critical path
    - ``ancestors(node)`` — all ancestors of node
    - ``descendants(node)`` — all descendants of node
    - ``predecessors(node)`` — direct predecessors of node
    - ``successors(node)`` — direct successors of node

    **Filters:**
    - ``depth <= N``, ``depth >= N``, ``depth == N`` — filter by topological depth
    - ``in_degree <= N``, ``out_degree >= N`` — filter by degree
    - ``name:pattern`` — glob pattern matching on node names

    **Set operations:**
    - ``A & B`` — intersection
    - ``A | B`` — union
    - ``A - B`` — difference

    Examples::

        dag.query("ancestors(deploy) & depth <= 3")
        dag.query("critical_path | roots")
        dag.query("name:test_*")
        dag.query("descendants(extract) - leaves")
        dag.query("roots & name:input_*")

    Args:
        dag: The DAG to query.
        expr: Query expression string.

    Returns:
        List of matching node names.
    """
    all_nodes = {n.name for n in dag.topological_sort()}
    result = _eval_expr(dag, expr.strip(), all_nodes)
    # Return in topological order
    topo_order = [n.name for n in dag.topological_sort()]
    return [n for n in topo_order if n in result]


def _eval_expr(dag: DAG, expr: str, all_nodes: set[str]) -> set[str]:
    """Evaluate a query expression recursively."""
    expr = expr.strip()

    # Handle set operations (lowest precedence: |, then -, then &)
    # Split by | first (lowest precedence)
    parts = _split_operator(expr, "|")
    if len(parts) > 1:
        result: set[str] = set()
        for part in parts:
            result |= _eval_expr(dag, part, all_nodes)
        return result

    # Split by -
    parts = _split_operator(expr, "-")
    if len(parts) > 1:
        result = _eval_expr(dag, parts[0], all_nodes)
        for part in parts[1:]:
            result -= _eval_expr(dag, part, all_nodes)
        return result

    # Split by &
    parts = _split_operator(expr, "&")
    if len(parts) > 1:
        result = _eval_expr(dag, parts[0], all_nodes)
        for part in parts[1:]:
            result &= _eval_expr(dag, part, all_nodes)
        return result

    # Handle parenthesized expressions
    if expr.startswith("(") and expr.endswith(")"):
        return _eval_expr(dag, expr[1:-1], all_nodes)

    # Handle set functions
    if expr == "roots":
        return {n.name for n in dag.roots()}

    if expr == "leaves":
        return {n.name for n in dag.leaves()}

    if expr == "critical_path":
        cp_nodes, _ = dag.critical_path()
        return {n.name for n in cp_nodes}

    # ancestors(node)
    m = re.match(r"ancestors\((\w+)\)", expr)
    if m:
        node_name = m.group(1)
        return {n.name for n in dag.ancestors(node_name)}

    # descendants(node)
    m = re.match(r"descendants\((\w+)\)", expr)
    if m:
        node_name = m.group(1)
        return {n.name for n in dag.descendants(node_name)}

    # predecessors(node)
    m = re.match(r"predecessors\((\w+)\)", expr)
    if m:
        node_name = m.group(1)
        return {n.name for n in dag.predecessors(node_name)}

    # successors(node)
    m = re.match(r"successors\((\w+)\)", expr)
    if m:
        node_name = m.group(1)
        return {n.name for n in dag.successors(node_name)}

    # depth comparisons
    m = re.match(r"depth\s*(<=|>=|==|<|>)\s*(\d+)", expr)
    if m:
        op = m.group(1)
        threshold = int(m.group(2))
        return _filter_by_depth(dag, op, threshold, all_nodes)

    # in_degree comparisons
    m = re.match(r"in_degree\s*(<=|>=|==|<|>)\s*(\d+)", expr)
    if m:
        op = m.group(1)
        threshold = int(m.group(2))
        return _filter_by_degree(dag, "in", op, threshold, all_nodes)

    # out_degree comparisons
    m = re.match(r"out_degree\s*(<=|>=|==|<|>)\s*(\d+)", expr)
    if m:
        op = m.group(1)
        threshold = int(m.group(2))
        return _filter_by_degree(dag, "out", op, threshold, all_nodes)

    # name:pattern (glob matching)
    m = re.match(r"name:(.+)", expr)
    if m:
        pattern = m.group(1).strip()
        return _filter_by_name(dag, pattern, all_nodes)

    # Single node name
    if expr in all_nodes:
        return {expr}

    raise ValueError(f"Unknown query expression: {expr!r}")


def _split_operator(expr: str, op: str) -> list[str]:
    """Split expression by operator, respecting parentheses."""
    parts: list[str] = []
    depth = 0
    current: list[str] = []

    i = 0
    while i < len(expr):
        ch = expr[i]
        if ch == "(":
            depth += 1
            current.append(ch)
        elif ch == ")":
            depth -= 1
            current.append(ch)
        elif ch == op and depth == 0:
            parts.append("".join(current))
            current = []
        else:
            current.append(ch)
        i += 1

    parts.append("".join(current))

    # Only consider it a valid split if we have multiple non-empty parts
    non_empty = [p.strip() for p in parts if p.strip()]
    if len(non_empty) > 1:
        return non_empty
    return [expr]


def _filter_by_depth(
    dag: DAG, op: str, threshold: int, all_nodes: set[str]
) -> set[str]:
    """Filter nodes by their topological depth."""
    levels = dag.topological_levels()
    result: set[str] = set()
    for depth, level in enumerate(levels):
        if _compare(depth, op, threshold):
            for node in level:
                if node.name in all_nodes:
                    result.add(node.name)
    return result


def _filter_by_degree(
    dag: DAG, kind: str, op: str, threshold: int, all_nodes: set[str]
) -> set[str]:
    """Filter nodes by in-degree or out-degree."""
    result: set[str] = set()
    for name in all_nodes:
        deg = dag.in_degree(name) if kind == "in" else dag.out_degree(name)
        if _compare(deg, op, threshold):
            result.add(name)
    return result


def _filter_by_name(dag: DAG, pattern: str, all_nodes: set[str]) -> set[str]:
    """Filter nodes by glob pattern on name."""
    regex = re.escape(pattern).replace(r"\*", ".*").replace(r"\?", ".")
    compiled = re.compile(f"^{regex}$")
    return {name for name in all_nodes if compiled.match(name)}


def _compare(value: int, op: str, threshold: int) -> bool:
    if op == "<=":
        return value <= threshold
    if op == ">=":
        return value >= threshold
    if op == "==":
        return value == threshold
    if op == "<":
        return value < threshold
    if op == ">":
        return value > threshold
    return False
