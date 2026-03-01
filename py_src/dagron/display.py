"""Pretty-print / ASCII rendering for DAG visualization."""

from __future__ import annotations

import subprocess
from typing import TYPE_CHECKING, Any, Callable
from xml.sax.saxutils import escape

if TYPE_CHECKING:
    from dagron._internal import DAG


def pretty_print(
    dag: DAG,
    *,
    layout: str = "vertical",
    max_nodes: int = 50,
    show_payloads: bool = False,
    node_formatter: Callable[[str, Any], str] | None = None,
) -> str:
    """Render the DAG as an ASCII diagram.

    Args:
        dag: The DAG to render.
        layout: "vertical" (top-to-bottom) or "horizontal" (left-to-right).
        max_nodes: Maximum number of nodes before raising ValueError.
        show_payloads: Include payload info in node labels.
        node_formatter: Optional callable (name, payload) -> label string.

    Returns:
        A string containing the ASCII rendering.

    Raises:
        ValueError: If node_count exceeds max_nodes.
    """
    nc = dag.node_count()
    if nc > max_nodes:
        raise ValueError(
            f"Graph has {nc} nodes, exceeding max_nodes={max_nodes}. "
            f"Increase max_nodes to render."
        )

    if nc == 0:
        return "(empty graph)"

    levels = dag.topological_levels()
    edges = dag.edges()

    # Build node labels
    labels: dict[str, str] = {}
    for level in levels:
        for node in level:
            name = node.name
            if node_formatter is not None:
                payload = dag.get_payload(name)
                labels[name] = node_formatter(name, payload)
            elif show_payloads:
                payload = dag.get_payload(name)
                if payload is not None:
                    labels[name] = f"{name}={payload}"
                else:
                    labels[name] = name
            else:
                labels[name] = name

    if layout == "horizontal":
        return _render_horizontal(levels, edges, labels)
    else:
        return _render_vertical(levels, edges, labels)


def _render_vertical(levels, edges, labels) -> str:
    """Render DAG top-to-bottom with levels as rows."""
    # Build edge set for lookup
    edge_set = set(edges)

    # Get node positions: level_idx -> list of node names
    level_nodes = [[n.name for n in level] for level in levels]

    # Calculate column positions for each node
    # Find max box width
    box_padding = 2
    max_label = max((len(labels[n]) for level in level_nodes for n in level), default=0)
    box_width = max_label + box_padding * 2 + 2  # +2 for [ ]

    # Assign column indices
    node_col: dict[str, int] = {}
    max_cols = max(len(level) for level in level_nodes)
    total_width = max_cols * (box_width + 2)

    for level in level_nodes:
        n = len(level)
        # Center nodes in the row
        spacing = total_width // max(n, 1)
        for i, name in enumerate(level):
            node_col[name] = i * spacing + spacing // 2

    lines: list[str] = []

    for li, level in enumerate(level_nodes):
        # Render node boxes
        row = [" "] * (total_width + box_width)
        for name in level:
            col = node_col[name]
            label = labels[name]
            box_str = f"[ {label} ]"
            start = max(0, col - len(box_str) // 2)
            for ci, ch in enumerate(box_str):
                pos = start + ci
                if pos < len(row):
                    row[pos] = ch
        lines.append("".join(row).rstrip())

        # Render edge connectors to next level
        if li < len(level_nodes) - 1:
            next_level = level_nodes[li + 1]
            connector_row = [" "] * (total_width + box_width)

            for name in level:
                src_col = node_col[name]
                for target in next_level:
                    if (name, target) in edge_set:
                        tgt_col = node_col[target]
                        mid = (src_col + tgt_col) // 2
                        if src_col == tgt_col:
                            # Straight down
                            if mid < len(connector_row):
                                connector_row[mid] = "|"
                        else:
                            # Diagonal/horizontal connector
                            lo = min(src_col, tgt_col)
                            hi = max(src_col, tgt_col)
                            for ci in range(lo, min(hi + 1, len(connector_row))):
                                if connector_row[ci] == " ":
                                    connector_row[ci] = "-"
                            if lo < len(connector_row):
                                connector_row[lo] = "+"
                            if hi < len(connector_row):
                                connector_row[hi] = "+"

            lines.append("".join(connector_row).rstrip())

    return "\n".join(lines)


def _render_horizontal(levels, edges, labels) -> str:
    """Render DAG left-to-right with levels as columns."""
    edge_set = set(edges)
    level_nodes = [[n.name for n in level] for level in levels]

    # Column width per level
    col_widths = []
    for level in level_nodes:
        max_w = max(len(labels[n]) for n in level) if level else 0
        col_widths.append(max_w + 6)  # padding for "[ label ]"

    # Number of rows = max nodes in any level
    max_rows = max(len(level) for level in level_nodes)

    # Build node positions: (level_idx, row_idx) -> node name
    node_pos: dict[str, tuple[int, int]] = {}
    for li, level in enumerate(level_nodes):
        for ri, name in enumerate(level):
            node_pos[name] = (li, ri)

    # Build the grid
    lines: list[str] = []
    for row in range(max_rows):
        parts = []
        for li, level in enumerate(level_nodes):
            col_w = col_widths[li]
            if row < len(level):
                name = level[row]
                label = labels[name]
                box_str = f"[ {label} ]"
                # Check if there's an edge to something in the next level
                has_right_edge = False
                if li < len(level_nodes) - 1:
                    for target_name in level_nodes[li + 1]:
                        if (name, target_name) in edge_set:
                            has_right_edge = True
                            break

                if has_right_edge:
                    padding = col_w - len(box_str)
                    parts.append(box_str + "-" * max(padding, 1) + ">")
                else:
                    parts.append(box_str.ljust(col_w + 2))
            else:
                parts.append(" " * (col_w + 2))
        lines.append("".join(parts).rstrip())

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Jupyter SVG repr
# ---------------------------------------------------------------------------


def _repr_svg_(dag: DAG, *, max_nodes: int = 100) -> str:
    """Return an SVG representation of the DAG for Jupyter notebooks.

    Strategy:
    1. Empty graph -> simple SVG with "(empty graph)" text.
    2. Too many nodes -> summary SVG.
    3. Try ``graphviz`` Python package -> ``Source(dot).pipe(format='svg')``.
    4. Fallback: try ``dot`` CLI via subprocess.
    5. Final fallback: wrap ASCII pretty-print in an SVG ``<text>`` element.
    """
    nc = dag.node_count()

    if nc == 0:
        return _svg_text("(empty graph)")

    if nc > max_nodes:
        ec = dag.edge_count()
        return _svg_text(f"DAG(nodes={nc}, edges={ec}) — too large to render")

    dot_str = dag.to_dot()

    # Try graphviz Python package
    try:
        import graphviz

        src = graphviz.Source(dot_str)
        return src.pipe(format="svg", encoding="utf-8")
    except Exception:
        pass

    # Try dot CLI
    try:
        result = subprocess.run(
            ["dot", "-Tsvg"],
            input=dot_str,
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout
    except Exception:
        pass

    # Final fallback: ASCII in SVG
    try:
        ascii_art = pretty_print(dag, max_nodes=max_nodes)
    except (ValueError, Exception):
        ascii_art = f"DAG(nodes={nc}, edges={dag.edge_count()})"
    return _svg_text(ascii_art)


def _svg_text(text: str) -> str:
    """Wrap plain text in a minimal SVG element."""
    lines = text.split("\n")
    line_height = 16
    height = max(len(lines) * line_height + 20, 40)
    max_len = max((len(line) for line in lines), default=0)
    width = max(max_len * 8 + 20, 200)

    text_elements = []
    for i, line in enumerate(lines):
        y = 20 + i * line_height
        text_elements.append(
            f'  <text x="10" y="{y}" '
            f'font-family="monospace" font-size="13" fill="#333">'
            f"{escape(line)}</text>"
        )

    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" '
        f'width="{width}" height="{height}">\n'
        + "\n".join(text_elements)
        + "\n</svg>"
    )
