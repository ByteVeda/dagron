"""Type stubs for dagron.builder."""

from typing import Any, Optional

from dagron._internal import DAG

class DAGBuilder:
    """Fluent builder for constructing DAGs."""

    def __init__(self) -> None: ...
    def add_node(
        self,
        name: str,
        payload: Any = None,
        metadata: Any = None,
    ) -> DAGBuilder: ...
    def add_edge(
        self,
        from_node: str,
        to_node: str,
        weight: Optional[float] = None,
        label: Optional[str] = None,
    ) -> DAGBuilder: ...
    def build(self) -> DAG: ...
