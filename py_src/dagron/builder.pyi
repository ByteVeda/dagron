"""Type stubs for dagron.builder."""

from typing import Any

from dagron._internal import DAG
from dagron.contracts import ContractViolation

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
        weight: float | None = None,
        label: str | None = None,
    ) -> DAGBuilder: ...
    def contract(
        self,
        node: str,
        *,
        inputs: dict[str, type] | None = None,
        output: type = ...,
    ) -> DAGBuilder: ...
    def validate_contracts(self) -> list[ContractViolation]: ...
    def build(self) -> DAG: ...
