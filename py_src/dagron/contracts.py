"""Typed data contracts for DAG edges — build-time type checking."""

from __future__ import annotations

import typing
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

    from dagron._internal import DAG


@dataclass(frozen=True)
class NodeContract:
    """Type contract for a single node's inputs and outputs.

    Args:
        inputs: Mapping of dependency name to expected type.
        output: The declared output type of this node.
    """

    inputs: dict[str, type] = field(default_factory=dict)
    output: type = object


@dataclass(frozen=True)
class ContractViolation:
    """A single type-contract violation detected at validation time."""

    from_node: str
    to_node: str
    message: str


class ContractValidator:
    """Validates type contracts across DAG edges.

    For every edge ``(u, v)`` in the DAG, the validator checks that the
    output type of ``u`` is compatible with the expected input type declared
    by ``v`` for dependency ``u``.  Compatibility is determined via
    ``issubclass``.  ``object`` acts as a wildcard (equivalent to ``Any``).

    Args:
        dag: The DAG to validate.
        contracts: Mapping of node names to their :class:`NodeContract`.
    """

    def __init__(self, dag: DAG, contracts: dict[str, NodeContract]) -> None:
        self._dag = dag
        self._contracts = contracts

    def validate(self) -> list[ContractViolation]:
        """Run validation and return all detected violations."""
        violations: list[ContractViolation] = []

        for node in self._dag.nodes():
            name = node.name
            contract = self._contracts.get(name)
            if contract is None:
                continue

            for dep_name, expected_type in contract.inputs.items():
                if expected_type is object:
                    continue

                dep_contract = self._contracts.get(dep_name)
                if dep_contract is None:
                    continue

                actual_type = dep_contract.output
                if actual_type is object:
                    continue

                if not _is_compatible(actual_type, expected_type):
                    violations.append(
                        ContractViolation(
                            from_node=dep_name,
                            to_node=name,
                            message=(
                                f"Type mismatch on edge {dep_name} -> {name}: "
                                f"producer outputs {actual_type.__name__}, "
                                f"but consumer expects {expected_type.__name__}"
                            ),
                        )
                    )

        return violations


def _is_compatible(actual: type, expected: type) -> bool:
    """Check if *actual* is a subclass of *expected*.

    Falls back to ``True`` if ``issubclass`` raises ``TypeError``
    (e.g. for generic aliases).
    """
    try:
        return issubclass(actual, expected)
    except TypeError:
        return True


def extract_contracts(
    pipeline: Any,
) -> dict[str, NodeContract]:
    """Auto-extract :class:`NodeContract` instances from a Pipeline's ``@task`` functions.

    Uses :func:`typing.get_type_hints` to read input parameter types and
    return annotations from each decorated function.

    Args:
        pipeline: A :class:`dagron.Pipeline` instance.

    Returns:
        Mapping of task names to their extracted contracts.
    """
    contracts: dict[str, NodeContract] = {}
    task_names = set(pipeline.task_names)

    for name in pipeline.task_names:
        spec = pipeline._specs[name]
        fn: Callable[..., Any] = spec.fn
        try:
            hints = typing.get_type_hints(fn)
        except Exception:
            hints = {}

        inputs: dict[str, type] = {}
        for dep in spec.dependencies:
            if dep in task_names and dep in hints:
                inputs[dep] = hints[dep]

        output = hints.get("return", object)
        contracts[name] = NodeContract(inputs=inputs, output=output)

    return contracts


def validate_contracts(
    pipeline: Any,
    extra_contracts: dict[str, NodeContract] | None = None,
) -> list[ContractViolation]:
    """Convenience: extract contracts from a pipeline and validate them.

    Args:
        pipeline: A :class:`dagron.Pipeline` instance.
        extra_contracts: Optional manually-specified contracts that override
            the auto-extracted ones.

    Returns:
        List of :class:`ContractViolation` instances (empty if all valid).
    """
    contracts = extract_contracts(pipeline)
    if extra_contracts:
        contracts.update(extra_contracts)
    validator = ContractValidator(pipeline.dag, contracts)
    return validator.validate()
