"""Typed data contracts for DAG edges — build-time type checking."""

from __future__ import annotations

import types
import typing
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Union

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

    inputs: dict[str, Any] = field(default_factory=dict)
    output: Any = object


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
                                f"producer outputs {_type_name(actual_type)}, "
                                f"but consumer expects {_type_name(expected_type)}"
                            ),
                        )
                    )

        return violations


def _type_name(t: Any) -> str:
    """Human-readable name for a type, including generic aliases."""
    return getattr(t, "__name__", str(t))


def _is_compatible(actual: Any, expected: Any) -> bool:
    """Check if *actual* type is compatible with *expected* type.

    Handles parameterized generics (e.g. list[int] vs list[str]),
    Union/Optional, bare generics, and plain types.
    """
    # object wildcard
    if actual is object or expected is object:
        return True

    actual_origin = typing.get_origin(actual)
    expected_origin = typing.get_origin(expected)
    actual_args = typing.get_args(actual)
    expected_args = typing.get_args(expected)

    # Handle Union on expected side: actual must match at least one member
    if expected_origin is Union or expected_origin is types.UnionType:
        return any(_is_compatible(actual, member) for member in expected_args)

    # Handle Union on actual side: all members must be compatible with expected
    if actual_origin is Union or actual_origin is types.UnionType:
        return all(_is_compatible(member, expected) for member in actual_args)

    # Both are parameterized generics
    if actual_origin is not None and expected_origin is not None:
        # Check origins are compatible
        if not _is_compatible_plain(actual_origin, expected_origin):
            return False
        # If both have args, check pairwise
        if actual_args and expected_args:
            if len(actual_args) != len(expected_args):
                return False
            return all(
                _is_compatible(a, e) for a, e in zip(actual_args, expected_args, strict=True)
            )
        return True

    # One bare generic, one parameterized: permissive (e.g. list vs list[int])
    if (actual_origin is not None) != (expected_origin is not None):
        # Get the plain type for each
        a_plain = actual_origin if actual_origin is not None else actual
        e_plain = expected_origin if expected_origin is not None else expected
        return _is_compatible_plain(a_plain, e_plain)

    # Plain types
    return _is_compatible_plain(actual, expected)


def _is_compatible_plain(actual: Any, expected: Any) -> bool:
    """Plain issubclass check with TypeError guard."""
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
