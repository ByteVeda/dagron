"""Extensible registries for serializers, executors, and node types."""

from __future__ import annotations

from typing import Any


class SerializerRegistry:
    """Registry for custom serialization formats."""

    def __init__(self) -> None:
        self._serializers: dict[str, tuple[Any, Any]] = {}

    def register(
        self,
        format_name: str,
        serializer: Any,
        deserializer: Any,
    ) -> None:
        """Register a serializer/deserializer pair for a named format."""
        self._serializers[format_name] = (serializer, deserializer)

    def get(self, format_name: str) -> tuple[Any, Any] | None:
        """Get (serializer, deserializer) for a format, or None."""
        return self._serializers.get(format_name)

    @property
    def formats(self) -> list[str]:
        """List of registered format names."""
        return list(self._serializers.keys())


class ExecutorRegistry:
    """Registry for custom executor implementations."""

    def __init__(self) -> None:
        self._executors: dict[str, type] = {}

    def register(self, name: str, executor_class: type) -> None:
        """Register an executor class by name."""
        self._executors[name] = executor_class

    def get(self, name: str) -> type | None:
        """Get an executor class by name, or None."""
        return self._executors.get(name)

    @property
    def names(self) -> list[str]:
        """List of registered executor names."""
        return list(self._executors.keys())


class NodeTypeRegistry:
    """Registry for custom node types that create task callables from metadata."""

    def __init__(self) -> None:
        self._types: dict[str, Any] = {}

    def register(self, type_name: str, factory: Any) -> None:
        """Register a node type factory.

        The factory is called with ``(node_name, metadata)`` and should return
        a callable suitable for execution.
        """
        self._types[type_name] = factory

    def get(self, type_name: str) -> Any | None:
        """Get a node type factory by name, or None."""
        return self._types.get(type_name)

    def create_task(self, type_name: str, node_name: str, metadata: Any) -> Any:
        """Create a task callable for a node type.

        Raises:
            KeyError: If the type_name is not registered.
        """
        factory = self._types.get(type_name)
        if factory is None:
            raise KeyError(f"Unknown node type: '{type_name}'")
        return factory(node_name, metadata)

    @property
    def type_names(self) -> list[str]:
        """List of registered node type names."""
        return list(self._types.keys())
