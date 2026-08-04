"""Hook system for dagron lifecycle events."""

from __future__ import annotations

import contextlib
import warnings
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable


class HookEvent(Enum):
    """Events that hooks can subscribe to."""

    PRE_EXECUTE = "pre_execute"
    POST_EXECUTE = "post_execute"
    PRE_NODE = "pre_node"
    POST_NODE = "post_node"
    ON_ERROR = "on_error"
    PRE_BUILD = "pre_build"
    POST_BUILD = "post_build"


@dataclass
class HookContext:
    """Context passed to hook callbacks."""

    event: HookEvent
    dag: Any = None
    node_name: str | None = None
    node_result: Any = None
    error: Exception | None = None
    execution_result: Any = None
    metadata: dict[str, Any] = field(default_factory=dict)


class HookRegistry:
    """Registry for event hooks with priority ordering.

    Hooks are fire-and-forget: exceptions are caught and warned, never propagated.
    """

    def __init__(self) -> None:
        self._hooks: dict[HookEvent, list[tuple[int, Callable[[HookContext], None]]]] = {
            event: [] for event in HookEvent
        }

    def register(
        self,
        event: HookEvent,
        callback: Callable[[HookContext], None],
        priority: int = 0,
    ) -> Callable[[], None]:
        """Register a hook callback for an event.

        Args:
            event: The event to subscribe to.
            callback: Function called with a HookContext.
            priority: Higher priority callbacks run first. Default 0.

        Returns:
            An unregister function that removes this hook when called.
        """
        entry = (priority, callback)
        self._hooks[event].append(entry)
        self._hooks[event].sort(key=lambda e: -e[0])  # descending priority

        def unregister() -> None:
            with contextlib.suppress(ValueError):
                self._hooks[event].remove(entry)

        return unregister

    def fire(self, context: HookContext) -> None:
        """Fire all hooks registered for the context's event.

        Exceptions in hooks are caught and issued as warnings.
        """
        for _priority, callback in self._hooks[context.event]:
            try:
                callback(context)
            except Exception as exc:
                warnings.warn(
                    f"Hook {callback!r} for {context.event.value} raised: {exc}",
                    RuntimeWarning,
                    stacklevel=2,
                )

    def clear(self, event: HookEvent | None = None) -> None:
        """Clear all hooks, or hooks for a specific event."""
        if event is not None:
            self._hooks[event] = []
        else:
            for ev in HookEvent:
                self._hooks[ev] = []

    def hook_count(self, event: HookEvent | None = None) -> int:
        """Return number of registered hooks."""
        if event is not None:
            return len(self._hooks[event])
        return sum(len(hooks) for hooks in self._hooks.values())
