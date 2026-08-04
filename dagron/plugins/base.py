"""Base class for dagron plugins."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dagron.plugins.hooks import HookRegistry


class DagronPlugin(ABC):
    """Abstract base class for dagron plugins.

    Subclass this and implement ``name``, ``initialize()``, and ``teardown()``
    to create a plugin that can be discovered and managed by the PluginManager.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Unique name for the plugin."""

    @abstractmethod
    def initialize(self, hooks: HookRegistry) -> None:
        """Called when the plugin is initialized. Register hooks here."""

    def teardown(self) -> None:  # noqa: B027
        """Called when the plugin is torn down. Clean up resources here."""
