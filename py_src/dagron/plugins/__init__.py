"""Plugin system for dagron."""

from dagron.plugins.base import DagronPlugin
from dagron.plugins.hooks import HookContext, HookEvent, HookRegistry
from dagron.plugins.manager import PluginManager, dagron_plugin
from dagron.plugins.registry import ExecutorRegistry, NodeTypeRegistry, SerializerRegistry

__all__ = [
    "DagronPlugin",
    "ExecutorRegistry",
    "HookContext",
    "HookEvent",
    "HookRegistry",
    "NodeTypeRegistry",
    "PluginManager",
    "SerializerRegistry",
    "dagron_plugin",
]
