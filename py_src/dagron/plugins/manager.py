"""Plugin manager with discovery and lifecycle management."""

from __future__ import annotations

import warnings

from dagron.plugins.base import DagronPlugin
from dagron.plugins.hooks import HookRegistry


class PluginManager:
    """Manages plugin discovery, initialization, and teardown."""

    def __init__(self, hooks: HookRegistry | None = None) -> None:
        self._hooks = hooks or HookRegistry()
        self._plugins: dict[str, DagronPlugin] = {}
        self._initialized: set[str] = set()

    @property
    def hooks(self) -> HookRegistry:
        """The hook registry shared by all plugins."""
        return self._hooks

    @property
    def plugins(self) -> dict[str, DagronPlugin]:
        """Currently registered plugins."""
        return dict(self._plugins)

    def register(self, plugin: DagronPlugin) -> None:
        """Register a plugin instance."""
        if plugin.name in self._plugins:
            warnings.warn(
                f"Plugin '{plugin.name}' already registered, replacing.",
                RuntimeWarning,
                stacklevel=2,
            )
        self._plugins[plugin.name] = plugin

    def discover(self) -> list[str]:
        """Discover plugins via entry_points(group='dagron.plugins').

        Returns:
            List of discovered plugin names.
        """
        discovered: list[str] = []
        from importlib.metadata import entry_points
        eps = entry_points(group="dagron.plugins")

        for ep in eps:
            try:
                plugin_cls = ep.load()
                if isinstance(plugin_cls, type) and issubclass(plugin_cls, DagronPlugin):
                    plugin = plugin_cls()
                    self.register(plugin)
                    discovered.append(plugin.name)
                elif isinstance(plugin_cls, DagronPlugin):
                    self.register(plugin_cls)
                    discovered.append(plugin_cls.name)
            except Exception as exc:
                warnings.warn(
                    f"Failed to load plugin entry point '{ep.name}': {exc}",
                    RuntimeWarning,
                    stacklevel=2,
                )
        return discovered

    def initialize_all(self) -> None:
        """Initialize all registered plugins that haven't been initialized yet."""
        for name, plugin in self._plugins.items():
            if name not in self._initialized:
                try:
                    plugin.initialize(self._hooks)
                    self._initialized.add(name)
                except Exception as exc:
                    warnings.warn(
                        f"Failed to initialize plugin '{name}': {exc}",
                        RuntimeWarning,
                        stacklevel=2,
                    )

    def teardown_all(self) -> None:
        """Tear down all initialized plugins."""
        for name in list(self._initialized):
            plugin = self._plugins.get(name)
            if plugin is not None:
                try:
                    plugin.teardown()
                except Exception as exc:
                    warnings.warn(
                        f"Failed to teardown plugin '{name}': {exc}",
                        RuntimeWarning,
                        stacklevel=2,
                    )
            self._initialized.discard(name)


def dagron_plugin(cls: type) -> type:
    """Class decorator that registers a plugin class with the global manager.

    Usage::

        @dagron_plugin
        class MyPlugin(DagronPlugin):
            ...
    """
    if not (isinstance(cls, type) and issubclass(cls, DagronPlugin)):
        raise TypeError(
            f"@dagron_plugin can only decorate DagronPlugin subclasses, got {cls}"
        )
    # Lazy import to avoid circular imports
    from dagron import _plugin_manager  # type: ignore[attr-defined]

    plugin = cls()
    _plugin_manager.register(plugin)
    return cls
