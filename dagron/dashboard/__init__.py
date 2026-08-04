"""Live execution dashboard plugin for dagron.

The web server is implemented in Rust (``dagron-ui`` crate) and exposed to
Python via the ``RustDashboardServer`` PyO3 class.  This module is a thin
hook-wiring layer.
"""

from __future__ import annotations

import webbrowser
from typing import TYPE_CHECKING, Any

from dagron.plugins.base import DagronPlugin

if TYPE_CHECKING:
    from dagron.execution.gates import GateController
    from dagron.plugins.hooks import HookRegistry

__all__ = ["DashboardPlugin"]


def _import_server() -> Any:
    """Lazily import the Rust-backed dashboard server."""
    try:
        from dagron._internal import RustDashboardServer
    except ImportError as exc:
        raise ImportError(
            "DashboardPlugin requires dagron to be built with the 'dashboard' "
            "Cargo feature.  Rebuild with:  maturin develop --features dashboard"
        ) from exc
    return RustDashboardServer


class DashboardPlugin(DagronPlugin):
    """A :class:`~dagron.plugins.base.DagronPlugin` that serves a live web
    dashboard showing real-time DAG execution status.

    The web server runs in Rust (axum + tokio) on a background OS thread.

    Usage::

        from dagron.dashboard import DashboardPlugin
        from dagron import DAGExecutor, HookRegistry

        plugin = DashboardPlugin(port=8765)
        hooks = HookRegistry()
        plugin.initialize(hooks)
        # -> prints "Dashboard: http://127.0.0.1:8765"

        executor = DAGExecutor(dag, hooks=hooks)
        result = executor.execute(tasks)

        plugin.teardown()
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 8765,
        gate_controller: GateController | None = None,
        open_browser: bool = False,
    ) -> None:
        self._host = host
        self._port = port
        self._gate_controller = gate_controller
        self._open_browser = open_browser

        self._server: Any = None
        self._unregister_fns: list[object] = []

    @property
    def name(self) -> str:
        return "dagron.dashboard"

    def initialize(self, hooks: HookRegistry) -> None:
        from dagron.plugins.hooks import HookContext, HookEvent

        RustDashboardServer = _import_server()
        server = RustDashboardServer(self._host, self._port)
        self._server = server

        # Wire gate callback if a GateController is provided
        gc = self._gate_controller
        if gc is not None:
            server.set_gate_callback(
                gc.approve,
                lambda name, reason="": gc.reject(name, reason=reason),
                gc.has_gate,
            )

        # -- Register hooks ------------------------------------------------

        def _on_pre_execute(ctx: HookContext) -> None:
            dag = ctx.dag
            server.reset(dag.to_dot(), dag.node_names(), dag.edges())
            if gc is not None:
                server.set_waiting_gates(gc.waiting_gates())

        def _on_pre_node(ctx: HookContext) -> None:
            if ctx.node_name is not None:
                server.node_started(ctx.node_name)

        def _on_post_node(ctx: HookContext) -> None:
            if ctx.node_name is not None:
                server.node_finished(ctx.node_name, "completed")
            if gc is not None:
                server.set_waiting_gates(gc.waiting_gates())

        def _on_error(ctx: HookContext) -> None:
            if ctx.node_name is not None:
                server.node_finished(
                    ctx.node_name,
                    "failed",
                    error=str(ctx.error) if ctx.error else None,
                )

        def _on_post_execute(ctx: HookContext) -> None:
            if ctx.execution_result is not None:
                r = ctx.execution_result
                server.execution_finished(
                    r.total_duration_seconds,
                    r.succeeded,
                    r.failed,
                    r.skipped,
                    r.timed_out,
                    r.cancelled,
                )

        self._unregister_fns = [
            hooks.register(HookEvent.PRE_EXECUTE, _on_pre_execute),
            hooks.register(HookEvent.PRE_NODE, _on_pre_node),
            hooks.register(HookEvent.POST_NODE, _on_post_node),
            hooks.register(HookEvent.ON_ERROR, _on_error),
            hooks.register(HookEvent.POST_EXECUTE, _on_post_execute),
        ]

        # -- Print URL -----------------------------------------------------
        url = f"http://{self._host}:{server.port}"
        print(f"Dashboard: {url}")

        if self._open_browser:
            webbrowser.open(url)

    def teardown(self) -> None:
        for unreg in self._unregister_fns:
            if callable(unreg):
                unreg()
        self._unregister_fns.clear()

        if self._server is not None:
            self._server.stop()
            self._server = None
