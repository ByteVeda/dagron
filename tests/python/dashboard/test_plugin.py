"""Integration tests for DashboardPlugin."""

import json
import urllib.request

import pytest

try:
    from dagron._internal import RustDashboardServer  # noqa: F401
except ImportError:
    pytest.skip(
        "Dashboard feature not compiled (build with --features dashboard)",
        allow_module_level=True,
    )

from dagron import DAG, DAGExecutor
from dagron.dashboard import DashboardPlugin
from dagron.plugins.hooks import HookEvent, HookRegistry


class TestPluginLifecycle:
    def test_initialize_registers_hooks(self):
        hooks = HookRegistry()
        plugin = DashboardPlugin(port=0)
        plugin.initialize(hooks)
        try:
            assert hooks.hook_count() >= 5
            assert hooks.hook_count(HookEvent.PRE_EXECUTE) >= 1
            assert hooks.hook_count(HookEvent.PRE_NODE) >= 1
            assert hooks.hook_count(HookEvent.POST_NODE) >= 1
            assert hooks.hook_count(HookEvent.ON_ERROR) >= 1
            assert hooks.hook_count(HookEvent.POST_EXECUTE) >= 1
        finally:
            plugin.teardown()

    def test_teardown_clears_hooks(self):
        hooks = HookRegistry()
        plugin = DashboardPlugin(port=0)
        plugin.initialize(hooks)
        assert hooks.hook_count() >= 5
        plugin.teardown()
        assert hooks.hook_count() == 0

    def test_name(self):
        plugin = DashboardPlugin()
        assert plugin.name == "dagron.dashboard"


class TestPluginIntegration:
    @pytest.mark.timeout(15)
    def test_full_execution_updates_all_states(self):
        dag = DAG()
        dag.add_nodes(["a", "b", "c"])
        dag.add_edge("a", "b")
        dag.add_edge("b", "c")

        hooks = HookRegistry()
        plugin = DashboardPlugin(port=0)
        plugin.initialize(hooks)

        try:
            # Get the server port from the Rust server
            server = plugin._server
            port = server.port

            executor = DAGExecutor(dag, hooks=hooks)
            result = executor.execute({
                "a": lambda: 1,
                "b": lambda: 2,
                "c": lambda: 3,
            })

            assert result.succeeded == 3

            # Verify via HTTP that the state was updated
            url = f"http://127.0.0.1:{port}/api/state"
            resp = urllib.request.urlopen(url)
            snap = json.loads(resp.read())
            assert snap["is_running"] is False
            for ns in snap["nodes"]:
                assert ns["status"] == "completed"
            assert snap["profile"] is not None
            assert snap["profile"]["succeeded"] == 3
        finally:
            plugin.teardown()

    @pytest.mark.timeout(15)
    def test_failed_node_tracked(self):
        dag = DAG()
        dag.add_node("ok")
        dag.add_node("bad")

        hooks = HookRegistry()
        plugin = DashboardPlugin(port=0)
        plugin.initialize(hooks)

        try:
            server = plugin._server
            port = server.port

            executor = DAGExecutor(dag, hooks=hooks)
            result = executor.execute({
                "ok": lambda: 42,
                "bad": lambda: (_ for _ in ()).throw(ValueError("boom")),
            })

            assert result.failed >= 1

            url = f"http://127.0.0.1:{port}/api/state"
            resp = urllib.request.urlopen(url)
            snap = json.loads(resp.read())
            node_map = {n["name"]: n for n in snap["nodes"]}
            assert node_map["ok"]["status"] == "completed"
            assert node_map["bad"]["status"] == "failed"
            assert node_map["bad"]["error"] is not None
        finally:
            plugin.teardown()
