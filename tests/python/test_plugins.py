"""Tests for Plugin/Hook System."""

import warnings

import pytest

from dagron import DAG, DAGExecutor
from dagron.plugins import (
    DagronPlugin,
    ExecutorRegistry,
    HookContext,
    HookEvent,
    HookRegistry,
    NodeTypeRegistry,
    PluginManager,
    SerializerRegistry,
)


class TestHookRegistry:
    def test_register_and_fire(self):
        registry = HookRegistry()
        events_seen = []

        registry.register(HookEvent.PRE_EXECUTE, lambda ctx: events_seen.append(ctx.event))
        registry.fire(HookContext(event=HookEvent.PRE_EXECUTE))

        assert events_seen == [HookEvent.PRE_EXECUTE]

    def test_priority_ordering(self):
        registry = HookRegistry()
        order = []

        registry.register(HookEvent.PRE_NODE, lambda ctx: order.append("low"), priority=0)
        registry.register(HookEvent.PRE_NODE, lambda ctx: order.append("high"), priority=10)
        registry.register(HookEvent.PRE_NODE, lambda ctx: order.append("mid"), priority=5)

        registry.fire(HookContext(event=HookEvent.PRE_NODE))
        assert order == ["high", "mid", "low"]

    def test_unregister(self):
        registry = HookRegistry()
        calls = []

        unregister = registry.register(
            HookEvent.POST_NODE, lambda ctx: calls.append(1)
        )
        registry.fire(HookContext(event=HookEvent.POST_NODE))
        assert calls == [1]

        unregister()
        registry.fire(HookContext(event=HookEvent.POST_NODE))
        assert calls == [1]  # not called again

    def test_exception_suppressed(self):
        registry = HookRegistry()
        calls = []

        def bad_hook(ctx):
            raise ValueError("boom")

        def good_hook(ctx):
            calls.append("ok")

        registry.register(HookEvent.PRE_EXECUTE, bad_hook, priority=10)
        registry.register(HookEvent.PRE_EXECUTE, good_hook, priority=0)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            registry.fire(HookContext(event=HookEvent.PRE_EXECUTE))
            assert calls == ["ok"]  # good_hook still runs
            assert len(w) == 1
            assert "boom" in str(w[0].message)

    def test_clear_all(self):
        registry = HookRegistry()
        registry.register(HookEvent.PRE_EXECUTE, lambda ctx: None)
        registry.register(HookEvent.POST_EXECUTE, lambda ctx: None)
        assert registry.hook_count() == 2
        registry.clear()
        assert registry.hook_count() == 0

    def test_clear_specific_event(self):
        registry = HookRegistry()
        registry.register(HookEvent.PRE_EXECUTE, lambda ctx: None)
        registry.register(HookEvent.POST_EXECUTE, lambda ctx: None)
        registry.clear(HookEvent.PRE_EXECUTE)
        assert registry.hook_count(HookEvent.PRE_EXECUTE) == 0
        assert registry.hook_count(HookEvent.POST_EXECUTE) == 1

    def test_hook_context_fields(self):
        registry = HookRegistry()
        captured = []

        def capture(ctx):
            captured.append(ctx)

        registry.register(HookEvent.ON_ERROR, capture)
        err = ValueError("test")
        registry.fire(HookContext(
            event=HookEvent.ON_ERROR,
            node_name="step1",
            error=err,
            metadata={"key": "val"},
        ))

        assert len(captured) == 1
        ctx = captured[0]
        assert ctx.node_name == "step1"
        assert ctx.error is err
        assert ctx.metadata == {"key": "val"}


class TestPluginBase:
    def test_plugin_lifecycle(self):
        initialized = []
        torn_down = []

        class TestPlugin(DagronPlugin):
            @property
            def name(self):
                return "test"

            def initialize(self, hooks):
                initialized.append(True)
                hooks.register(HookEvent.PRE_EXECUTE, lambda ctx: None)

            def teardown(self):
                torn_down.append(True)

        manager = PluginManager()
        plugin = TestPlugin()
        manager.register(plugin)
        manager.initialize_all()
        assert initialized == [True]

        manager.teardown_all()
        assert torn_down == [True]

    def test_double_init_skipped(self):
        count = []

        class CountPlugin(DagronPlugin):
            @property
            def name(self):
                return "counter"

            def initialize(self, hooks):
                count.append(1)

        manager = PluginManager()
        manager.register(CountPlugin())
        manager.initialize_all()
        manager.initialize_all()  # should not re-init
        assert count == [1]

    def test_discover_returns_empty(self):
        manager = PluginManager()
        result = manager.discover()
        # No dagron.plugins entry points registered
        assert isinstance(result, list)

    def test_register_replace_warning(self):
        class P(DagronPlugin):
            @property
            def name(self):
                return "dup"

            def initialize(self, hooks):
                pass

        manager = PluginManager()
        manager.register(P())
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            manager.register(P())
            assert len(w) == 1
            assert "already registered" in str(w[0].message)


class TestHooksInExecutor:
    def test_executor_fires_hooks(self):
        dag = DAG()
        dag.add_node("a")
        dag.add_node("b")
        dag.add_edge("a", "b")

        hooks = HookRegistry()
        events = []

        hooks.register(HookEvent.PRE_EXECUTE, lambda ctx: events.append("pre_exec"))
        hooks.register(HookEvent.POST_EXECUTE, lambda ctx: events.append("post_exec"))
        hooks.register(HookEvent.PRE_NODE, lambda ctx: events.append(f"pre_{ctx.node_name}"))
        hooks.register(HookEvent.POST_NODE, lambda ctx: events.append(f"post_{ctx.node_name}"))

        executor = DAGExecutor(dag, hooks=hooks)
        executor.execute({"a": lambda: 1, "b": lambda: 2})

        assert "pre_exec" in events
        assert "post_exec" in events
        assert "pre_a" in events
        assert "post_a" in events
        assert "pre_b" in events
        assert "post_b" in events

    def test_executor_fires_on_error(self):
        dag = DAG()
        dag.add_node("fail")

        hooks = HookRegistry()
        errors = []
        hooks.register(HookEvent.ON_ERROR, lambda ctx: errors.append(ctx.node_name))

        executor = DAGExecutor(dag, hooks=hooks)
        executor.execute({"fail": lambda: (_ for _ in ()).throw(ValueError("oops"))})

        assert "fail" in errors

    def test_executor_works_without_hooks(self):
        dag = DAG()
        dag.add_node("a")
        executor = DAGExecutor(dag)  # no hooks
        result = executor.execute({"a": lambda: 42})
        assert result.succeeded == 1


class TestRegistries:
    def test_serializer_registry(self):
        reg = SerializerRegistry()
        reg.register("yaml", lambda d: "yaml", lambda s: {})
        assert "yaml" in reg.formats
        result = reg.get("yaml")
        assert result is not None
        ser, des = result
        assert ser({}) == "yaml"
        assert des("yaml") == {}
        assert reg.get("unknown") is None

    def test_executor_registry(self):
        reg = ExecutorRegistry()
        reg.register("custom", DAGExecutor)
        assert reg.get("custom") is DAGExecutor
        assert "custom" in reg.names

    def test_node_type_registry(self):
        reg = NodeTypeRegistry()
        reg.register("sql", lambda name, meta: lambda: f"SQL({meta})")
        task = reg.create_task("sql", "query1", "SELECT 1")
        assert task() == "SQL(SELECT 1)"
        assert "sql" in reg.type_names

        with pytest.raises(KeyError, match="Unknown node type"):
            reg.create_task("missing", "x", {})
