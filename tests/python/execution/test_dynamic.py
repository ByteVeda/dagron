"""Tests for Dynamic DAG Modification."""


from dagron import DAG
from dagron.execution._types import NodeStatus
from dagron.execution.dynamic import (
    DynamicExecutor,
    DynamicModification,
    DynamicNodeSpec,
)


class TestDynamicNodeSpec:
    def test_basic_spec(self):
        spec = DynamicNodeSpec(name="new_node", task=lambda: 42)
        assert spec.name == "new_node"
        assert spec.task() == 42
        assert spec.dependencies == []
        assert spec.dependents == []


class TestDynamicModification:
    def test_empty_mod(self):
        mod = DynamicModification()
        assert mod.add_nodes == []
        assert mod.remove_nodes == []


class TestDynamicExecutor:
    def test_no_expansion(self):
        dag = DAG()
        dag.add_node("a")
        dag.add_node("b")
        dag.add_edge("a", "b")

        executor = DynamicExecutor(dag)
        result = executor.execute({"a": lambda: 1, "b": lambda: 2})
        assert result.succeeded == 2

    def test_simple_expansion(self):
        dag = DAG()
        dag.add_node("discover")
        dag.add_node("finish")
        dag.add_edge("discover", "finish")

        order = []

        def expander(name, result):
            return DynamicModification(
                add_nodes=[
                    DynamicNodeSpec(
                        name="dynamic_1",
                        task=lambda: order.append("dynamic_1") or "d1",  # type: ignore[func-returns-value]
                        dependencies=["discover"],
                        dependents=["finish"],
                    ),
                ]
            )

        executor = DynamicExecutor(dag, expanders={"discover": expander})
        result = executor.execute({
            "discover": lambda: order.append("discover") or ["item1"],  # type: ignore[func-returns-value]
            "finish": lambda: order.append("finish") or "done",  # type: ignore[func-returns-value]
        })

        assert result.succeeded == 3
        assert "dynamic_1" in result.node_results
        # discover runs first, then dynamic_1, then finish
        assert order.index("discover") < order.index("dynamic_1")
        assert order.index("dynamic_1") < order.index("finish")

    def test_multiple_dynamic_nodes(self):
        dag = DAG()
        dag.add_node("scan")

        def expander(name, result):
            nodes = [
                DynamicNodeSpec(
                    name=f"process_{i}",
                    task=lambda i=i: f"result_{i}",  # type: ignore[misc]
                    dependencies=["scan"],
                )
                for i in range(3)
            ]
            return DynamicModification(add_nodes=nodes)

        executor = DynamicExecutor(dag, expanders={"scan": expander})
        result = executor.execute({"scan": lambda: ["a", "b", "c"]})

        assert result.succeeded == 4  # scan + 3 dynamic
        for i in range(3):
            assert f"process_{i}" in result.node_results

    def test_original_dag_not_mutated(self):
        dag = DAG()
        dag.add_node("a")
        original_count = dag.node_count()

        def expander(name, result):
            return DynamicModification(
                add_nodes=[DynamicNodeSpec(name="new", task=lambda: 1, dependencies=["a"])]
            )

        executor = DynamicExecutor(dag, expanders={"a": expander})
        executor.execute({"a": lambda: "ok"})

        assert dag.node_count() == original_count

    def test_expander_returns_none(self):
        dag = DAG()
        dag.add_node("a")

        executor = DynamicExecutor(dag, expanders={"a": lambda n, r: None})
        result = executor.execute({"a": lambda: 1})
        assert result.succeeded == 1

    def test_fail_fast_skips_dynamic_nodes(self):
        dag = DAG()
        dag.add_node("a")
        dag.add_node("b")
        dag.add_edge("a", "b")

        def expander(name, result):
            return DynamicModification(
                add_nodes=[
                    DynamicNodeSpec(
                        name="dynamic",
                        task=lambda: "should not run",
                        dependencies=["b"],
                    )
                ]
            )

        executor = DynamicExecutor(dag, expanders={"a": expander}, fail_fast=True)

        def fail():
            raise ValueError("boom")

        result = executor.execute({"a": lambda: "ok", "b": fail})
        assert result.failed == 1
        assert result.node_results["b"].status == NodeStatus.FAILED
        # dynamic should be skipped because b (its dependency) failed
        if "dynamic" in result.node_results:
            assert result.node_results["dynamic"].status == NodeStatus.SKIPPED

    def test_remove_node(self):
        dag = DAG()
        dag.add_node("a")
        dag.add_node("b")
        dag.add_node("c")
        dag.add_edge("a", "b")
        dag.add_edge("a", "c")

        def expander(name, result):
            return DynamicModification(remove_nodes=["c"])

        executor = DynamicExecutor(dag, expanders={"a": expander})
        result = executor.execute({
            "a": lambda: 1,
            "b": lambda: 2,
            "c": lambda: 3,
        })

        assert result.succeeded == 2
        assert "c" not in result.node_results

    def test_dynamic_expand_callback(self):
        dag = DAG()
        dag.add_node("a")

        expansions = []

        def on_expand(name, mod):
            expansions.append((name, len(mod.add_nodes)))

        from dagron.execution._types import ExecutionCallbacks

        callbacks = ExecutionCallbacks(on_dynamic_expand=on_expand)

        def expander(name, result):
            return DynamicModification(
                add_nodes=[DynamicNodeSpec(name="dyn", task=lambda: 1, dependencies=["a"])]
            )

        executor = DynamicExecutor(dag, expanders={"a": expander}, callbacks=callbacks)
        executor.execute({"a": lambda: "ok"})

        assert expansions == [("a", 1)]

    def test_chained_expansion(self):
        dag = DAG()
        dag.add_node("root")

        def root_expander(name, result):
            return DynamicModification(
                add_nodes=[
                    DynamicNodeSpec(
                        name="level1",
                        task=lambda: "l1",
                        dependencies=["root"],
                    )
                ]
            )

        def level1_expander(name, result):
            return DynamicModification(
                add_nodes=[
                    DynamicNodeSpec(
                        name="level2",
                        task=lambda: "l2",
                        dependencies=["level1"],
                    )
                ]
            )

        executor = DynamicExecutor(
            dag,
            expanders={"root": root_expander, "level1": level1_expander},
        )
        result = executor.execute({"root": lambda: "r"})

        assert result.succeeded == 3
        assert "level1" in result.node_results
        assert "level2" in result.node_results

    def test_with_tracing(self):
        dag = DAG()
        dag.add_node("a")
        executor = DynamicExecutor(dag, enable_tracing=True)
        result = executor.execute({"a": lambda: 42})
        assert result.trace is not None
        assert len(result.trace.events) > 0
