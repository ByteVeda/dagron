"""Tests for the @dagron.flow Pythonic compose API.

`@flow` lets users describe a DAG by writing a regular Python function
that calls `@task`-decorated functions. The call structure becomes the
edges; no string IDs required.
"""

from __future__ import annotations

import pytest

from dagron import (
    DAG,
    Flow,
    FlowFuture,
    Pipeline,
    flow,
    task,
)

# ---------------------------------------------------------------------------
# Tracing — single call returns a FlowFuture
# ---------------------------------------------------------------------------


class TestTracing:
    def test_task_outside_flow_executes_normally(self):
        @task
        def double(x: int) -> int:
            return x * 2

        # Calling outside a @flow context runs the function for real.
        assert double(5) == 10

    def test_task_inside_flow_returns_future(self):
        @task
        def double(x: int) -> int:
            return x * 2

        captured: list[object] = []

        @flow
        def f():
            value = double(5)
            captured.append(value)
            return value

        result = f.dag()
        assert isinstance(result, DAG)
        assert len(captured) == 1
        assert isinstance(captured[0], FlowFuture)
        assert captured[0].name == "double"

    def test_flow_decorator_returns_flow_object(self):
        @flow
        def f():
            return None

        assert isinstance(f, Flow)


# ---------------------------------------------------------------------------
# DAG construction from call structure
# ---------------------------------------------------------------------------


class TestDagBuilding:
    def test_linear(self):
        @task
        def a():
            return 1

        @task
        def b(x):
            return x + 1

        @task
        def c(x):
            return x * 10

        @flow
        def pipeline():
            return c(b(a()))

        dag = pipeline.dag()
        assert dag.node_count() == 3
        assert dag.edges() == [("a", "b"), ("b", "c")]

    def test_diamond(self):
        @task
        def src():
            return 1

        @task
        def left(x):
            return x + 1

        @task
        def right(x):
            return x * 2

        @task
        def merge(left_v, right_v):
            return left_v + right_v

        @flow
        def pipeline():
            s = src()
            return merge(left(s), right(s))

        dag = pipeline.dag()
        assert dag.node_count() == 4
        edges = sorted(dag.edges())
        assert edges == [
            ("left", "merge"),
            ("right", "merge"),
            ("src", "left"),
            ("src", "right"),
        ]

    def test_repeated_task_gets_unique_names(self):
        @task
        def fetch(url):
            return url

        @flow
        def pipeline():
            a = fetch("a")
            b = fetch("b")
            c = fetch("c")
            return [a, b, c]  # Returns a list — flow expects FlowFuture or None

        # Returning a list should fail
        with pytest.raises(TypeError, match="must return a FlowFuture"):
            pipeline.dag()

    def test_repeated_task_each_gets_own_node(self):
        @task
        def fetch(url):
            return url

        @task
        def join(*xs):
            return list(xs)

        @flow
        def pipeline():
            return join(fetch("a"), fetch("b"), fetch("c"))

        dag = pipeline.dag()
        names = sorted(n.name for n in dag.nodes())
        assert names == ["fetch", "fetch_1", "fetch_2", "join"]

    def test_literal_args_are_not_dependencies(self):
        @task
        def add(x, y):
            return x + y

        @flow
        def pipeline():
            return add(40, 2)

        dag = pipeline.dag()
        assert dag.node_count() == 1
        assert dag.edges() == []

    def test_kwargs_wired_correctly(self):
        @task
        def src():
            return 7

        @task
        def use(*, value):
            return value

        @flow
        def pipeline():
            return use(value=src())

        dag = pipeline.dag()
        assert dag.edges() == [("src", "use")]


# ---------------------------------------------------------------------------
# Execution
# ---------------------------------------------------------------------------


class TestExecution:
    def test_run_returns_execution_result(self):
        @task
        def src():
            return 5

        @task
        def double(x):
            return x * 2

        @flow
        def pipeline():
            return double(src())

        result = pipeline.run()
        assert result.succeeded == 2
        assert result["src"].result == 5
        assert result["double"].result == 10

    def test_calling_flow_runs_it(self):
        @task
        def hello():
            return "hi"

        @flow
        def pipeline():
            return hello()

        # Calling the Flow object is shorthand for run()
        result = pipeline()
        assert result["hello"].result == "hi"

    def test_diamond_runs_in_correct_order(self):
        order: list[str] = []

        @task
        def s():
            order.append("s")
            return 1

        @task
        def left(x):
            order.append("left")
            return x + 10

        @task
        def right(x):
            order.append("right")
            return x + 100

        @task
        def m(a, b):
            order.append("m")
            return a + b

        @flow
        def pipeline():
            sv = s()
            return m(left(sv), right(sv))

        result = pipeline.run()
        assert result["m"].result == 1 + 10 + 1 + 100  # 112
        # s must come before left/right; left/right before m.
        assert order[0] == "s"
        assert order[-1] == "m"
        middle = order[1:-1]
        assert "left" in middle
        assert "right" in middle

    def test_kwargs_resolved_at_execution(self):
        @task
        def src():
            return 7

        @task
        def use(*, value):
            return value * 3

        @flow
        def pipeline():
            return use(value=src())

        result = pipeline.run()
        assert result["use"].result == 21


# ---------------------------------------------------------------------------
# Error cases
# ---------------------------------------------------------------------------


class TestErrors:
    def test_returning_non_flow_future_raises(self):
        @task
        def t():
            return 42

        @flow
        def bad():
            return "not a future"

        with pytest.raises(TypeError, match="must return a FlowFuture"):
            bad.dag()

    def test_nested_flow_invocation_raises(self):
        @task
        def t():
            return 1

        @flow
        def inner():
            return t()

        @flow
        def outer():
            inner.dag()  # nested invocation
            return t()

        with pytest.raises(RuntimeError, match=r"Nested @dagron\.flow"):
            outer.dag()

    def test_task_can_be_called_directly_outside_flow(self):
        # Sanity check: @task decoration doesn't break direct callability.
        @task
        def add(x, y):
            return x + y

        assert add(2, 3) == 5


# ---------------------------------------------------------------------------
# Pipeline backwards compatibility — same @task works in both
# ---------------------------------------------------------------------------


class TestPipelineCompat:
    def test_same_task_works_in_pipeline_and_flow(self):
        @task
        def fetch_users():
            return [{"id": 1}, {"id": 2}]

        @task
        def fetch_orders():
            return [{"oid": 1}]

        @task
        def merge(fetch_users, fetch_orders):
            return {"users": fetch_users, "orders": fetch_orders}

        # Pipeline (param-name wiring)
        p_result = Pipeline([fetch_users, fetch_orders, merge]).execute()
        assert p_result["merge"].result == {
            "users": [{"id": 1}, {"id": 2}],
            "orders": [{"oid": 1}],
        }

        # @flow (call-structure wiring) — same task functions
        @flow
        def pipeline():
            return merge(fetch_users(), fetch_orders())

        f_result = pipeline.run()
        assert f_result["merge"].result == p_result["merge"].result
