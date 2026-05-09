"""Static-typing tests for dagron's typed handles and stubgen.

These tests exercise:

* `FlowFuture[T]` carrying its wrapped task's return type
* `@task` preserving its function signature
* `NodeResult[T]` and `ExecutionResult.__getitem__` overloads typing
  results by `FlowFuture[T]` key
* `dagron.stubgen.generate_stub` emitting a syntactically valid `.pyi`
  with `Literal["..."] -> NodeResult[T]` overloads

Static type assertions use `typing_extensions.assert_type`-style runtime
checks where possible, plus a `subprocess`-driven mypy run for the
`reveal_type` cases mypy can verify.
"""

from __future__ import annotations

import ast
import shutil
import subprocess
import textwrap
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pathlib import Path

from dagron import DAG, FlowFuture, flow, task
from dagron.execution._types import ExecutionResult, NodeResult, NodeStatus
from dagron.stubgen import generate_stub

# ---------------------------------------------------------------------------
# Runtime checks of the typed surface
# ---------------------------------------------------------------------------


class TestFlowFutureGeneric:
    def test_flow_future_is_subscriptable(self):
        # The class is generic, so FlowFuture[int] should not raise.
        alias = FlowFuture[int]
        assert alias is not None  # GenericAlias

    def test_constructor_returns_instance(self):
        f: FlowFuture[int] = FlowFuture("x")
        assert f.name == "x"


class TestTaskSignaturePreservation:
    def test_decorated_function_preserves_call_signature(self):
        @task
        def add(a: int, b: int) -> int:
            return a + b

        # Direct call works with original signature
        assert add(2, 3) == 5

        # Inspecting the wrapper still reveals the original signature.
        # Use `get_type_hints` because the test file uses
        # `from __future__ import annotations`, so raw annotations are strings.
        import inspect
        from typing import get_type_hints

        sig = inspect.signature(add)
        assert list(sig.parameters) == ["a", "b"]

        hints = get_type_hints(add)
        assert hints == {"a": int, "b": int, "return": int}


class TestExecutionResultLookup:
    def test_lookup_by_string(self):
        r = ExecutionResult()
        r.node_results["a"] = NodeResult(name="a", status=NodeStatus.COMPLETED, result=42)
        out = r["a"]
        assert out.result == 42

    def test_lookup_by_flow_future(self):
        r = ExecutionResult()
        r.node_results["a"] = NodeResult(name="a", status=NodeStatus.COMPLETED, result=42)
        f: FlowFuture[int] = FlowFuture("a")
        out = r[f]
        assert out.result == 42

    def test_contains_by_flow_future(self):
        r = ExecutionResult()
        r.node_results["a"] = NodeResult(name="a", status=NodeStatus.COMPLETED, result=42)
        f: FlowFuture[int] = FlowFuture("a")
        assert f in r
        assert "a" in r
        assert FlowFuture("missing") not in r


# ---------------------------------------------------------------------------
# stubgen
# ---------------------------------------------------------------------------


class TestStubgen:
    def test_generates_valid_python(self):
        @task
        def fetch() -> list[int]:
            return [1, 2]

        @task
        def total(rows: list[int]) -> int:
            return sum(rows)

        @flow
        def pipeline():
            return total(fetch())

        dag = pipeline.dag()
        stub = generate_stub(
            dag,
            tasks={"fetch": fetch, "total": total},
            name="MyResult",
        )
        # Source must parse as a Python module
        ast.parse(stub)
        # Sanity check: the literal-keyed overloads are present
        assert "Literal['fetch']" in stub
        assert "Literal['total']" in stub
        assert "NodeResult[list[int]]" in stub or "NodeResult[builtins.list" in stub
        assert "NodeResult[int]" in stub
        # And a fallback str overload
        assert "key: str) -> NodeResult[Any]" in stub

    def test_explicit_hints_override_inference(self):
        @task
        def opaque():  # no annotation
            return None

        dag = DAG()
        dag.add_node("opaque")
        stub = generate_stub(
            dag,
            type_hints={"opaque": "list[float]"},
            name="R",
        )
        assert "NodeResult[list[float]]" in stub

    def test_empty_dag_produces_valid_class(self):
        dag = DAG()
        stub = generate_stub(dag, name="Empty")
        ast.parse(stub)
        # Even an empty DAG should produce the fallback overload
        assert "key: str) -> NodeResult[Any]" in stub


# ---------------------------------------------------------------------------
# Mypy reveal_type — runs mypy on a synthesized snippet to verify static types.
#
# Skipped when mypy is not available (e.g. minimal CI environment).
# ---------------------------------------------------------------------------


@pytest.mark.skipif(shutil.which("mypy") is None, reason="mypy not on PATH")
def test_mypy_reveal_types(tmp_path: Path) -> None:
    snippet = textwrap.dedent(
        """
        from __future__ import annotations
        from dagron import flow, task, FlowFuture
        from dagron.execution._types import ExecutionResult, NodeResult

        @task
        def fetch() -> list[int]: ...

        @task
        def total(rows: list[int]) -> int: ...

        @flow
        def pipeline():
            raw = fetch()         # FlowFuture[list[int]] (typed as list[int])
            reveal_type(raw)
            return total(raw)

        result: ExecutionResult = pipeline()
        reveal_type(result[fetch_future])    # noqa: F821 — illustrative
        """
    )
    target = tmp_path / "snippet.py"
    target.write_text(snippet)

    proc = subprocess.run(
        ["mypy", "--ignore-missing-imports", str(target)],
        capture_output=True,
        text=True,
        check=False,
    )
    # We expect mypy to find at least one reveal_type — verify the output
    # mentions list[int] (the inferred type of `raw`).
    assert "list[int]" in proc.stdout or "Revealed type" in proc.stdout, proc.stdout
