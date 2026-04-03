"""Tests for typed data contracts (Feature 1)."""

import dagron
from dagron import DAG, DAGBuilder, Pipeline
from dagron.contracts import (
    ContractValidator,
    ContractViolation,
    NodeContract,
    extract_contracts,
    validate_contracts,
)


class TestNodeContract:
    def test_frozen_dataclass(self):
        c = NodeContract(inputs={"a": int}, output=str)
        assert c.inputs == {"a": int}
        assert c.output is str

    def test_defaults(self):
        c = NodeContract()
        assert c.inputs == {}
        assert c.output is object


class TestContractViolation:
    def test_frozen_dataclass(self):
        v = ContractViolation(from_node="a", to_node="b", message="oops")
        assert v.from_node == "a"
        assert v.to_node == "b"
        assert v.message == "oops"


class TestContractValidator:
    def test_compatible_types_no_violations(self):
        dag = DAG()
        dag.add_nodes(["a", "b"])
        dag.add_edge("a", "b")
        contracts = {
            "a": NodeContract(output=list),
            "b": NodeContract(inputs={"a": list}, output=dict),
        }
        violations = ContractValidator(dag, contracts).validate()
        assert violations == []

    def test_subclass_is_compatible(self):
        dag = DAG()
        dag.add_nodes(["a", "b"])
        dag.add_edge("a", "b")
        # bool is a subclass of int
        contracts = {
            "a": NodeContract(output=bool),
            "b": NodeContract(inputs={"a": int}, output=str),
        }
        violations = ContractValidator(dag, contracts).validate()
        assert violations == []

    def test_incompatible_types_produces_violation(self):
        dag = DAG()
        dag.add_nodes(["a", "b"])
        dag.add_edge("a", "b")
        contracts = {
            "a": NodeContract(output=str),
            "b": NodeContract(inputs={"a": int}, output=dict),
        }
        violations = ContractValidator(dag, contracts).validate()
        assert len(violations) == 1
        assert violations[0].from_node == "a"
        assert violations[0].to_node == "b"
        assert "str" in violations[0].message
        assert "int" in violations[0].message

    def test_object_wildcard_skips_check(self):
        dag = DAG()
        dag.add_nodes(["a", "b"])
        dag.add_edge("a", "b")
        # object on either side acts as wildcard
        contracts = {
            "a": NodeContract(output=object),
            "b": NodeContract(inputs={"a": int}, output=dict),
        }
        violations = ContractValidator(dag, contracts).validate()
        assert violations == []

    def test_object_input_wildcard(self):
        dag = DAG()
        dag.add_nodes(["a", "b"])
        dag.add_edge("a", "b")
        contracts = {
            "a": NodeContract(output=str),
            "b": NodeContract(inputs={"a": object}, output=dict),
        }
        violations = ContractValidator(dag, contracts).validate()
        assert violations == []

    def test_missing_contract_is_ignored(self):
        dag = DAG()
        dag.add_nodes(["a", "b"])
        dag.add_edge("a", "b")
        # Only b has a contract, a does not
        contracts = {
            "b": NodeContract(inputs={"a": int}, output=dict),
        }
        violations = ContractValidator(dag, contracts).validate()
        assert violations == []

    def test_diamond_dag_multiple_violations(self):
        dag = DAG()
        dag.add_nodes(["a", "b", "c", "d"])
        dag.add_edge("a", "b")
        dag.add_edge("a", "c")
        dag.add_edge("b", "d")
        dag.add_edge("c", "d")
        contracts = {
            "a": NodeContract(output=str),
            "b": NodeContract(inputs={"a": int}, output=list),
            "c": NodeContract(inputs={"a": float}, output=dict),
            "d": NodeContract(inputs={"b": list, "c": dict}, output=str),
        }
        violations = ContractValidator(dag, contracts).validate()
        # a->b (str vs int) and a->c (str vs float) should fail
        assert len(violations) == 2
        from_to_pairs = {(v.from_node, v.to_node) for v in violations}
        assert ("a", "b") in from_to_pairs
        assert ("a", "c") in from_to_pairs

    def test_empty_dag_no_violations(self):
        dag = DAG()
        violations = ContractValidator(dag, {}).validate()
        assert violations == []


class TestExtractContracts:
    def test_extract_from_annotated_tasks(self):
        @dagron.task
        def fetch() -> list:
            return [1, 2, 3]

        @dagron.task
        def process(fetch: list) -> dict:
            return {"data": fetch}

        pipeline = Pipeline([fetch, process])
        contracts = extract_contracts(pipeline)

        assert "fetch" in contracts
        assert contracts["fetch"].output is list
        assert contracts["fetch"].inputs == {}

        assert "process" in contracts
        assert contracts["process"].output is dict
        assert contracts["process"].inputs == {"fetch": list}

    def test_extract_unannotated_uses_object(self):
        @dagron.task
        def raw():
            return 42

        pipeline = Pipeline([raw])
        contracts = extract_contracts(pipeline)
        assert contracts["raw"].output is object


class TestValidateContracts:
    def test_valid_pipeline_no_violations(self):
        @dagron.task
        def source() -> list:
            return []

        @dagron.task
        def sink(source: list) -> dict:
            return {}

        pipeline = Pipeline([source, sink])
        violations = validate_contracts(pipeline)
        assert violations == []

    def test_invalid_pipeline_detects_mismatch(self):
        @dagron.task
        def produce() -> str:
            return "hello"

        @dagron.task
        def consume(produce: int) -> None:
            pass

        pipeline = Pipeline([produce, consume])
        violations = validate_contracts(pipeline)
        assert len(violations) == 1
        assert violations[0].from_node == "produce"
        assert violations[0].to_node == "consume"

    def test_extra_contracts_override(self):
        @dagron.task
        def source() -> str:
            return "hello"

        @dagron.task
        def sink(source: int) -> None:
            pass

        pipeline = Pipeline([source, sink])
        # Override source's output to int to fix the mismatch
        extra = {"source": NodeContract(output=int)}
        violations = validate_contracts(pipeline, extra_contracts=extra)
        assert violations == []


class TestPipelineValidateContracts:
    def test_pipeline_convenience_method(self):
        @dagron.task
        def a() -> list:
            return []

        @dagron.task
        def b(a: list) -> dict:
            return {}

        pipeline = Pipeline([a, b])
        violations = pipeline.validate_contracts()
        assert violations == []


class TestDAGBuilderContracts:
    def test_contract_fluent_api(self):
        builder = (
            DAGBuilder()
            .add_node("a")
            .add_node("b")
            .add_edge("a", "b")
            .contract("a", output=list)
            .contract("b", inputs={"a": list}, output=dict)
        )
        violations = builder.validate_contracts()
        assert violations == []

    def test_builder_detects_violation(self):
        builder = (
            DAGBuilder()
            .add_node("a")
            .add_node("b")
            .add_edge("a", "b")
            .contract("a", output=str)
            .contract("b", inputs={"a": int}, output=dict)
        )
        violations = builder.validate_contracts()
        assert len(violations) == 1
        assert violations[0].from_node == "a"
        assert violations[0].to_node == "b"


class TestGenericTypeValidation:
    def test_list_int_vs_list_str_violation(self):
        dag = DAG()
        dag.add_nodes(["a", "b"])
        dag.add_edge("a", "b")
        contracts = {
            "a": NodeContract(output=list[int]),
            "b": NodeContract(inputs={"a": list[str]}, output=object),
        }
        violations = ContractValidator(dag, contracts).validate()
        assert len(violations) == 1

    def test_list_int_vs_list_int_passes(self):
        dag = DAG()
        dag.add_nodes(["a", "b"])
        dag.add_edge("a", "b")
        contracts = {
            "a": NodeContract(output=list[int]),
            "b": NodeContract(inputs={"a": list[int]}, output=object),
        }
        violations = ContractValidator(dag, contracts).validate()
        assert violations == []

    def test_dict_str_int_vs_dict_str_str_violation(self):
        dag = DAG()
        dag.add_nodes(["a", "b"])
        dag.add_edge("a", "b")
        contracts = {
            "a": NodeContract(output=dict[str, int]),
            "b": NodeContract(inputs={"a": dict[str, str]}, output=object),
        }
        violations = ContractValidator(dag, contracts).validate()
        assert len(violations) == 1

    def test_optional_str_accepts_str(self):
        dag = DAG()
        dag.add_nodes(["a", "b"])
        dag.add_edge("a", "b")
        contracts = {
            "a": NodeContract(output=str),
            "b": NodeContract(inputs={"a": str | None}, output=object),
        }
        violations = ContractValidator(dag, contracts).validate()
        assert violations == []

    def test_optional_str_rejects_int(self):
        dag = DAG()
        dag.add_nodes(["a", "b"])
        dag.add_edge("a", "b")
        contracts = {
            "a": NodeContract(output=int),
            "b": NodeContract(inputs={"a": str | None}, output=object),
        }
        violations = ContractValidator(dag, contracts).validate()
        assert len(violations) == 1

    def test_union_int_str_accepts_int(self):
        from typing import Union

        dag = DAG()
        dag.add_nodes(["a", "b"])
        dag.add_edge("a", "b")
        contracts = {
            "a": NodeContract(output=int),
            "b": NodeContract(inputs={"a": Union[int, str]}, output=object),
        }
        violations = ContractValidator(dag, contracts).validate()
        assert violations == []

    def test_union_int_str_rejects_float(self):
        from typing import Union

        dag = DAG()
        dag.add_nodes(["a", "b"])
        dag.add_edge("a", "b")
        contracts = {
            "a": NodeContract(output=float),
            "b": NodeContract(inputs={"a": Union[int, str]}, output=object),
        }
        violations = ContractValidator(dag, contracts).validate()
        assert len(violations) == 1

    def test_bare_list_vs_list_int_passes(self):
        dag = DAG()
        dag.add_nodes(["a", "b"])
        dag.add_edge("a", "b")
        contracts = {
            "a": NodeContract(output=list),
            "b": NodeContract(inputs={"a": list[int]}, output=object),
        }
        violations = ContractValidator(dag, contracts).validate()
        assert violations == []

    def test_nested_generics_violation(self):
        dag = DAG()
        dag.add_nodes(["a", "b"])
        dag.add_edge("a", "b")
        contracts = {
            "a": NodeContract(output=list[dict[str, int]]),
            "b": NodeContract(inputs={"a": list[dict[str, str]]}, output=object),
        }
        violations = ContractValidator(dag, contracts).validate()
        assert len(violations) == 1

    def test_list_bool_vs_list_int_passes(self):
        """bool is a subclass of int, so list[bool] should be compatible with list[int]."""
        dag = DAG()
        dag.add_nodes(["a", "b"])
        dag.add_edge("a", "b")
        contracts = {
            "a": NodeContract(output=list[bool]),
            "b": NodeContract(inputs={"a": list[int]}, output=object),
        }
        violations = ContractValidator(dag, contracts).validate()
        assert violations == []
