"""Tests for DAG Templates / Parameterization."""

import pytest

from dagron import DAGBuilder
from dagron.template import DAGTemplate, TemplateError, TemplateParam


class TestTemplateParam:
    def test_basic_param(self):
        p = TemplateParam(name="env", type=str, default="staging")
        assert p.name == "env"
        assert p.type is str
        assert p.default == "staging"

    def test_validate_correct_type(self):
        p = TemplateParam(name="count", type=int)
        p.validate(5)  # should not raise

    def test_validate_wrong_type(self):
        p = TemplateParam(name="count", type=int)
        with pytest.raises(TemplateError, match="expects int"):
            p.validate("hello")

    def test_validate_custom_validator(self):
        p = TemplateParam(name="x", type=int, validator=lambda v: v > 0)
        p.validate(1)  # ok
        with pytest.raises(TemplateError, match="failed custom validation"):
            p.validate(-1)

    def test_frozen(self):
        p = TemplateParam(name="x", type=str)
        with pytest.raises(AttributeError):
            p.name = "y"  # type: ignore[misc]


class TestDAGTemplate:
    def test_simple_render(self):
        t = DAGTemplate(params={"env": str})
        t.add_node("extract_{{env}}")
        t.add_node("load_{{env}}")
        t.add_edge("extract_{{env}}", "load_{{env}}")

        dag = t.render(env="prod")
        assert dag.has_node("extract_prod")
        assert dag.has_node("load_prod")
        assert dag.has_edge("extract_prod", "load_prod")

    def test_defaults(self):
        t = DAGTemplate(params={"env": str}, defaults={"env": "staging"})
        t.add_node("run_{{env}}")
        dag = t.render()  # uses default
        assert dag.has_node("run_staging")

    def test_override_default(self):
        t = DAGTemplate(params={"env": str}, defaults={"env": "staging"})
        t.add_node("run_{{env}}")
        dag = t.render(env="prod")
        assert dag.has_node("run_prod")

    def test_missing_required_param(self):
        t = DAGTemplate(params={"env": str, "date": str})
        t.add_node("run_{{env}}_{{date}}")
        with pytest.raises(TemplateError, match="Missing required parameter"):
            t.render(env="prod")  # date is missing

    def test_unknown_param(self):
        t = DAGTemplate(params={"env": str})
        t.add_node("run_{{env}}")
        with pytest.raises(TemplateError, match="Unknown parameters"):
            t.render(env="prod", foo="bar")

    def test_type_validation(self):
        t = DAGTemplate(params={"count": int})
        t.add_node("step")
        with pytest.raises(TemplateError, match="expects int"):
            t.render(count="five")

    def test_type_preserving_substitution(self):
        """Whole-string placeholder returns raw Python object."""
        t = DAGTemplate(params={"count": int})
        t.add_node("step", payload="{{count}}")
        builder = t.render_builder(count=42)
        # Check that the builder has the node with integer payload
        assert any(n[0] == "step" and n[1] == 42 for n in builder._nodes)

    def test_partial_substitution_stringifies(self):
        t = DAGTemplate(params={"date": str})
        t.add_node("run_{{date}}_v2")
        dag = t.render(date="2024-01-01")
        assert dag.has_node("run_2024-01-01_v2")

    def test_multiple_params(self):
        t = DAGTemplate(params={"env": str, "version": int})
        t.add_node("deploy_{{env}}_v{{version}}")
        dag = t.render(env="prod", version=3)
        assert dag.has_node("deploy_prod_v3")

    def test_validate_params(self):
        t = DAGTemplate(
            params={"env": str, "count": int},
            validators={"count": lambda v: v > 0},
        )
        errors = t.validate_params(env="prod", count=5)
        assert errors == []

        errors = t.validate_params(env="prod", count=-1)
        assert any("failed custom validation" in e for e in errors)

        errors = t.validate_params(env="prod")
        assert any("Missing required" in e for e in errors)

        errors = t.validate_params(env="prod", count=5, unknown="x")
        assert any("Unknown parameter" in e for e in errors)

    def test_fluent_chaining(self):
        t = DAGTemplate(params={"x": str})
        result = t.add_node("a_{{x}}").add_node("b_{{x}}").add_edge("a_{{x}}", "b_{{x}}")
        assert result is t

    def test_render_builder(self):
        t = DAGTemplate(params={"env": str})
        t.add_node("step_{{env}}")
        builder = t.render_builder(env="dev")
        assert isinstance(builder, DAGBuilder)
        # Can add more nodes before building
        builder.add_node("extra")
        dag = builder.build()
        assert dag.has_node("step_dev")
        assert dag.has_node("extra")

    def test_custom_delimiters(self):
        t = DAGTemplate(params={"env": str}, delimiters=("${", "}"))
        t.add_node("run_${env}")
        dag = t.render(env="prod")
        assert dag.has_node("run_prod")

    def test_edge_with_label(self):
        t = DAGTemplate(params={"env": str})
        t.add_node("a_{{env}}")
        t.add_node("b_{{env}}")
        t.add_edge("a_{{env}}", "b_{{env}}", weight=2.0, label="dep")
        dag = t.render(env="prod")
        assert dag.has_edge("a_prod", "b_prod")

    def test_repr(self):
        t = DAGTemplate(params={"env": str, "date": str})
        t.add_node("a")
        r = repr(t)
        assert "date" in r
        assert "env" in r
        assert "nodes=1" in r

    def test_params_property(self):
        t = DAGTemplate(params={"env": str}, defaults={"env": "dev"})
        params = t.params
        assert "env" in params
        assert params["env"].default == "dev"

    def test_empty_template(self):
        t = DAGTemplate()
        dag = t.render()
        assert dag.node_count() == 0

    def test_no_placeholder_passthrough(self):
        t = DAGTemplate(params={"env": str})
        t.add_node("static_node")
        dag = t.render(env="prod")
        assert dag.has_node("static_node")

    def test_descriptions(self):
        t = DAGTemplate(
            params={"env": str},
            descriptions={"env": "Target environment"},
        )
        assert t.params["env"].description == "Target environment"
