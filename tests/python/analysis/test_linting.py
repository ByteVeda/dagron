"""Tests for DAG linting and schema validation."""

from dagron import DAG, DAGBuilder, DAGSchema, LintReport


class TestLint:
    def test_empty_dag(self):
        dag = DAG()
        report = dag.lint()
        assert isinstance(report, LintReport)
        assert report.ok
        assert report.info_count == 1  # EMPTY_GRAPH

    def test_clean_dag(self):
        dag = DAGBuilder().add_node("a").add_node("b").add_edge("a", "b").build()
        report = dag.lint()
        assert report.ok

    def test_disconnected_components(self):
        dag = DAG()
        dag.add_nodes(["a", "b", "c", "d"])
        dag.add_edge("a", "b")
        dag.add_edge("c", "d")
        report = dag.lint()
        codes = [w.code for w in report.warnings]
        assert "DISCONNECTED" in codes

    def test_high_fan_in(self):
        dag = DAG()
        dag.add_node("target")
        for i in range(12):
            name = f"source_{i}"
            dag.add_node(name)
            dag.add_edge(name, "target")
        report = dag.lint(max_fan_in=10)
        codes = [w.code for w in report.warnings]
        assert "HIGH_FAN_IN" in codes

    def test_high_fan_out(self):
        dag = DAG()
        dag.add_node("source")
        for i in range(12):
            name = f"target_{i}"
            dag.add_node(name)
            dag.add_edge("source", name)
        report = dag.lint(max_fan_out=10)
        codes = [w.code for w in report.warnings]
        assert "HIGH_FAN_OUT" in codes

    def test_excessive_depth(self):
        dag = DAG()
        prev = "node_0"
        dag.add_node(prev)
        for i in range(1, 55):
            name = f"node_{i}"
            dag.add_node(name)
            dag.add_edge(prev, name)
            prev = name
        report = dag.lint(max_depth=50)
        codes = [w.code for w in report.warnings]
        assert "EXCESSIVE_DEPTH" in codes

    def test_isolated_nodes(self):
        dag = DAG()
        dag.add_nodes(["a", "b", "c"])
        dag.add_edge("a", "b")
        # "c" is isolated
        report = dag.lint()
        codes = [w.code for w in report.warnings]
        assert "ISOLATED_NODES" in codes

    def test_redundant_edges(self):
        dag = DAG()
        dag.add_nodes(["a", "b", "c"])
        dag.add_edge("a", "b")
        dag.add_edge("b", "c")
        dag.add_edge("a", "c")  # redundant
        report = dag.lint()
        codes = [w.code for w in report.warnings]
        assert "REDUNDANT_EDGES" in codes

    def test_summary(self):
        dag = DAG()
        report = dag.lint()
        summary = report.summary()
        assert "Lint Report" in summary

    def test_custom_thresholds(self):
        dag = DAG()
        dag.add_node("hub")
        for i in range(5):
            name = f"s_{i}"
            dag.add_node(name)
            dag.add_edge(name, "hub")
        # With default threshold 10, this is fine
        report = dag.lint(max_fan_in=10)
        assert not any(w.code == "HIGH_FAN_IN" for w in report.warnings)
        # With lower threshold, it's flagged
        report = dag.lint(max_fan_in=3)
        assert any(w.code == "HIGH_FAN_IN" for w in report.warnings)


class TestDAGSchema:
    def test_single_root_pass(self):
        dag = DAGBuilder().add_node("a").add_node("b").add_edge("a", "b").build()
        schema = DAGSchema(single_root=True)
        errors = schema.validate(dag)
        assert errors == []

    def test_single_root_fail(self):
        dag = DAG()
        dag.add_nodes(["a", "b", "c"])
        dag.add_edge("a", "c")
        dag.add_edge("b", "c")
        schema = DAGSchema(single_root=True)
        errors = schema.validate(dag)
        assert any("single root" in e for e in errors)

    def test_max_depth(self):
        dag = DAG()
        dag.add_nodes(["a", "b", "c", "d"])
        dag.add_edges([("a", "b"), ("b", "c"), ("c", "d")])
        schema = DAGSchema(max_depth=2)
        errors = schema.validate(dag)
        assert any("Depth" in e for e in errors)

    def test_required_nodes(self):
        dag = DAG()
        dag.add_nodes(["a", "b"])
        dag.add_edge("a", "b")
        schema = DAGSchema(required_nodes=["a", "b", "c"])
        errors = schema.validate(dag)
        assert any("'c' not found" in e for e in errors)

    def test_forbidden_nodes(self):
        dag = DAG()
        dag.add_nodes(["a", "debug_node"])
        dag.add_edge("a", "debug_node")
        schema = DAGSchema(forbidden_nodes=["debug_node"])
        errors = schema.validate(dag)
        assert any("debug_node" in e for e in errors)

    def test_connected(self):
        dag = DAG()
        dag.add_nodes(["a", "b", "c"])
        dag.add_edge("a", "b")
        # "c" is disconnected
        schema = DAGSchema(connected=True)
        errors = schema.validate(dag)
        assert any("not connected" in e for e in errors)

    def test_leaf_pattern(self):
        dag = DAG()
        dag.add_nodes(["input_a", "process", "output_x"])
        dag.add_edges([("input_a", "process"), ("process", "output_x")])
        schema = DAGSchema(leaf_pattern="output_*")
        errors = schema.validate(dag)
        assert errors == []

    def test_leaf_pattern_fail(self):
        dag = DAG()
        dag.add_nodes(["a", "b"])
        dag.add_edge("a", "b")
        schema = DAGSchema(leaf_pattern="output_*")
        errors = schema.validate(dag)
        assert any("does not match" in e for e in errors)

    def test_min_max_nodes(self):
        dag = DAG()
        dag.add_nodes(["a", "b"])
        dag.add_edge("a", "b")
        schema = DAGSchema(min_nodes=5)
        errors = schema.validate(dag)
        assert any("below minimum" in e for e in errors)

        schema = DAGSchema(max_nodes=1)
        errors = schema.validate(dag)
        assert any("exceeds maximum" in e for e in errors)

    def test_max_in_degree(self):
        dag = DAG()
        dag.add_nodes(["a", "b", "c", "d"])
        dag.add_edges([("a", "d"), ("b", "d"), ("c", "d")])
        schema = DAGSchema(max_in_degree=2)
        errors = schema.validate(dag)
        assert any("in-degree" in e for e in errors)
