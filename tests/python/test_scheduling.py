from dagron import ExecutionPlan, ExecutionStep, ScheduledNode


class TestExecutionPlan:
    def test_diamond_structure(self, diamond_dag):
        plan = diamond_dag.execution_plan()
        assert isinstance(plan, ExecutionPlan)
        assert plan.total_nodes == 4
        assert len(plan.steps) == 3
        assert plan.max_parallelism == 2

    def test_step_types(self, diamond_dag):
        plan = diamond_dag.execution_plan()
        for step in plan.steps:
            assert isinstance(step, ExecutionStep)
            for node in step.nodes:
                assert isinstance(node, ScheduledNode)
                assert hasattr(node, "node")
                assert hasattr(node, "start_time")
                assert hasattr(node, "duration")

    def test_diamond_steps(self, diamond_dag):
        plan = diamond_dag.execution_plan()
        step0_names = [n.node.name for n in plan.steps[0].nodes]
        step1_names = {n.node.name for n in plan.steps[1].nodes}
        step2_names = [n.node.name for n in plan.steps[2].nodes]
        assert step0_names == ["a"]
        assert step1_names == {"b", "c"}
        assert step2_names == ["d"]

    def test_with_costs(self, diamond_dag):
        costs = {"a": 2.0, "b": 5.0, "c": 1.0, "d": 3.0}
        plan = diamond_dag.execution_plan(costs)
        assert plan.estimated_makespan == 10.0  # a(2) + b(5) + d(3)

    def test_timing(self, diamond_dag):
        costs = {"a": 2.0, "b": 5.0, "c": 1.0, "d": 3.0}
        plan = diamond_dag.execution_plan(costs)
        # Step 0 starts at 0
        assert plan.steps[0].nodes[0].start_time == 0.0
        assert plan.steps[0].nodes[0].duration == 2.0
        # Step 1 starts at 2 (after step 0 max duration)
        for node in plan.steps[1].nodes:
            assert node.start_time == 2.0

    def test_empty(self, empty_dag):
        plan = empty_dag.execution_plan()
        assert plan.total_nodes == 0
        assert plan.steps == []
        assert plan.max_parallelism == 0

    def test_single_node(self, empty_dag):
        empty_dag.add_node("only")
        plan = empty_dag.execution_plan()
        assert plan.total_nodes == 1
        assert len(plan.steps) == 1
        assert plan.estimated_makespan == 1.0

    def test_complex_dag(self, complex_dag):
        plan = complex_dag.execution_plan()
        assert plan.total_nodes == 6
        assert plan.critical_path is not None
        assert len(plan.critical_path) > 0

    def test_repr(self, diamond_dag):
        plan = diamond_dag.execution_plan()
        assert "ExecutionPlan" in repr(plan)
        assert "ExecutionStep" in repr(plan.steps[0])
        assert "ScheduledNode" in repr(plan.steps[0].nodes[0])


class TestExecutionPlanConstrained:
    def test_single_worker(self, diamond_dag):
        plan = diamond_dag.execution_plan_constrained(1)
        assert plan.total_nodes == 4
        assert plan.max_parallelism == 1

    def test_two_workers(self, diamond_dag):
        plan = diamond_dag.execution_plan_constrained(2)
        assert plan.total_nodes == 4
        assert plan.max_parallelism == 2

    def test_unlimited_workers(self, diamond_dag):
        plan = diamond_dag.execution_plan_constrained(100)
        assert plan.total_nodes == 4
        assert plan.max_parallelism == 2  # limited by graph structure


class TestCriticalPath:
    def test_diamond_default_costs(self, diamond_dag):
        path, total = diamond_dag.critical_path()
        names = [n.name for n in path]
        assert names == ["a", "b", "d"]
        assert total == 3.0

    def test_diamond_with_costs(self, diamond_dag):
        costs = {"a": 1.0, "b": 1.0, "c": 10.0, "d": 1.0}
        path, total = diamond_dag.critical_path(costs)
        names = [n.name for n in path]
        assert names == ["a", "c", "d"]
        assert total == 12.0

    def test_empty(self, empty_dag):
        path, total = empty_dag.critical_path()
        assert path == []
        assert total == 0.0

    def test_complex_dag(self, complex_dag):
        costs = {"a": 1.0, "b": 3.0, "c": 2.0, "d": 2.0, "e": 1.0, "f": 1.0}
        path, total = complex_dag.critical_path(costs)
        names = [n.name for n in path]
        assert names == ["a", "b", "d", "f"]
        assert total == 7.0
