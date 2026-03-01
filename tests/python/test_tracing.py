"""Tests for execution tracing."""

import json

from dagron import DAG, DAGExecutor, ExecutionTrace, TraceEventType


def test_tracing_disabled_by_default():
    dag = DAG()
    dag.add_node("a")
    executor = DAGExecutor(dag)
    result = executor.execute({"a": lambda: 1})
    assert result.trace is None


def test_tracing_enabled():
    dag = DAG()
    dag.add_nodes(["a", "b"])
    dag.add_edge("a", "b")
    executor = DAGExecutor(dag, enable_tracing=True)
    result = executor.execute({"a": lambda: 1, "b": lambda: 2})
    assert result.trace is not None
    assert isinstance(result.trace, ExecutionTrace)


def test_trace_event_sequence():
    dag = DAG()
    dag.add_nodes(["a", "b"])
    dag.add_edge("a", "b")
    executor = DAGExecutor(dag, enable_tracing=True)
    result = executor.execute({"a": lambda: 1, "b": lambda: 2})

    events = result.trace.events
    event_types = [e.event_type for e in events]

    assert event_types[0] == TraceEventType.EXECUTION_STARTED
    assert event_types[-1] == TraceEventType.EXECUTION_COMPLETED
    assert TraceEventType.NODE_STARTED in event_types
    assert TraceEventType.NODE_COMPLETED in event_types
    assert TraceEventType.STEP_STARTED in event_types
    assert TraceEventType.STEP_COMPLETED in event_types


def test_events_for_node():
    dag = DAG()
    dag.add_nodes(["a", "b"])
    dag.add_edge("a", "b")
    executor = DAGExecutor(dag, enable_tracing=True)
    result = executor.execute({"a": lambda: 1, "b": lambda: 2})

    a_events = result.trace.events_for_node("a")
    assert len(a_events) >= 2  # at least STARTED and COMPLETED
    assert all(e.node_name == "a" for e in a_events)


def test_trace_to_json():
    dag = DAG()
    dag.add_node("a")
    executor = DAGExecutor(dag, enable_tracing=True)
    result = executor.execute({"a": lambda: 42})

    json_str = result.trace.to_json()
    data = json.loads(json_str)
    assert isinstance(data, list)
    assert len(data) > 0
    assert "event_type" in data[0]
    assert "timestamp" in data[0]


def test_trace_to_chrome_trace():
    dag = DAG()
    dag.add_nodes(["a", "b"])
    dag.add_edge("a", "b")
    executor = DAGExecutor(dag, enable_tracing=True)
    result = executor.execute({"a": lambda: 1, "b": lambda: 2})

    chrome_json = result.trace.to_chrome_trace()
    data = json.loads(chrome_json)
    assert "traceEvents" in data
    events = data["traceEvents"]
    assert len(events) > 0
    # Should have B/E pairs for nodes
    phases = {e["ph"] for e in events}
    assert "B" in phases
    assert "E" in phases


def test_trace_summary():
    dag = DAG()
    dag.add_nodes(["a", "b"])
    dag.add_edge("a", "b")
    executor = DAGExecutor(dag, enable_tracing=True)
    result = executor.execute({"a": lambda: 1, "b": lambda: 2})

    summary = result.trace.summary()
    assert "Execution Trace Summary" in summary
    assert "Completed: 2" in summary


def test_trace_with_failure():
    dag = DAG()
    dag.add_nodes(["a", "b"])
    dag.add_edge("a", "b")

    def fail():
        raise ValueError("boom")

    executor = DAGExecutor(dag, enable_tracing=True)
    result = executor.execute({"a": fail, "b": lambda: 2})

    events = result.trace.events
    event_types = [e.event_type for e in events]
    assert TraceEventType.NODE_FAILED in event_types

    failed_events = [e for e in events if e.event_type == TraceEventType.NODE_FAILED]
    assert len(failed_events) == 1
    assert failed_events[0].error is not None
    assert "boom" in failed_events[0].error


def test_trace_timestamps_are_monotonic():
    dag = DAG()
    dag.add_nodes(["a", "b", "c"])
    dag.add_edge("a", "b")
    dag.add_edge("b", "c")
    executor = DAGExecutor(dag, enable_tracing=True)
    result = executor.execute({"a": lambda: 1, "b": lambda: 2, "c": lambda: 3})

    events = result.trace.events
    for i in range(1, len(events)):
        assert events[i].timestamp >= events[i - 1].timestamp
