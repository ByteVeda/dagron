use std::io::Cursor;

use dagron_core::{DagronError, DAG};

fn diamond_dag() -> DAG {
    let mut dag = DAG::new();
    dag.add_node("a".into(), ()).unwrap();
    dag.add_node("b".into(), ()).unwrap();
    dag.add_node("c".into(), ()).unwrap();
    dag.add_node("d".into(), ()).unwrap();
    dag.add_edge("a", "b", None, None).unwrap();
    dag.add_edge("a", "c", None, None).unwrap();
    dag.add_edge("b", "d", None, None).unwrap();
    dag.add_edge("c", "d", None, None).unwrap();
    dag
}

// --- JSON round-trip tests ---

#[test]
fn json_round_trip_empty() {
    let dag: DAG = DAG::new();
    let json = dag.to_json(|_| None).unwrap();
    let dag2: DAG = DAG::from_json(&json, |_| ()).unwrap();
    assert_eq!(dag2.node_count(), 0);
    assert_eq!(dag2.edge_count(), 0);
}

#[test]
fn json_round_trip_diamond() {
    let dag = diamond_dag();
    let json = dag.to_json(|_| None).unwrap();
    let dag2: DAG = DAG::from_json(&json, |_| ()).unwrap();

    assert_eq!(dag2.node_count(), 4);
    assert_eq!(dag2.edge_count(), 4);
    assert!(dag2.has_node("a"));
    assert!(dag2.has_node("b"));
    assert!(dag2.has_node("c"));
    assert!(dag2.has_node("d"));
    assert!(dag2.has_edge("a", "b").unwrap());
    assert!(dag2.has_edge("a", "c").unwrap());
    assert!(dag2.has_edge("b", "d").unwrap());
    assert!(dag2.has_edge("c", "d").unwrap());
}

#[test]
fn json_round_trip_edge_weights() {
    let mut dag = DAG::new();
    dag.add_node("x".into(), ()).unwrap();
    dag.add_node("y".into(), ()).unwrap();
    dag.add_edge("x", "y", Some(3.5), None).unwrap();

    let json = dag.to_json(|_| None).unwrap();
    let dag2: DAG = DAG::from_json(&json, |_| ()).unwrap();

    // Verify weight preserved via serializable
    let sg = dag2.to_serializable(|_| None);
    assert_eq!(sg.edges.len(), 1);
    assert!((sg.edges[0].weight - 3.5).abs() < f64::EPSILON);
}

#[test]
fn json_round_trip_edge_labels() {
    let mut dag = DAG::new();
    dag.add_node("x".into(), ()).unwrap();
    dag.add_node("y".into(), ()).unwrap();
    dag.add_edge("x", "y", None, Some("depends_on".to_string()))
        .unwrap();

    let json = dag.to_json(|_| None).unwrap();
    let dag2: DAG = DAG::from_json(&json, |_| ()).unwrap();

    let sg = dag2.to_serializable(|_| None);
    assert_eq!(sg.edges[0].label.as_deref(), Some("depends_on"));
}

#[test]
fn json_payload_callback() {
    let mut dag: DAG<i32> = DAG::new();
    dag.add_node("a".into(), 10).unwrap();
    dag.add_node("b".into(), 20).unwrap();
    dag.add_edge("a", "b", None, None).unwrap();

    let json = dag
        .to_json(|p| Some(serde_json::Value::Number((*p).into())))
        .unwrap();

    let dag2: DAG<i32> = DAG::from_json(&json, |v| {
        v.and_then(|val| val.as_i64())
            .map(|n| n as i32)
            .unwrap_or(0)
    })
    .unwrap();

    assert_eq!(*dag2.get_payload("a").unwrap(), 10);
    assert_eq!(*dag2.get_payload("b").unwrap(), 20);
}

#[test]
fn json_no_payload_skips() {
    let mut dag: DAG<i32> = DAG::new();
    dag.add_node("a".into(), 42).unwrap();

    let json = dag.to_json(|_| None).unwrap();
    // Payload field should not appear in JSON
    assert!(!json.contains("payload"));
}

#[test]
fn from_json_invalid() {
    let result: Result<DAG, _> = DAG::from_json("not valid json {{{", |_| ());
    match result {
        Err(DagronError::Graph(msg)) => assert!(msg.contains("JSON deserialization failed")),
        Err(other) => panic!("Expected Graph error, got: {other:?}"),
        Ok(_) => panic!("Expected error for invalid JSON"),
    }
}

// --- DOT tests ---

#[test]
fn to_dot_diamond() {
    let dag = diamond_dag();
    let dot = dag.to_dot();

    assert!(dot.starts_with("digraph {"));
    assert!(dot.ends_with('}'));
    assert!(dot.contains("\"a\""));
    assert!(dot.contains("\"b\""));
    assert!(dot.contains("\"c\""));
    assert!(dot.contains("\"d\""));
    assert!(dot.contains("\"a\" -> \"b\""));
    assert!(dot.contains("\"a\" -> \"c\""));
    assert!(dot.contains("\"b\" -> \"d\""));
    assert!(dot.contains("\"c\" -> \"d\""));
}

#[test]
fn to_dot_with_labels() {
    let mut dag = DAG::new();
    dag.add_node("x".into(), ()).unwrap();
    dag.add_node("y".into(), ()).unwrap();
    dag.add_edge("x", "y", None, Some("dep".into())).unwrap();

    let dot = dag.to_dot();
    assert!(dot.contains("label=\"dep\""));
}

#[test]
fn to_dot_with_node_attrs() {
    let dag = diamond_dag();
    let dot = dag.to_dot_with(|name, _| {
        if name == "a" {
            Some("shape=box, color=red".to_string())
        } else {
            None
        }
    });

    assert!(dot.contains("\"a\" [shape=box, color=red]"));
    // Other nodes should not have attributes
    assert!(dot.contains("\"b\";"));
}

// --- Mermaid tests ---

#[test]
fn to_mermaid_diamond() {
    let dag = diamond_dag();
    let mermaid = dag.to_mermaid();

    assert!(mermaid.starts_with("graph TD\n"));
    assert!(mermaid.contains("a[\"a\"]"));
    assert!(mermaid.contains("b[\"b\"]"));
    assert!(mermaid.contains("a --> b"));
    assert!(mermaid.contains("a --> c"));
    assert!(mermaid.contains("b --> d"));
    assert!(mermaid.contains("c --> d"));
}

#[test]
fn to_mermaid_with_labels() {
    let mut dag = DAG::new();
    dag.add_node("x".into(), ()).unwrap();
    dag.add_node("y".into(), ()).unwrap();
    dag.add_edge("x", "y", None, Some("depends".into()))
        .unwrap();

    let mermaid = dag.to_mermaid();
    assert!(mermaid.contains("x -->|\"depends\"| y"));
}

// --- Bincode round-trip tests ---

#[test]
fn bincode_round_trip_empty() {
    let dag: DAG = DAG::new();
    let bytes = dag.to_bincode(|_| None).unwrap();
    let dag2: DAG = DAG::from_bincode(&bytes, |_| ()).unwrap();
    assert_eq!(dag2.node_count(), 0);
    assert_eq!(dag2.edge_count(), 0);
}

#[test]
fn bincode_round_trip_diamond() {
    let dag = diamond_dag();
    let bytes = dag.to_bincode(|_| None).unwrap();
    let dag2: DAG = DAG::from_bincode(&bytes, |_| ()).unwrap();

    assert_eq!(dag2.node_count(), 4);
    assert_eq!(dag2.edge_count(), 4);
    assert!(dag2.has_edge("a", "b").unwrap());
    assert!(dag2.has_edge("a", "c").unwrap());
    assert!(dag2.has_edge("b", "d").unwrap());
    assert!(dag2.has_edge("c", "d").unwrap());
}

#[test]
fn bincode_round_trip_with_payloads() {
    let mut dag: DAG<i32> = DAG::new();
    dag.add_node("a".into(), 10).unwrap();
    dag.add_node("b".into(), 20).unwrap();
    dag.add_edge("a", "b", None, None).unwrap();

    let bytes = dag
        .to_bincode(|p| Some(serde_json::Value::Number((*p).into())))
        .unwrap();

    let dag2: DAG<i32> = DAG::from_bincode(&bytes, |v| {
        v.and_then(|val| val.as_i64())
            .map(|n| n as i32)
            .unwrap_or(0)
    })
    .unwrap();

    assert_eq!(*dag2.get_payload("a").unwrap(), 10);
    assert_eq!(*dag2.get_payload("b").unwrap(), 20);
}

#[test]
fn bincode_preserves_edge_weights_and_labels() {
    let mut dag = DAG::new();
    dag.add_node("x".into(), ()).unwrap();
    dag.add_node("y".into(), ()).unwrap();
    dag.add_edge("x", "y", Some(3.5), Some("dep".into()))
        .unwrap();

    let bytes = dag.to_bincode(|_| None).unwrap();
    let dag2: DAG = DAG::from_bincode(&bytes, |_| ()).unwrap();

    let sg = dag2.to_serializable(|_| None);
    assert_eq!(sg.edges.len(), 1);
    assert!((sg.edges[0].weight - 3.5).abs() < f64::EPSILON);
    assert_eq!(sg.edges[0].label.as_deref(), Some("dep"));
}

#[test]
fn bincode_streaming_round_trip() {
    let dag = diamond_dag();

    let mut buf = Vec::new();
    dag.to_bincode_writer(&mut buf, |_| None).unwrap();

    let cursor = Cursor::new(&buf);
    let dag2: DAG = DAG::from_bincode_reader(cursor, |_| ()).unwrap();

    assert_eq!(dag2.node_count(), 4);
    assert_eq!(dag2.edge_count(), 4);
    assert!(dag2.has_edge("a", "b").unwrap());
    assert!(dag2.has_edge("c", "d").unwrap());
}

#[test]
fn bincode_streaming_round_trip_large() {
    let mut dag: DAG<i32> = DAG::new();
    let n = 10_000;
    for i in 0..n {
        dag.add_node(format!("node_{i}"), i as i32).unwrap();
    }
    for i in 0..(n - 1) {
        dag.add_edge(
            &format!("node_{i}"),
            &format!("node_{}", i + 1),
            Some(i as f64 * 0.1),
            Some(format!("edge_{i}")),
        )
        .unwrap();
    }

    let bytes = dag
        .to_bincode(|p| Some(serde_json::Value::Number((*p).into())))
        .unwrap();
    let dag2: DAG<i32> = DAG::from_bincode(&bytes, |v| {
        v.and_then(|val| val.as_i64())
            .map(|n| n as i32)
            .unwrap_or(0)
    })
    .unwrap();

    assert_eq!(dag2.node_count(), n);
    assert_eq!(dag2.edge_count(), n - 1);

    // Spot-check payloads
    assert_eq!(*dag2.get_payload("node_0").unwrap(), 0);
    assert_eq!(*dag2.get_payload("node_9999").unwrap(), 9999);

    // Spot-check edges
    assert!(dag2.has_edge("node_0", "node_1").unwrap());
    assert!(dag2.has_edge("node_9998", "node_9999").unwrap());
}

#[test]
fn bincode_deterministic_output() {
    let mut dag = DAG::new();
    dag.add_node("a".into(), ()).unwrap();
    dag.add_node("b".into(), ()).unwrap();
    dag.add_node("c".into(), ()).unwrap();
    dag.add_edge("a", "b", Some(1.5), Some("x".into())).unwrap();
    dag.add_edge("b", "c", None, None).unwrap();

    let bytes1 = dag.to_bincode(|_| None).unwrap();
    let bytes2 = dag.to_bincode(|_| None).unwrap();
    assert_eq!(
        bytes1, bytes2,
        "Two serializations of the same graph must be byte-identical"
    );
}

#[test]
fn bincode_size_matches_actual() {
    let mut dag: DAG<i32> = DAG::new();
    for i in 0..500 {
        dag.add_node(format!("n_{i}"), i).unwrap();
    }
    for i in 0..499 {
        dag.add_edge(
            &format!("n_{i}"),
            &format!("n_{}", i + 1),
            Some(i as f64),
            None,
        )
        .unwrap();
    }

    let predicted = dag
        .bincode_size(|p| Some(serde_json::Value::Number((*p).into())))
        .unwrap();
    let actual = dag
        .to_bincode(|p| Some(serde_json::Value::Number((*p).into())))
        .unwrap();
    assert_eq!(
        predicted,
        actual.len(),
        "bincode_size() must equal to_bincode().len()"
    );
}
