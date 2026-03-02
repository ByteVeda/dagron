use std::sync::Arc;

use dagron_ui::server::GateCallback;
use dagron_ui::DashboardHandle;

struct MockGateCallback {
    gates: Vec<String>,
}

impl GateCallback for MockGateCallback {
    fn approve(&self, _name: &str) -> Result<(), String> {
        Ok(())
    }
    fn reject(&self, _name: &str, _reason: &str) -> Result<(), String> {
        Ok(())
    }
    fn has_gate(&self, name: &str) -> bool {
        self.gates.iter().any(|g| g == name)
    }
}

fn start_server() -> DashboardHandle {
    DashboardHandle::start("127.0.0.1", 0).expect("server should start")
}

fn base_url(handle: &DashboardHandle) -> String {
    format!("http://127.0.0.1:{}", handle.port())
}

#[test]
fn test_index_returns_html() {
    let mut handle = start_server();
    let url = base_url(&handle);

    let resp = reqwest::blocking::get(format!("{url}/")).unwrap();
    assert_eq!(resp.status(), 200);
    let ct = resp.headers().get("content-type").unwrap().to_str().unwrap().to_string();
    assert!(ct.contains("text/html"));
    let body = resp.text().unwrap();
    assert!(body.contains("dagron"));

    handle.stop();
}

#[test]
fn test_api_state_returns_json() {
    let mut handle = start_server();
    let url = base_url(&handle);

    let resp = reqwest::blocking::get(format!("{url}/api/state")).unwrap();
    assert_eq!(resp.status(), 200);
    let data: serde_json::Value = resp.json().unwrap();
    assert!(data.get("dag_dot").is_some());
    assert!(data.get("nodes").is_some());
    assert!(data.get("is_running").is_some());

    handle.stop();
}

#[test]
fn test_api_state_after_reset() {
    let mut handle = start_server();
    let url = base_url(&handle);

    handle.reset(
        "digraph { x -> y }".into(),
        vec!["x".into(), "y".into()],
        vec![("x".into(), "y".into())],
    );

    let resp = reqwest::blocking::get(format!("{url}/api/state")).unwrap();
    let data: serde_json::Value = resp.json().unwrap();
    assert_eq!(data["is_running"], true);
    let nodes = data["dag_nodes"].as_array().unwrap();
    assert_eq!(nodes.len(), 2);

    handle.stop();
}

#[test]
fn test_api_profile_204_before_execution() {
    let mut handle = start_server();
    let url = base_url(&handle);

    let resp = reqwest::blocking::get(format!("{url}/api/profile")).unwrap();
    assert_eq!(resp.status(), 204);

    handle.stop();
}

#[test]
fn test_api_profile_200_after_execution() {
    let mut handle = start_server();
    let url = base_url(&handle);

    handle.execution_finished(0.5, 1, 0, 0, 0, 0);

    let resp = reqwest::blocking::get(format!("{url}/api/profile")).unwrap();
    assert_eq!(resp.status(), 200);
    let data: serde_json::Value = resp.json().unwrap();
    assert_eq!(data["succeeded"], 1);

    handle.stop();
}

#[test]
fn test_gate_approve_unknown_returns_404() {
    let mut handle = start_server();
    let url = base_url(&handle);

    let client = reqwest::blocking::Client::new();
    let resp = client
        .post(format!("{url}/api/gates/nope/approve"))
        .send()
        .unwrap();
    assert_eq!(resp.status(), 404);

    handle.stop();
}

#[test]
fn test_gate_reject_unknown_returns_404() {
    let mut handle = start_server();
    let url = base_url(&handle);

    let client = reqwest::blocking::Client::new();
    let resp = client
        .post(format!("{url}/api/gates/nope/reject"))
        .send()
        .unwrap();
    assert_eq!(resp.status(), 404);

    handle.stop();
}

#[test]
fn test_gate_approve_existing() {
    let mut handle = start_server();
    let url = base_url(&handle);

    handle.set_gate_callback(Arc::new(MockGateCallback {
        gates: vec!["deploy".into()],
    }));

    let client = reqwest::blocking::Client::new();
    let resp = client
        .post(format!("{url}/api/gates/deploy/approve"))
        .send()
        .unwrap();
    assert_eq!(resp.status(), 200);
    let data: serde_json::Value = resp.json().unwrap();
    assert_eq!(data["ok"], true);

    handle.stop();
}

#[test]
fn test_gate_reject_with_reason() {
    let mut handle = start_server();
    let url = base_url(&handle);

    handle.set_gate_callback(Arc::new(MockGateCallback {
        gates: vec!["deploy".into()],
    }));

    let client = reqwest::blocking::Client::new();
    let resp = client
        .post(format!("{url}/api/gates/deploy/reject"))
        .header("Content-Type", "application/json")
        .body(r#"{"reason": "not ready"}"#)
        .send()
        .unwrap();
    assert_eq!(resp.status(), 200);
    let data: serde_json::Value = resp.json().unwrap();
    assert_eq!(data["ok"], true);

    handle.stop();
}

#[test]
fn test_node_lifecycle() {
    let mut handle = start_server();
    let url = base_url(&handle);

    handle.reset(
        "digraph { a -> b }".into(),
        vec!["a".into(), "b".into()],
        vec![("a".into(), "b".into())],
    );
    handle.node_started("a");
    handle.node_finished("a", "completed", None);
    handle.node_started("b");
    handle.node_finished("b", "failed", Some("boom"));
    handle.execution_finished(1.0, 1, 1, 0, 0, 0);

    let resp = reqwest::blocking::get(format!("{url}/api/state")).unwrap();
    let data: serde_json::Value = resp.json().unwrap();
    assert_eq!(data["is_running"], false);

    let nodes = data["nodes"].as_array().unwrap();
    assert_eq!(nodes.len(), 2);

    let a = &nodes[0];
    assert_eq!(a["name"], "a");
    assert_eq!(a["status"], "completed");

    let b = &nodes[1];
    assert_eq!(b["name"], "b");
    assert_eq!(b["status"], "failed");
    assert_eq!(b["error"], "boom");

    let resp = reqwest::blocking::get(format!("{url}/api/profile")).unwrap();
    assert_eq!(resp.status(), 200);
    let profile: serde_json::Value = resp.json().unwrap();
    assert_eq!(profile["succeeded"], 1);
    assert_eq!(profile["failed"], 1);

    handle.stop();
}
