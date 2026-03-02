use std::sync::RwLock;
use std::time::Instant;

use indexmap::IndexMap;
use serde::Serialize;
use tokio::sync::broadcast;

// ---------------------------------------------------------------------------
// Public types (all Serialize for JSON snapshot)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
pub struct NodeState {
    pub name: String,
    pub status: String,
    pub started_at: Option<f64>,
    pub duration_seconds: Option<f64>,
    pub error: Option<String>,
}

impl NodeState {
    fn new(name: String) -> Self {
        Self {
            name,
            status: "pending".into(),
            started_at: None,
            duration_seconds: None,
            error: None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ExecutionProfile {
    pub total_duration_seconds: f64,
    pub succeeded: u32,
    pub failed: u32,
    pub skipped: u32,
    pub timed_out: u32,
    pub cancelled: u32,
}

/// SSE events — serde-tagged enum produces the same JSON shape as the Python
/// dashboard's `_emit()` calls.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SseEvent {
    ExecutionStarted {
        dag_dot: String,
        nodes: Vec<String>,
        edges: Vec<(String, String)>,
        started_at: f64,
    },
    NodeStarted {
        name: String,
        timestamp: f64,
    },
    NodeFinished {
        name: String,
        status: String,
        duration_seconds: Option<f64>,
        error: Option<String>,
    },
    ExecutionFinished(ExecutionProfile),
    /// Sent when a subscriber is lagged and should re-fetch full state.
    Lagged,
}

// ---------------------------------------------------------------------------
// Snapshot (returned by GET /api/state)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
pub struct StateSnapshot {
    pub dag_dot: String,
    pub dag_nodes: Vec<String>,
    pub dag_edges: Vec<(String, String)>,
    pub is_running: bool,
    pub started_at: Option<f64>,
    pub nodes: Vec<NodeState>,
    pub profile: Option<ExecutionProfile>,
    pub waiting_gates: Vec<String>,
}

// ---------------------------------------------------------------------------
// Internal mutable state behind RwLock
// ---------------------------------------------------------------------------

struct Inner {
    dag_dot: String,
    dag_nodes: Vec<String>,
    dag_edges: Vec<(String, String)>,
    is_running: bool,
    started_at: Option<f64>,
    /// Monotonic reference for computing durations (not serialized).
    mono_base: Option<Instant>,
    nodes: IndexMap<String, NodeState>,
    profile: Option<ExecutionProfile>,
    waiting_gates: Vec<String>,
}

impl Inner {
    fn new() -> Self {
        Self {
            dag_dot: String::new(),
            dag_nodes: Vec::new(),
            dag_edges: Vec::new(),
            is_running: false,
            started_at: None,
            mono_base: None,
            nodes: IndexMap::new(),
            profile: None,
            waiting_gates: Vec::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// DashboardState — thread-safe, broadcast-capable
// ---------------------------------------------------------------------------

/// Channel capacity for the broadcast SSE fan-out.
const BROADCAST_CAPACITY: usize = 256;

pub struct DashboardState {
    inner: RwLock<Inner>,
    tx: broadcast::Sender<SseEvent>,
}

impl DashboardState {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(BROADCAST_CAPACITY);
        Self {
            inner: RwLock::new(Inner::new()),
            tx,
        }
    }

    // -- Hook handlers ----------------------------------------------------

    /// PRE_EXECUTE: snapshot the DAG and init all nodes to "pending".
    pub fn reset(
        &self,
        dag_dot: String,
        nodes: Vec<String>,
        edges: Vec<(String, String)>,
    ) {
        let now_wall = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();
        let now_mono = Instant::now();

        let node_map: IndexMap<String, NodeState> = nodes
            .iter()
            .map(|n| (n.clone(), NodeState::new(n.clone())))
            .collect();

        {
            let mut w = self.inner.write().unwrap();
            w.dag_dot = dag_dot.clone();
            w.dag_nodes = nodes.clone();
            w.dag_edges = edges.clone();
            w.is_running = true;
            w.started_at = Some(now_wall);
            w.mono_base = Some(now_mono);
            w.profile = None;
            w.nodes = node_map;
        }

        let _ = self.tx.send(SseEvent::ExecutionStarted {
            dag_dot,
            nodes,
            edges,
            started_at: now_wall,
        });
    }

    /// PRE_NODE: mark a node as running.
    pub fn node_started(&self, name: &str) {
        let ts = Instant::now();
        let mono_secs;
        {
            let mut w = self.inner.write().unwrap();
            mono_secs = w.mono_base.map(|b| ts.duration_since(b).as_secs_f64());
            if let Some(ns) = w.nodes.get_mut(name) {
                ns.status = "running".into();
                ns.started_at = mono_secs;
            }
        }

        let _ = self.tx.send(SseEvent::NodeStarted {
            name: name.into(),
            timestamp: mono_secs.unwrap_or(0.0),
        });
    }

    /// POST_NODE / ON_ERROR: record outcome and duration.
    pub fn node_finished(&self, name: &str, status: &str, error: Option<&str>) {
        let ts = Instant::now();
        let mut duration: Option<f64> = None;
        {
            let mut w = self.inner.write().unwrap();
            let mono_base = w.mono_base;
            if let Some(ns) = w.nodes.get_mut(name) {
                ns.status = status.into();
                ns.error = error.map(|e| e.into());
                if let (Some(mono_base), Some(started_at)) = (mono_base, ns.started_at) {
                    let now_mono = ts.duration_since(mono_base).as_secs_f64();
                    let d = now_mono - started_at;
                    ns.duration_seconds = Some(d);
                    duration = Some(d);
                }
            }
        }

        let _ = self.tx.send(SseEvent::NodeFinished {
            name: name.into(),
            status: status.into(),
            duration_seconds: duration,
            error: error.map(|e| e.into()),
        });
    }

    /// POST_EXECUTE: compute summary profile and store it.
    pub fn execution_finished(
        &self,
        total_duration: f64,
        succeeded: u32,
        failed: u32,
        skipped: u32,
        timed_out: u32,
        cancelled: u32,
    ) {
        let profile = ExecutionProfile {
            total_duration_seconds: total_duration,
            succeeded,
            failed,
            skipped,
            timed_out,
            cancelled,
        };

        {
            let mut w = self.inner.write().unwrap();
            w.is_running = false;
            w.profile = Some(profile.clone());
        }

        let _ = self.tx.send(SseEvent::ExecutionFinished(profile));
    }

    // -- Read-only accessors -----------------------------------------------

    /// Full JSON-serialisable state for GET /api/state.
    pub fn snapshot(&self) -> StateSnapshot {
        let r = self.inner.read().unwrap();
        StateSnapshot {
            dag_dot: r.dag_dot.clone(),
            dag_nodes: r.dag_nodes.clone(),
            dag_edges: r.dag_edges.clone(),
            is_running: r.is_running,
            started_at: r.started_at,
            nodes: r.nodes.values().cloned().collect(),
            profile: r.profile.clone(),
            waiting_gates: r.waiting_gates.clone(),
        }
    }

    /// Profile for GET /api/profile (None before execution finishes).
    pub fn profile(&self) -> Option<ExecutionProfile> {
        self.inner.read().unwrap().profile.clone()
    }

    /// Subscribe to the SSE event stream.
    pub fn subscribe(&self) -> broadcast::Receiver<SseEvent> {
        self.tx.subscribe()
    }

    // -- Gate helpers -------------------------------------------------------

    pub fn set_waiting_gates(&self, gates: Vec<String>) {
        self.inner.write().unwrap().waiting_gates = gates;
    }

    pub fn waiting_gates(&self) -> Vec<String> {
        self.inner.read().unwrap().waiting_gates.clone()
    }
}

impl Default for DashboardState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reset_populates_nodes_as_pending() {
        let state = DashboardState::new();
        state.reset(
            "digraph{}".into(),
            vec!["a".into(), "b".into(), "c".into()],
            vec![("a".into(), "b".into()), ("b".into(), "c".into())],
        );

        let snap = state.snapshot();
        assert!(snap.is_running);
        assert!(snap.started_at.is_some());
        assert_eq!(snap.dag_nodes.len(), 3);
        assert_eq!(snap.nodes.len(), 3);
        for ns in &snap.nodes {
            assert_eq!(ns.status, "pending");
        }
    }

    #[test]
    fn node_started_marks_running() {
        let state = DashboardState::new();
        state.reset("digraph{}".into(), vec!["a".into()], vec![]);
        state.node_started("a");

        let snap = state.snapshot();
        assert_eq!(snap.nodes[0].status, "running");
        assert!(snap.nodes[0].started_at.is_some());
    }

    #[test]
    fn node_finished_computes_duration() {
        let state = DashboardState::new();
        state.reset("digraph{}".into(), vec!["a".into()], vec![]);
        state.node_started("a");
        state.node_finished("a", "completed", None);

        let snap = state.snapshot();
        assert_eq!(snap.nodes[0].status, "completed");
        assert!(snap.nodes[0].duration_seconds.is_some());
        assert!(snap.nodes[0].duration_seconds.unwrap() >= 0.0);
    }

    #[test]
    fn node_finished_with_error() {
        let state = DashboardState::new();
        state.reset("digraph{}".into(), vec!["b".into()], vec![]);
        state.node_started("b");
        state.node_finished("b", "failed", Some("boom"));

        let snap = state.snapshot();
        assert_eq!(snap.nodes[0].status, "failed");
        assert_eq!(snap.nodes[0].error.as_deref(), Some("boom"));
    }

    #[test]
    fn execution_finished_stores_profile() {
        let state = DashboardState::new();
        state.reset("digraph{}".into(), vec!["a".into()], vec![]);
        state.execution_finished(1.5, 2, 1, 0, 0, 0);

        assert!(!state.snapshot().is_running);
        let p = state.profile().unwrap();
        assert_eq!(p.succeeded, 2);
        assert_eq!(p.failed, 1);
        assert!((p.total_duration_seconds - 1.5).abs() < 1e-9);
    }

    #[test]
    fn profile_none_before_execution() {
        let state = DashboardState::new();
        assert!(state.profile().is_none());
        assert!(state.snapshot().profile.is_none());
    }

    #[test]
    fn snapshot_is_json_serializable() {
        let state = DashboardState::new();
        state.reset("digraph{}".into(), vec!["x".into()], vec![]);
        let snap = state.snapshot();
        serde_json::to_string(&snap).unwrap();
    }

    #[test]
    fn broadcast_delivers_events() {
        let state = DashboardState::new();
        let mut rx = state.subscribe();

        state.reset("digraph{}".into(), vec!["a".into()], vec![]);
        let evt = rx.try_recv().unwrap();
        assert!(matches!(evt, SseEvent::ExecutionStarted { .. }));

        state.node_started("a");
        let evt = rx.try_recv().unwrap();
        assert!(matches!(evt, SseEvent::NodeStarted { .. }));

        state.node_finished("a", "completed", None);
        let evt = rx.try_recv().unwrap();
        assert!(matches!(evt, SseEvent::NodeFinished { .. }));

        state.execution_finished(0.5, 1, 0, 0, 0, 0);
        let evt = rx.try_recv().unwrap();
        assert!(matches!(evt, SseEvent::ExecutionFinished(_)));
    }

    #[test]
    fn thread_safety_under_concurrent_writes() {
        let state = std::sync::Arc::new(DashboardState::new());
        let names: Vec<String> = (0..50).map(|i| format!("n{i}")).collect();
        state.reset("digraph{}".into(), names.clone(), vec![]);

        let mut handles = Vec::new();
        for name in &names {
            let s = state.clone();
            let n = name.clone();
            handles.push(std::thread::spawn(move || {
                s.node_started(&n);
                s.node_finished(&n, "completed", None);
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        let snap = state.snapshot();
        assert_eq!(snap.nodes.len(), 50);
        for ns in &snap.nodes {
            assert_eq!(ns.status, "completed");
        }
    }

    #[test]
    fn waiting_gates() {
        let state = DashboardState::new();
        assert!(state.waiting_gates().is_empty());
        state.set_waiting_gates(vec!["deploy".into(), "review".into()]);
        assert_eq!(state.waiting_gates(), vec!["deploy", "review"]);
    }
}
