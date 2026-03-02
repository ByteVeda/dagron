pub mod html;
pub mod server;
pub mod state;

use std::sync::Arc;
use std::thread::JoinHandle;

use server::{AppState, GateCallback};
use state::DashboardState;
use tokio::sync::oneshot;

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum DashboardError {
    BindFailed(String),
    RuntimeError(String),
}

impl std::fmt::Display for DashboardError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BindFailed(e) => write!(f, "failed to bind dashboard server: {e}"),
            Self::RuntimeError(e) => write!(f, "dashboard runtime error: {e}"),
        }
    }
}

impl std::error::Error for DashboardError {}

// ---------------------------------------------------------------------------
// DashboardHandle — manages background server thread
// ---------------------------------------------------------------------------

pub struct DashboardHandle {
    app_state: Arc<AppState>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    thread: Option<JoinHandle<()>>,
    port: u16,
}

impl DashboardHandle {
    /// Start the dashboard server on a background OS thread with its own
    /// tokio runtime.  Blocks until the server is ready to accept connections.
    /// Returns `(handle, actual_port)`.
    pub fn start(host: &str, port: u16) -> Result<Self, DashboardError> {
        let dashboard = Arc::new(DashboardState::new());
        let app_state = Arc::new(AppState {
            dashboard: dashboard.clone(),
            gate_callback: std::sync::RwLock::new(None),
        });

        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<Result<u16, String>>();

        let host_owned = host.to_string();
        let app_state_clone = app_state.clone();

        let thread = std::thread::Builder::new()
            .name("dagron-dashboard".into())
            .spawn(move || {
                let rt = match tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(1)
                    .enable_all()
                    .build()
                {
                    Ok(rt) => rt,
                    Err(e) => {
                        let _ = ready_tx.send(Err(e.to_string()));
                        return;
                    }
                };

                rt.block_on(async move {
                    let router = server::build_router(app_state_clone);
                    let addr = format!("{host_owned}:{port}");
                    let listener = match tokio::net::TcpListener::bind(&addr).await {
                        Ok(l) => l,
                        Err(e) => {
                            let _ = ready_tx.send(Err(e.to_string()));
                            return;
                        }
                    };

                    let actual_port = listener.local_addr().map(|a| a.port()).unwrap_or(port);
                    let _ = ready_tx.send(Ok(actual_port));

                    axum::serve(listener, router)
                        .with_graceful_shutdown(async {
                            let _ = shutdown_rx.await;
                        })
                        .await
                        .ok();
                });
            })
            .map_err(|e| DashboardError::RuntimeError(e.to_string()))?;

        // Block until server is ready or fails to bind.
        let actual_port = ready_rx
            .recv()
            .map_err(|_| DashboardError::RuntimeError("server thread died".into()))?
            .map_err(DashboardError::BindFailed)?;

        Ok(Self {
            app_state,
            shutdown_tx: Some(shutdown_tx),
            thread: Some(thread),
            port: actual_port,
        })
    }

    /// The actual port the server is listening on (useful when `port=0`).
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Gracefully shut down the server and join the background thread.
    pub fn stop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }

    // -- State mutation API (called from executor hooks) --------------------

    pub fn reset(&self, dag_dot: String, nodes: Vec<String>, edges: Vec<(String, String)>) {
        self.app_state.dashboard.reset(dag_dot, nodes, edges);
    }

    pub fn node_started(&self, name: &str) {
        self.app_state.dashboard.node_started(name);
    }

    pub fn node_finished(&self, name: &str, status: &str, error: Option<&str>) {
        self.app_state.dashboard.node_finished(name, status, error);
    }

    pub fn execution_finished(
        &self,
        total_duration: f64,
        succeeded: u32,
        failed: u32,
        skipped: u32,
        timed_out: u32,
        cancelled: u32,
    ) {
        self.app_state
            .dashboard
            .execution_finished(total_duration, succeeded, failed, skipped, timed_out, cancelled);
    }

    pub fn set_gate_callback(&self, cb: Arc<dyn GateCallback>) {
        *self.app_state.gate_callback.write().unwrap() = Some(cb);
    }

    pub fn set_waiting_gates(&self, gates: Vec<String>) {
        self.app_state.dashboard.set_waiting_gates(gates);
    }
}

impl Drop for DashboardHandle {
    fn drop(&mut self) {
        self.stop();
    }
}
