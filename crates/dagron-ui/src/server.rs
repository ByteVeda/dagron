use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path, State};
use axum::http::{header, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Json, Response};
use axum::routing::{get, post};
use axum::Router;
use futures::stream::Stream;
use tower_http::cors::CorsLayer;

use crate::html::DASHBOARD_HTML;
use crate::state::{DashboardState, ExecutionProfile, SseEvent};

// ---------------------------------------------------------------------------
// Gate callback trait
// ---------------------------------------------------------------------------

/// Trait that the host (Python, TS, etc.) implements to handle gate actions.
pub trait GateCallback: Send + Sync {
    fn approve(&self, name: &str) -> Result<(), String>;
    fn reject(&self, name: &str, reason: &str) -> Result<(), String>;
    fn has_gate(&self, name: &str) -> bool;
}

// ---------------------------------------------------------------------------
// Shared app state
// ---------------------------------------------------------------------------

pub struct AppState {
    pub dashboard: Arc<DashboardState>,
    pub gate_callback: std::sync::RwLock<Option<Arc<dyn GateCallback>>>,
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

pub fn build_router(app_state: Arc<AppState>) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/api/state", get(api_state))
        .route("/api/events", get(api_events))
        .route("/api/profile", get(api_profile))
        .route("/api/gates/{name}/approve", post(api_gate_approve))
        .route("/api/gates/{name}/reject", post(api_gate_reject))
        .layer(CorsLayer::permissive())
        .with_state(app_state)
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

async fn index() -> impl IntoResponse {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/html; charset=utf-8")
        .body(DASHBOARD_HTML.to_string())
        .unwrap()
}

async fn api_state(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    Json(state.dashboard.snapshot())
}

async fn api_events(
    State(state): State<Arc<AppState>>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let mut rx = state.dashboard.subscribe();

    let stream = async_stream::stream! {
        loop {
            match rx.recv().await {
                Ok(evt) => {
                    if let Ok(json) = serde_json::to_string(&evt) {
                        yield Ok(Event::default().data(json));
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                    // Client fell behind — tell it to re-fetch full state.
                    if let Ok(json) = serde_json::to_string(&SseEvent::Lagged) {
                        yield Ok(Event::default().data(json));
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    };

    Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("heartbeat"),
    )
}

async fn api_profile(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match state.dashboard.profile() {
        Some(profile) => Json::<ExecutionProfile>(profile).into_response(),
        None => StatusCode::NO_CONTENT.into_response(),
    }
}

#[derive(serde::Deserialize)]
struct GateRejectBody {
    #[serde(default)]
    reason: String,
}

async fn api_gate_approve(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let cb_guard = state.gate_callback.read().unwrap();
    let cb = match cb_guard.as_ref() {
        Some(cb) if cb.has_gate(&name) => cb,
        _ => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error": "gate not found"})),
            )
                .into_response()
        }
    };

    match cb.approve(&name) {
        Ok(()) => Json(serde_json::json!({"ok": true})).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e})),
        )
            .into_response(),
    }
}

async fn api_gate_reject(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    body: Option<Json<GateRejectBody>>,
) -> impl IntoResponse {
    let reason = body.map(|b| b.0.reason).unwrap_or_default();

    let cb_guard = state.gate_callback.read().unwrap();
    let cb = match cb_guard.as_ref() {
        Some(cb) if cb.has_gate(&name) => cb,
        _ => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error": "gate not found"})),
            )
                .into_response()
        }
    };

    match cb.reject(&name, &reason) {
        Ok(()) => Json(serde_json::json!({"ok": true})).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e})),
        )
            .into_response(),
    }
}
