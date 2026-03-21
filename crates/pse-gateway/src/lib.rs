//! PSE Gateway — REST API for universal PSE endpoints.
//!
//! Exposes: /health, /crystals, /observe, /navigate, /constitution, /benchmarks, /accumulation.

use std::sync::Arc;
use tokio::sync::RwLock;
use axum::{
    extract::State,
    response::Json,
    routing::get,
    Router,
};
use serde::{Deserialize, Serialize};
use tower_http::cors::CorsLayer;

/// Shared application state for the gateway.
pub struct AppState {
    pub crystal_count: usize,
    pub tick_count: u64,
    pub healthy: bool,
}

impl Default for AppState {
    fn default() -> Self {
        Self { crystal_count: 0, tick_count: 0, healthy: true }
    }
}

/// Shared state wrapper.
pub type SharedState = Arc<RwLock<AppState>>;

/// Health check response.
#[derive(Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
    pub crystal_count: usize,
    pub tick_count: u64,
}

/// Crystal list response.
#[derive(Serialize, Deserialize)]
pub struct CrystalListResponse {
    pub crystals: Vec<CrystalSummary>,
    pub total: usize,
}

/// Crystal summary for API responses.
#[derive(Serialize, Deserialize)]
pub struct CrystalSummary {
    pub id: String,
    pub stability_score: f64,
    pub created_at: u64,
    pub region_size: usize,
}

/// Build the PSE gateway router.
pub fn build_router(state: SharedState) -> Router {
    Router::new()
        .route("/health", get(health_handler))
        .route("/crystals", get(crystals_handler))
        .layer(CorsLayer::permissive())
        .with_state(state)
}

async fn health_handler(State(state): State<SharedState>) -> Json<HealthResponse> {
    let s = state.read().await;
    Json(HealthResponse {
        status: if s.healthy { "ok".to_string() } else { "degraded".to_string() },
        version: env!("CARGO_PKG_VERSION").to_string(),
        crystal_count: s.crystal_count,
        tick_count: s.tick_count,
    })
}

async fn crystals_handler(State(_state): State<SharedState>) -> Json<CrystalListResponse> {
    Json(CrystalListResponse { crystals: vec![], total: 0 })
}

/// Start the gateway server on the given address.
pub async fn serve(addr: &str, state: SharedState) -> std::io::Result<()> {
    let router = build_router(state);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("PSE gateway listening on {}", addr);
    axum::serve(listener, router).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    #[tokio::test]
    async fn health_endpoint() {
        let state = Arc::new(RwLock::new(AppState::default()));
        let app = build_router(state);
        let req = Request::builder().uri("/health").body(Body::empty()).unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn crystals_endpoint() {
        let state = Arc::new(RwLock::new(AppState::default()));
        let app = build_router(state);
        let req = Request::builder().uri("/crystals").body(Body::empty()).unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
}
