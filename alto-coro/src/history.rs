use axum::{
    extract::{Path as AxumPath, State as AxumState},
    http::{header, StatusCode},
    response::IntoResponse,
    routing::get,
    Router,
};
use commonware_formatting::from_hex;
use coro::{BatchCursor, BatchNumber};
use coro_demo::{HistoryServerConfig, SequencerHistory};
use serde::Serialize;
use std::{error::Error, net::SocketAddr, sync::Arc};
use tower_http::cors::CorsLayer;
use tracing::warn;

use crate::soft::{SoftBlock, SoftCommit, SoftStatusIndex};
use crate::util::{hex, hex29, hex32};

#[derive(Clone)]
struct HistoryApiState {
    history: Arc<dyn coro_demo::SequencerHistory>,
    soft_status: Option<Arc<SoftStatusIndex>>,
    serve_payloads: bool,
}

fn history_router(
    history: Arc<dyn coro_demo::SequencerHistory>,
    soft_status: Option<Arc<SoftStatusIndex>>,
    config: HistoryServerConfig,
) -> Router {
    let state = HistoryApiState {
        history: history.clone(),
        soft_status,
        serve_payloads: config.serve_payloads,
    };
    Router::new()
        .route("/health", get(health))
        .route("/healthz", get(health))
        .route("/head", get(history_head))
        .route("/archived-head", get(history_archived_head))
        .route("/block-head", get(block_archived_head))
        .route("/published-block-head", get(block_published_head))
        .route("/status/{sequence}", get(history_status))
        .route("/block-status/{height}", get(block_status))
        .route("/cursor/{sequence}", get(history_cursor))
        .route("/payload/{sequence}", get(history_payload))
        .route("/block/{query}", get(block_get))
        .with_state(state)
        .layer(CorsLayer::permissive())
}

pub(crate) async fn serve_history(
    history: Arc<dyn SequencerHistory>,
    soft_status: Option<Arc<SoftStatusIndex>>,
    config: HistoryServerConfig,
    bind: SocketAddr,
) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(bind).await?;
    axum::serve(listener, history_router(history, soft_status, config)).await
}

async fn health() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

#[derive(Serialize)]
struct HeadResponse {
    head: Option<u64>,
}

#[derive(Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum StatusResponse {
    Archived {
        #[serde(skip_serializing_if = "Option::is_none")]
        soft: Option<SoftConfirmResponse>,
    },
    Published {
        cursor: CursorResponse,
        #[serde(skip_serializing_if = "Option::is_none")]
        soft: Option<SoftConfirmResponse>,
        #[serde(skip_serializing_if = "Option::is_none")]
        commit: Option<CommitResponse>,
    },
}

#[derive(Serialize)]
struct CursorResponse {
    sequence: u64,
    blob_ref: BlobRefResponse,
    payload_hash: String,
}

#[derive(Serialize)]
struct BlobRefResponse {
    height: u64,
    namespace: String,
    commitment: String,
}

#[derive(Serialize)]
struct SoftConfirmResponse {
    block_timestamp_ms: u64,
    soft_confirmed_at_ms: u64,
    soft_latency_ms: u64,
}

#[derive(Serialize)]
struct CommitResponse {
    tx_hash: String,
    pfb_broadcasted_at_ms: u64,
    celestia_committed_at_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    celestia_block_time_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    publish_latency_ms: Option<u64>,
    backend_commit_latency_ms: u64,
    batch_wait_ms: u64,
    soft_to_pfb_broadcast_ms: u64,
    broadcast_latency_ms: u64,
    confirmation_wait_ms: u64,
}

impl From<BatchCursor> for CursorResponse {
    fn from(cursor: BatchCursor) -> Self {
        Self {
            sequence: cursor.sequence.0,
            blob_ref: BlobRefResponse {
                height: cursor.blob_ref.height,
                namespace: hex29(cursor.blob_ref.namespace.0),
                commitment: hex32(cursor.blob_ref.commitment.0),
            },
            payload_hash: hex(cursor.payload_hash.as_slice()),
        }
    }
}

impl From<&SoftBlock> for SoftConfirmResponse {
    fn from(block: &SoftBlock) -> Self {
        Self {
            block_timestamp_ms: block.metadata.timestamp,
            soft_confirmed_at_ms: block.soft_confirmed_at,
            soft_latency_ms: block
                .soft_confirmed_at
                .saturating_sub(block.metadata.timestamp),
        }
    }
}

impl From<SoftCommit> for CommitResponse {
    fn from(commit: SoftCommit) -> Self {
        Self {
            tx_hash: commit.tx_hash,
            pfb_broadcasted_at_ms: commit.pfb_broadcasted_at_ms,
            celestia_committed_at_ms: commit.celestia_committed_at_ms,
            celestia_block_time_ms: commit.celestia_block_time_ms,
            publish_latency_ms: commit.publish_latency_ms,
            backend_commit_latency_ms: commit.backend_commit_latency_ms,
            batch_wait_ms: commit.batch_wait_ms,
            soft_to_pfb_broadcast_ms: commit.soft_to_pfb_broadcast_ms,
            broadcast_latency_ms: commit.broadcast_latency_ms,
            confirmation_wait_ms: commit.confirmation_wait_ms,
        }
    }
}

impl From<coro::BatchStatus> for StatusResponse {
    fn from(status: coro::BatchStatus) -> Self {
        match status {
            coro::BatchStatus::Archived => Self::Archived { soft: None },
            coro::BatchStatus::Published(cursor) => Self::Published {
                cursor: cursor.into(),
                soft: None,
                commit: None,
            },
        }
    }
}

async fn history_head(AxumState(state): AxumState<HistoryApiState>) -> impl IntoResponse {
    match state.history.head().await {
        Ok(head) => axum::Json(HeadResponse {
            head: head.map(|head| head.0),
        })
        .into_response(),
        Err(error) => {
            warn!(error = %error, "failed to load published head");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

async fn history_archived_head(AxumState(state): AxumState<HistoryApiState>) -> impl IntoResponse {
    match state.history.archived_head().await {
        Ok(head) => axum::Json(HeadResponse {
            head: head.map(|head| head.0),
        })
        .into_response(),
        Err(error) => {
            warn!(error = %error, "failed to load archived head");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

async fn block_archived_head(AxumState(state): AxumState<HistoryApiState>) -> impl IntoResponse {
    match &state.soft_status {
        Some(index) => axum::Json(HeadResponse {
            head: index.archived_head().await,
        })
        .into_response(),
        None => history_archived_head(AxumState(state))
            .await
            .into_response(),
    }
}

async fn block_published_head(AxumState(state): AxumState<HistoryApiState>) -> impl IntoResponse {
    match &state.soft_status {
        Some(index) => axum::Json(HeadResponse {
            head: index.published_head().await,
        })
        .into_response(),
        None => history_head(AxumState(state)).await.into_response(),
    }
}

async fn history_status(
    AxumPath(sequence): AxumPath<u64>,
    AxumState(state): AxumState<HistoryApiState>,
) -> impl IntoResponse {
    let sequence = BatchNumber(sequence);
    match state.history.status(sequence).await {
        Ok(Some(status)) => {
            let response = match status {
                coro::BatchStatus::Archived => StatusResponse::Archived { soft: None },
                coro::BatchStatus::Published(cursor) => StatusResponse::Published {
                    cursor: cursor.into(),
                    soft: None,
                    commit: None,
                },
            };
            axum::Json(response).into_response()
        }
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(error) => {
            warn!(sequence = sequence.0, error = %error, "failed to load batch status");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

async fn block_status(
    AxumPath(height): AxumPath<u64>,
    AxumState(state): AxumState<HistoryApiState>,
) -> impl IntoResponse {
    let Some(index) = &state.soft_status else {
        return history_status(AxumPath(height), AxumState(state))
            .await
            .into_response();
    };
    let Some(sequence) = index.batch_for_height(height).await else {
        return StatusCode::NOT_FOUND.into_response();
    };
    match state.history.status(sequence).await {
        Ok(Some(status)) => {
            let soft = index
                .soft_block(height)
                .await
                .as_ref()
                .map(SoftConfirmResponse::from);
            let commit = index.commit_height(height).await;
            let response = match status {
                coro::BatchStatus::Archived => StatusResponse::Archived { soft },
                coro::BatchStatus::Published(cursor) => match commit {
                    Some(commit) => StatusResponse::Published {
                        cursor: cursor.into(),
                        soft,
                        commit: Some(commit.into()),
                    },
                    None => StatusResponse::Archived { soft },
                },
            };
            axum::Json(response).into_response()
        }
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(error) => {
            warn!(height, sequence = sequence.0, error = %error, "failed to load block status");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

async fn history_cursor(
    AxumPath(sequence): AxumPath<u64>,
    AxumState(state): AxumState<HistoryApiState>,
) -> impl IntoResponse {
    match state.history.cursor(BatchNumber(sequence)).await {
        Ok(Some(cursor)) => axum::Json(CursorResponse::from(cursor)).into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(error) => {
            warn!(sequence, error = %error, "failed to load batch cursor");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

async fn history_payload(
    AxumPath(sequence): AxumPath<u64>,
    AxumState(state): AxumState<HistoryApiState>,
) -> impl IntoResponse {
    if !state.serve_payloads {
        return StatusCode::NOT_FOUND.into_response();
    }
    match state.history.payload(BatchNumber(sequence)).await {
        Ok(Some(payload)) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/octet-stream")],
            payload,
        )
            .into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(error) => {
            warn!(sequence, error = %error, "failed to load batch payload");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

async fn block_get(
    AxumPath(query): AxumPath<String>,
    AxumState(state): AxumState<HistoryApiState>,
) -> impl IntoResponse {
    if !state.serve_payloads {
        return StatusCode::NOT_FOUND.into_response();
    }

    if let Some(index) = &state.soft_status {
        let height = match block_height_query(index, &query).await {
            Some(height) => height,
            None => return StatusCode::NOT_FOUND.into_response(),
        };
        return match index.block_payload(height).await {
            Some(payload) => (
                StatusCode::OK,
                [(header::CONTENT_TYPE, "application/octet-stream")],
                payload,
            )
                .into_response(),
            None => StatusCode::NOT_FOUND.into_response(),
        };
    }

    let sequence = match payload_sequence(&state.history, &query).await {
        Ok(Some(sequence)) => sequence,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(error) => {
            warn!(query, error = %error, "failed to resolve block query");
            return StatusCode::BAD_REQUEST.into_response();
        }
    };

    match state.history.payload(sequence).await {
        Ok(Some(payload)) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/octet-stream")],
            payload,
        )
            .into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(error) => {
            warn!(sequence = sequence.0, error = %error, "failed to load block payload");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

async fn block_height_query(index: &SoftStatusIndex, query: &str) -> Option<u64> {
    if query == "latest" {
        return index.archived_head().await;
    }
    parse_u64_query(query)
}

async fn payload_sequence(
    history: &Arc<dyn coro_demo::SequencerHistory>,
    query: &str,
) -> Result<Option<BatchNumber>, Box<dyn Error>> {
    if query == "latest" {
        return Ok(history.archived_head().await?);
    }

    if let Some(height) = parse_u64_query(query) {
        if height == 0 {
            return Ok(None);
        }
        return Ok(Some(BatchNumber(height - 1)));
    }

    Ok(None)
}

fn parse_u64_query(query: &str) -> Option<u64> {
    if let Ok(value) = query.parse::<u64>() {
        return Some(value);
    }
    let raw = from_hex(query)?;
    if raw.len() != 8 {
        return None;
    }
    let bytes: [u8; 8] = raw.try_into().ok()?;
    Some(u64::from_be_bytes(bytes))
}
