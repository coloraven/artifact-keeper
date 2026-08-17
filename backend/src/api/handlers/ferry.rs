//! Ferry ingest HTTP API for air-gap archive unpack.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::api::middleware::auth::AuthExtension;
use crate::api::SharedState;
use crate::error::AppError;
use crate::services::ferry_ingest_service::{self, FerryIngestProgress};

pub fn router() -> Router<SharedState> {
    Router::new()
        .route("/:key/ferry/ingest", post(start_ingest))
        .route("/:key/ferry/jobs/:artifact_id", get(get_ingest_status))
}

#[derive(Debug, Deserialize)]
pub struct StartFerryIngestRequest {
    /// Path of the uploaded ferry zip (e.g. `ak-ferry/pack.zip`).
    pub artifact_path: Option<String>,
    /// Or the artifact UUID of the ferry zip.
    pub artifact_id: Option<Uuid>,
    /// When true, run ingest inline and return the final progress (for 联调).
    #[serde(default)]
    pub wait: bool,
}

#[derive(Debug, Serialize)]
pub struct StartFerryIngestResponse {
    pub artifact_id: Uuid,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub progress: Option<FerryIngestProgress>,
}

async fn start_ingest(
    State(state): State<SharedState>,
    Extension(auth): Extension<Option<AuthExtension>>,
    Path(key): Path<String>,
    Json(body): Json<StartFerryIngestRequest>,
) -> Result<Response, Response> {
    let auth = auth
        .ok_or_else(|| AppError::Authentication("Authentication required".into()))
        .map_err(|e| e.into_response())?;
    auth.require_scope("write:artifacts")
        .map_err(|e| e.into_response())?;

    let repo_svc = state.create_repository_service();
    let repo = repo_svc
        .get_by_key(&key)
        .await
        .map_err(|e| e.into_response())?;

    let artifact_id = resolve_ferry_artifact(&state, repo.id, &body)
        .await
        .map_err(|e| e.into_response())?;

    if body.wait {
        let progress = ferry_ingest_service::run_ingest(state, repo.id, artifact_id, auth.user_id)
            .await
            .map_err(|e| e.into_response())?;
        return Ok((
            StatusCode::OK,
            Json(StartFerryIngestResponse {
                artifact_id,
                status: progress.status.as_str().to_string(),
                progress: Some(progress),
            }),
        )
            .into_response());
    }

    let queued = FerryIngestProgress {
        status: ferry_ingest_service::FerryIngestStatus::Queued,
        ..Default::default()
    };
    let _ = sqlx::query(r#"
        INSERT INTO artifact_metadata (artifact_id, format, metadata)
        VALUES ($1, 'ferry', $2)
        ON CONFLICT (artifact_id) DO UPDATE SET
            format = EXCLUDED.format,
            metadata = COALESCE(artifact_metadata.metadata, '{}'::jsonb) || EXCLUDED.metadata
        "#)
    .bind(artifact_id)
    .bind(serde_json::json!({ "ferry_ingest": queued }),
    )
    .execute(&state.db)
    .await;

    ferry_ingest_service::spawn_ingest(state, repo.id, artifact_id, auth.user_id);

    Ok((
        StatusCode::ACCEPTED,
        Json(StartFerryIngestResponse {
            artifact_id,
            status: "queued".into(),
            progress: None,
        }),
    )
        .into_response())
}

#[derive(Debug, Serialize)]
pub struct FerryJobStatusResponse {
    pub artifact_id: Uuid,
    pub progress: Option<FerryIngestProgress>,
}

async fn get_ingest_status(
    State(state): State<SharedState>,
    Extension(auth): Extension<Option<AuthExtension>>,
    Path((key, artifact_id)): Path<(String, Uuid)>,
) -> Result<Response, Response> {
    let auth = auth
        .ok_or_else(|| AppError::Authentication("Authentication required".into()))
        .map_err(|e| e.into_response())?;
    if auth.require_scope("read:artifacts").is_err() {
        auth.require_scope("write:artifacts")
            .map_err(|e| e.into_response())?;
    }

    let repo_svc = state.create_repository_service();
    let repo = repo_svc
        .get_by_key(&key)
        .await
        .map_err(|e| e.into_response())?;

    let exists = sqlx::query_scalar::<_, Uuid>("SELECT id FROM artifacts WHERE id = $1 AND repository_id = $2 AND is_deleted = false")
    .bind(artifact_id)
    .bind(repo.id)
    .fetch_optional(&state.db)
    .await
    .map_err(|e| AppError::Database(e.to_string()).into_response())?;
    if exists.is_none() {
        return Err(AppError::NotFound("Ferry artifact not found".into()).into_response());
    }

    let progress = ferry_ingest_service::read_progress(&state, artifact_id)
        .await
        .map_err(|e| e.into_response())?;

    Ok((
        StatusCode::OK,
        Json(FerryJobStatusResponse {
            artifact_id,
            progress,
        }),
    )
        .into_response())
}

async fn resolve_ferry_artifact(
    state: &SharedState,
    repository_id: Uuid,
    body: &StartFerryIngestRequest,
) -> Result<Uuid, AppError> {
    if let Some(id) = body.artifact_id {
        let path = sqlx::query_scalar::<_, String>("SELECT path FROM artifacts WHERE id = $1 AND repository_id = $2 AND is_deleted = false")
    .bind(id)
    .bind(repository_id)
        .fetch_optional(&state.db)
        .await
        .map_err(|e| AppError::Database(e.to_string()))?
        .ok_or_else(|| AppError::NotFound("Artifact not found".into()))?;
        if !ferry_ingest_service::is_ferry_archive_path(&path) {
            return Err(AppError::Validation(format!(
                "Artifact path '{path}' is not an ak-ferry archive"
            )));
        }
        return Ok(id);
    }

    let path = body
        .artifact_path
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| AppError::Validation("Provide artifact_id or artifact_path".into()))?;
    let path = path.trim_start_matches('/');
    if !ferry_ingest_service::is_ferry_archive_path(path) {
        return Err(AppError::Validation(format!(
            "Artifact path '{path}' is not an ak-ferry archive"
        )));
    }

    sqlx::query_scalar::<_, Uuid>("SELECT id FROM artifacts WHERE repository_id = $1 AND path = $2 AND is_deleted = false")
    .bind(repository_id)
    .bind(path)
    .fetch_optional(&state.db)
    .await
    .map_err(|e| AppError::Database(e.to_string()))?
    .ok_or_else(|| AppError::NotFound(format!("Artifact '{path}' not found")))
}
