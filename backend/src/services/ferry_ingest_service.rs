//! Air-gap ferry zip ingest (`ak-ferry/*.zip` from `ak artifact push --from-archive`).
//!
//! The CLI uploads the ferry archive as one generic blob. This service unpacks it,
//! reads `ak-ferry.json`, and stores each module via the same coordinate layout as
//! the Go / npm / PyPI / Cargo protocol handlers, including `PackageService` catalog
//! rows so `ak download catalog` stays accurate.

use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;

use bytes::Bytes;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use tracing::{info, warn};
use uuid::Uuid;

use crate::api::handlers::proxy_helpers;
use crate::api::SharedState;
use crate::error::{AppError, Result};
use crate::formats::pypi::PypiHandler;
use crate::models::repository::{Repository, RepositoryFormat};
use crate::services::artifact_service::{ArtifactService, ContentDigests, MultiHasher};
use crate::services::package_service::PackageService;

pub const FERRY_PATH_PREFIX: &str = "ak-ferry/";
pub const MANIFEST_JSON: &str = "ak-ferry.json";
pub const MANIFEST_JSONL: &str = "ak-ferry.jsonl";
pub const MANIFEST_KIND: &str = "ak-ferry";

/// Default extract budget for ferry zips (multi-GB air-gap packs). Override with
/// `FERRY_MAX_EXTRACTED_BYTES`.
const DEFAULT_MAX_EXTRACTED_BYTES: u64 = 50 * 1024 * 1024 * 1024;
const DEFAULT_MAX_EXTRACTED_ENTRIES: u64 = 500_000;
const FERRY_MAX_EXTRACTED_BYTES_ENV: &str = "FERRY_MAX_EXTRACTED_BYTES";
const FERRY_MAX_EXTRACTED_ENTRIES_ENV: &str = "FERRY_MAX_EXTRACTED_ENTRIES";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FerryManifest {
    pub version: u32,
    pub kind: String,
    pub ecosystem: String,
    #[serde(default)]
    pub modules: Vec<ModuleEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModuleEntry {
    pub name: String,
    #[serde(default)]
    pub name_encoded: Option<String>,
    pub version: String,
    #[serde(default)]
    pub files: Vec<FileEntry>,
    #[serde(default)]
    pub via: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileEntry {
    pub relpath: String,
    #[serde(default)]
    pub sha256: String,
    #[serde(default)]
    pub size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FerryIngestStatus {
    Queued,
    Running,
    Completed,
    Failed,
    Partial,
}

impl FerryIngestStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Partial => "partial",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FerryIngestProgress {
    pub status: FerryIngestStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ecosystem: Option<String>,
    pub done: u64,
    pub skipped: u64,
    pub failed: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

impl Default for FerryIngestProgress {
    fn default() -> Self {
        Self {
            status: FerryIngestStatus::Queued,
            ecosystem: None,
            done: 0,
            skipped: 0,
            failed: 0,
            errors: Vec::new(),
            message: None,
        }
    }
}

pub fn is_ferry_archive_path(path: &str) -> bool {
    let path = path.trim_start_matches('/');
    path.starts_with(FERRY_PATH_PREFIX)
        && path.to_ascii_lowercase().ends_with(".zip")
        && !path[FERRY_PATH_PREFIX.len()..].contains('/')
}

/// Fire-and-forget ingest after a successful generic upload of `ak-ferry/*.zip`.
pub fn spawn_ingest(
    state: SharedState,
    repository_id: Uuid,
    ferry_artifact_id: Uuid,
    user_id: Uuid,
) {
    tokio::spawn(async move {
        if let Err(e) = run_ingest(state, repository_id, ferry_artifact_id, user_id).await {
            warn!(
                "ferry ingest failed for artifact {}: {}",
                ferry_artifact_id, e
            );
        }
    });
}

pub async fn run_ingest(
    state: SharedState,
    repository_id: Uuid,
    ferry_artifact_id: Uuid,
    user_id: Uuid,
) -> Result<FerryIngestProgress> {
    let mut progress = FerryIngestProgress {
        status: FerryIngestStatus::Running,
        ..Default::default()
    };
    write_progress(&state, ferry_artifact_id, &progress).await;

    let result = ingest_inner(&state, repository_id, ferry_artifact_id, user_id, &mut progress)
        .await;

    match result {
        Ok(()) => {
            if progress.failed > 0 && progress.done > 0 {
                progress.status = FerryIngestStatus::Partial;
            } else if progress.failed > 0 {
                progress.status = FerryIngestStatus::Failed;
            } else {
                progress.status = FerryIngestStatus::Completed;
            }
            write_progress(&state, ferry_artifact_id, &progress).await;
            info!(
                "ferry ingest {}: done={} skipped={} failed={}",
                ferry_artifact_id, progress.done, progress.skipped, progress.failed
            );
            Ok(progress)
        }
        Err(e) => {
            progress.status = FerryIngestStatus::Failed;
            progress.message = Some(e.to_string());
            push_error(&mut progress, e.to_string());
            write_progress(&state, ferry_artifact_id, &progress).await;
            Err(e)
        }
    }
}

async fn ingest_inner(
    state: &SharedState,
    repository_id: Uuid,
    ferry_artifact_id: Uuid,
    user_id: Uuid,
    progress: &mut FerryIngestProgress,
) -> Result<()> {
    let repo_svc = state.create_repository_service();
    let repo = repo_svc.get_by_id(repository_id).await?;

    let storage = state
        .storage_for_repo(&repo.storage_location())
        .map_err(|e| AppError::Internal(e.to_string()))?;
    let artifact_svc = state.create_artifact_service(storage.clone());
    let ferry = artifact_svc.get_by_id(ferry_artifact_id).await?;
    if ferry.repository_id != repository_id {
        return Err(AppError::Validation(
            "Ferry artifact does not belong to this repository".into(),
        ));
    }
    if !is_ferry_archive_path(&ferry.path) {
        return Err(AppError::Validation(format!(
            "Artifact path '{}' is not an ak-ferry archive",
            ferry.path
        )));
    }

    let work = TempDir::new().map_err(|e| AppError::Internal(format!("tempdir: {e}")))?;
    let zip_path = work.path().join("ferry.zip");
    download_storage_object(storage.as_ref(), &ferry.storage_key, &zip_path).await?;

    let unpack_dir = work.path().join("unpacked");
    std::fs::create_dir_all(&unpack_dir)
        .map_err(|e| AppError::Internal(format!("mkdir unpack: {e}")))?;
    let zip_file = File::open(&zip_path)
        .map_err(|e| AppError::Internal(format!("open ferry zip: {e}")))?;
    unpack_zip_limited(
        zip_file,
        &unpack_dir,
        ferry_max_extracted_bytes(),
        ferry_max_extracted_entries(),
    )?;

    let manifest = load_manifest(&unpack_dir)?;
    if manifest.kind != MANIFEST_KIND {
        return Err(AppError::Validation(format!(
            "Unexpected ferry kind '{}'",
            manifest.kind
        )));
    }
    progress.ecosystem = Some(manifest.ecosystem.clone());
    write_progress(state, ferry_artifact_id, progress).await;

    ensure_repo_format_matches(&repo, &manifest.ecosystem)?;

    match manifest.ecosystem.as_str() {
        "go" => {
            ingest_go(
                state,
                &repo,
                ferry_artifact_id,
                &unpack_dir,
                &manifest,
                user_id,
                progress,
            )
            .await?
        }
        "npm" => {
            ingest_npm(
                state,
                &repo,
                ferry_artifact_id,
                &unpack_dir,
                &manifest,
                user_id,
                progress,
            )
            .await?
        }
        "pypi" => {
            ingest_pypi(
                state,
                &repo,
                &artifact_svc,
                ferry_artifact_id,
                &unpack_dir,
                &manifest,
                user_id,
                progress,
            )
            .await?
        }
        "cargo" => {
            ingest_cargo(
                state,
                &repo,
                ferry_artifact_id,
                &unpack_dir,
                &manifest,
                user_id,
                progress,
            )
            .await?
        }
        other => {
            return Err(AppError::Validation(format!(
                "Unsupported ferry ecosystem '{other}'"
            )));
        }
    }

    let _ = sqlx::query("UPDATE repositories SET updated_at = NOW() WHERE id = $1")
    .bind(repo.id)
    .execute(&state.db)
    .await;

    Ok(())
}

fn ensure_repo_format_matches(repo: &Repository, ecosystem: &str) -> Result<()> {
    let expected = match ecosystem {
        "go" => RepositoryFormat::Go,
        "npm" => RepositoryFormat::Npm,
        "pypi" => RepositoryFormat::Pypi,
        "cargo" => RepositoryFormat::Cargo,
        _ => {
            return Err(AppError::Validation(format!(
                "Unsupported ferry ecosystem '{ecosystem}'"
            )))
        }
    };
    if repo.format != expected {
        return Err(AppError::Validation(format!(
            "Ferry ecosystem '{ecosystem}' does not match repository format '{}'",
            repo.format.as_key()
        )));
    }
    Ok(())
}

fn load_manifest(dir: &Path) -> Result<FerryManifest> {
    let json_path = dir.join(MANIFEST_JSON);
    if json_path.is_file() {
        let text = std::fs::read_to_string(&json_path)
            .map_err(|e| AppError::Validation(format!("read {MANIFEST_JSON}: {e}")))?;
        return serde_json::from_str(&text)
            .map_err(|e| AppError::Validation(format!("invalid {MANIFEST_JSON}: {e}")));
    }

    let jsonl_path = dir.join(MANIFEST_JSONL);
    if !jsonl_path.is_file() {
        return Err(AppError::Validation(
            "Ferry archive missing ak-ferry.json / ak-ferry.jsonl".into(),
        ));
    }
    let text = std::fs::read_to_string(&jsonl_path)
        .map_err(|e| AppError::Validation(format!("read {MANIFEST_JSONL}: {e}")))?;
    let mut modules = Vec::new();
    for (i, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let entry: ModuleEntry = serde_json::from_str(line).map_err(|e| {
            AppError::Validation(format!("invalid {MANIFEST_JSONL} line {}: {e}", i + 1))
        })?;
        modules.push(entry);
    }
    if modules.is_empty() {
        return Err(AppError::Validation(
            "Ferry manifest has no modules".into(),
        ));
    }
    Ok(FerryManifest {
        version: 1,
        kind: MANIFEST_KIND.into(),
        ecosystem: guess_ecosystem_from_paths(&modules)?,
        modules,
    })
}

fn guess_ecosystem_from_paths(modules: &[ModuleEntry]) -> Result<String> {
    for m in modules {
        for f in &m.files {
            let p = f.relpath.replace('\\', "/");
            if p.starts_with("download/") || p.contains("/@v/") {
                return Ok("go".into());
            }
            if p.starts_with("npm/") || p.ends_with(".tgz") {
                return Ok("npm".into());
            }
            if p.starts_with("pypi/") || p.ends_with(".whl") || p.contains(".tar.gz") {
                return Ok("pypi".into());
            }
            if p.starts_with("cargo/") || p.ends_with(".crate") {
                return Ok("cargo".into());
            }
        }
    }
    Err(AppError::Validation(
        "Cannot infer ferry ecosystem from module paths".into(),
    ))
}

async fn download_storage_object(
    storage: &dyn crate::storage::StorageBackend,
    key: &str,
    dest: &Path,
) -> Result<()> {
    let mut stream = storage.get_stream(key).await?;
    let mut file = tokio::fs::File::create(dest)
        .await
        .map_err(|e| AppError::Internal(format!("create ferry zip file: {e}")))?;
    use tokio::io::AsyncWriteExt;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        file.write_all(&chunk)
            .await
            .map_err(|e| AppError::Internal(format!("write ferry zip: {e}")))?;
    }
    file.flush()
        .await
        .map_err(|e| AppError::Internal(format!("flush ferry zip: {e}")))?;
    Ok(())
}

fn unpack_zip_limited(
    file: File,
    dst: &Path,
    max_bytes: u64,
    max_entries: u64,
) -> Result<()> {
    let mut archive = zip::ZipArchive::new(file)
        .map_err(|e| AppError::Validation(format!("Failed to open ferry zip: {e}")))?;

    if archive.len() as u64 > max_entries {
        return Err(AppError::Validation(format!(
            "Ferry zip has too many entries (> {max_entries})"
        )));
    }

    let mut remaining = max_bytes;
    for i in 0..archive.len() {
        let mut entry = archive
            .by_index(i)
            .map_err(|e| AppError::Validation(format!("zip entry {i}: {e}")))?;
        let Some(rel) = entry.enclosed_name() else {
            continue;
        };
        let out_path = dst.join(rel);
        if entry.is_dir() {
            std::fs::create_dir_all(&out_path)
                .map_err(|e| AppError::Internal(format!("mkdir {}: {e}", out_path.display())))?;
            continue;
        }
        if entry
            .unix_mode()
            .map(|m| m & 0o170000 == 0o120000)
            .unwrap_or(false)
        {
            continue;
        }
        if let Some(parent) = out_path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| AppError::Internal(format!("mkdir {}: {e}", parent.display())))?;
        }
        let mut out = File::create(&out_path)
            .map_err(|e| AppError::Internal(format!("create {}: {e}", out_path.display())))?;
        copy_entry_bounded(&mut entry, &mut out, &mut remaining)?;
    }
    Ok(())
}

fn copy_entry_bounded<R: Read, W: Write>(
    reader: &mut R,
    writer: &mut W,
    remaining: &mut u64,
) -> Result<()> {
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = reader
            .read(&mut buf)
            .map_err(|e| AppError::Internal(format!("read zip entry: {e}")))?;
        if n == 0 {
            break;
        }
        let n_u64 = n as u64;
        if n_u64 > *remaining {
            return Err(AppError::Validation(
                "Ferry zip exceeds extract byte budget (suspected zip bomb)".into(),
            ));
        }
        *remaining -= n_u64;
        writer
            .write_all(&buf[..n])
            .map_err(|e| AppError::Internal(format!("write zip entry: {e}")))?;
    }
    Ok(())
}

fn ferry_max_extracted_bytes() -> u64 {
    positive_env_or(FERRY_MAX_EXTRACTED_BYTES_ENV, DEFAULT_MAX_EXTRACTED_BYTES)
}

fn ferry_max_extracted_entries() -> u64 {
    positive_env_or(FERRY_MAX_EXTRACTED_ENTRIES_ENV, DEFAULT_MAX_EXTRACTED_ENTRIES)
}

fn positive_env_or(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .filter(|&v| v > 0)
        .unwrap_or(default)
}

fn push_error(progress: &mut FerryIngestProgress, msg: String) {
    if progress.errors.len() < 50 {
        progress.errors.push(msg);
    }
}

async fn write_progress(state: &SharedState, artifact_id: Uuid, progress: &FerryIngestProgress) {
    let meta = serde_json::json!({ "ferry_ingest": progress });
    let _ = sqlx::query(r#"
        INSERT INTO artifact_metadata (artifact_id, format, metadata)
        VALUES ($1, 'ferry', $2)
        ON CONFLICT (artifact_id) DO UPDATE SET
            format = EXCLUDED.format,
            metadata = COALESCE(artifact_metadata.metadata, '{}'::jsonb) || EXCLUDED.metadata
        "#)
    .bind(artifact_id)
    .bind(meta)
    .execute(&state.db)
    .await;
}

pub async fn read_progress(state: &SharedState, artifact_id: Uuid) -> Result<Option<FerryIngestProgress>> {
    let metadata: Option<serde_json::Value> = sqlx::query_scalar::<_, serde_json::Value>(
        "SELECT metadata FROM artifact_metadata WHERE artifact_id = $1",
    )
    .bind(artifact_id)
    .fetch_optional(&state.db)
    .await
    .map_err(|e| AppError::Database(e.to_string()))?;

    let Some(metadata) = metadata else {
        return Ok(None);
    };
    let Some(fi) = metadata.get("ferry_ingest") else {
        return Ok(None);
    };
    let progress: FerryIngestProgress = serde_json::from_value(fi.clone())
        .map_err(|e| AppError::Internal(format!("parse ferry_ingest metadata: {e}")))?;
    Ok(Some(progress))
}

// ---------------------------------------------------------------------------
// Go
// ---------------------------------------------------------------------------

async fn ingest_go(
    state: &SharedState,
    repo: &Repository,
    ferry_artifact_id: Uuid,
    root: &Path,
    manifest: &FerryManifest,
    user_id: Uuid,
    progress: &mut FerryIngestProgress,
) -> Result<()> {
    for module in &manifest.modules {
        match store_go_module(state, repo, root, module, user_id).await {
            Ok(StoreOutcome::Stored) => progress.done += 1,
            Ok(StoreOutcome::Skipped) => progress.skipped += 1,
            Err(e) => {
                progress.failed += 1;
                push_error(
                    progress,
                    format!("{}@{}: {e}", module.name, module.version),
                );
            }
        }
        let processed = progress.done + progress.skipped + progress.failed;
        if processed > 0 && processed % 25 == 0 {
            write_progress(state, ferry_artifact_id, progress).await;
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StoreOutcome {
    Stored,
    Skipped,
}

async fn store_go_module(
    state: &SharedState,
    repo: &Repository,
    root: &Path,
    module: &ModuleEntry,
    user_id: Uuid,
) -> Result<StoreOutcome> {
    let zip_rel = module
        .files
        .iter()
        .find(|f| f.relpath.replace('\\', "/").ends_with(".zip"))
        .map(|f| f.relpath.replace('\\', "/"));
    let mod_rel = module
        .files
        .iter()
        .find(|f| {
            let p = f.relpath.replace('\\', "/");
            p.ends_with(".mod") || p.ends_with("/go.mod")
        })
        .map(|f| f.relpath.replace('\\', "/"));

    let Some(zip_rel) = zip_rel else {
        return Err(AppError::Validation(format!(
            "Go module {}@{} missing .zip in manifest",
            module.name, module.version
        )));
    };

    let zip_bytes = read_file_bytes(&root.join(&zip_rel))?;
    let mut outcome = StoreOutcome::Skipped;

    match store_go_zip(state, repo, &module.name, &module.version, zip_bytes, user_id).await {
        Ok(StoreOutcome::Stored) => outcome = StoreOutcome::Stored,
        Ok(StoreOutcome::Skipped) => {}
        Err(e) => return Err(e),
    }

    if let Some(mod_rel) = mod_rel {
        let mod_bytes = read_file_bytes(&root.join(&mod_rel))?;
        match store_go_mod(state, repo, &module.name, &module.version, mod_bytes, user_id).await {
            Ok(StoreOutcome::Stored) => outcome = StoreOutcome::Stored,
            Ok(StoreOutcome::Skipped) => {}
            Err(e) => return Err(e),
        }
    }

    Ok(outcome)
}

async fn store_go_zip(
    state: &SharedState,
    repo: &Repository,
    module: &str,
    version: &str,
    body: Bytes,
    user_id: Uuid,
) -> Result<StoreOutcome> {
    let existing = sqlx::query_scalar::<_, Uuid>("SELECT id FROM artifacts WHERE repository_id = $1 AND name = $2 AND version = $3 AND path LIKE '%.zip' AND is_deleted = false")
    .bind(repo.id)
    .bind(module)
    .bind(version)
    .fetch_optional(&state.db)
    .await
    .map_err(|e| AppError::Database(e.to_string()))?;
    if existing.is_some() {
        return Ok(StoreOutcome::Skipped);
    }

    let artifact_path = build_go_zip_artifact_path(module, version);
    let storage_key = build_go_zip_storage_key(module, version);
    crate::api::handlers::cleanup_soft_deleted_artifact(&state.db, repo.id, &artifact_path).await;

    let checksum = sha256_hex(&body);
    let size_bytes = body.len() as i64;
    proxy_helpers::guard_cross_repo_write(state, repo.id, &repo.storage_backend, &storage_key)
        .await
        .map_err(response_to_app_error)?;

    let storage = state
        .storage_for_repo(&repo.storage_location())
        .map_err(|e| AppError::Internal(e.to_string()))?;
    storage
        .put(&storage_key, body)
        .await
        .map_err(|e| AppError::Storage(e.to_string()))?;

    let artifact_id = sqlx::query_scalar::<_, Uuid>(r#"
        INSERT INTO artifacts (
            repository_id, path, name, version, size_bytes,
            checksum_sha256, content_type, storage_key, uploaded_by
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        RETURNING id
        "#)
    .bind(repo.id)
    .bind(&artifact_path)
    .bind(module)
    .bind(version)
    .bind(size_bytes)
    .bind(&checksum)
    .bind("application/zip")
    .bind(&storage_key)
    .bind(user_id)
    .fetch_one(&state.db)
    .await
    .map_err(|e| AppError::Database(e.to_string()))?;

    crate::services::quarantine_service::apply_upload_hold_hosted(&state.db, repo.id, artifact_id)
        .await;

    let metadata = serde_json::json!({
        "module": module,
        "version": version,
        "type": "zip",
    });
    let _ = sqlx::query(r#"
        INSERT INTO artifact_metadata (artifact_id, format, metadata)
        VALUES ($1, 'go', $2)
        ON CONFLICT (artifact_id) DO UPDATE SET metadata = $2
        "#)
    .bind(artifact_id)
    .bind(metadata)
    .execute(&state.db)
    .await;

    PackageService::new(state.db.clone())
        .try_create_or_update_from_artifact(
            repo.id,
            module,
            version,
            size_bytes,
            &checksum,
            None,
            Some(serde_json::json!({ "format": "go" })),
        )
        .await;

    Ok(StoreOutcome::Stored)
}

async fn store_go_mod(
    state: &SharedState,
    repo: &Repository,
    module: &str,
    version: &str,
    body: Bytes,
    user_id: Uuid,
) -> Result<StoreOutcome> {
    let existing = sqlx::query_scalar::<_, Uuid>("SELECT id FROM artifacts WHERE repository_id = $1 AND name = $2 AND version = $3 AND path LIKE '%.mod' AND is_deleted = false")
    .bind(repo.id)
    .bind(module)
    .bind(version)
    .fetch_optional(&state.db)
    .await
    .map_err(|e| AppError::Database(e.to_string()))?;
    if existing.is_some() {
        return Ok(StoreOutcome::Skipped);
    }

    let artifact_path = build_go_mod_artifact_path(module, version);
    let storage_key = build_go_mod_storage_key(module, version);
    crate::api::handlers::cleanup_soft_deleted_artifact(&state.db, repo.id, &artifact_path).await;

    let checksum = sha256_hex(&body);
    let size_bytes = body.len() as i64;
    proxy_helpers::guard_cross_repo_write(state, repo.id, &repo.storage_backend, &storage_key)
        .await
        .map_err(response_to_app_error)?;

    let storage = state
        .storage_for_repo(&repo.storage_location())
        .map_err(|e| AppError::Internal(e.to_string()))?;
    storage
        .put(&storage_key, body)
        .await
        .map_err(|e| AppError::Storage(e.to_string()))?;

    let artifact_id = sqlx::query_scalar::<_, Uuid>(r#"
        INSERT INTO artifacts (
            repository_id, path, name, version, size_bytes,
            checksum_sha256, content_type, storage_key, uploaded_by
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        RETURNING id
        "#)
    .bind(repo.id)
    .bind(&artifact_path)
    .bind(module)
    .bind(version)
    .bind(size_bytes)
    .bind(&checksum)
    .bind("text/plain")
    .bind(&storage_key)
    .bind(user_id)
    .fetch_one(&state.db)
    .await
    .map_err(|e| AppError::Database(e.to_string()))?;

    crate::services::quarantine_service::apply_upload_hold_hosted(&state.db, repo.id, artifact_id)
        .await;

    let metadata = serde_json::json!({
        "module": module,
        "version": version,
        "type": "mod",
    });
    let _ = sqlx::query(r#"
        INSERT INTO artifact_metadata (artifact_id, format, metadata)
        VALUES ($1, 'go', $2)
        ON CONFLICT (artifact_id) DO UPDATE SET metadata = $2
        "#)
    .bind(artifact_id)
    .bind(metadata)
    .execute(&state.db)
    .await;

    Ok(StoreOutcome::Stored)
}

fn encode_module_path(path: &str) -> String {
    let mut out = String::with_capacity(path.len());
    for c in path.chars() {
        if c.is_ascii_uppercase() {
            out.push('!');
            out.push(c.to_ascii_lowercase());
        } else {
            out.push(c);
        }
    }
    out
}

fn build_go_zip_artifact_path(module: &str, version: &str) -> String {
    let encoded = encode_module_path(module);
    format!("{encoded}/{version}/{version}.zip")
}

fn build_go_zip_storage_key(module: &str, version: &str) -> String {
    let encoded = encode_module_path(module);
    format!("go/{encoded}/{version}/{version}.zip")
}

fn build_go_mod_artifact_path(module: &str, version: &str) -> String {
    let encoded = encode_module_path(module);
    format!("{encoded}/{version}/go.mod")
}

fn build_go_mod_storage_key(module: &str, version: &str) -> String {
    let encoded = encode_module_path(module);
    format!("go/{encoded}/{version}/go.mod")
}

// ---------------------------------------------------------------------------
// npm
// ---------------------------------------------------------------------------

async fn ingest_npm(
    state: &SharedState,
    repo: &Repository,
    ferry_artifact_id: Uuid,
    root: &Path,
    manifest: &FerryManifest,
    user_id: Uuid,
    progress: &mut FerryIngestProgress,
) -> Result<()> {
    for module in &manifest.modules {
        match store_npm_module(state, repo, root, module, user_id).await {
            Ok(StoreOutcome::Stored) => progress.done += 1,
            Ok(StoreOutcome::Skipped) => progress.skipped += 1,
            Err(e) => {
                progress.failed += 1;
                push_error(
                    progress,
                    format!("{}@{}: {e}", module.name, module.version),
                );
            }
        }
        let processed = progress.done + progress.skipped + progress.failed;
        if processed > 0 && processed % 25 == 0 {
            write_progress(state, ferry_artifact_id, progress).await;
        }
    }
    Ok(())
}

async fn store_npm_module(
    state: &SharedState,
    repo: &Repository,
    root: &Path,
    module: &ModuleEntry,
    user_id: Uuid,
) -> Result<StoreOutcome> {
    let tgz_rel = module
        .files
        .iter()
        .find(|f| f.relpath.replace('\\', "/").ends_with(".tgz"))
        .map(|f| f.relpath.replace('\\', "/"))
        .ok_or_else(|| {
            AppError::Validation(format!(
                "npm package {}@{} missing .tgz",
                module.name, module.version
            ))
        })?;

    let tarball = read_file_bytes(&root.join(&tgz_rel))?;
    let (claimed_name, claimed_version, version_data) = npm_identity_from_tarball(&tarball)
        .unwrap_or_else(|| {
            (
                module.name.clone(),
                module.version.clone(),
                serde_json::json!({
                    "name": module.name,
                    "version": module.version,
                }),
            )
        });

    let filename = Path::new(&tgz_rel)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("package.tgz")
        .to_string();
    let artifact_path = format!("{claimed_name}/{claimed_version}/{filename}");

    let existing = sqlx::query_scalar::<_, Uuid>("SELECT id FROM artifacts WHERE repository_id = $1 AND path = $2 AND is_deleted = false")
    .bind(repo.id)
    .bind(&artifact_path)
    .fetch_optional(&state.db)
    .await
    .map_err(|e| AppError::Database(e.to_string()))?;
    if existing.is_some() {
        return Ok(StoreOutcome::Skipped);
    }

    let checksum = sha256_hex(&tarball);
    crate::api::handlers::cleanup_soft_deleted_artifact_checked(
        &state.db,
        &RepositoryFormat::Npm,
        repo.id,
        &artifact_path,
        &checksum,
    )
    .await?;

    let storage_key = format!("npm/{claimed_name}/{claimed_version}/{filename}");
    proxy_helpers::guard_cross_repo_write(state, repo.id, &repo.storage_backend, &storage_key)
        .await
        .map_err(response_to_app_error)?;
    let storage = state
        .storage_for_repo(&repo.storage_location())
        .map_err(|e| AppError::Internal(e.to_string()))?;
    storage
        .put(&storage_key, tarball.clone())
        .await
        .map_err(|e| AppError::Storage(e.to_string()))?;

    let size_bytes = tarball.len() as i64;
    let artifact_id = sqlx::query_scalar::<_, Uuid>(r#"
        INSERT INTO artifacts (
            repository_id, path, name, version, size_bytes,
            checksum_sha256, content_type, storage_key, uploaded_by
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        RETURNING id
        "#)
    .bind(repo.id)
    .bind(&artifact_path)
    .bind(&claimed_name)
    .bind(&claimed_version)
    .bind(size_bytes)
    .bind(&checksum)
    .bind("application/gzip")
    .bind(&storage_key)
    .bind(user_id)
    .fetch_one(&state.db)
    .await
    .map_err(|e| AppError::Database(e.to_string()))?;

    crate::services::quarantine_service::apply_upload_hold_hosted(&state.db, repo.id, artifact_id)
        .await;

    let npm_metadata = serde_json::json!({
        "name": claimed_name,
        "version": claimed_version,
        "version_data": version_data,
    });
    let _ = sqlx::query(r#"
        INSERT INTO artifact_metadata (artifact_id, format, metadata)
        VALUES ($1, 'npm', $2)
        ON CONFLICT (artifact_id) DO UPDATE SET metadata = $2
        "#)
    .bind(artifact_id)
    .bind(npm_metadata)
    .execute(&state.db)
    .await;

    let description = version_data
        .get("description")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    PackageService::new(state.db.clone())
        .try_create_or_update_from_artifact(
            repo.id,
            &claimed_name,
            &claimed_version,
            size_bytes,
            &checksum,
            description.as_deref(),
            Some(serde_json::json!({ "format": "npm" })),
        )
        .await;

    Ok(StoreOutcome::Stored)
}

fn npm_identity_from_tarball(tarball: &Bytes) -> Option<(String, String, serde_json::Value)> {
    let body = crate::util::bounded_archive::read_metadata_from_tar_gz(&tarball[..], |path| {
        path == Path::new("package/package.json")
    })
    .ok()??;
    let v: serde_json::Value = serde_json::from_slice(&body).ok()?;
    let name = v.get("name")?.as_str()?.trim().to_string();
    let version = v.get("version")?.as_str()?.trim().to_string();
    if name.is_empty() || version.is_empty() {
        return None;
    }
    Some((name, version, v))
}

// ---------------------------------------------------------------------------
// PyPI
// ---------------------------------------------------------------------------

async fn ingest_pypi(
    state: &SharedState,
    repo: &Repository,
    artifact_svc: &ArtifactService,
    ferry_artifact_id: Uuid,
    root: &Path,
    manifest: &FerryManifest,
    user_id: Uuid,
    progress: &mut FerryIngestProgress,
) -> Result<()> {
    for module in &manifest.modules {
        for file in &module.files {
            let rel = file.relpath.replace('\\', "/");
            let path = root.join(&rel);
            if !path.is_file() {
                progress.failed += 1;
                push_error(
                    progress,
                    format!("{}@{} missing file {rel}", module.name, module.version),
                );
                continue;
            }
            match store_pypi_file(state, repo, artifact_svc, &path, module, user_id).await {
                Ok(StoreOutcome::Stored) => progress.done += 1,
                Ok(StoreOutcome::Skipped) => progress.skipped += 1,
                Err(e) => {
                    progress.failed += 1;
                    push_error(
                        progress,
                        format!("{}@{} ({rel}): {e}", module.name, module.version),
                    );
                }
            }
            let processed = progress.done + progress.skipped + progress.failed;
            if processed > 0 && processed % 25 == 0 {
                write_progress(state, ferry_artifact_id, progress).await;
            }
        }
    }
    Ok(())
}

async fn store_pypi_file(
    state: &SharedState,
    repo: &Repository,
    artifact_svc: &ArtifactService,
    file_path: &Path,
    module: &ModuleEntry,
    user_id: Uuid,
) -> Result<StoreOutcome> {
    let filename = file_path
        .file_name()
        .and_then(|s| s.to_str())
        .ok_or_else(|| AppError::Validation("Invalid PyPI filename".into()))?
        .to_string();
    let normalized = PypiHandler::normalize_name(&module.name);
    let artifact_path = format!("{normalized}/{}/{filename}", module.version);

    let existing = sqlx::query_scalar::<_, Uuid>("SELECT id FROM artifacts WHERE repository_id = $1 AND path = $2 AND is_deleted = false")
    .bind(repo.id)
    .bind(&artifact_path)
    .fetch_optional(&state.db)
    .await
    .map_err(|e| AppError::Database(e.to_string()))?;
    if existing.is_some() {
        return Ok(StoreOutcome::Skipped);
    }

    let (digests, size_bytes) = hash_file(file_path)?;
    crate::api::handlers::cleanup_soft_deleted_artifact_checked(
        &state.db,
        &RepositoryFormat::Pypi,
        repo.id,
        &artifact_path,
        &digests.sha256,
    )
    .await?;

    let content_type = pypi_content_type(&filename);
    let file = tokio::fs::File::open(file_path)
        .await
        .map_err(|e| AppError::Internal(format!("open pypi file: {e}")))?;
    let stream = tokio_util::io::ReaderStream::new(file)
        .map(|r| r.map_err(|e| AppError::Storage(format!("pypi read: {e}"))));
    let artifact = artifact_svc
        .upload_stream_with_sync_options(
            repo.id,
            &artifact_path,
            &normalized,
            Some(&module.version),
            content_type,
            Box::pin(stream),
            digests.clone(),
            size_bytes,
            Some(user_id),
            false,
        )
        .await?;

    let pkg_metadata = serde_json::json!({
        "name": module.name,
        "version": module.version,
        "filename": filename,
    });
    let _ = artifact_svc
        .set_metadata(artifact.id, "pypi", pkg_metadata, serde_json::json!({}))
        .await;

    PackageService::new(state.db.clone())
        .try_create_or_update_from_artifact(
            repo.id,
            &normalized,
            &module.version,
            size_bytes,
            &digests.sha256,
            None,
            Some(serde_json::json!({
                "format": "pypi",
                "filename": filename,
            })),
        )
        .await;

    Ok(StoreOutcome::Stored)
}

fn pypi_content_type(filename: &str) -> &'static str {
    if filename.ends_with(".whl") || filename.ends_with(".zip") {
        "application/zip"
    } else if filename.ends_with(".tar.gz") {
        "application/gzip"
    } else if filename.ends_with(".tar.bz2") {
        "application/x-bzip2"
    } else {
        "application/octet-stream"
    }
}

// ---------------------------------------------------------------------------
// Cargo
// ---------------------------------------------------------------------------

async fn ingest_cargo(
    state: &SharedState,
    repo: &Repository,
    ferry_artifact_id: Uuid,
    root: &Path,
    manifest: &FerryManifest,
    user_id: Uuid,
    progress: &mut FerryIngestProgress,
) -> Result<()> {
    for module in &manifest.modules {
        match store_cargo_module(state, repo, root, module, user_id).await {
            Ok(StoreOutcome::Stored) => progress.done += 1,
            Ok(StoreOutcome::Skipped) => progress.skipped += 1,
            Err(e) => {
                progress.failed += 1;
                push_error(
                    progress,
                    format!("{}@{}: {e}", module.name, module.version),
                );
            }
        }
        let processed = progress.done + progress.skipped + progress.failed;
        if processed > 0 && processed % 25 == 0 {
            write_progress(state, ferry_artifact_id, progress).await;
        }
    }
    Ok(())
}

async fn store_cargo_module(
    state: &SharedState,
    repo: &Repository,
    root: &Path,
    module: &ModuleEntry,
    user_id: Uuid,
) -> Result<StoreOutcome> {
    let crate_rel = module
        .files
        .iter()
        .find(|f| f.relpath.replace('\\', "/").ends_with(".crate"))
        .map(|f| f.relpath.replace('\\', "/"))
        .ok_or_else(|| {
            AppError::Validation(format!(
                "cargo crate {}@{} missing .crate",
                module.name, module.version
            ))
        })?;

    let crate_bytes = read_file_bytes(&root.join(&crate_rel))?;
    let name_lower = module.name.to_ascii_lowercase();
    let filename = format!("{}-{}.crate", name_lower, module.version);
    let artifact_path = format!("{name_lower}/{}/{filename}", module.version);

    let existing = sqlx::query_scalar::<_, Uuid>("SELECT id FROM artifacts WHERE repository_id = $1 AND path = $2 AND is_deleted = false")
    .bind(repo.id)
    .bind(&artifact_path)
    .fetch_optional(&state.db)
    .await
    .map_err(|e| AppError::Database(e.to_string()))?;
    if existing.is_some() {
        return Ok(StoreOutcome::Skipped);
    }

    let checksum = sha256_hex(&crate_bytes);
    crate::api::handlers::cleanup_soft_deleted_artifact_checked(
        &state.db,
        &RepositoryFormat::Cargo,
        repo.id,
        &artifact_path,
        &checksum,
    )
    .await?;

    let storage_key = format!("cargo/{name_lower}/{}/{filename}", module.version);
    proxy_helpers::guard_cross_repo_write(state, repo.id, &repo.storage_backend, &storage_key)
        .await
        .map_err(response_to_app_error)?;
    let storage = state
        .storage_for_repo(&repo.storage_location())
        .map_err(|e| AppError::Internal(e.to_string()))?;
    storage
        .put(&storage_key, crate_bytes.clone())
        .await
        .map_err(|e| AppError::Storage(e.to_string()))?;

    let size_bytes = crate_bytes.len() as i64;
    let cargo_metadata = serde_json::json!({
        "name": name_lower,
        "vers": module.version,
        "deps": [],
        "cksum": checksum,
        "features": {},
        "yanked": false,
    });

    let artifact_id = sqlx::query_scalar::<_, Uuid>(r#"
        INSERT INTO artifacts (
            repository_id, path, name, version, size_bytes,
            checksum_sha256, content_type, storage_key, uploaded_by
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        RETURNING id
        "#)
    .bind(repo.id)
    .bind(&artifact_path)
    .bind(&name_lower)
    .bind(&module.version)
    .bind(size_bytes)
    .bind(&checksum)
    .bind("application/x-tar")
    .bind(&storage_key)
    .bind(user_id)
    .fetch_one(&state.db)
    .await
    .map_err(|e| AppError::Database(e.to_string()))?;

    crate::services::quarantine_service::apply_upload_hold_hosted(&state.db, repo.id, artifact_id)
        .await;

    let _ = sqlx::query(r#"
        INSERT INTO artifact_metadata (artifact_id, format, metadata)
        VALUES ($1, 'cargo', $2)
        ON CONFLICT (artifact_id) DO UPDATE SET metadata = $2
        "#)
    .bind(artifact_id)
    .bind(cargo_metadata)
    .execute(&state.db)
    .await;

    PackageService::new(state.db.clone())
        .try_create_or_update_from_artifact(
            repo.id,
            &name_lower,
            &module.version,
            size_bytes,
            &checksum,
            None,
            Some(serde_json::json!({ "format": "cargo" })),
        )
        .await;

    Ok(StoreOutcome::Stored)
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

fn read_file_bytes(path: &Path) -> Result<Bytes> {
    let bytes = std::fs::read(path)
        .map_err(|e| AppError::Validation(format!("read {}: {e}", path.display())))?;
    Ok(Bytes::from(bytes))
}

fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    format!("{:x}", hasher.finalize())
}

fn hash_file(path: &Path) -> Result<(ContentDigests, i64)> {
    let mut file =
        File::open(path).map_err(|e| AppError::Internal(format!("open {}: {e}", path.display())))?;
    let mut hasher = MultiHasher::new();
    let mut buf = [0u8; 64 * 1024];
    let mut total: i64 = 0;
    loop {
        let n = file
            .read(&mut buf)
            .map_err(|e| AppError::Internal(format!("hash {}: {e}", path.display())))?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        total += n as i64;
    }
    Ok((hasher.finalize(), total))
}

fn response_to_app_error(resp: axum::response::Response) -> AppError {
    let status = resp.status();
    AppError::Internal(format!("cross-repo write rejected ({status})"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ferry_path_detection() {
        assert!(is_ferry_archive_path("ak-ferry/pack.zip"));
        assert!(is_ferry_archive_path("/ak-ferry/pack.ZIP"));
        assert!(!is_ferry_archive_path("ak-ferry/nested/pack.zip"));
        assert!(!is_ferry_archive_path("other/pack.zip"));
        assert!(!is_ferry_archive_path("ak-ferry/pack.tar"));
    }

    #[test]
    fn go_encode_matches_goproxy_style() {
        assert_eq!(encode_module_path("github.com/Azure/go-sdk"), "github.com/!azure/go-sdk");
    }
}
