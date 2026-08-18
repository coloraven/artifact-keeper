//! Air-gap ferry zip ingest (`ak-ferry/*.zip` from `ak artifact push --from-archive`).
//!
//! The CLI uploads the ferry archive as one generic blob. This service streams entries
//! from the zip (no full unpack tree), reads `ak-ferry.json` / `ak-ferry.jsonl`, and
//! stores each module via the same coordinate layout as the Go / npm / PyPI / Cargo
//! protocol handlers, including `PackageService` catalog rows so `ak download catalog`
//! stays accurate.
//!
//! Directory-style ferry layout is CLI-side only; the server always receives a zip.
//! Content-addressed packs (`kind = ak-ferry-set` or entries under `blobs/sha256/`)
//! resolve module files via digest paths.

use std::fs::File;
use std::io::{Read, Seek, Write};
use std::path::Path;

use futures::StreamExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use tracing::{info, warn};
use uuid::Uuid;
use zip::ZipArchive;

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
pub const MANIFEST_KIND_SET: &str = "ak-ferry-set";
pub const BLOBS_SHA256_PREFIX: &str = "blobs/sha256/";

/// Default extract budget for ferry zips (multi-GB air-gap packs). Override with
/// `FERRY_MAX_EXTRACTED_BYTES`.
const DEFAULT_MAX_EXTRACTED_BYTES: u64 = 50 * 1024 * 1024 * 1024;
const DEFAULT_MAX_EXTRACTED_ENTRIES: u64 = 500_000;
const FERRY_MAX_EXTRACTED_BYTES_ENV: &str = "FERRY_MAX_EXTRACTED_BYTES";
const FERRY_MAX_EXTRACTED_ENTRIES_ENV: &str = "FERRY_MAX_EXTRACTED_ENTRIES";
const FERRY_GC_ARCHIVE_AFTER_INGEST_ENV: &str = "FERRY_GC_ARCHIVE_AFTER_INGEST";

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
    #[serde(default)]
    pub cursor_module_index: i32,
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
            cursor_module_index: 0,
            errors: Vec::new(),
            message: None,
        }
    }
}

/// Cumulative extract budget while streaming zip entries (zip-bomb guard).
#[derive(Debug, Clone)]
struct ExtractBudget {
    remaining_bytes: u64,
    remaining_entries: u64,
}

impl ExtractBudget {
    fn new(max_bytes: u64, max_entries: u64) -> Self {
        Self {
            remaining_bytes: max_bytes,
            remaining_entries: max_entries,
        }
    }

    fn consume_entry(&mut self) -> Result<()> {
        if self.remaining_entries == 0 {
            return Err(AppError::Validation(format!(
                "Ferry zip exceeds extract entry budget (> {})",
                ferry_max_extracted_entries()
            )));
        }
        self.remaining_entries -= 1;
        Ok(())
    }
}

pub fn is_ferry_archive_path(path: &str) -> bool {
    let path = path.trim_start_matches('/');
    path.starts_with(FERRY_PATH_PREFIX)
        && path.to_ascii_lowercase().ends_with(".zip")
        && !path[FERRY_PATH_PREFIX.len()..].contains('/')
}

/// Upsert a queued job row and mirror progress into artifact_metadata.
/// Used by the HTTP start endpoint before `spawn_ingest`.
pub async fn ensure_queued_job(
    state: &SharedState,
    repository_id: Uuid,
    ferry_artifact_id: Uuid,
    user_id: Uuid,
    progress: &FerryIngestProgress,
) -> Option<Uuid> {
    let job_id = upsert_job(state, repository_id, ferry_artifact_id, user_id, progress).await;
    write_progress(state, ferry_artifact_id, job_id, progress).await;
    job_id
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
    let job_id = upsert_job(&state, repository_id, ferry_artifact_id, user_id, &progress).await;
    write_progress(&state, ferry_artifact_id, job_id, &progress).await;

    let result =
        ingest_inner(&state, repository_id, ferry_artifact_id, user_id, job_id, &mut progress)
            .await;

    match result {
        Ok((storage, storage_key)) => {
            if progress.failed > 0 && progress.done > 0 {
                progress.status = FerryIngestStatus::Partial;
            } else if progress.failed > 0 {
                progress.status = FerryIngestStatus::Failed;
            } else {
                progress.status = FerryIngestStatus::Completed;
            }
            write_progress(&state, ferry_artifact_id, job_id, &progress).await;
            info!(
                "ferry ingest {}: done={} skipped={} failed={} cursor={}",
                ferry_artifact_id,
                progress.done,
                progress.skipped,
                progress.failed,
                progress.cursor_module_index
            );
            let artifact_svc = state.create_artifact_service(storage.clone());
            maybe_gc_ferry_archive(
                &artifact_svc,
                storage.as_ref(),
                ferry_artifact_id,
                &storage_key,
                &progress,
            )
            .await;
            Ok(progress)
        }
        Err(e) => {
            progress.status = FerryIngestStatus::Failed;
            progress.message = Some(e.to_string());
            push_error(&mut progress, e.to_string());
            write_progress(&state, ferry_artifact_id, job_id, &progress).await;
            Err(e)
        }
    }
}

async fn ingest_inner(
    state: &SharedState,
    repository_id: Uuid,
    ferry_artifact_id: Uuid,
    user_id: Uuid,
    job_id: Option<Uuid>,
    progress: &mut FerryIngestProgress,
) -> Result<(
    std::sync::Arc<dyn crate::storage::StorageBackend>,
    String,
)> {
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

    let zip_file = File::open(&zip_path)
        .map_err(|e| AppError::Internal(format!("open ferry zip: {e}")))?;
    let mut archive = ZipArchive::new(zip_file)
        .map_err(|e| AppError::Validation(format!("Failed to open ferry zip: {e}")))?;

    let mut budget = ExtractBudget::new(
        ferry_max_extracted_bytes(),
        ferry_max_extracted_entries(),
    );
    let (manifest, ca_layout) = load_manifest_from_zip(&mut archive, &mut budget)?;
    if manifest.kind != MANIFEST_KIND && manifest.kind != MANIFEST_KIND_SET {
        return Err(AppError::Validation(format!(
            "Unexpected ferry kind '{}'",
            manifest.kind
        )));
    }
    progress.ecosystem = Some(manifest.ecosystem.clone());
    write_progress(state, ferry_artifact_id, job_id, progress).await;

    ensure_repo_format_matches(&repo, &manifest.ecosystem)?;

    let scratch = work.path().join("scratch");
    std::fs::create_dir_all(&scratch)
        .map_err(|e| AppError::Internal(format!("mkdir scratch: {e}")))?;

    match manifest.ecosystem.as_str() {
        "go" => {
            ingest_go(
                state,
                &repo,
                ferry_artifact_id,
                job_id,
                &mut archive,
                &mut budget,
                ca_layout,
                &scratch,
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
                job_id,
                &mut archive,
                &mut budget,
                ca_layout,
                &scratch,
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
                job_id,
                &mut archive,
                &mut budget,
                ca_layout,
                &scratch,
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
                job_id,
                &mut archive,
                &mut budget,
                ca_layout,
                &scratch,
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

    // Drop archive so the zip file handle is released before any later GC.
    drop(archive);

    Ok((storage, ferry.storage_key.clone()))
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

// ---------------------------------------------------------------------------
// Zip streaming + CA layout
// ---------------------------------------------------------------------------

fn normalize_zip_name(name: &str) -> String {
    name.replace('\\', "/")
        .trim_start_matches("./")
        .trim_start_matches('/')
        .to_string()
}

/// Nested content-addressed blob path: `blobs/sha256/xx/yy/<hash>`.
fn blob_path_for_sha256(sha256: &str) -> String {
    let h = sha256.trim().to_ascii_lowercase();
    if h.len() < 4 {
        return format!("{BLOBS_SHA256_PREFIX}{h}");
    }
    format!("{BLOBS_SHA256_PREFIX}{}/{}/{}", &h[0..2], &h[2..4], h)
}

fn flat_blob_path_for_sha256(sha256: &str) -> String {
    let h = sha256.trim().to_ascii_lowercase();
    format!("{BLOBS_SHA256_PREFIX}{h}")
}

fn uses_ca_blob_layout<R: Read + Seek>(kind: &str, archive: &ZipArchive<R>) -> bool {
    if kind == MANIFEST_KIND_SET {
        return true;
    }
    archive.file_names().any(|n| {
        let n = normalize_zip_name(n);
        n.starts_with(BLOBS_SHA256_PREFIX)
    })
}

fn resolve_zip_entry_name(relpath: &str, sha256: &str, ca_layout: bool) -> String {
    let rel = normalize_zip_name(relpath);
    if rel.starts_with(BLOBS_SHA256_PREFIX) {
        return rel;
    }
    let sha = sha256.trim();
    if ca_layout && !sha.is_empty() {
        return blob_path_for_sha256(sha);
    }
    rel
}

/// Candidate zip entry names for a manifest file (logical + CA variants).
fn candidate_zip_names(file_entry: &FileEntry, ca_layout: bool) -> Vec<String> {
    let rel = normalize_zip_name(&file_entry.relpath);
    let sha = file_entry.sha256.trim();
    let mut out = Vec::new();
    let push_unique = |v: &mut Vec<String>, s: String| {
        if !s.is_empty() && !v.iter().any(|x| x == &s) {
            v.push(s);
        }
    };

    push_unique(
        &mut out,
        resolve_zip_entry_name(&rel, sha, ca_layout),
    );
    push_unique(&mut out, rel.clone());
    if !sha.is_empty() {
        push_unique(&mut out, blob_path_for_sha256(sha));
        push_unique(&mut out, flat_blob_path_for_sha256(sha));
    }
    out
}

/// Find entry index by name using shared `&ZipArchive` (`file_names`; `by_index` needs `&mut`).
fn find_zip_index<R: Read + Seek>(archive: &ZipArchive<R>, wanted: &str) -> Option<usize> {
    let wanted = normalize_zip_name(wanted);
    archive
        .file_names()
        .position(|n| normalize_zip_name(n) == wanted)
}

fn load_manifest_from_zip<R: Read + Seek>(
    archive: &mut ZipArchive<R>,
    budget: &mut ExtractBudget,
) -> Result<(FerryManifest, bool)> {
    if let Some(idx) = find_zip_index(archive, MANIFEST_JSON) {
        let text = read_zip_entry_string(archive, idx, budget)?;
        let manifest: FerryManifest = serde_json::from_str(&text)
            .map_err(|e| AppError::Validation(format!("invalid {MANIFEST_JSON}: {e}")))?;
        let ca = uses_ca_blob_layout(&manifest.kind, archive);
        return Ok((manifest, ca));
    }

    if let Some(idx) = find_zip_index(archive, MANIFEST_JSONL) {
        let text = read_zip_entry_string(archive, idx, budget)?;
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
        let kind = MANIFEST_KIND.to_string();
        let ca = uses_ca_blob_layout(&kind, archive);
        let ecosystem = guess_ecosystem_from_paths(&modules)?;
        return Ok((
            FerryManifest {
                version: 1,
                kind,
                ecosystem,
                modules,
            },
            ca,
        ));
    }

    Err(AppError::Validation(
        "Ferry archive missing ak-ferry.json / ak-ferry.jsonl".into(),
    ))
}

fn read_zip_entry_string<R: Read + Seek>(
    archive: &mut ZipArchive<R>,
    index: usize,
    budget: &mut ExtractBudget,
) -> Result<String> {
    budget.consume_entry()?;
    let mut entry = archive
        .by_index(index)
        .map_err(|e| AppError::Validation(format!("zip entry {index}: {e}")))?;
    let mut buf = Vec::new();
    copy_entry_bounded(&mut entry, &mut buf, &mut budget.remaining_bytes)?;
    String::from_utf8(buf).map_err(|e| AppError::Validation(format!("manifest utf-8: {e}")))
}

fn extract_zip_entry_to_path<R: Read + Seek>(
    archive: &mut ZipArchive<R>,
    file_entry: &FileEntry,
    ca_layout: bool,
    dest: &Path,
    budget: &mut ExtractBudget,
) -> Result<()> {
    let candidates = candidate_zip_names(file_entry, ca_layout);
    let idx = candidates
        .iter()
        .find_map(|name| find_zip_index(archive, name))
        .ok_or_else(|| {
            AppError::Validation(format!(
                "Ferry zip missing entry for '{}' (tried: {})",
                file_entry.relpath,
                candidates.join(", ")
            ))
        })?;

    budget.consume_entry()?;
    let mut entry = archive
        .by_index(idx)
        .map_err(|e| AppError::Validation(format!("zip entry {idx}: {e}")))?;
    if entry.is_dir() {
        return Err(AppError::Validation(format!(
            "Ferry zip entry '{}' is a directory",
            file_entry.relpath
        )));
    }

    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| AppError::Internal(format!("mkdir {}: {e}", parent.display())))?;
    }
    let mut out = File::create(dest)
        .map_err(|e| AppError::Internal(format!("create {}: {e}", dest.display())))?;

    let expected = file_entry.sha256.trim();
    if expected.is_empty() {
        copy_entry_bounded(&mut entry, &mut out, &mut budget.remaining_bytes)?;
        return Ok(());
    }

    let mut hasher = Sha256::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = entry
            .read(&mut buf)
            .map_err(|e| AppError::Internal(format!("read zip entry: {e}")))?;
        if n == 0 {
            break;
        }
        let n_u64 = n as u64;
        if n_u64 > budget.remaining_bytes {
            return Err(AppError::Validation(
                "Ferry zip exceeds extract byte budget (suspected zip bomb)".into(),
            ));
        }
        budget.remaining_bytes -= n_u64;
        hasher.update(&buf[..n]);
        out.write_all(&buf[..n])
            .map_err(|e| AppError::Internal(format!("write zip entry: {e}")))?;
    }
    let actual = format!("{:x}", hasher.finalize());
    if !actual.eq_ignore_ascii_case(expected) {
        let _ = std::fs::remove_file(dest);
        return Err(AppError::Validation(format!(
            "sha256 mismatch for '{}': expected {expected}, got {actual}",
            file_entry.relpath
        )));
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

/// Full-tree unpack retained for unit tests only (production uses stream extract).
#[cfg(test)]
fn unpack_zip_limited(file: File, dst: &Path, max_bytes: u64, max_entries: u64) -> Result<()> {
    let mut archive = ZipArchive::new(file)
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

fn ferry_max_extracted_bytes() -> u64 {
    positive_env_or(FERRY_MAX_EXTRACTED_BYTES_ENV, DEFAULT_MAX_EXTRACTED_BYTES)
}

fn ferry_max_extracted_entries() -> u64 {
    positive_env_or(
        FERRY_MAX_EXTRACTED_ENTRIES_ENV,
        DEFAULT_MAX_EXTRACTED_ENTRIES,
    )
}

fn ferry_gc_archive_after_ingest() -> bool {
    matches!(
        std::env::var(FERRY_GC_ARCHIVE_AFTER_INGEST_ENV)
            .ok()
            .as_deref()
            .map(str::trim),
        Some("1") | Some("true") | Some("TRUE") | Some("yes") | Some("YES")
    )
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

fn clear_scratch_dir(scratch: &Path) -> Result<()> {
    if scratch.exists() {
        std::fs::remove_dir_all(scratch)
            .map_err(|e| AppError::Internal(format!("clear scratch: {e}")))?;
    }
    std::fs::create_dir_all(scratch)
        .map_err(|e| AppError::Internal(format!("mkdir scratch: {e}")))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Durable jobs + progress
// ---------------------------------------------------------------------------

fn lease_owner_id() -> String {
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .unwrap_or_else(|_| format!("ak-{}", Uuid::new_v4()))
}

async fn upsert_job(
    state: &SharedState,
    repository_id: Uuid,
    ferry_artifact_id: Uuid,
    user_id: Uuid,
    progress: &FerryIngestProgress,
) -> Option<Uuid> {
    let error = progress
        .message
        .clone()
        .or_else(|| progress.errors.last().cloned());
    let owner = lease_owner_id();
    let result: std::result::Result<Uuid, sqlx::Error> = sqlx::query_scalar(
        r#"
        INSERT INTO ferry_ingest_jobs (
            repository_id, ferry_artifact_id, user_id, status, ecosystem,
            done, skipped, failed, cursor_module_index, error,
            lease_owner, lease_until, updated_at
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
            $11, NOW() + INTERVAL '1 hour', NOW()
        )
        ON CONFLICT (ferry_artifact_id) DO UPDATE SET
            repository_id = EXCLUDED.repository_id,
            user_id = EXCLUDED.user_id,
            status = EXCLUDED.status,
            ecosystem = EXCLUDED.ecosystem,
            done = EXCLUDED.done,
            skipped = EXCLUDED.skipped,
            failed = EXCLUDED.failed,
            cursor_module_index = EXCLUDED.cursor_module_index,
            error = EXCLUDED.error,
            lease_owner = EXCLUDED.lease_owner,
            lease_until = EXCLUDED.lease_until,
            updated_at = NOW(),
            completed_at = NULL
        RETURNING id
        "#,
    )
    .bind(repository_id)
    .bind(ferry_artifact_id)
    .bind(user_id)
    .bind(progress.status.as_str())
    .bind(progress.ecosystem.as_deref())
    .bind(progress.done as i64)
    .bind(progress.skipped as i64)
    .bind(progress.failed as i64)
    .bind(progress.cursor_module_index)
    .bind(error)
    .bind(owner)
    .fetch_one(&state.db)
    .await;

    match result {
        Ok(id) => Some(id),
        Err(e) => {
            warn!(
                "ferry_ingest_jobs upsert failed (migration may be missing); \
                 continuing with metadata-only progress: {e}"
            );
            None
        }
    }
}

async fn write_progress(
    state: &SharedState,
    artifact_id: Uuid,
    job_id: Option<Uuid>,
    progress: &FerryIngestProgress,
) {
    if let Some(jid) = job_id {
        let terminal = matches!(
            progress.status,
            FerryIngestStatus::Completed | FerryIngestStatus::Failed | FerryIngestStatus::Partial
        );
        let error = progress
            .message
            .clone()
            .or_else(|| progress.errors.last().cloned());
        let owner = lease_owner_id();
        let _ = sqlx::query(
            r#"
            UPDATE ferry_ingest_jobs SET
                status = $2,
                ecosystem = $3,
                done = $4,
                skipped = $5,
                failed = $6,
                cursor_module_index = $7,
                error = $8,
                lease_owner = $9,
                lease_until = CASE
                    WHEN $10 THEN lease_until
                    ELSE NOW() + INTERVAL '1 hour'
                END,
                updated_at = NOW(),
                completed_at = CASE
                    WHEN $10 THEN COALESCE(completed_at, NOW())
                    ELSE completed_at
                END
            WHERE id = $1
            "#,
        )
        .bind(jid)
        .bind(progress.status.as_str())
        .bind(progress.ecosystem.as_deref())
        .bind(progress.done as i64)
        .bind(progress.skipped as i64)
        .bind(progress.failed as i64)
        .bind(progress.cursor_module_index)
        .bind(error)
        .bind(owner)
        .bind(terminal)
        .execute(&state.db)
        .await;
    }

    let meta = serde_json::json!({ "ferry_ingest": progress });
    let _ = sqlx::query(
        r#"
        INSERT INTO artifact_metadata (artifact_id, format, metadata)
        VALUES ($1, 'ferry', $2)
        ON CONFLICT (artifact_id) DO UPDATE SET
            format = EXCLUDED.format,
            metadata = COALESCE(artifact_metadata.metadata, '{}'::jsonb) || EXCLUDED.metadata
        "#,
    )
    .bind(artifact_id)
    .bind(meta)
    .execute(&state.db)
    .await;
}

pub async fn read_progress(
    state: &SharedState,
    artifact_id: Uuid,
) -> Result<Option<FerryIngestProgress>> {
    match read_progress_from_jobs(state, artifact_id).await {
        Ok(Some(p)) => return Ok(Some(p)),
        Ok(None) => {}
        Err(e) => {
            warn!(
                "ferry_ingest_jobs read failed; falling back to artifact_metadata: {e}"
            );
        }
    }
    read_progress_from_metadata(state, artifact_id).await
}

async fn read_progress_from_jobs(
    state: &SharedState,
    artifact_id: Uuid,
) -> Result<Option<FerryIngestProgress>> {
    let row: Option<(String, Option<String>, i64, i64, i64, i32, Option<String>)> =
        sqlx::query_as(
            r#"
            SELECT status, ecosystem, done, skipped, failed, cursor_module_index, error
            FROM ferry_ingest_jobs
            WHERE ferry_artifact_id = $1
            "#,
        )
        .bind(artifact_id)
        .fetch_optional(&state.db)
        .await
        .map_err(|e| AppError::Database(e.to_string()))?;

    let Some((status, ecosystem, done, skipped, failed, cursor, error)) = row else {
        return Ok(None);
    };

    let status = match status.as_str() {
        "queued" => FerryIngestStatus::Queued,
        "running" => FerryIngestStatus::Running,
        "completed" => FerryIngestStatus::Completed,
        "failed" => FerryIngestStatus::Failed,
        "partial" => FerryIngestStatus::Partial,
        other => {
            return Err(AppError::Internal(format!(
                "unknown ferry_ingest_jobs.status '{other}'"
            )));
        }
    };

    let mut progress = FerryIngestProgress {
        status,
        ecosystem,
        done: done as u64,
        skipped: skipped as u64,
        failed: failed as u64,
        cursor_module_index: cursor,
        errors: Vec::new(),
        message: error.clone(),
    };
    if let Some(err) = error {
        push_error(&mut progress, err);
    }
    Ok(Some(progress))
}

async fn read_progress_from_metadata(
    state: &SharedState,
    artifact_id: Uuid,
) -> Result<Option<FerryIngestProgress>> {
    let metadata: Option<serde_json::Value> = sqlx::query_scalar(
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

async fn maybe_gc_ferry_archive(
    artifact_svc: &ArtifactService,
    storage: &dyn crate::storage::StorageBackend,
    ferry_artifact_id: Uuid,
    storage_key: &str,
    progress: &FerryIngestProgress,
) {
    let should_gc = ferry_gc_archive_after_ingest()
        && (matches!(progress.status, FerryIngestStatus::Completed)
            || (matches!(progress.status, FerryIngestStatus::Partial) && progress.done > 0));

    if !should_gc {
        return;
    }

    info!(
        "ferry GC (FERRY_GC_ARCHIVE_AFTER_INGEST): soft-deleting ferry artifact {} \
         then deleting storage key {}",
        ferry_artifact_id, storage_key
    );

    if let Err(e) = artifact_svc
        .delete_with_sync_options(ferry_artifact_id, false)
        .await
    {
        warn!(
            "ferry GC soft-delete failed for artifact {}: {}",
            ferry_artifact_id, e
        );
        return;
    }

    if let Err(e) = storage.delete(storage_key).await {
        warn!(
            "ferry GC storage.delete failed for key {}: {}",
            storage_key, e
        );
    } else {
        info!(
            "ferry GC completed for artifact {} (storage key {})",
            ferry_artifact_id, storage_key
        );
    }
}

fn maybe_checkpoint(
    progress: &FerryIngestProgress,
) -> bool {
    let processed = progress.done + progress.skipped + progress.failed;
    processed > 0 && processed % 25 == 0
}

// ---------------------------------------------------------------------------
// Go
// ---------------------------------------------------------------------------

async fn ingest_go(
    state: &SharedState,
    repo: &Repository,
    ferry_artifact_id: Uuid,
    job_id: Option<Uuid>,
    archive: &mut ZipArchive<File>,
    budget: &mut ExtractBudget,
    ca_layout: bool,
    scratch: &Path,
    manifest: &FerryManifest,
    user_id: Uuid,
    progress: &mut FerryIngestProgress,
) -> Result<()> {
    for (idx, module) in manifest.modules.iter().enumerate() {
        progress.cursor_module_index = idx as i32;
        clear_scratch_dir(scratch)?;
        match store_go_module(
            state,
            repo,
            archive,
            budget,
            ca_layout,
            scratch,
            module,
            user_id,
        )
        .await
        {
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
        progress.cursor_module_index = (idx as i32).saturating_add(1);
        if maybe_checkpoint(progress) {
            write_progress(state, ferry_artifact_id, job_id, progress).await;
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StoreOutcome {
    Stored,
    Skipped,
}

/// Live artifact at a coordinate: same checksum → skip (resume); different → 409.
fn live_checksum_store_outcome(
    existing_checksum: Option<&str>,
    new_checksum: &str,
) -> Result<Option<StoreOutcome>> {
    match existing_checksum {
        None => Ok(None),
        Some(checksum) if checksum.eq_ignore_ascii_case(new_checksum) => {
            Ok(Some(StoreOutcome::Skipped))
        }
        Some(_) => Err(AppError::Conflict(
            "Artifact version already exists and is immutable".to_string(),
        )),
    }
}

async fn live_artifact_checksum(
    db: &sqlx::PgPool,
    repository_id: Uuid,
    path: &str,
) -> Result<Option<String>> {
    sqlx::query_scalar(
        "SELECT checksum_sha256 FROM artifacts \
         WHERE repository_id = $1 AND path = $2 AND is_deleted = false",
    )
    .bind(repository_id)
    .bind(path)
    .fetch_optional(db)
    .await
    .map_err(|e| AppError::Database(e.to_string()))
}

async fn live_go_file_checksum(
    db: &sqlx::PgPool,
    repository_id: Uuid,
    module: &str,
    version: &str,
    path_like: &str,
) -> Result<Option<String>> {
    sqlx::query_scalar(
        "SELECT checksum_sha256 FROM artifacts \
         WHERE repository_id = $1 AND name = $2 AND version = $3 \
           AND path LIKE $4 AND is_deleted = false",
    )
    .bind(repository_id)
    .bind(module)
    .bind(version)
    .bind(path_like)
    .fetch_optional(db)
    .await
    .map_err(|e| AppError::Database(e.to_string()))
}

fn pick_go_zip_file<'a>(module: &'a ModuleEntry, ca_layout: bool) -> Result<&'a FileEntry> {
    if let Some(f) = module
        .files
        .iter()
        .find(|f| normalize_zip_name(&f.relpath).ends_with(".zip"))
    {
        return Ok(f);
    }
    if ca_layout {
        if let Some(f) = module.files.iter().find(|f| {
            let p = normalize_zip_name(&f.relpath);
            !p.ends_with(".mod") && !p.ends_with("/go.mod")
        }) {
            return Ok(f);
        }
    }
    Err(AppError::Validation(format!(
        "Go module {}@{} missing .zip in manifest",
        module.name, module.version
    )))
}

fn pick_go_mod_file(module: &ModuleEntry) -> Option<&FileEntry> {
    module.files.iter().find(|f| {
        let p = normalize_zip_name(&f.relpath);
        p.ends_with(".mod") || p.ends_with("/go.mod")
    })
}

async fn store_go_module(
    state: &SharedState,
    repo: &Repository,
    archive: &mut ZipArchive<File>,
    budget: &mut ExtractBudget,
    ca_layout: bool,
    scratch: &Path,
    module: &ModuleEntry,
    user_id: Uuid,
) -> Result<StoreOutcome> {
    let zip_fe = pick_go_zip_file(module, ca_layout)?;
    let zip_path = scratch.join("module.zip");
    extract_zip_entry_to_path(archive, zip_fe, ca_layout, &zip_path, budget)?;

    let mut outcome = StoreOutcome::Skipped;
    match store_go_zip(
        state,
        repo,
        &module.name,
        &module.version,
        &zip_path,
        user_id,
    )
    .await
    {
        Ok(StoreOutcome::Stored) => outcome = StoreOutcome::Stored,
        Ok(StoreOutcome::Skipped) => {}
        Err(e) => return Err(e),
    }

    if let Some(mod_fe) = pick_go_mod_file(module) {
        let mod_path = scratch.join("go.mod");
        extract_zip_entry_to_path(archive, mod_fe, ca_layout, &mod_path, budget)?;
        match store_go_mod(
            state,
            repo,
            &module.name,
            &module.version,
            &mod_path,
            user_id,
        )
        .await
        {
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
    path: &Path,
    user_id: Uuid,
) -> Result<StoreOutcome> {
    let artifact_path = build_go_zip_artifact_path(module, version);
    let (digests, size_bytes) = hash_file(path)?;
    let checksum = digests.sha256.clone();
    if let Some(outcome) = live_checksum_store_outcome(
        live_go_file_checksum(&state.db, repo.id, module, version, "%.zip")
            .await?
            .as_deref(),
        &checksum,
    )? {
        return Ok(outcome);
    }

    let storage_key = build_go_zip_storage_key(module, version);
    crate::api::handlers::cleanup_soft_deleted_artifact_checked(
        &state.db,
        &RepositoryFormat::Go,
        repo.id,
        &artifact_path,
        &checksum,
    )
    .await?;
    proxy_helpers::guard_cross_repo_write(state, repo.id, &repo.storage_backend, &storage_key)
        .await
        .map_err(response_to_app_error)?;

    let storage = state
        .storage_for_repo(&repo.storage_location())
        .map_err(|e| AppError::Internal(e.to_string()))?;
    storage
        .put_file(&storage_key, path)
        .await
        .map_err(|e| AppError::Storage(e.to_string()))?;

    let artifact_id: Uuid = sqlx::query_scalar(
        r#"
        INSERT INTO artifacts (
            repository_id, path, name, version, size_bytes,
            checksum_sha256, content_type, storage_key, uploaded_by
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        RETURNING id
        "#,
    )
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
    let _ = sqlx::query(
        r#"
        INSERT INTO artifact_metadata (artifact_id, format, metadata)
        VALUES ($1, 'go', $2)
        ON CONFLICT (artifact_id) DO UPDATE SET metadata = $2
        "#,
    )
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
    path: &Path,
    user_id: Uuid,
) -> Result<StoreOutcome> {
    let artifact_path = build_go_mod_artifact_path(module, version);
    let (digests, size_bytes) = hash_file(path)?;
    let checksum = digests.sha256.clone();
    if let Some(outcome) = live_checksum_store_outcome(
        live_go_file_checksum(&state.db, repo.id, module, version, "%.mod")
            .await?
            .as_deref(),
        &checksum,
    )? {
        return Ok(outcome);
    }

    let storage_key = build_go_mod_storage_key(module, version);
    crate::api::handlers::cleanup_soft_deleted_artifact_checked(
        &state.db,
        &RepositoryFormat::Go,
        repo.id,
        &artifact_path,
        &checksum,
    )
    .await?;
    proxy_helpers::guard_cross_repo_write(state, repo.id, &repo.storage_backend, &storage_key)
        .await
        .map_err(response_to_app_error)?;

    let storage = state
        .storage_for_repo(&repo.storage_location())
        .map_err(|e| AppError::Internal(e.to_string()))?;
    storage
        .put_file(&storage_key, path)
        .await
        .map_err(|e| AppError::Storage(e.to_string()))?;

    let artifact_id: Uuid = sqlx::query_scalar(
        r#"
        INSERT INTO artifacts (
            repository_id, path, name, version, size_bytes,
            checksum_sha256, content_type, storage_key, uploaded_by
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        RETURNING id
        "#,
    )
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
    let _ = sqlx::query(
        r#"
        INSERT INTO artifact_metadata (artifact_id, format, metadata)
        VALUES ($1, 'go', $2)
        ON CONFLICT (artifact_id) DO UPDATE SET metadata = $2
        "#,
    )
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
    job_id: Option<Uuid>,
    archive: &mut ZipArchive<File>,
    budget: &mut ExtractBudget,
    ca_layout: bool,
    scratch: &Path,
    manifest: &FerryManifest,
    user_id: Uuid,
    progress: &mut FerryIngestProgress,
) -> Result<()> {
    for (idx, module) in manifest.modules.iter().enumerate() {
        progress.cursor_module_index = idx as i32;
        clear_scratch_dir(scratch)?;
        match store_npm_module(
            state,
            repo,
            archive,
            budget,
            ca_layout,
            scratch,
            module,
            user_id,
        )
        .await
        {
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
        progress.cursor_module_index = (idx as i32).saturating_add(1);
        if maybe_checkpoint(progress) {
            write_progress(state, ferry_artifact_id, job_id, progress).await;
        }
    }
    Ok(())
}

fn pick_npm_tarball<'a>(module: &'a ModuleEntry, ca_layout: bool) -> Result<&'a FileEntry> {
    if let Some(f) = module
        .files
        .iter()
        .find(|f| normalize_zip_name(&f.relpath).ends_with(".tgz"))
    {
        return Ok(f);
    }
    if ca_layout {
        if let Some(f) = module.files.first() {
            return Ok(f);
        }
    }
    Err(AppError::Validation(format!(
        "npm package {}@{} missing .tgz",
        module.name, module.version
    )))
}

async fn store_npm_module(
    state: &SharedState,
    repo: &Repository,
    archive: &mut ZipArchive<File>,
    budget: &mut ExtractBudget,
    ca_layout: bool,
    scratch: &Path,
    module: &ModuleEntry,
    user_id: Uuid,
) -> Result<StoreOutcome> {
    let tgz_fe = pick_npm_tarball(module, ca_layout)?;
    let tgz_path = scratch.join("package.tgz");
    extract_zip_entry_to_path(archive, tgz_fe, ca_layout, &tgz_path, budget)?;

    let (claimed_name, claimed_version, version_data) =
        npm_identity_from_tarball_path(&tgz_path).unwrap_or_else(|| {
            (
                module.name.clone(),
                module.version.clone(),
                serde_json::json!({
                    "name": module.name,
                    "version": module.version,
                }),
            )
        });

    let filename = Path::new(&normalize_zip_name(&tgz_fe.relpath))
        .file_name()
        .and_then(|s| s.to_str())
        .filter(|s| s.ends_with(".tgz"))
        .unwrap_or("package.tgz")
        .to_string();
    let artifact_path = format!("{claimed_name}/{claimed_version}/{filename}");

    let (digests, size_bytes) = hash_file(&tgz_path)?;
    let checksum = digests.sha256.clone();
    if let Some(outcome) = live_checksum_store_outcome(
        live_artifact_checksum(&state.db, repo.id, &artifact_path)
            .await?
            .as_deref(),
        &checksum,
    )? {
        return Ok(outcome);
    }
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
        .put_file(&storage_key, &tgz_path)
        .await
        .map_err(|e| AppError::Storage(e.to_string()))?;

    let artifact_id: Uuid = sqlx::query_scalar(
        r#"
        INSERT INTO artifacts (
            repository_id, path, name, version, size_bytes,
            checksum_sha256, content_type, storage_key, uploaded_by
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        RETURNING id
        "#,
    )
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
    let _ = sqlx::query(
        r#"
        INSERT INTO artifact_metadata (artifact_id, format, metadata)
        VALUES ($1, 'npm', $2)
        ON CONFLICT (artifact_id) DO UPDATE SET metadata = $2
        "#,
    )
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

fn npm_identity_from_tarball_path(
    path: &Path,
) -> Option<(String, String, serde_json::Value)> {
    let file = File::open(path).ok()?;
    let body = crate::util::bounded_archive::read_metadata_from_tar_gz(file, |p| {
        p == Path::new("package/package.json")
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
    job_id: Option<Uuid>,
    archive: &mut ZipArchive<File>,
    budget: &mut ExtractBudget,
    ca_layout: bool,
    scratch: &Path,
    manifest: &FerryManifest,
    user_id: Uuid,
    progress: &mut FerryIngestProgress,
) -> Result<()> {
    for (idx, module) in manifest.modules.iter().enumerate() {
        progress.cursor_module_index = idx as i32;
        for file in &module.files {
            clear_scratch_dir(scratch)?;
            let rel = normalize_zip_name(&file.relpath);
            let filename = Path::new(&rel)
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("dist.bin");
            let path = scratch.join(filename);
            if let Err(e) = extract_zip_entry_to_path(archive, file, ca_layout, &path, budget) {
                progress.failed += 1;
                push_error(
                    progress,
                    format!("{}@{} ({rel}): {e}", module.name, module.version),
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
            if maybe_checkpoint(progress) {
                write_progress(state, ferry_artifact_id, job_id, progress).await;
            }
        }
        progress.cursor_module_index = (idx as i32).saturating_add(1);
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

    let (digests, size_bytes) = hash_file(file_path)?;
    if let Some(outcome) = live_checksum_store_outcome(
        live_artifact_checksum(&state.db, repo.id, &artifact_path)
            .await?
            .as_deref(),
        &digests.sha256,
    )? {
        return Ok(outcome);
    }
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
    job_id: Option<Uuid>,
    archive: &mut ZipArchive<File>,
    budget: &mut ExtractBudget,
    ca_layout: bool,
    scratch: &Path,
    manifest: &FerryManifest,
    user_id: Uuid,
    progress: &mut FerryIngestProgress,
) -> Result<()> {
    for (idx, module) in manifest.modules.iter().enumerate() {
        progress.cursor_module_index = idx as i32;
        clear_scratch_dir(scratch)?;
        match store_cargo_module(
            state,
            repo,
            archive,
            budget,
            ca_layout,
            scratch,
            module,
            user_id,
        )
        .await
        {
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
        progress.cursor_module_index = (idx as i32).saturating_add(1);
        if maybe_checkpoint(progress) {
            write_progress(state, ferry_artifact_id, job_id, progress).await;
        }
    }
    Ok(())
}

fn pick_cargo_crate<'a>(module: &'a ModuleEntry, ca_layout: bool) -> Result<&'a FileEntry> {
    if let Some(f) = module
        .files
        .iter()
        .find(|f| normalize_zip_name(&f.relpath).ends_with(".crate"))
    {
        return Ok(f);
    }
    if ca_layout {
        if let Some(f) = module.files.first() {
            return Ok(f);
        }
    }
    Err(AppError::Validation(format!(
        "cargo crate {}@{} missing .crate",
        module.name, module.version
    )))
}

async fn store_cargo_module(
    state: &SharedState,
    repo: &Repository,
    archive: &mut ZipArchive<File>,
    budget: &mut ExtractBudget,
    ca_layout: bool,
    scratch: &Path,
    module: &ModuleEntry,
    user_id: Uuid,
) -> Result<StoreOutcome> {
    let crate_fe = pick_cargo_crate(module, ca_layout)?;
    let crate_path = scratch.join("pkg.crate");
    extract_zip_entry_to_path(archive, crate_fe, ca_layout, &crate_path, budget)?;

    let name_lower = module.name.to_ascii_lowercase();
    let filename = format!("{}-{}.crate", name_lower, module.version);
    let artifact_path = format!("{name_lower}/{}/{filename}", module.version);

    let (digests, size_bytes) = hash_file(&crate_path)?;
    let checksum = digests.sha256.clone();
    if let Some(outcome) = live_checksum_store_outcome(
        live_artifact_checksum(&state.db, repo.id, &artifact_path)
            .await?
            .as_deref(),
        &checksum,
    )? {
        return Ok(outcome);
    }
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
        .put_file(&storage_key, &crate_path)
        .await
        .map_err(|e| AppError::Storage(e.to_string()))?;

    let cargo_metadata = serde_json::json!({
        "name": &name_lower,
        "vers": module.version,
        "deps": [],
        "cksum": &checksum,
        "features": {},
        "yanked": false,
    });

    let artifact_id: Uuid = sqlx::query_scalar(
        r#"
        INSERT INTO artifacts (
            repository_id, path, name, version, size_bytes,
            checksum_sha256, content_type, storage_key, uploaded_by
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        RETURNING id
        "#,
    )
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

    let _ = sqlx::query(
        r#"
        INSERT INTO artifact_metadata (artifact_id, format, metadata)
        VALUES ($1, 'cargo', $2)
        ON CONFLICT (artifact_id) DO UPDATE SET metadata = $2
        "#,
    )
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
    use std::io::Cursor;
    use zip::write::SimpleFileOptions;

    fn sha256_hex(data: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(data);
        format!("{:x}", hasher.finalize())
    }

    fn write_zip(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let mut cursor = Cursor::new(Vec::new());
        {
            let mut zip = zip::ZipWriter::new(&mut cursor);
            let opts = SimpleFileOptions::default();
            for (name, data) in entries {
                zip.start_file(*name, opts).unwrap();
                zip.write_all(data).unwrap();
            }
            zip.finish().unwrap();
        }
        cursor.into_inner()
    }

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
        assert_eq!(
            encode_module_path("github.com/Azure/go-sdk"),
            "github.com/!azure/go-sdk"
        );
    }

    #[test]
    fn blob_path_layout() {
        let h = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
        assert_eq!(
            blob_path_for_sha256(h),
            format!("blobs/sha256/ab/cd/{h}")
        );
        assert_eq!(flat_blob_path_for_sha256(h), format!("blobs/sha256/{h}"));
    }

    #[test]
    fn resolve_prefers_explicit_blob_relpath() {
        let rel = "blobs/sha256/aa/bb/aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899";
        let name = resolve_zip_entry_name(rel, "deadbeef", true);
        assert_eq!(name, rel);
    }

    #[test]
    fn resolve_maps_sha256_when_ca_layout() {
        let h = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let name = resolve_zip_entry_name("go/mod.zip", h, true);
        assert_eq!(name, blob_path_for_sha256(h));
        let plain = resolve_zip_entry_name("go/mod.zip", h, false);
        assert_eq!(plain, "go/mod.zip");
    }

    #[test]
    fn load_manifest_from_zip_json() {
        let manifest = serde_json::json!({
            "version": 1,
            "kind": "ak-ferry",
            "ecosystem": "go",
            "modules": [{
                "name": "example.com/m",
                "version": "v1.0.0",
                "files": [{"relpath": "m.zip", "sha256": "", "size": 1}]
            }]
        });
        let bytes = write_zip(&[(
            "ak-ferry.json",
            serde_json::to_string(&manifest).unwrap().as_bytes(),
        )]);
        let cursor = Cursor::new(bytes);
        let mut archive = ZipArchive::new(cursor).unwrap();
        let mut budget = ExtractBudget::new(1024 * 1024, 100);
        let (loaded, ca) = load_manifest_from_zip(&mut archive, &mut budget).unwrap();
        assert_eq!(loaded.ecosystem, "go");
        assert_eq!(loaded.modules.len(), 1);
        assert!(!ca);
    }

    #[test]
    fn extract_verifies_sha256() {
        let payload = b"hello-ferry";
        let good = sha256_hex(payload);
        let bytes = write_zip(&[("pkg/data.bin", payload)]);
        let cursor = Cursor::new(bytes);
        let mut archive = ZipArchive::new(cursor).unwrap();
        let mut budget = ExtractBudget::new(1024 * 1024, 100);
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("out.bin");

        let ok_entry = FileEntry {
            relpath: "pkg/data.bin".into(),
            sha256: good.clone(),
            size: payload.len() as u64,
        };
        extract_zip_entry_to_path(&mut archive, &ok_entry, false, &dest, &mut budget).unwrap();
        assert_eq!(std::fs::read(&dest).unwrap(), payload);

        let bad_entry = FileEntry {
            relpath: "pkg/data.bin".into(),
            sha256: "0".repeat(64),
            size: payload.len() as u64,
        };
        let mut budget2 = ExtractBudget::new(1024 * 1024, 100);
        let dest2 = dir.path().join("bad.bin");
        let err =
            extract_zip_entry_to_path(&mut archive, &bad_entry, false, &dest2, &mut budget2);
        assert!(err.is_err());
    }

    #[test]
    fn extract_from_ca_blob_path() {
        let payload = b"ca-blob-bytes";
        let h = sha256_hex(payload);
        let nested = blob_path_for_sha256(&h);
        let bytes = write_zip(&[(&nested, payload)]);
        let cursor = Cursor::new(bytes);
        let mut archive = ZipArchive::new(cursor).unwrap();
        assert!(uses_ca_blob_layout(MANIFEST_KIND, &archive));

        let mut budget = ExtractBudget::new(1024 * 1024, 100);
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("blob.bin");
        let entry = FileEntry {
            relpath: "logical/name.bin".into(),
            sha256: h,
            size: payload.len() as u64,
        };
        extract_zip_entry_to_path(&mut archive, &entry, true, &dest, &mut budget).unwrap();
        assert_eq!(std::fs::read(&dest).unwrap(), payload);
    }

    #[test]
    fn unpack_zip_limited_budget() {
        let big = vec![b'x'; 4096];
        let bytes = write_zip(&[("a.bin", &big), ("b.bin", &big)]);
        let dir = tempfile::tempdir().unwrap();
        let zip_path = dir.path().join("t.zip");
        std::fs::write(&zip_path, &bytes).unwrap();
        let file = File::open(&zip_path).unwrap();
        let out = dir.path().join("out");
        std::fs::create_dir_all(&out).unwrap();
        let err = unpack_zip_limited(file, &out, 100, 100);
        assert!(err.is_err());
    }

    #[test]
    fn find_zip_index_uses_shared_ref() {
        let bytes = write_zip(&[("ak-ferry.json", b"{}"), ("x.bin", b"1")]);
        let cursor = Cursor::new(bytes);
        let archive = ZipArchive::new(cursor).unwrap();
        assert_eq!(find_zip_index(&archive, "x.bin"), Some(1));
        assert!(find_zip_index(&archive, "missing").is_none());
    }

    #[test]
    fn live_checksum_skips_identical_and_conflicts_on_swap() {
        assert_eq!(live_checksum_store_outcome(None, "aaaa").unwrap(), None);
        assert_eq!(
            live_checksum_store_outcome(Some("AaAa"), "aaaa").unwrap(),
            Some(StoreOutcome::Skipped)
        );
        let err = live_checksum_store_outcome(Some("aaaa"), "bbbb").unwrap_err();
        assert!(
            matches!(err, AppError::Conflict(msg) if msg.contains("immutable")),
            "different bytes at a live coordinate must conflict, got {err:?}"
        );
    }
}
