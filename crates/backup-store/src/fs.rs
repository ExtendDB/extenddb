// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Filesystem-backed store.
//!
//! Keys map to paths under a root directory that is canonicalized at
//! construction. Every operation re-resolves its target and refuses to act
//! when the resolved path escapes the root, so a symlink planted inside the
//! root cannot redirect reads or writes elsewhere. Writes go to a temporary
//! sibling file and rename into place, so a crash never leaves a truncated
//! file under its final name.

use std::path::{Path, PathBuf};

use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt, TryStreamExt};
use tokio::io::AsyncWriteExt;

use crate::key::{key_matches_prefix, normalize_prefix, validate_key, validate_prefix};
use crate::{BackupStore, ByteStream, ObjectMeta, StoreError};

/// Prefix and suffix marking in-flight temporary files, hidden from listings.
const TEMP_PREFIX: &str = ".put-";
const TEMP_SUFFIX: &str = ".tmp";

/// Store writing objects under a local root directory.
pub struct FilesystemStore {
    /// Canonicalized root; every resolved target must stay under it.
    root: PathBuf,
}

impl FilesystemStore {
    /// Open a store rooted at `root`. The directory must already exist; it is
    /// canonicalized here and the canonical form anchors every escape check.
    ///
    /// # Errors
    ///
    /// Returns an error when the root does not exist, cannot be resolved, or
    /// is not a directory.
    pub async fn open(root: impl AsRef<Path>) -> Result<Self, StoreError> {
        let root = root.as_ref();
        let canonical = tokio::fs::canonicalize(root).await.map_err(|e| {
            StoreError::io(format!("backup root {} is not usable", root.display()), e)
        })?;
        let meta = tokio::fs::metadata(&canonical)
            .await
            .map_err(|e| StoreError::io(format!("stat {}", canonical.display()), e))?;
        if !meta.is_dir() {
            return Err(StoreError::Other(format!(
                "backup root {} is not a directory",
                canonical.display()
            )));
        }
        Ok(Self { root: canonical })
    }

    /// Map a validated key to its path under the root. Key validation has
    /// already refused `..`, empty components, and separators, so the join
    /// cannot step outside the root without a symlink, which the resolve
    /// checks catch.
    fn key_path(&self, key: &str) -> PathBuf {
        let mut path = self.root.clone();
        for component in key.split('/') {
            path.push(component);
        }
        path
    }

    /// Canonicalize `path` and refuse it when it resolves outside the root.
    /// Missing paths surface as `NotFound`.
    async fn resolve_within_root(&self, path: &Path) -> Result<PathBuf, StoreError> {
        let canonical = tokio::fs::canonicalize(path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                StoreError::NotFound
            } else {
                StoreError::io(format!("resolve {}", path.display()), e)
            }
        })?;
        if canonical.starts_with(&self.root) {
            Ok(canonical)
        } else {
            Err(StoreError::PermissionDenied(format!(
                "{} resolves outside the backup root",
                path.display()
            )))
        }
    }

    /// Prepare the directory a key's object will be written into, without
    /// ever creating a directory outside the root.
    ///
    /// `create_dir_all` follows symlinks in existing path components, so
    /// checking the parent only after creating it would already have created
    /// directories on the far side of a planted symlink. Instead: walk to the
    /// deepest existing ancestor of the destination, canonicalize it, and
    /// refuse before creating anything when it resolves outside the root.
    /// The remaining components are then created one at a time with
    /// `create_dir`, re-verifying after each step that the component is a
    /// real directory and not a symlink.
    async fn prepare_parent(&self, parent_components: &[&str]) -> Result<PathBuf, StoreError> {
        let mut existing = self.root.clone();
        let mut created_from = parent_components.len();
        for (index, component) in parent_components.iter().enumerate() {
            let candidate = existing.join(component);
            match tokio::fs::symlink_metadata(&candidate).await {
                Ok(_) => existing = candidate,
                // NotADirectory: an existing ancestor is a file or a symlink
                // to one, so the walk stops there and the checks below decide
                // (an escaping symlink is refused, an in-root file is an
                // ancestor collision).
                Err(e)
                    if e.kind() == std::io::ErrorKind::NotFound
                        || e.kind() == std::io::ErrorKind::NotADirectory =>
                {
                    created_from = index;
                    break;
                }
                Err(e) => {
                    return Err(StoreError::io(format!("stat {}", candidate.display()), e));
                }
            }
        }

        // The deepest existing ancestor is checked before anything is
        // created: if it resolves outside the root (a planted symlink
        // anywhere in the chain), the put is refused with nothing written.
        let canonical = tokio::fs::canonicalize(&existing)
            .await
            .map_err(|e| StoreError::io(format!("resolve {}", existing.display()), e))?;
        if !canonical.starts_with(&self.root) {
            return Err(StoreError::PermissionDenied(format!(
                "{} resolves outside the backup root",
                existing.display()
            )));
        }
        let meta = tokio::fs::metadata(&canonical)
            .await
            .map_err(|e| StoreError::io(format!("stat {}", canonical.display()), e))?;
        if !meta.is_dir() {
            return Err(StoreError::io(
                format!("create under {}", canonical.display()),
                std::io::Error::new(
                    std::io::ErrorKind::NotADirectory,
                    "an object already exists at an ancestor of the key",
                ),
            ));
        }

        let mut current = canonical;
        for component in &parent_components[created_from..] {
            let next = current.join(component);
            match tokio::fs::create_dir(&next).await {
                Ok(()) => {}
                // A concurrent put may have created it; the check below
                // verifies what is actually there.
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(e) => return Err(StoreError::io(format!("create {}", next.display()), e)),
            }
            let meta = tokio::fs::symlink_metadata(&next)
                .await
                .map_err(|e| StoreError::io(format!("stat {}", next.display()), e))?;
            if meta.is_symlink() || !meta.is_dir() {
                return Err(StoreError::PermissionDenied(format!(
                    "{} is not a directory inside the backup root",
                    next.display()
                )));
            }
            current = next;
        }
        Ok(current)
    }

    async fn put_impl(&self, key: &str, mut body: ByteStream) -> Result<(), StoreError> {
        validate_key(key)?;
        let components: Vec<&str> = key.split('/').collect();
        let (file_name, parent_components) = components
            .split_last()
            .expect("a validated key has at least one component");
        let parent = self.prepare_parent(parent_components).await?;
        let final_path = parent.join(file_name);

        // If the destination already exists, refuse when it resolves outside
        // the root, so a put cannot be redirected through a planted symlink.
        match tokio::fs::symlink_metadata(&final_path).await {
            Ok(meta) if meta.is_symlink() => match self.resolve_within_root(&final_path).await {
                Ok(_) => {}
                Err(StoreError::NotFound) => {
                    return Err(StoreError::PermissionDenied(format!(
                        "{} is a symlink that does not resolve inside the backup root",
                        final_path.display()
                    )));
                }
                Err(e) => return Err(e),
            },
            Ok(_) | Err(_) => {}
        }

        let temp_path = parent.join(format!(
            "{TEMP_PREFIX}{}{TEMP_SUFFIX}",
            uuid::Uuid::new_v4()
        ));

        // Durability contract: the temp file is synced before the rename and
        // the parent directory is synced after it, so an Ok return means both
        // the object bytes and its directory entry are on disk. Without the
        // directory sync the rename itself could be lost on power failure
        // even though the file data had been written.
        let write_result = async {
            let mut file = tokio::fs::File::create(&temp_path)
                .await
                .map_err(|e| StoreError::io(format!("create {}", temp_path.display()), e))?;
            while let Some(chunk) = body.next().await {
                let chunk = chunk?;
                file.write_all(&chunk)
                    .await
                    .map_err(|e| StoreError::io(format!("write {}", temp_path.display()), e))?;
            }
            file.sync_all()
                .await
                .map_err(|e| StoreError::io(format!("sync {}", temp_path.display()), e))?;
            drop(file);
            tokio::fs::rename(&temp_path, &final_path)
                .await
                .map_err(|e| {
                    StoreError::io(
                        format!("rename {} to {}", temp_path.display(), final_path.display()),
                        e,
                    )
                })?;
            sync_dir(&parent).await
        }
        .await;

        if write_result.is_err() {
            // Best effort: never leave the temporary behind on failure.
            let _ = tokio::fs::remove_file(&temp_path).await;
        }
        write_result
    }

    async fn get_impl(&self, key: &str) -> Result<ByteStream, StoreError> {
        validate_key(key)?;
        let path = self.resolve_within_root(&self.key_path(key)).await?;
        let meta = tokio::fs::metadata(&path)
            .await
            .map_err(|e| StoreError::io(format!("stat {}", path.display()), e))?;
        if !meta.is_file() {
            return Err(StoreError::NotFound);
        }
        let file = tokio::fs::File::open(&path)
            .await
            .map_err(|e| StoreError::io(format!("open {}", path.display()), e))?;
        let display = path.display().to_string();
        let stream = tokio_util::io::ReaderStream::new(file)
            .map_err(move |e| StoreError::io(format!("read {display}"), e));
        Ok(Box::pin(stream))
    }

    async fn head_impl(&self, key: &str) -> Result<Option<ObjectMeta>, StoreError> {
        validate_key(key)?;
        let path = match self.resolve_within_root(&self.key_path(key)).await {
            Ok(path) => path,
            Err(StoreError::NotFound) => return Ok(None),
            Err(e) => return Err(e),
        };
        let meta = tokio::fs::metadata(&path)
            .await
            .map_err(|e| StoreError::io(format!("stat {}", path.display()), e))?;
        if !meta.is_file() {
            return Ok(None);
        }
        Ok(Some(ObjectMeta {
            key: key.to_owned(),
            size: meta.len(),
            last_modified: meta.modified().ok(),
        }))
    }

    async fn list_impl(&self, prefix: &str) -> Result<Vec<ObjectMeta>, StoreError> {
        validate_prefix(prefix)?;
        let normalized = normalize_prefix(prefix).to_owned();
        let root = self.root.clone();
        tokio::task::spawn_blocking(move || collect_objects(&root, &normalized))
            .await
            .map_err(|e| StoreError::Other(format!("listing task failed: {e}")))?
    }

    async fn delete_prefix_impl(&self, prefix: &str) -> Result<u64, StoreError> {
        validate_prefix(prefix)?;
        let normalized = normalize_prefix(prefix).to_owned();
        let root = self.root.clone();
        tokio::task::spawn_blocking(move || delete_objects(&root, &normalized))
            .await
            .map_err(|e| StoreError::Other(format!("deletion task failed: {e}")))?
    }

    async fn validate_impl(&self) -> Result<(), StoreError> {
        let meta = tokio::fs::metadata(&self.root).await.map_err(|e| {
            StoreError::io(
                format!("backup root {} is not usable", self.root.display()),
                e,
            )
        })?;
        if !meta.is_dir() {
            return Err(StoreError::Other(format!(
                "backup root {} is not a directory",
                self.root.display()
            )));
        }
        let probe = self.root.join(format!(
            "{TEMP_PREFIX}validate-{}{TEMP_SUFFIX}",
            uuid::Uuid::new_v4()
        ));
        tokio::fs::write(&probe, b"probe").await.map_err(|e| {
            map_permission(
                format!("backup root {} is not writable", self.root.display()),
                e,
            )
        })?;
        tokio::fs::remove_file(&probe)
            .await
            .map_err(|e| StoreError::io(format!("remove probe {}", probe.display()), e))?;
        Ok(())
    }
}

impl BackupStore for FilesystemStore {
    fn put<'a>(&'a self, key: &'a str, body: ByteStream) -> BoxFuture<'a, Result<(), StoreError>> {
        self.put_impl(key, body).boxed()
    }

    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<ByteStream, StoreError>> {
        self.get_impl(key).boxed()
    }

    fn head<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<ObjectMeta>, StoreError>> {
        self.head_impl(key).boxed()
    }

    fn list<'a>(&'a self, prefix: &'a str) -> BoxStream<'a, Result<ObjectMeta, StoreError>> {
        let items = async move {
            match self.list_impl(prefix).await {
                Ok(metas) => metas.into_iter().map(Ok).collect::<Vec<_>>(),
                Err(e) => vec![Err(e)],
            }
        };
        Box::pin(
            futures::stream::once(items)
                .map(futures::stream::iter)
                .flatten(),
        )
    }

    fn delete_prefix<'a>(&'a self, prefix: &'a str) -> BoxFuture<'a, Result<u64, StoreError>> {
        self.delete_prefix_impl(prefix).boxed()
    }

    fn validate(&self) -> BoxFuture<'_, Result<(), StoreError>> {
        self.validate_impl().boxed()
    }
}

fn map_permission(context: String, e: std::io::Error) -> StoreError {
    if e.kind() == std::io::ErrorKind::PermissionDenied {
        StoreError::PermissionDenied(context)
    } else {
        StoreError::io(context, e)
    }
}

/// Fsync a directory so a rename inside it is durable. Windows has no
/// directory handle to sync; there the rename's durability is left to the
/// operating system.
async fn sync_dir(dir: &std::path::Path) -> Result<(), StoreError> {
    #[cfg(unix)]
    {
        let handle = tokio::fs::File::open(dir)
            .await
            .map_err(|e| StoreError::io(format!("open directory {}", dir.display()), e))?;
        handle
            .sync_all()
            .await
            .map_err(|e| StoreError::io(format!("sync directory {}", dir.display()), e))?;
    }
    #[cfg(not(unix))]
    {
        let _ = dir;
    }
    Ok(())
}

fn is_temp_name(name: &str) -> bool {
    name.starts_with(TEMP_PREFIX) && name.ends_with(TEMP_SUFFIX)
}

/// Path for a normalized (possibly empty) prefix.
fn prefix_path(root: &Path, prefix: &str) -> PathBuf {
    if prefix.is_empty() {
        return root.to_path_buf();
    }
    let mut path = root.to_path_buf();
    for component in prefix.split('/') {
        path.push(component);
    }
    path
}

/// Collect object metadata under a normalized prefix, sorted by key.
fn collect_objects(root: &Path, prefix: &str) -> Result<Vec<ObjectMeta>, StoreError> {
    let base = prefix_path(root, prefix);
    let mut out = Vec::new();
    match std::fs::symlink_metadata(&base) {
        Ok(meta) if meta.is_file() => {
            // The prefix names an object directly (key == prefix).
            out.push(ObjectMeta {
                key: prefix.to_owned(),
                size: meta.len(),
                last_modified: meta.modified().ok(),
            });
        }
        Ok(meta) if meta.is_dir() => {
            walk_files(root, &base, &mut out)?;
        }
        // A symlink at the prefix, or nothing there at all: no objects.
        Ok(_) | Err(_) => {}
    }
    // Defense in depth: the walk starts at the prefix directory, so every key
    // matches by construction; keep the shared rule as the final filter.
    out.retain(|meta| key_matches_prefix(&meta.key, prefix));
    out.sort_by(|a, b| a.key.cmp(&b.key));
    Ok(out)
}

fn walk_files(root: &Path, dir: &Path, out: &mut Vec<ObjectMeta>) -> Result<(), StoreError> {
    let entries = std::fs::read_dir(dir)
        .map_err(|e| StoreError::io(format!("read directory {}", dir.display()), e))?;
    for entry in entries {
        let entry =
            entry.map_err(|e| StoreError::io(format!("read directory {}", dir.display()), e))?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(|e| StoreError::io(format!("stat {}", path.display()), e))?;
        if file_type.is_dir() {
            walk_files(root, &path, out)?;
        } else if file_type.is_file() {
            let name = entry.file_name();
            if is_temp_name(&name.to_string_lossy()) {
                continue;
            }
            let meta = entry
                .metadata()
                .map_err(|e| StoreError::io(format!("stat {}", path.display()), e))?;
            let key = path
                .strip_prefix(root)
                .expect("walk stays under the root")
                .components()
                .map(|c| c.as_os_str().to_string_lossy().into_owned())
                .collect::<Vec<_>>()
                .join("/");
            out.push(ObjectMeta {
                key,
                size: meta.len(),
                last_modified: meta.modified().ok(),
            });
        }
        // Symlinks are never objects this store wrote; skip them.
    }
    Ok(())
}

/// Delete everything under a normalized prefix; returns the number of files
/// removed. Directories emptied by the deletion are removed too, including
/// the prefix directory itself.
fn delete_objects(root: &Path, prefix: &str) -> Result<u64, StoreError> {
    let base = prefix_path(root, prefix);
    match std::fs::symlink_metadata(&base) {
        Ok(meta) if meta.is_file() => {
            std::fs::remove_file(&base)
                .map_err(|e| StoreError::io(format!("remove {}", base.display()), e))?;
            Ok(1)
        }
        Ok(meta) if meta.is_dir() => {
            if prefix.is_empty() {
                // Deleting everything must not remove the root itself.
                let mut count = 0;
                let entries = std::fs::read_dir(&base)
                    .map_err(|e| StoreError::io(format!("read directory {}", base.display()), e))?;
                for entry in entries {
                    let entry = entry.map_err(|e| {
                        StoreError::io(format!("read directory {}", base.display()), e)
                    })?;
                    count += delete_entry(&entry.path())?;
                }
                Ok(count)
            } else {
                delete_entry(&base)
            }
        }
        Ok(_) | Err(_) => Ok(0),
    }
}

fn delete_entry(path: &Path) -> Result<u64, StoreError> {
    let meta = std::fs::symlink_metadata(path)
        .map_err(|e| StoreError::io(format!("stat {}", path.display()), e))?;
    if meta.is_dir() {
        let mut count = 0;
        let entries = std::fs::read_dir(path)
            .map_err(|e| StoreError::io(format!("read directory {}", path.display()), e))?;
        for entry in entries {
            let entry = entry
                .map_err(|e| StoreError::io(format!("read directory {}", path.display()), e))?;
            count += delete_entry(&entry.path())?;
        }
        std::fs::remove_dir(path)
            .map_err(|e| StoreError::io(format!("remove directory {}", path.display()), e))?;
        Ok(count)
    } else {
        std::fs::remove_file(path)
            .map_err(|e| StoreError::io(format!("remove {}", path.display()), e))?;
        Ok(1)
    }
}
