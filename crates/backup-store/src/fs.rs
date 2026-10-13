// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Filesystem-backed store.
//!
//! Keys map to paths under a root directory that is canonicalized at
//! construction. Every operation resolves its key one component at a time
//! with `symlink_metadata` and refuses any component that is a symlink,
//! whether it points inside or outside the root, so a key never resolves
//! through a link.
//!
//! Trust boundary. The symlink checks defeat links that exist when an
//! operation starts. They do not defend against another process mutating
//! the tree while an operation runs: between a component check and the next
//! path step the kernel re-resolves the textual path, so a process with
//! write access inside the root could swap a checked directory for a
//! symlink mid-operation. Closing that window needs descriptor-relative
//! resolution (`openat2` with `RESOLVE_BENEATH`), which the standard library
//! does not expose. The store therefore requires that no other user can
//! reach anything under the root, and enforces it on Unix: `open` and
//! `validate` refuse a root that grants any permission to group or others
//! or is owned by a different user, refuse a root whose ancestor directory
//! is writable by others without the sticky bit (such an ancestor lets
//! another user rename the root away and plant a symlink at its path),
//! every operation re-checks the root and every existing directory it
//! descends into against the same owner-only rule (the ancestor rule is
//! checked at `open` and `validate` only), every directory the store
//! creates is `0700`, and every object is created `0600` (set at creation,
//! not fixed up afterwards). A process that can write under the root holds
//! full read, write, and delete over every backup in it; these checks make
//! that set equal to the owner, for the tree the store can see. They cannot
//! observe a bind mount or a filesystem without Unix permission semantics;
//! the deployment guide names those as operator responsibilities.
//!
//! `list` and `delete_prefix` skip symlinks rather than refusing them: a
//! listing is not an access through the link, and deletion removes a link
//! found below the prefix without following it. A symlink at the prefix
//! path itself is not an object under the prefix; both operations treat it
//! as nothing there.
//!
//! Writes go to a temporary sibling file and rename into place, so a crash
//! never leaves a truncated file under its final name.
//!
//! Two limits of this store narrow the shared key space. A key component
//! longer than the filesystem's name limit (`NAME_MAX`, 255 bytes on Linux)
//! fails with an I/O error even though key validation accepts it. An object
//! whose file name matches the temporary-file pattern (`.put-*.tmp`) can be
//! written and read but is hidden from listings.

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
        check_root_ownership(&canonical, &meta)?;
        Ok(Self { root: canonical })
    }

    /// Re-check the root's owner-only rule at the start of an operation, so
    /// a root widened after `open` is refused by the next call rather than
    /// trusted until the next `validate`. The ancestor rule is not repeated
    /// here; it is a startup check.
    async fn check_root(&self) -> Result<(), StoreError> {
        let meta = tokio::fs::symlink_metadata(&self.root)
            .await
            .map_err(|e| StoreError::io(format!("stat {}", self.root.display()), e))?;
        check_owner_only(&self.root, &meta)
    }

    /// Resolve a validated key to its path under the root without following
    /// symlinks: every component is checked with `symlink_metadata` and any
    /// symlink, intermediate or final, is refused whether it points inside
    /// or outside the root, so `get` and `head` agree with `put`, `list`,
    /// and `delete_prefix` on what a key names. Key validation has already
    /// refused `..`, empty components, and separators, so with no symlink in
    /// the walk the path cannot leave the root. Missing paths surface as
    /// `NotFound`. The returned metadata describes the final component.
    async fn resolve_no_follow(
        &self,
        key: &str,
    ) -> Result<(PathBuf, std::fs::Metadata), StoreError> {
        self.check_root().await?;
        let mut path = self.root.clone();
        let mut meta = None;
        for component in key.split('/') {
            path.push(component);
            let m = match tokio::fs::symlink_metadata(&path).await {
                Ok(m) => m,
                Err(e)
                    if e.kind() == std::io::ErrorKind::NotFound
                        || e.kind() == std::io::ErrorKind::NotADirectory =>
                {
                    return Err(StoreError::NotFound);
                }
                Err(e) => return Err(StoreError::io(format!("stat {}", path.display()), e)),
            };
            if m.is_symlink() {
                return Err(symlink_refused(&path));
            }
            if m.is_dir() {
                check_owner_only(&path, &m)?;
            }
            meta = Some(m);
        }
        let meta = meta.expect("a validated key has at least one component");
        Ok((path, meta))
    }

    /// Prepare the directory a key's object will be written into, without
    /// ever creating a directory outside the root.
    ///
    /// `create_dir_all` follows symlinks in existing path components, so
    /// checking the parent only after creating it would already have created
    /// directories on the far side of a planted symlink. Instead: walk to the
    /// deepest existing ancestor of the destination, refusing any component
    /// that is a symlink (in-root aliases included), and verify the ancestor
    /// still resolves under the root. The remaining components are then
    /// created one at a time with `create_dir`, re-verifying after each step
    /// that the component is a real directory and not a symlink.
    ///
    /// Returns the parent directory and, when this call created directories,
    /// the shallowest one it created, so a failed put can remove the empty
    /// skeleton again.
    async fn prepare_parent(
        &self,
        parent_components: &[&str],
    ) -> Result<(PathBuf, Option<PathBuf>), StoreError> {
        self.check_root().await?;
        let mut existing = self.root.clone();
        let mut created_from = parent_components.len();
        for (index, component) in parent_components.iter().enumerate() {
            let candidate = existing.join(component);
            match tokio::fs::symlink_metadata(&candidate).await {
                Ok(meta) if meta.is_symlink() => {
                    return Err(symlink_refused(&candidate));
                }
                Ok(meta) => {
                    if meta.is_dir() {
                        check_owner_only(&candidate, &meta)?;
                    }
                    existing = candidate;
                }
                // NotADirectory: an existing ancestor is a file, so the walk
                // stops there and the checks below decide (an in-root file
                // is an ancestor collision).
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
        // created: no component was a symlink at the walk above, and as
        // defense in depth its canonical form must still sit under the root.
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
        let mut first_created = None;
        for component in &parent_components[created_from..] {
            let next = current.join(component);
            match create_private_dir(&next).await {
                Ok(()) => {
                    if first_created.is_none() {
                        first_created = Some(next.clone());
                    }
                }
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
            check_owner_only(&next, &meta)?;
            current = next;
        }
        Ok((current, first_created))
    }

    async fn put_impl(&self, key: &str, mut body: ByteStream) -> Result<ObjectMeta, StoreError> {
        validate_key(key)?;
        let components: Vec<&str> = key.split('/').collect();
        let (file_name, parent_components) = components
            .split_last()
            .expect("a validated key has at least one component");
        let (parent, created) = self.prepare_parent(parent_components).await?;
        let final_path = parent.join(file_name);

        // A symlink at the destination, in-root or escaping, is refused: a
        // put never resolves through a link.
        match tokio::fs::symlink_metadata(&final_path).await {
            Ok(meta) if meta.is_symlink() => {
                return Err(symlink_refused(&final_path));
            }
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
            let mut file = create_private_file(&temp_path).await?;
            let mut size: u64 = 0;
            while let Some(chunk) = body.next().await {
                let chunk = chunk?;
                file.write_all(&chunk)
                    .await
                    .map_err(|e| StoreError::io(format!("write {}", temp_path.display()), e))?;
                size += chunk.len() as u64;
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
            // The object is visible under its key from here on. If the
            // directory sync fails the put still fails, and the file is
            // removed so a failed put leaves nothing observable under the
            // key, as the trait promises.
            if let Err(e) = sync_dir(&parent).await {
                let _ = tokio::fs::remove_file(&final_path).await;
                return Err(e);
            }
            let last_modified = tokio::fs::symlink_metadata(&final_path)
                .await
                .ok()
                .and_then(|m| m.modified().ok());
            Ok(ObjectMeta {
                key: key.to_owned(),
                size,
                last_modified,
                etag: None,
            })
        }
        .await;

        if write_result.is_err() {
            // Best effort: never leave the temporary or an empty directory
            // skeleton behind on failure, so a failed put is unobservable
            // through list() on this store as it is on S3. remove_dir only
            // removes empty directories, so a concurrent put that filled one
            // of them stops the walk.
            let _ = tokio::fs::remove_file(&temp_path).await;
            if let Some(first) = created {
                let mut dir = parent.clone();
                loop {
                    if tokio::fs::remove_dir(&dir).await.is_err() || dir == first {
                        break;
                    }
                    match dir.parent() {
                        Some(p) => dir = p.to_path_buf(),
                        None => break,
                    }
                }
            }
        }
        write_result
    }

    async fn get_impl(&self, key: &str) -> Result<ByteStream, StoreError> {
        validate_key(key)?;
        let (path, meta) = self.resolve_no_follow(key).await?;
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
        let (_, meta) = match self.resolve_no_follow(key).await {
            Ok(resolved) => resolved,
            Err(StoreError::NotFound) => return Ok(None),
            Err(e) => return Err(e),
        };
        if !meta.is_file() {
            return Ok(None);
        }
        Ok(Some(ObjectMeta {
            key: key.to_owned(),
            size: meta.len(),
            last_modified: meta.modified().ok(),
            etag: None,
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
        check_root_ownership(&self.root, &meta)?;
        let probe = self.root.join(format!(
            "{TEMP_PREFIX}validate-{}{TEMP_SUFFIX}",
            uuid::Uuid::new_v4()
        ));
        // The probe is written and synced so a filesystem that accepts the
        // create but refuses data (ENOSPC at flush, a read-only remount,
        // a stale network handle) is caught here rather than at the first
        // backup. The probe is removed on every path, success or failure.
        let outcome = async {
            let mut file = create_private_file(&probe).await?;
            file.write_all(b"probe")
                .await
                .map_err(|e| StoreError::io(format!("write probe {}", probe.display()), e))?;
            file.sync_all()
                .await
                .map_err(|e| StoreError::io(format!("sync probe {}", probe.display()), e))?;
            Ok::<(), StoreError>(())
        }
        .await;
        let removed = tokio::fs::remove_file(&probe).await;
        outcome.map_err(|e| match e {
            StoreError::PermissionDenied(_) => StoreError::PermissionDenied(format!(
                "backup root {} is not writable by this process",
                self.root.display()
            )),
            other => other,
        })?;
        if let Err(e) = removed {
            return Err(StoreError::io(
                format!("remove probe {}", probe.display()),
                e,
            ));
        }
        Ok(())
    }
}

impl BackupStore for FilesystemStore {
    fn put<'a>(
        &'a self,
        key: &'a str,
        body: ByteStream,
    ) -> BoxFuture<'a, Result<ObjectMeta, StoreError>> {
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

/// Refuse a root that another user could reach or displace. On Unix the
/// root must be owned by the effective user and grant no permission to group
/// or others (`mode & 0o077 == 0`), and every ancestor must be owned by the
/// effective user or by root and must not be writable by others unless the
/// sticky bit is set: an ancestor that others can write to lets them rename
/// the root away and plant a symlink at its path, which the textual
/// re-resolution in this module cannot detect. Messages name the path and
/// the fix. No check is made on other platforms.
fn check_root_ownership(root: &Path, meta: &std::fs::Metadata) -> Result<(), StoreError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        check_owner_only(root, meta)?;
        let euid = effective_uid();
        let mut ancestor = root.parent();
        while let Some(dir) = ancestor {
            let m = std::fs::symlink_metadata(dir)
                .map_err(|e| StoreError::io(format!("stat {}", dir.display()), e))?;
            let mode = m.mode() & 0o7777;
            let sticky = mode & 0o1000 != 0;
            if m.uid() != euid && m.uid() != 0 {
                return Err(StoreError::PermissionDenied(format!(
                    "{} (an ancestor of the backup root) is owned by uid {}; the backup root \
                     must sit under directories owned by root or by the extenddb user",
                    dir.display(),
                    m.uid()
                )));
            }
            if mode & 0o022 != 0 && !sticky {
                return Err(StoreError::PermissionDenied(format!(
                    "{} (an ancestor of the backup root) has mode {mode:04o} and is writable by \
                     other users without the sticky bit; another user could replace the root. \
                     chmod it to remove group and world write, or move the backup root",
                    dir.display()
                )));
            }
            ancestor = dir.parent();
        }
    }
    #[cfg(not(unix))]
    {
        let _ = (root, meta);
    }
    Ok(())
}

/// Refuse a directory under the root that grants any permission to group or
/// others, or is not owned by the effective user. Applied to the root and to
/// every existing directory an operation descends into, so a permissive
/// directory created before the store adopted the root cannot be used by
/// another user to reach or replace objects.
fn check_owner_only(dir: &Path, meta: &std::fs::Metadata) -> Result<(), StoreError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let euid = effective_uid();
        if meta.uid() != euid {
            return Err(StoreError::PermissionDenied(format!(
                "{} is owned by uid {} but extenddb runs as uid {euid}; chown -R it to the \
                 extenddb user (or, when extenddb starts as root and drops privileges, to the \
                 user it drops to)",
                dir.display(),
                meta.uid()
            )));
        }
        let mode = meta.mode() & 0o7777;
        if mode & 0o077 != 0 {
            return Err(StoreError::PermissionDenied(format!(
                "{} has mode {mode:04o} and is accessible to other users; chmod it to 0700 \
                 (on a filesystem without Unix permissions, mount it with a mask that \
                 yields 0700)",
                dir.display()
            )));
        }
    }
    #[cfg(not(unix))]
    {
        let _ = (dir, meta);
    }
    Ok(())
}

#[cfg(unix)]
fn effective_uid() -> u32 {
    // SAFETY: geteuid has no preconditions and cannot fail.
    unsafe { libc::geteuid() }
}

/// Create a directory readable and writable by the owner only. The mode is
/// applied at creation so there is no window with a wider mode.
async fn create_private_dir(path: &Path) -> std::io::Result<()> {
    let mut builder = tokio::fs::DirBuilder::new();
    #[cfg(unix)]
    {
        builder.mode(0o700);
    }
    builder.create(path).await
}

/// Create a new file readable and writable by the owner only, failing if a
/// file already exists at `path`. The mode is applied at creation; the
/// process umask can only narrow it.
async fn create_private_file(path: &Path) -> Result<tokio::fs::File, StoreError> {
    let mut options = tokio::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        options.mode(0o600);
    }
    options
        .open(path)
        .await
        .map_err(|e| map_permission(format!("create {}", path.display()), e))
}

fn map_permission(context: String, e: std::io::Error) -> StoreError {
    if e.kind() == std::io::ErrorKind::PermissionDenied {
        StoreError::PermissionDenied(context)
    } else {
        StoreError::io(context, e)
    }
}

/// Refusal for a symlink found in a key's path. The message names the path
/// under the root that was refused, never the link's target.
fn symlink_refused(path: &Path) -> StoreError {
    StoreError::PermissionDenied(format!(
        "{} is a symlink; keys never resolve through symlinks",
        path.display()
    ))
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
    check_root_blocking(root)?;
    let base = prefix_path(root, prefix);
    let mut out = Vec::new();
    match std::fs::symlink_metadata(&base) {
        Ok(meta) if meta.is_file() => {
            // The prefix names an object directly (key == prefix).
            out.push(ObjectMeta {
                key: prefix.to_owned(),
                size: meta.len(),
                last_modified: meta.modified().ok(),
                etag: None,
            });
        }
        Ok(meta) if meta.is_dir() => {
            walk_files(root, &base, &meta, &mut out)?;
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

/// Synchronous twin of `FilesystemStore::check_root` for the blocking
/// `list` and `delete_prefix` walks.
fn check_root_blocking(root: &Path) -> Result<(), StoreError> {
    let meta = std::fs::symlink_metadata(root)
        .map_err(|e| StoreError::io(format!("stat {}", root.display()), e))?;
    check_owner_only(root, &meta)
}

/// Walk `dir`, whose `symlink_metadata` is `dir_meta`, and append every
/// regular file as an object. The directory must pass the owner-only rule
/// before it is read, the same rule `get`, `head`, and `put` apply on their
/// descent; a permissive directory fails the listing rather than being
/// silently included.
fn walk_files(
    root: &Path,
    dir: &Path,
    dir_meta: &std::fs::Metadata,
    out: &mut Vec<ObjectMeta>,
) -> Result<(), StoreError> {
    check_owner_only(dir, dir_meta)?;
    let entries = std::fs::read_dir(dir)
        .map_err(|e| StoreError::io(format!("read directory {}", dir.display()), e))?;
    for entry in entries {
        let entry =
            entry.map_err(|e| StoreError::io(format!("read directory {}", dir.display()), e))?;
        let path = entry.path();
        // `symlink_metadata` rather than `DirEntry::file_type` so the same
        // metadata that decides the entry's kind is the one the owner-only
        // check sees.
        let meta = std::fs::symlink_metadata(&path)
            .map_err(|e| StoreError::io(format!("stat {}", path.display()), e))?;
        if meta.is_dir() {
            walk_files(root, &path, &meta, out)?;
        } else if meta.is_file() {
            let name = entry.file_name();
            if is_temp_name(&name.to_string_lossy()) {
                continue;
            }
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
                etag: None,
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
    check_root_blocking(root)?;
    let base = prefix_path(root, prefix);
    match std::fs::symlink_metadata(&base) {
        Ok(meta) if meta.is_file() => {
            std::fs::remove_file(&base)
                .map_err(|e| StoreError::io(format!("remove {}", base.display()), e))?;
            Ok(1)
        }
        Ok(meta) if meta.is_dir() => {
            if prefix.is_empty() {
                // Deleting everything must not remove the root itself; the
                // root passed the owner-only check above.
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

/// Remove a file, a symlink (the link, not its target), or a directory tree.
/// A directory is checked against the owner-only rule before it is read, so
/// the recursive delete, the operation with the widest reach in this
/// module, never descends into a directory another user can write to.
fn delete_entry(path: &Path) -> Result<u64, StoreError> {
    let meta = std::fs::symlink_metadata(path)
        .map_err(|e| StoreError::io(format!("stat {}", path.display()), e))?;
    if meta.is_dir() {
        check_owner_only(path, &meta)?;
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
