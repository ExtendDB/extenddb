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
//! is owned by a third user or is writable by group or others without the
//! sticky bit (such an ancestor lets another user rename the root away and
//! plant a symlink at its path), every operation re-checks the root and
//! every existing directory it descends into against the same owner-only
//! rule and refuses a root whose canonical path has changed since `open`
//! (an ancestor moved or replaced by a symlink; the ancestor rule itself is
//! checked at `open` and `validate` only), every directory the store
//! creates is `0700`, and every object is created `0600` (set at creation,
//! not fixed up afterwards). A process that can write under the root holds
//! full read, write, and delete over every backup in it; these checks make
//! that set equal to the owner, for the tree the store can see. They cannot
//! observe a bind mount or a filesystem without Unix permission semantics;
//! the deployment guide names those as operator responsibilities.
//!
//! Every operation resolves its path with one component walk
//! (`walk_components`): each component is `lstat`ed, every existing
//! directory on the way is checked against the owner-only rule, a symlink in
//! an intermediate component is refused. `get`, `head` and `put` refuse a
//! symlink at the final component too; for `list` and `delete_prefix` a
//! symlink at the prefix path itself is not an object under the prefix and
//! is treated as nothing there. Below the prefix, symlinks are skipped
//! rather than refused: a listing is not an access through the link, and
//! deletion removes the link without following it. A listing reports only
//! names that pass the shared key rule (`key::validate_key`); an entry the
//! store did not write (a non-UTF-8 name, a backslash, a control character,
//! an in-flight temporary) is skipped by `list` and removed by
//! `delete_prefix`. A deletion treats an entry another process removed first
//! as already gone and does not count it, so two reconcilers deleting the
//! same prefix do not fail each other. `delete_prefix` checks every
//! directory under the prefix against the owner-only rule before it removes
//! anything, so a refusal leaves the prefix whole. Its walk runs on the
//! blocking pool, where dropping the future does not stop it; a guard in
//! the future sets a flag when it is dropped, and the walk checks the flag
//! before each removal, so a dropped `delete_prefix` stops at its next
//! entry.
//!
//! Writes go to a temporary sibling file and rename into place, so a crash
//! never leaves a truncated file under its final name. The temporary is
//! flushed before it is synced: tokio's `File::sync_all` waits for the last
//! background write but does not return its error (tokio keeps only its
//! kind for a later write or flush), so without a flush before the drop a
//! short file would be renamed into place over the previous object. A put
//! that does not reach the rename leaves no partial object under the key
//! and no temporary: a guard, created with the temporary on the blocking
//! pool, removes it when the put fails, when its future is dropped at any
//! point before the rename (a caller's timeout, a cancelled job), the
//! temporary's own open included, and when the body panics, and an object
//! that existed before the put is untouched. A failure after the rename
//! (the directory sync) leaves the new, fully written object in place, as
//! S3 leaves the previous object after a failed `PutObject`. Only a process
//! that ends mid-put without running the guard (killed, or exiting without
//! waiting for the runtime's blocking threads) leaves a `.put-*.tmp`, which
//! listings hide and `delete_prefix` removes; so does a guard whose unlink
//! fails, which it ignores.
//! Directories a failed put created are left in place: they are
//! empty, listings do not show them, and `delete_prefix` of the backup
//! prefix removes them. (An earlier revision removed them; doing so raced a
//! concurrent put into the same new directory, and could not cover a
//! failure inside the creation walk itself.)
//!
//! `put_if_absent` writes and syncs its temporary the same way, created
//! `O_CREAT | O_EXCL` as every temporary is, and then `link`s it to the key
//! instead of renaming: `link(2)` fails with `EEXIST` when anything is at
//! the new path, so of two concurrent calls on one key exactly one links and
//! the other reads `None`, the first body stays, and the object appears
//! under its key complete or not at all. The temporary is unlinked after the
//! link on every path (the two names are one inode, so the object stays). A
//! regular file already at the destination is reported `None` before the
//! body is read.
//!
//! Where this store narrows the shared key space (temporary names, a file
//! and a directory at one path, the filesystem's own name limit and case
//! folding) is listed in the `key` module doc.
//!
//! Defence in depth without a test. These paths are kept but no test drives
//! them, because each needs a race inside one operation or a crash:
//!
//! - `get` of a file removed between the walk and the open answers
//!   `NotFound`, and `get` opens with `O_NOFOLLOW` after the walk refused a
//!   link at the final component;
//! - `list` skips an entry, or a directory, removed during its walk;
//! - `delete_prefix` leaves a directory a concurrent put refilled, and
//!   `delete_entry` re-applies the owner-only rule to a directory that
//!   changed after `check_tree` checked it;
//! - `prepare_parent` checks the canonical form of the deepest existing
//!   ancestor after the root's canonical path and every component were
//!   checked, and re-checks each directory it creates (a symlink or a
//!   non-directory put there by a concurrent creator, and the owner-only
//!   rule on a directory another process created first);
//! - `check_root_ownership` names an ancestor that is a symlink, which only
//!   a swap after the canonical-root check can produce;
//! - the temporary is synced before the rename, and every directory from
//!   the parent up to the root after it (crash durability).
//!
//! The parent's directory sync after the rename is covered by
//! `post_rename_sync_failure_keeps_the_new_object`, except when the suite
//! runs as root, which bypasses the permission that test relies on.

use std::path::{Path, PathBuf};

use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt, TryStreamExt};
use tokio::io::AsyncWriteExt;

use crate::key::{normalize_prefix, validate_key, validate_prefix};
use crate::{BackupStore, ByteStream, ObjectMeta, StoreError};

/// Prefix and suffix marking in-flight temporary files, hidden from listings.
const TEMP_PREFIX: &str = ".put-";
const TEMP_SUFFIX: &str = ".tmp";

/// Store writing objects under a local root directory.
pub struct FilesystemStore {
    /// Canonicalized root; every resolved target must stay under it.
    root: PathBuf,
    /// Run by the deletion walk before each removal, with the number of
    /// removals made so far; `None` except in the crate's own tests, which
    /// pause the walk at a chosen entry to drop the call there.
    removal_hook: Option<RemovalHook>,
}

/// The deletion walk's removal hook; set only through the `test-util`
/// setter on [`FilesystemStore`].
pub type RemovalHook = std::sync::Arc<dyn Fn(u64) + Send + Sync>;

impl FilesystemStore {
    /// Open a store rooted at `root`. The directory must already exist; it is
    /// canonicalized here and the canonical form anchors every escape check.
    ///
    /// # Errors
    ///
    /// Returns an error when the root path is empty, the root does not exist,
    /// cannot be resolved, or is not a directory.
    pub async fn open(root: impl AsRef<Path>) -> Result<Self, StoreError> {
        let root = root.as_ref();
        if root.as_os_str().is_empty() {
            return Err(StoreError::Other(
                "the backup root path is empty; name the directory".to_owned(),
            ));
        }
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
        Ok(Self {
            root: canonical,
            removal_hook: None,
        })
    }

    /// The root label a catalog row records for this store: the canonical
    /// root directory. Two servers configured for one directory (by any
    /// path that resolves to it) spell the same label.
    #[must_use]
    pub fn store_root(&self) -> String {
        self.root.display().to_string()
    }

    /// Run `hook` on the blocking thread of every `delete_prefix` walk
    /// right before each removal (a file, a symlink or a directory), with
    /// the number of removals that walk has made so far. The crate's own
    /// tests block in it to hold the walk at a chosen entry and drop the
    /// call while it is held. Compiles only with the `test-util` feature;
    /// not part of the supported API surface.
    #[cfg(feature = "test-util")]
    #[doc(hidden)]
    pub fn set_removal_hook(&mut self, hook: RemovalHook) {
        self.removal_hook = Some(hook);
    }

    /// Re-check the root's owner-only rule and its canonical path at the
    /// start of an operation, so a root widened or moved after `open` is
    /// refused by the next call rather than trusted until the next
    /// `validate`. The ancestor rule is not repeated here; it is a startup
    /// check.
    async fn check_root(&self) -> Result<(), StoreError> {
        let root = self.root.clone();
        tokio::task::spawn_blocking(move || check_root_blocking(&root))
            .await
            .map_err(|e| StoreError::Other(format!("root check task failed: {e}")))?
    }

    /// Resolve a validated key to its path under the root with the one
    /// component walk (`walk_components`, `FinalSymlink::Refuse`), so `get`
    /// and `head` agree with `put`, `list` and `delete_prefix` on what a key
    /// names. Missing paths surface as `NotFound`. The returned metadata
    /// describes the final component.
    async fn resolve_no_follow(
        &self,
        key: &str,
    ) -> Result<(PathBuf, std::fs::Metadata), StoreError> {
        let root = self.root.clone();
        let key = key.to_owned();
        tokio::task::spawn_blocking(move || {
            check_root_blocking(&root)?;
            match walk_components(&root, &key, FinalSymlink::Refuse)? {
                Walked::Found(path, meta) => Ok((path, meta)),
                Walked::Nothing => Err(StoreError::NotFound),
            }
        })
        .await
        .map_err(|e| StoreError::Other(format!("resolve task failed: {e}")))?
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
    /// Returns the parent directory. Directories this call creates stay if
    /// the put later fails (see the module doc).
    async fn prepare_parent(&self, parent_components: &[&str]) -> Result<PathBuf, StoreError> {
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
        // created: the root's canonical path was unchanged and no component
        // was a symlink at the walk above, and as defence in depth its
        // canonical form must still sit under the root. No test: only a
        // component or an ancestor swapped for a link between those checks
        // and this call reaches it, a race the suite does not drive.
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
            match create_private_dir(&next).await {
                Ok(()) => {}
                // A concurrent put may have created it; the check below
                // verifies what is actually there.
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(e) => return Err(StoreError::io(format!("create {}", next.display()), e)),
            }
            // Defence in depth, no test: a directory this call created is
            // a 0700 directory, so only a concurrent creator at the same name
            // makes either check below fail.
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
        Ok(current)
    }

    /// Resolve `key` to the directory its object is written into and the
    /// object's final path, creating the directories (`prepare_parent`) and
    /// refusing a symlink or a directory at the destination: a put never
    /// resolves through a link, and a directory there is a key-space
    /// collision (`a/b` exists, `a` is put), refused before the body is read
    /// rather than after the rename or link fails. Reports whether a regular
    /// file is already at the destination, which `put_if_absent` reads.
    async fn destination(&self, key: &str) -> Result<Destination, StoreError> {
        validate_key(key)?;
        let components: Vec<&str> = key.split('/').collect();
        let (file_name, parent_components) = components
            .split_last()
            .expect("a validated key has at least one component");
        let parent = self.prepare_parent(parent_components).await?;
        let final_path = parent.join(file_name);
        let exists = match tokio::fs::symlink_metadata(&final_path).await {
            Ok(meta) if meta.is_symlink() => {
                return Err(symlink_refused(&final_path));
            }
            Ok(meta) if meta.is_dir() => {
                return Err(StoreError::io(
                    format!("create {}", final_path.display()),
                    std::io::Error::new(
                        std::io::ErrorKind::IsADirectory,
                        "objects already exist under this key",
                    ),
                ));
            }
            Ok(_) => true,
            Err(_) => false,
        };
        Ok(Destination {
            parent,
            final_path,
            exists,
        })
    }

    /// Write `body` to a new temporary sibling of `final_path`, flushed and
    /// synced, and return its path, the guard that removes it, and the
    /// number of bytes written. The temporary is created `O_CREAT | O_EXCL`
    /// with mode `0600` (`create_private_file`).
    ///
    /// The temporary and its guard come back together. The guard removes
    /// the temporary on every exit the caller does not disarm it for: an
    /// error, the put's future dropped (a caller's timeout, a cancelled
    /// job), including while the open itself is in flight on the blocking
    /// pool, or a body that panics.
    async fn write_temporary(
        parent: &Path,
        mut body: ByteStream,
    ) -> Result<(PathBuf, RemoveOnDrop, u64), StoreError> {
        let temp_path = parent.join(format!(
            "{TEMP_PREFIX}{}{TEMP_SUFFIX}",
            uuid::Uuid::new_v4()
        ));
        let (mut file, temp_guard) = create_private_file(&temp_path).await?;
        let mut size: u64 = 0;
        while let Some(chunk) = body.next().await {
            let chunk = chunk?;
            file.write_all(&chunk)
                .await
                .map_err(|e| StoreError::io(format!("write {}", temp_path.display()), e))?;
            size += chunk.len() as u64;
        }
        // tokio's `File` hands each write to a background task and
        // reports its error on the next operation. `sync_all` waits for
        // the last background write but does not return its error (tokio
        // keeps only its kind for a later write or flush), so without a
        // flush before the drop a short file would be renamed into place
        // on a full disk (or past RLIMIT_FSIZE). `flush` returns it.
        file.flush()
            .await
            .map_err(|e| StoreError::io(format!("write {}", temp_path.display()), e))?;
        // Defence in depth, no test: the fsync of the temporary matters
        // only across a crash or power loss, which the suite cannot
        // inject.
        file.sync_all()
            .await
            .map_err(|e| StoreError::io(format!("sync {}", temp_path.display()), e))?;
        drop(file);
        Ok((temp_path, temp_guard, size))
    }

    /// Sync `parent` and every directory between it and the root, so the
    /// entry a rename or link just made, and the directories a put created
    /// on the way, are durable. Syncing every ancestor is a few extra
    /// fsyncs per put (one per path component) against a bounded depth;
    /// cheaper than tracking which ones this put created, and correct when a
    /// concurrent put created them. Defence in depth, no test: it matters
    /// only across a crash.
    async fn sync_parent_chain(&self, parent: &Path) -> Result<(), StoreError> {
        sync_dir(parent).await?;
        let mut dir = parent.to_path_buf();
        while let Some(p) = dir.parent() {
            if !p.starts_with(&self.root) || p == self.root {
                sync_dir(&self.root).await?;
                break;
            }
            sync_dir(p).await?;
            dir = p.to_path_buf();
        }
        Ok(())
    }

    /// The metadata of the object now at `final_path`.
    async fn stored_meta(key: &str, final_path: &Path, size: u64) -> ObjectMeta {
        let last_modified = tokio::fs::symlink_metadata(final_path)
            .await
            .ok()
            .and_then(|m| m.modified().ok());
        ObjectMeta {
            key: key.to_owned(),
            size,
            last_modified,
            etag: None,
        }
    }

    async fn put_impl(&self, key: &str, body: ByteStream) -> Result<ObjectMeta, StoreError> {
        let Destination {
            parent, final_path, ..
        } = self.destination(key).await?;

        // Durability contract: the temp file is synced before the rename and
        // the parent directory is synced after it, so an Ok return means both
        // the object bytes and its directory entry are on disk. Without the
        // directory sync the rename itself could be lost on power failure
        // even though the file data had been written.
        async {
            // The guard lives inside this block so dropping the block's
            // future drops it (`write_temporary`).
            let (temp_path, mut temp_guard, size) = Self::write_temporary(&parent, body).await?;
            tokio::fs::rename(&temp_path, &final_path)
                .await
                .map_err(|e| {
                    StoreError::io(
                        format!("rename {} to {}", temp_path.display(), final_path.display()),
                        e,
                    )
                })?;
            // The temporary name holds nothing from here on.
            temp_guard.disarm();
            // The object is visible under its key from here on, fully
            // written and synced. A failure below fails the put but leaves
            // the file: removing it would turn a key that held an object
            // before the put into one that holds nothing, which is worse
            // than either complete object (the trait contract is "no partial
            // object", not "no object").
            self.sync_parent_chain(&parent).await?;
            Ok(Self::stored_meta(key, &final_path, size).await)
        }
        .await
    }

    /// `put_if_absent`: the temporary is written and synced as `put` writes
    /// it, then `link`ed to the key. `link(2)` fails with `EEXIST` when
    /// anything is at the new path (a file, a directory, a symlink, whatever
    /// it points at), so the object appears under its key complete or not at
    /// all, and an object that was there first is untouched. A regular file
    /// already at the destination skips the body: the link would fail, and
    /// writing the body first would only cost disk. The temporary is removed
    /// after the link on every path; the link and the temporary are one
    /// inode, so the object stays.
    async fn put_if_absent_impl(
        &self,
        key: &str,
        body: ByteStream,
    ) -> Result<Option<ObjectMeta>, StoreError> {
        let Destination {
            parent,
            final_path,
            exists,
        } = self.destination(key).await?;
        if exists {
            return Ok(None);
        }
        async {
            let (temp_path, temp_guard, size) = Self::write_temporary(&parent, body).await?;
            let linked = tokio::fs::hard_link(&temp_path, &final_path).await;
            // The temporary's name is surplus from here on whether the
            // link landed or not; the guard removes it when it drops at the
            // end of this block (and on every early return below).
            match linked {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    drop(temp_guard);
                    return Ok(None);
                }
                Err(e) => {
                    return Err(StoreError::io(
                        format!("link {} to {}", temp_path.display(), final_path.display()),
                        e,
                    ));
                }
            }
            drop(temp_guard);
            self.sync_parent_chain(&parent).await?;
            Ok(Some(Self::stored_meta(key, &final_path, size).await))
        }
        .await
    }

    async fn get_impl(&self, key: &str) -> Result<ByteStream, StoreError> {
        validate_key(key)?;
        let (path, meta) = self.resolve_no_follow(key).await?;
        if !meta.is_file() {
            return Err(StoreError::NotFound);
        }
        // The walk above refused symlinks by path; O_NOFOLLOW makes the
        // open itself refuse one planted at the final component since then.
        // Defence in depth with no test: only a link planted between the
        // walk and the open reaches it.
        let mut options = tokio::fs::OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            options.custom_flags(libc::O_NOFOLLOW);
        }
        let file = match options.open(&path).await {
            Ok(file) => file,
            // Removed between the walk and the open (a concurrent
            // `delete_prefix`): the key holds nothing, as `head` would say.
            // Defence in depth with no test: it needs a removal between the
            // walk and the open.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(StoreError::NotFound);
            }
            Err(e) => return Err(StoreError::io(format!("open {}", path.display()), e)),
        };
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
        // The walk runs on the blocking pool, where dropping this future
        // does not stop it. The guard sets the flag when this future is
        // dropped (a caller's timeout), and the walk checks it before each
        // removal, so a dropped call stops at its next entry, as the S3
        // store's stops at its next request, rather than deleting what a
        // later put stores under the prefix.
        let cancel = CancelOnDrop::default();
        let walk = DeleteWalk {
            cancel: cancel.0.clone(),
            removal_hook: self.removal_hook.clone(),
            removals: std::sync::atomic::AtomicU64::new(0),
        };
        tokio::task::spawn_blocking(move || delete_objects(&root, &normalized, &walk))
            .await
            .map_err(|e| StoreError::Other(format!("deletion task failed: {e}")))?
    }

    async fn validate_impl(&self) -> Result<(), StoreError> {
        // The root's own checks are synchronous filesystem calls (`lstat` of
        // the root and each ancestor, `realpath`); they run on the blocking
        // pool so a root on a mount that has stopped answering parks a
        // blocking thread and not the runtime, and a caller's timeout on
        // this future can fire.
        let root = self.root.clone();
        tokio::task::spawn_blocking(move || {
            let meta = std::fs::symlink_metadata(&root).map_err(|e| {
                StoreError::io(format!("backup root {} is not usable", root.display()), e)
            })?;
            check_root_is_directory(&root, &meta)?;
            check_root_location(&root)?;
            check_root_ownership(&root, &meta)
        })
        .await
        .map_err(|e| StoreError::Other(format!("root check task failed: {e}")))??;
        let probe = self.root.join(format!(
            "{TEMP_PREFIX}validate-{}{TEMP_SUFFIX}",
            uuid::Uuid::new_v4()
        ));
        // The probe is written and synced so a filesystem that accepts the
        // create but refuses data (ENOSPC at flush, a read-only remount,
        // a stale network handle) is caught here rather than at the first
        // backup. The probe is removed on every path, success or failure:
        // explicitly below, or by its guard when this future is dropped or
        // a step fails.
        let outcome = async {
            let (mut file, guard) = create_private_file(&probe).await?;
            file.write_all(b"probe")
                .await
                .map_err(|e| StoreError::io(format!("write probe {}", probe.display()), e))?;
            // As in `put_impl`: `sync_all` waits for the background write but
            // does not return its error, `flush` returns it.
            file.flush()
                .await
                .map_err(|e| StoreError::io(format!("write probe {}", probe.display()), e))?;
            file.sync_all()
                .await
                .map_err(|e| StoreError::io(format!("sync probe {}", probe.display()), e))?;
            Ok::<RemoveOnDrop, StoreError>(guard)
        }
        .await;
        // `NotFound` is success: a concurrent `delete_prefix("")` removes
        // temporaries, the probe among them, and the probe had been written
        // and synced (or the step that failed is reported below).
        let removed = match tokio::fs::remove_file(&probe).await {
            Err(e) if vanished(&e) => Ok(()),
            other => other,
        };
        let mut guard = outcome.map_err(|e| match e {
            StoreError::PermissionDenied(_) => StoreError::PermissionDenied(format!(
                "backup root {} is not writable by this process",
                self.root.display()
            )),
            other => other,
        })?;
        // The explicit removal above reports its error; the guard has
        // nothing left to do.
        guard.disarm();
        if let Err(e) = removed {
            return Err(StoreError::io(
                format!("remove probe {}", probe.display()),
                e,
            ));
        }
        Ok(())
    }
}

/// Where a put writes: the directory the object goes into (created and
/// checked by `prepare_parent`), the object's final path, and whether a
/// regular file is already there.
struct Destination {
    parent: PathBuf,
    final_path: PathBuf,
    exists: bool,
}

impl BackupStore for FilesystemStore {
    fn put<'a>(
        &'a self,
        key: &'a str,
        body: ByteStream,
    ) -> BoxFuture<'a, Result<ObjectMeta, StoreError>> {
        self.put_impl(key, body).boxed()
    }

    fn put_if_absent<'a>(
        &'a self,
        key: &'a str,
        body: ByteStream,
    ) -> BoxFuture<'a, Result<Option<ObjectMeta>, StoreError>> {
        self.put_if_absent_impl(key, body).boxed()
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
/// or others (`mode & 0o077 == 0`), and every ancestor must be a directory
/// (not a symlink) owned by the effective user or by root and must not be
/// writable by group or others unless the sticky bit is set: an ancestor
/// that others can write to lets them rename the root away and plant a
/// symlink at its path, which the textual re-resolution in this module
/// cannot detect. Messages name the path and the fix. No check is made on
/// other platforms.
fn check_root_ownership(root: &Path, meta: &std::fs::Metadata) -> Result<(), StoreError> {
    check_private_tree(root, meta, "backup root")
}

/// The owner-only rule of a directory the server keeps private, by its
/// canonical path: [`check_root_ownership`]'s rule (the directory owned by
/// the effective user with no group or other permission bits; every ancestor
/// a directory owned by the effective user or by root, not writable by group
/// or others unless sticky) for a directory that is not a store root, named
/// `what` in the messages (the server's restore spool). No check is made on
/// other platforms.
///
/// # Errors
///
/// `PermissionDenied` naming the directory or ancestor and the mode it
/// needs; `Io` when a directory on the way cannot be read.
pub fn check_private_directory(dir: &Path, what: &str) -> Result<(), StoreError> {
    let meta = std::fs::symlink_metadata(dir)
        .map_err(|e| StoreError::io(format!("stat {}", dir.display()), e))?;
    check_private_tree(dir, &meta, what)
}

fn check_private_tree(root: &Path, meta: &std::fs::Metadata, what: &str) -> Result<(), StoreError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        check_owner_only(root, meta)?;
        let euid = effective_uid();
        let mut ancestor = root.parent();
        while let Some(dir) = ancestor {
            let m = std::fs::symlink_metadata(dir)
                .map_err(|e| StoreError::io(format!("stat {}", dir.display()), e))?;
            // The root is canonical and `check_root_location` runs first, so
            // only a swap between the two reaches this (module doc).
            if m.is_symlink() {
                return Err(StoreError::PermissionDenied(format!(
                    "{} (an ancestor of the {what}) is a symlink; configure the {what} by its \
                     resolved path",
                    dir.display()
                )));
            }
            let mode = m.mode() & 0o7777;
            let sticky = mode & 0o1000 != 0;
            if m.uid() != euid && m.uid() != 0 {
                return Err(StoreError::PermissionDenied(format!(
                    "{} (an ancestor of the {what}) is owned by uid {}; the {what} must sit \
                     under directories owned by root or by the extenddb user",
                    dir.display(),
                    m.uid()
                )));
            }
            if mode & 0o022 != 0 && !sticky {
                return Err(StoreError::PermissionDenied(format!(
                    "{} (an ancestor of the {what}) has mode {mode:04o} and is writable by group \
                     or others without the sticky bit; another user could replace the {what}. \
                     chmod it to remove group and world write, or move the {what}",
                    dir.display()
                )));
            }
            ancestor = dir.parent();
        }
    }
    #[cfg(not(unix))]
    {
        let _ = (root, meta, what);
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
                 extenddb user",
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

/// The canonical root was a directory at `open`; refuse it if it has since
/// been replaced by a symlink (the owner removed the directory and planted a
/// link at its path) or by anything else. Named explicitly rather than
/// relying on a link's `0777` mode failing the owner-only check.
fn check_root_is_directory(root: &Path, meta: &std::fs::Metadata) -> Result<(), StoreError> {
    if meta.is_symlink() {
        return Err(StoreError::PermissionDenied(format!(
            "backup root {} is a symlink; the root must be a directory",
            root.display()
        )));
    }
    if !meta.is_dir() {
        return Err(StoreError::Other(format!(
            "backup root {} is not a directory",
            root.display()
        )));
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
/// file already exists at `path`, and return it with the guard that removes
/// it. The mode is applied at creation; the process umask can only narrow
/// it.
///
/// The open runs on the blocking pool, and the guard is created in the same
/// closure as soon as the open succeeds, with no await in between. A caller
/// whose future is dropped while the open is in flight does not stop it:
/// the closure runs to the end, its result is dropped with nobody to
/// receive it, and the guard removes the file. (A guard created after the
/// `await` would not exist yet in that case, and the file would stay.)
async fn create_private_file(path: &Path) -> Result<(tokio::fs::File, RemoveOnDrop), StoreError> {
    let owned = path.to_path_buf();
    let opened = tokio::task::spawn_blocking(move || {
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.mode(0o600);
        }
        options
            .open(&owned)
            .map(|file| (file, RemoveOnDrop::new(&owned)))
    })
    .await
    .map_err(|e| StoreError::Other(format!("create task failed: {e}")))?;
    let (file, guard) =
        opened.map_err(|e| map_permission(format!("create {}", path.display()), e))?;
    Ok((tokio::fs::File::from_std(file), guard))
}

fn map_permission(context: String, e: std::io::Error) -> StoreError {
    if e.kind() == std::io::ErrorKind::PermissionDenied {
        StoreError::PermissionDenied(context)
    } else {
        StoreError::io(context, e)
    }
}

/// Removes a put's temporary file (or `validate`'s probe) from `Drop`
/// unless disarmed. The put's error path, its future being dropped (a
/// caller's timeout, a cancelled job) at any await before the rename, the
/// temporary's own open included (`create_private_file`), and a panicking
/// body all end here, so none of them leaves a `.put-*.tmp` behind (a
/// listing hides it, but it holds up to a data file's worth of disk until
/// a `delete_prefix` covers it). The unlink is synchronous: `Drop` cannot
/// await, and it must not depend on a runtime. A removal error is ignored:
/// the temporary is best effort on this path, and the next `delete_prefix`
/// removes what is left.
struct RemoveOnDrop {
    path: PathBuf,
    armed: bool,
}

impl RemoveOnDrop {
    fn new(path: &Path) -> Self {
        Self {
            path: path.to_path_buf(),
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for RemoveOnDrop {
    fn drop(&mut self) {
        if self.armed {
            let _ = std::fs::remove_file(&self.path);
        }
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

/// What to do with a symlink at the final component of a walk.
#[derive(Clone, Copy)]
enum FinalSymlink {
    /// `get`, `head`, `put`: a key never resolves through a link.
    Refuse,
    /// `list`, `delete_prefix`: a link at the prefix path is not an object
    /// under the prefix; there is nothing there.
    Nothing,
}

/// Outcome of a component walk.
enum Walked {
    /// The final component exists; its path and `lstat` metadata.
    Found(PathBuf, std::fs::Metadata),
    /// Nothing the store wrote: a missing path, a file at an ancestor, or
    /// (under `FinalSymlink::Nothing`) a symlink at the final component.
    Nothing,
}

/// The one component walk every operation resolves its path with: each
/// component is `lstat`ed, every existing directory on the way is checked
/// against the owner-only rule, a symlink in an intermediate component is
/// refused (whatever it points at; a recursive delete or a read would run on
/// its far side), and a symlink at the final component is handled as `last`
/// says. `NotFound` and `NotADirectory` from `lstat` mean nothing there (the
/// path, or an ancestor that is a file). The root has already been checked
/// by the caller. An empty path names the root itself.
fn walk_components(root: &Path, path: &str, last: FinalSymlink) -> Result<Walked, StoreError> {
    let mut current = root.to_path_buf();
    if path.is_empty() {
        let meta = std::fs::symlink_metadata(root)
            .map_err(|e| StoreError::io(format!("stat {}", root.display()), e))?;
        return Ok(Walked::Found(current, meta));
    }
    let components: Vec<&str> = path.split('/').collect();
    let mut meta = None;
    for (index, component) in components.iter().enumerate() {
        current.push(component);
        let is_last = index + 1 == components.len();
        let m = match std::fs::symlink_metadata(&current) {
            Ok(m) => m,
            Err(e)
                if e.kind() == std::io::ErrorKind::NotFound
                    || e.kind() == std::io::ErrorKind::NotADirectory =>
            {
                return Ok(Walked::Nothing);
            }
            Err(e) => return Err(StoreError::io(format!("stat {}", current.display()), e)),
        };
        if m.is_symlink() {
            return match (is_last, last) {
                (true, FinalSymlink::Nothing) => Ok(Walked::Nothing),
                _ => Err(symlink_refused(&current)),
            };
        }
        if m.is_dir() {
            check_owner_only(&current, &m)?;
        } else if !is_last {
            // A file (or other non-directory) at an ancestor: nothing under
            // the path.
            return Ok(Walked::Nothing);
        }
        meta = Some(m);
    }
    let meta = meta.expect("a non-empty path has at least one component");
    Ok(Walked::Found(current, meta))
}

/// What a normalized prefix names on disk.
enum PrefixTarget {
    /// A regular file: the prefix is itself a key.
    File(PathBuf, std::fs::Metadata),
    /// A directory: the prefix covers everything under it.
    Dir(PathBuf, std::fs::Metadata),
    /// Nothing the store wrote.
    Nothing,
}

/// Resolve a normalized prefix with the shared walk; a non-file, non-directory
/// entry at the prefix (a FIFO, a socket) is nothing the store wrote.
fn resolve_prefix(root: &Path, prefix: &str) -> Result<PrefixTarget, StoreError> {
    Ok(
        match walk_components(root, prefix, FinalSymlink::Nothing)? {
            Walked::Found(path, meta) if meta.is_dir() => PrefixTarget::Dir(path, meta),
            Walked::Found(path, meta) if meta.is_file() => PrefixTarget::File(path, meta),
            Walked::Found(..) | Walked::Nothing => PrefixTarget::Nothing,
        },
    )
}

/// Collect object metadata under a normalized prefix, sorted by key.
fn collect_objects(root: &Path, prefix: &str) -> Result<Vec<ObjectMeta>, StoreError> {
    check_root_blocking(root)?;
    let mut out = Vec::new();
    match resolve_prefix(root, prefix)? {
        PrefixTarget::File(path, meta) => {
            // The prefix names an object directly (key == prefix), unless
            // it names an in-flight temporary, which listings never show.
            // (The prefix passed `validate_prefix` and is non-empty here, so
            // it is a valid key.)
            let is_temp = path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(is_temp_name);
            if !is_temp {
                out.push(ObjectMeta {
                    key: prefix.to_owned(),
                    size: meta.len(),
                    last_modified: meta.modified().ok(),
                    etag: None,
                });
            }
        }
        PrefixTarget::Dir(base, meta) => {
            walk_files(root, &base, &meta, &mut out)?;
        }
        PrefixTarget::Nothing => {}
    }
    // The walk starts at the prefix (or is the prefix itself), so every key
    // matches the prefix by construction.
    out.sort_by(|a, b| a.key.cmp(&b.key));
    Ok(out)
}

/// Synchronous twin of `FilesystemStore::check_root` for the blocking
/// `list` and `delete_prefix` walks: the root is a directory, passes the
/// owner-only rule, and is still at its canonical path.
fn check_root_blocking(root: &Path) -> Result<(), StoreError> {
    let meta = std::fs::symlink_metadata(root)
        .map_err(|e| StoreError::io(format!("stat {}", root.display()), e))?;
    check_root_is_directory(root, &meta)?;
    check_owner_only(root, &meta)?;
    check_root_location(root)
}

/// `root` is the path `open` canonicalized. If it no longer canonicalizes to
/// itself, an ancestor was renamed and a symlink left at its old path (or
/// replaced by one), which the ancestor rule leaves only the owner and root
/// able to do. Every operation and `validate` refuse such a root, so they
/// agree on which tree the store uses rather than reading through the moved
/// ancestor while a put refuses to create under it.
fn check_root_location(root: &Path) -> Result<(), StoreError> {
    let now = std::fs::canonicalize(root)
        .map_err(|e| StoreError::io(format!("resolve {}", root.display()), e))?;
    if now != root {
        return Err(StoreError::PermissionDenied(format!(
            "{}: an ancestor of the backup root was moved or replaced by a symlink (the path \
             now resolves to {}); restore the original path, or configure the root by its \
             new path and restart",
            root.display(),
            now.display()
        )));
    }
    Ok(())
}

fn vanished(e: &std::io::Error) -> bool {
    e.kind() == std::io::ErrorKind::NotFound
}

/// Walk `dir`, whose `symlink_metadata` is `dir_meta`, and append every
/// regular file as an object. The directory must pass the owner-only rule
/// before it is read, the same rule `get`, `head`, and `put` apply on their
/// descent; a permissive directory fails the listing rather than being
/// silently included. An entry removed by another process between
/// `read_dir` and its `stat` is skipped, and a directory removed between its
/// `stat` and `read_dir` lists as empty; both are defence in depth with no
/// test, since they need a removal inside the walk.
fn walk_files(
    root: &Path,
    dir: &Path,
    dir_meta: &std::fs::Metadata,
    out: &mut Vec<ObjectMeta>,
) -> Result<(), StoreError> {
    check_owner_only(dir, dir_meta)?;
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if vanished(&e) => return Ok(()),
        Err(e) => {
            return Err(StoreError::io(
                format!("read directory {}", dir.display()),
                e,
            ));
        }
    };
    for entry in entries {
        let entry =
            entry.map_err(|e| StoreError::io(format!("read directory {}", dir.display()), e))?;
        let path = entry.path();
        // `symlink_metadata` rather than `DirEntry::file_type` so the same
        // metadata that decides the entry's kind is the one the owner-only
        // check sees.
        let meta = match std::fs::symlink_metadata(&path) {
            Ok(m) => m,
            Err(e) if vanished(&e) => continue,
            Err(e) => return Err(StoreError::io(format!("stat {}", path.display()), e)),
        };
        if meta.is_dir() {
            walk_files(root, &path, &meta, out)?;
        } else if meta.is_file() {
            // The store writes only UTF-8 names that pass the key rule (keys
            // are validated `str`s); an entry whose relative path fails it
            // (non-UTF-8, a backslash, a control character) cannot be a key
            // and so is not an object, and neither is an in-flight
            // temporary. `delete_prefix` still removes them.
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            if is_temp_name(&name) {
                continue;
            }
            let Some(key) = path
                .strip_prefix(root)
                .expect("walk stays under the root")
                .to_str()
                .map(|k| k.replace(std::path::MAIN_SEPARATOR, "/"))
            else {
                continue;
            };
            if validate_key(&key).is_err() {
                continue;
            }
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

/// Sets its flag when dropped: `delete_prefix_impl` holds it while the
/// blocking walk runs, and the walk stops at its next removal once the flag
/// is set.
#[derive(Default)]
struct CancelOnDrop(std::sync::Arc<std::sync::atomic::AtomicBool>);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.store(true, std::sync::atomic::Ordering::Release);
    }
}

/// What one `delete_prefix` walk carries down the tree: the flag its
/// `CancelOnDrop` sets, and the store's removal hook (set only in the
/// crate's own tests) with the count it is called with.
struct DeleteWalk {
    cancel: std::sync::Arc<std::sync::atomic::AtomicBool>,
    removal_hook: Option<RemovalHook>,
    removals: std::sync::atomic::AtomicU64,
}

impl DeleteWalk {
    /// The error a cancelled walk ends with; nobody receives it, since the
    /// caller's future is gone.
    fn check_cancel(&self) -> Result<(), StoreError> {
        if self.cancel.load(std::sync::atomic::Ordering::Acquire) {
            return Err(StoreError::Other(
                "delete_prefix was dropped before it finished".to_owned(),
            ));
        }
        Ok(())
    }

    /// Called right before each removal; runs the hook when one is set.
    fn before_removal(&self) {
        if let Some(hook) = &self.removal_hook {
            hook(
                self.removals
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst),
            );
        }
    }
}

/// Delete everything under a normalized prefix; returns the number of files
/// removed. The prefix directory and every directory under it are removed
/// too; directories above the prefix are left in place. Once `cancel` is
/// set the walk stops before its next removal (an entry already being
/// removed is removed).
///
/// Every directory under the prefix is checked against the owner-only rule
/// before anything is removed, so a refusal leaves the prefix as it was
/// rather than half deleted. (`delete_entry` repeats the check as it
/// descends, for a directory that changed after this walk.)
fn delete_objects(root: &Path, prefix: &str, walk: &DeleteWalk) -> Result<u64, StoreError> {
    check_root_blocking(root)?;
    match resolve_prefix(root, prefix)? {
        PrefixTarget::File(base, _) => {
            walk.check_cancel()?;
            remove_file_counted(&base, walk)
        }
        PrefixTarget::Dir(base, meta) => {
            check_tree(&base, &meta)?;
            if prefix.is_empty() {
                // Deleting everything must not remove the root itself.
                let mut count = 0;
                let entries = match std::fs::read_dir(&base) {
                    Ok(entries) => entries,
                    Err(e) if vanished(&e) => return Ok(0),
                    Err(e) => {
                        return Err(StoreError::io(
                            format!("read directory {}", base.display()),
                            e,
                        ));
                    }
                };
                for entry in entries {
                    let entry = entry.map_err(|e| {
                        StoreError::io(format!("read directory {}", base.display()), e)
                    })?;
                    count += delete_entry(&entry.path(), walk)?;
                }
                Ok(count)
            } else {
                delete_entry(&base, walk)
            }
        }
        PrefixTarget::Nothing => Ok(0),
    }
}

/// Check `dir` and every directory below it against the owner-only rule
/// without following symlinks. An entry removed while the walk runs is
/// skipped, as in the deletion that follows.
fn check_tree(dir: &Path, dir_meta: &std::fs::Metadata) -> Result<(), StoreError> {
    check_owner_only(dir, dir_meta)?;
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if vanished(&e) => return Ok(()),
        Err(e) => {
            return Err(StoreError::io(
                format!("read directory {}", dir.display()),
                e,
            ));
        }
    };
    for entry in entries {
        let entry =
            entry.map_err(|e| StoreError::io(format!("read directory {}", dir.display()), e))?;
        let path = entry.path();
        let meta = match std::fs::symlink_metadata(&path) {
            Ok(m) => m,
            Err(e) if vanished(&e) => continue,
            Err(e) => return Err(StoreError::io(format!("stat {}", path.display()), e)),
        };
        if meta.is_dir() {
            check_tree(&path, &meta)?;
        }
    }
    Ok(())
}

/// Remove one file or symlink (the link, not its target); a file another
/// process removed first is already gone and counts 0.
fn remove_file_counted(path: &Path, walk: &DeleteWalk) -> Result<u64, StoreError> {
    walk.before_removal();
    match std::fs::remove_file(path) {
        Ok(()) => Ok(1),
        Err(e) if vanished(&e) => Ok(0),
        Err(e) => Err(StoreError::io(format!("remove {}", path.display()), e)),
    }
}

/// Remove a file, a symlink (the link, not its target), or a directory tree.
/// A directory is checked against the owner-only rule before it is read, so
/// the recursive delete, the operation with the widest reach in this
/// module, never descends into a directory another user can write to.
/// Anything another deleter removed first is already gone and not counted
/// (covered by `concurrent_delete_prefix_calls_do_not_fail_each_other`). A
/// directory a concurrent put refilled between the walk and `remove_dir` is
/// left standing; that is defence in depth with no test, since it needs a
/// put inside that window. The owner-only check here repeats `check_tree`'s
/// for a directory changed since; defence in depth, no test. Each call
/// checks the walk's cancel flag first, and a directory checks it again
/// after its entries, so a dropped `delete_prefix` stops before its next
/// removal, a file's or a directory's (held to that by
/// `dropped_delete_prefix_stops_at_its_next_entry`).
fn delete_entry(path: &Path, walk: &DeleteWalk) -> Result<u64, StoreError> {
    walk.check_cancel()?;
    let meta = match std::fs::symlink_metadata(path) {
        Ok(m) => m,
        Err(e) if vanished(&e) => return Ok(0),
        Err(e) => return Err(StoreError::io(format!("stat {}", path.display()), e)),
    };
    if meta.is_dir() {
        check_owner_only(path, &meta)?;
        let mut count = 0;
        let entries = match std::fs::read_dir(path) {
            Ok(entries) => entries,
            Err(e) if vanished(&e) => return Ok(0),
            Err(e) => {
                return Err(StoreError::io(
                    format!("read directory {}", path.display()),
                    e,
                ));
            }
        };
        for entry in entries {
            let entry = entry
                .map_err(|e| StoreError::io(format!("read directory {}", path.display()), e))?;
            count += delete_entry(&entry.path(), walk)?;
        }
        walk.check_cancel()?;
        walk.before_removal();
        match std::fs::remove_dir(path) {
            Ok(()) => {}
            Err(e) if vanished(&e) || e.kind() == std::io::ErrorKind::DirectoryNotEmpty => {}
            Err(e) => {
                return Err(StoreError::io(
                    format!("remove directory {}", path.display()),
                    e,
                ));
            }
        }
        Ok(count)
    } else {
        remove_file_counted(path, walk)
    }
}
