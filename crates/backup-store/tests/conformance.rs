// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Conformance suite run against both store implementations.
//!
//! The `suite` module holds store-agnostic checks; the `filesystem` and `s3`
//! modules bind them to a tempdir-backed [`FilesystemStore`] and an
//! [`S3Store`] against a local S3-compatible endpoint. S3 tests run only when
//! `EXTENDDB_TEST_S3_ENDPOINT` is set and skip with a printed reason
//! otherwise, except when `EXTENDDB_TEST_S3_REQUIRED` is also set (CI), where
//! a missing endpoint is a test failure. See the crate README for how to run
//! them.

use bytes::Bytes;
use extenddb_backup_store::{BackupStore, ByteStream, StoreError};
use extenddb_backup_store::{MIN_PART_SIZE_BYTES, ObjectMeta};
use futures::TryStreamExt;

/// Part size every conformance run uses: the smallest S3 accepts, so the
/// multipart body stays cheap to generate.
const PART_SIZE: u64 = MIN_PART_SIZE_BYTES;

/// Deterministic pseudo-random bytes, so content equality proves the store
/// reassembled parts in order.
fn patterned_bytes(len: usize) -> Vec<u8> {
    let mut state: u64 = 0x2545_f491_4f6c_dd1d;
    let mut out = Vec::with_capacity(len);
    while out.len() < len {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        out.extend_from_slice(&state.to_le_bytes());
    }
    out.truncate(len);
    out
}

/// Stream a buffer in 1 MiB pieces so puts exercise real chunk reassembly
/// rather than a single-frame fast path.
fn chunked_body(data: impl AsRef<[u8]>) -> ByteStream {
    let pieces: Vec<Result<Bytes, StoreError>> = data
        .as_ref()
        .chunks(1024 * 1024)
        .map(|c| Ok(Bytes::copy_from_slice(c)))
        .collect();
    Box::pin(futures::stream::iter(pieces))
}

/// A body that yields `good` and then fails, for abort-path checks.
fn failing_body(good: impl AsRef<[u8]>) -> ByteStream {
    let pieces: Vec<Result<Bytes, StoreError>> = good
        .as_ref()
        .chunks(1024 * 1024)
        .map(|c| Ok(Bytes::copy_from_slice(c)))
        .chain(std::iter::once(Err(StoreError::Other(
            "injected mid-body failure".to_owned(),
        ))))
        .collect();
    Box::pin(futures::stream::iter(pieces))
}

async fn read_all(stream: ByteStream) -> Vec<u8> {
    let chunks: Vec<Bytes> = stream.try_collect().await.expect("stream reads cleanly");
    chunks.concat()
}

async fn list_keys(store: &dyn BackupStore, prefix: &str) -> Vec<String> {
    store
        .list(prefix)
        .map_ok(|meta| meta.key)
        .try_collect()
        .await
        .expect("list succeeds")
}

mod suite {
    use super::*;

    pub async fn round_trip(store: &dyn BackupStore) {
        let body = b"hello backup store".to_vec();
        store
            .put(
                "account/table/backup/object.json",
                chunked_body(body.clone()),
            )
            .await
            .expect("put succeeds");

        let read = read_all(
            store
                .get("account/table/backup/object.json")
                .await
                .expect("get succeeds"),
        )
        .await;
        assert_eq!(read, body);

        let meta = store
            .head("account/table/backup/object.json")
            .await
            .expect("head succeeds")
            .expect("object exists");
        assert_eq!(meta.key, "account/table/backup/object.json");
        assert_eq!(meta.size, body.len() as u64);
        assert!(meta.last_modified.is_some(), "both stores report mtime");
    }

    pub async fn overwrite_replaces_content(store: &dyn BackupStore) {
        store
            .put("over/write.bin", chunked_body(b"first"))
            .await
            .unwrap();
        store
            .put("over/write.bin", chunked_body(b"second and longer"))
            .await
            .unwrap();
        let read = read_all(store.get("over/write.bin").await.unwrap()).await;
        assert_eq!(read, b"second and longer");
    }

    pub async fn empty_body_round_trips(store: &dyn BackupStore) {
        let put = store
            .put("empty/object", chunked_body(Vec::new()))
            .await
            .unwrap();
        assert_eq!(put.size, 0, "put reports the size it wrote");
        let read = read_all(store.get("empty/object").await.unwrap()).await;
        assert!(read.is_empty());
        let meta = store.head("empty/object").await.unwrap().unwrap();
        assert_eq!(meta.size, 0);
    }

    pub async fn list_with_and_without_prefix(store: &dyn BackupStore) {
        for key in ["a/b/1", "a/b/2", "a/c/3", "ab/4", "d"] {
            store.put(key, chunked_body(vec![1, 2, 3])).await.unwrap();
        }

        assert_eq!(
            list_keys(store, "").await,
            vec!["a/b/1", "a/b/2", "a/c/3", "ab/4", "d"],
            "empty prefix lists everything in key order"
        );
        assert_eq!(list_keys(store, "a").await, vec!["a/b/1", "a/b/2", "a/c/3"]);
        assert_eq!(list_keys(store, "a/b").await, vec!["a/b/1", "a/b/2"]);
        assert_eq!(
            list_keys(store, "a/b/").await,
            vec!["a/b/1", "a/b/2"],
            "a trailing slash means the same prefix"
        );
        assert_eq!(
            list_keys(store, "d").await,
            vec!["d"],
            "a prefix equal to a key matches that key"
        );
        assert!(
            list_keys(store, "a/b/1/x").await.is_empty(),
            "prefix under a leaf matches nothing"
        );
        assert!(
            list_keys(store, "nope").await.is_empty(),
            "unmatched prefix lists nothing"
        );
    }

    pub async fn prefix_matching_is_component_aligned(store: &dyn BackupStore) {
        store.put("pre/fix", chunked_body(vec![1])).await.unwrap();
        store
            .put("pre/fixture", chunked_body(vec![2]))
            .await
            .unwrap();
        assert_eq!(
            list_keys(store, "pre/fix").await,
            vec!["pre/fix"],
            "string-prefix sibling must not match"
        );
        assert_eq!(
            store.delete_prefix("pre/fix").await.unwrap(),
            1,
            "delete_prefix uses the same matching rule"
        );
        assert_eq!(list_keys(store, "pre").await, vec!["pre/fixture"]);
    }

    pub async fn delete_prefix_removes_and_counts(store: &dyn BackupStore) {
        for key in ["del/a/1", "del/a/2", "del/b/3", "keep/4"] {
            store.put(key, chunked_body(vec![9])).await.unwrap();
        }
        assert_eq!(store.delete_prefix("del").await.unwrap(), 3);
        assert!(list_keys(store, "del").await.is_empty());
        assert_eq!(list_keys(store, "keep").await, vec!["keep/4"]);
        assert_eq!(
            store.delete_prefix("del").await.unwrap(),
            0,
            "a prefix that matches nothing deletes nothing"
        );
    }

    pub async fn missing_object_is_not_found(store: &dyn BackupStore) {
        let err = store.get("no/such/object").await.err().expect("get fails");
        assert!(matches!(err, StoreError::NotFound), "{err}");
        let head = store.head("no/such/object").await.expect("head succeeds");
        assert!(head.is_none());
    }

    pub async fn invalid_keys_are_rejected(store: &dyn BackupStore) {
        let bad_keys: &[&str] = &[
            "",
            "/",
            "/abs",
            "trail/",
            "dou//ble",
            ".",
            "..",
            "dot/./inner",
            "dot/../escape",
            "../up",
            "up/..",
            "back\\slash",
            "seg/back\\slash",
            "ctl\x00char",
            "ctl/\x1f",
        ];
        for key in bad_keys {
            let err = store
                .put(key, chunked_body(vec![1]))
                .await
                .err()
                .unwrap_or_else(|| panic!("put must reject {key:?}"));
            assert!(
                matches!(err, StoreError::InvalidKey(_)),
                "put {key:?}: {err}"
            );

            let err = store
                .get(key)
                .await
                .err()
                .unwrap_or_else(|| panic!("get must reject {key:?}"));
            assert!(
                matches!(err, StoreError::InvalidKey(_)),
                "get {key:?}: {err}"
            );

            let err = store
                .head(key)
                .await
                .err()
                .unwrap_or_else(|| panic!("head must reject {key:?}"));
            assert!(
                matches!(err, StoreError::InvalidKey(_)),
                "head {key:?}: {err}"
            );
        }

        let overlong = "a/".repeat(600);
        let err = store
            .put(overlong.trim_end_matches('/'), chunked_body(vec![1]))
            .await
            .expect_err("overlong key rejected");
        assert!(matches!(err, StoreError::InvalidKey(_)), "{err}");

        for bad_prefix in ["/", "dou//ble", "dot/../escape", "ctl\x02"] {
            let err = store
                .list(bad_prefix)
                .try_collect::<Vec<_>>()
                .await
                .err()
                .unwrap_or_else(|| panic!("list must reject {bad_prefix:?}"));
            assert!(matches!(err, StoreError::InvalidKey(_)), "{err}");
            let err = store
                .delete_prefix(bad_prefix)
                .await
                .err()
                .unwrap_or_else(|| panic!("delete_prefix must reject {bad_prefix:?}"));
            assert!(matches!(err, StoreError::InvalidKey(_)), "{err}");
        }
    }

    /// A body larger than the part size: multipart on S3, plain streaming on
    /// the filesystem. Content equality proves part order.
    /// `put` reports the size it wrote, and when the store assigns an `ETag` the
    /// same value is visible through `head` and `list`, so a manifest writer
    /// can record both from the put alone.
    pub async fn put_metadata_matches_head_and_list(store: &dyn BackupStore) {
        let data = patterned_bytes(3 * 1024 * 1024 + 17);
        let put = store.put("meta/one", chunked_body(&data)).await.unwrap();
        assert_eq!(put.key, "meta/one");
        assert_eq!(put.size, data.len() as u64);
        let head = store.head("meta/one").await.unwrap().expect("exists");
        assert_eq!(head.size, put.size);
        assert_eq!(head.etag, put.etag);
        let listed: Vec<ObjectMeta> = store.list("meta").try_collect().await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].size, put.size);
        assert_eq!(listed[0].etag, put.etag);
        if let Some(etag) = &put.etag {
            assert!(!etag.starts_with('"'), "etag must be unquoted: {etag}");
        }
        // A multipart put reports its size too.
        let big = patterned_bytes(usize::try_from(PART_SIZE).unwrap() + 1);
        let put = store.put("meta/two", chunked_body(&big)).await.unwrap();
        assert_eq!(put.size, big.len() as u64);
        assert_eq!(
            store.head("meta/two").await.unwrap().unwrap().etag,
            put.etag
        );
    }

    pub async fn body_larger_than_part_size(store: &dyn BackupStore) {
        let len = usize::try_from(PART_SIZE).unwrap() * 2 + 4096;
        let body = patterned_bytes(len);
        store
            .put("multi/part.bin", chunked_body(body.clone()))
            .await
            .expect("large put succeeds");
        let meta = store.head("multi/part.bin").await.unwrap().unwrap();
        assert_eq!(meta.size, len as u64);
        let read = read_all(store.get("multi/part.bin").await.unwrap()).await;
        assert_eq!(read.len(), body.len());
        assert_eq!(read, body, "reassembled content must match exactly");
    }

    /// A put whose body fails midway must surface the error and leave nothing
    /// under the key.
    pub async fn failed_put_leaves_nothing(store: &dyn BackupStore) {
        let good = patterned_bytes(usize::try_from(PART_SIZE).unwrap() + 4096);
        let err = store
            .put("aborted/upload.bin", failing_body(good))
            .await
            .expect_err("mid-body failure surfaces");
        assert!(
            matches!(&err, StoreError::Other(msg) if msg.contains("injected")),
            "the injected error must propagate, got: {err}"
        );
        assert!(
            store.head("aborted/upload.bin").await.unwrap().is_none(),
            "a failed put must leave nothing under the key"
        );
        assert!(list_keys(store, "aborted").await.is_empty());
    }
}

#[cfg(unix)]
mod filesystem {
    use super::*;
    use extenddb_backup_store::FilesystemStore;

    /// A private root: `tempfile` creates directories with the umask applied
    /// to 0777, which is usually 0755 and would be refused.
    fn private_tempdir() -> tempfile::TempDir {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700))
            .expect("chmod");
        dir
    }

    /// Create a directory under a test root with the mode the store would
    /// have used, so the test exercises the behaviour it names rather than
    /// the permissive-directory refusal.
    fn private_dir(path: &std::path::Path) {
        use std::os::unix::fs::DirBuilderExt;
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(path)
            .expect("create private dir");
    }

    async fn fs_store() -> (tempfile::TempDir, FilesystemStore) {
        let dir = private_tempdir();
        let store = FilesystemStore::open(dir.path()).await.expect("open");
        (dir, store)
    }

    #[tokio::test]
    async fn round_trip() {
        let (_dir, store) = fs_store().await;
        suite::round_trip(&store).await;
    }

    #[tokio::test]
    async fn overwrite_replaces_content() {
        let (_dir, store) = fs_store().await;
        suite::overwrite_replaces_content(&store).await;
    }

    #[tokio::test]
    async fn empty_body_round_trips() {
        let (_dir, store) = fs_store().await;
        suite::empty_body_round_trips(&store).await;
    }

    #[tokio::test]
    async fn list_with_and_without_prefix() {
        let (_dir, store) = fs_store().await;
        suite::list_with_and_without_prefix(&store).await;
    }

    #[tokio::test]
    async fn prefix_matching_is_component_aligned() {
        let (_dir, store) = fs_store().await;
        suite::prefix_matching_is_component_aligned(&store).await;
    }

    #[tokio::test]
    async fn delete_prefix_removes_and_counts() {
        let (_dir, store) = fs_store().await;
        suite::delete_prefix_removes_and_counts(&store).await;
    }

    #[tokio::test]
    async fn missing_object_is_not_found() {
        let (_dir, store) = fs_store().await;
        suite::missing_object_is_not_found(&store).await;
    }

    #[tokio::test]
    async fn invalid_keys_are_rejected() {
        let (_dir, store) = fs_store().await;
        suite::invalid_keys_are_rejected(&store).await;
    }

    #[tokio::test]
    async fn body_larger_than_part_size() {
        let (_dir, store) = fs_store().await;
        suite::body_larger_than_part_size(&store).await;
    }

    #[tokio::test]
    async fn failed_put_leaves_nothing() {
        let (_dir, store) = fs_store().await;
        suite::failed_put_leaves_nothing(&store).await;
        // The rename-into-place discipline: a failed put must also leave no
        // temporary file behind anywhere under the root.
        let (dir, store) = fs_store().await;
        let good = patterned_bytes(64 * 1024);
        let _ = store.put("aborted/again.bin", failing_body(good)).await;
        let mut stack = vec![dir.path().to_path_buf()];
        while let Some(current) = stack.pop() {
            for entry in std::fs::read_dir(&current).expect("read_dir") {
                let entry = entry.expect("entry");
                if entry.file_type().expect("file_type").is_dir() {
                    stack.push(entry.path());
                } else {
                    panic!("stray file after failed put: {}", entry.path().display());
                }
            }
        }
    }

    #[tokio::test]
    async fn put_metadata_matches_head_and_list() {
        let (_dir, store) = fs_store().await;
        suite::put_metadata_matches_head_and_list(&store).await;
    }

    fn mode_of(path: &std::path::Path) -> u32 {
        use std::os::unix::fs::PermissionsExt;
        std::fs::symlink_metadata(path)
            .unwrap()
            .permissions()
            .mode()
            & 0o777
    }

    /// Objects and the directories the store creates are private to the
    /// owner regardless of the process umask.
    #[tokio::test]
    async fn created_files_and_directories_are_private() {
        let (dir, store) = fs_store().await;
        store
            .put(
                "acct/table/backup-1/data/000001.json.gz",
                chunked_body(vec![1, 2, 3]),
            )
            .await
            .unwrap();
        for sub in [
            "acct",
            "acct/table",
            "acct/table/backup-1",
            "acct/table/backup-1/data",
        ] {
            let mode = mode_of(&dir.path().join(sub));
            assert_eq!(mode, 0o700, "directory {sub} has mode {mode:o}");
        }
        let file = dir.path().join("acct/table/backup-1/data/000001.json.gz");
        assert_eq!(mode_of(&file), 0o600, "object mode {:o}", mode_of(&file));
    }

    #[tokio::test]
    async fn validate_leaves_no_probe_behind() {
        let (dir, store) = fs_store().await;
        store.validate().await.unwrap();
        let leftovers: Vec<_> = std::fs::read_dir(dir.path()).unwrap().collect();
        assert!(leftovers.is_empty(), "probe not removed: {leftovers:?}");
    }

    /// Any group or other permission on the root is refused: the root holds
    /// backups, and a readable or traversable root lets another user reach
    /// a permissive subdirectory that predates the store.
    #[tokio::test]
    async fn open_refuses_a_root_accessible_to_others() {
        use std::os::unix::fs::PermissionsExt;
        for mode in [0o720u32, 0o702, 0o777, 0o1777, 0o750, 0o755, 0o705, 0o701] {
            let dir = tempfile::tempdir().expect("tempdir");
            std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(mode)).unwrap();
            let err = FilesystemStore::open(dir.path())
                .await
                .err()
                .unwrap_or_else(|| panic!("root with mode {mode:o} must be refused"));
            assert!(
                matches!(err, StoreError::PermissionDenied(_)),
                "{mode:o}: {err}"
            );
            assert!(
                err.to_string().contains("accessible to other users"),
                "{err}"
            );
            assert!(
                err.to_string().contains(&format!("{mode:04o}")),
                "the message prints the full mode: {err}"
            );
        }
    }

    /// A pre-existing permissive directory under the root is refused when an
    /// operation descends into it, and the put creates nothing beneath it.
    #[tokio::test]
    async fn permissive_existing_subdirectory_is_refused() {
        use std::os::unix::fs::PermissionsExt;
        let (dir, store) = fs_store().await;
        private_dir(&dir.path().join("acct"));
        std::fs::set_permissions(
            dir.path().join("acct"),
            std::fs::Permissions::from_mode(0o777),
        )
        .unwrap();
        let err = store
            .put("acct/table/obj", chunked_body(vec![1]))
            .await
            .expect_err("put through a permissive directory refused");
        assert!(matches!(err, StoreError::PermissionDenied(_)), "{err}");
        assert!(!dir.path().join("acct/table").exists());
        std::fs::write(dir.path().join("acct/obj"), b"x").unwrap();
        let err = store.get("acct/obj").await.err().expect("get refused");
        assert!(matches!(err, StoreError::PermissionDenied(_)), "{err}");
        let err = store.head("acct/obj").await.expect_err("head refused");
        assert!(matches!(err, StoreError::PermissionDenied(_)), "{err}");
        // list and delete_prefix apply the same rule on their descent, from
        // the prefix directory and from the root.
        for prefix in ["acct", ""] {
            let err = store
                .list(prefix)
                .try_collect::<Vec<_>>()
                .await
                .expect_err("list through a permissive directory refused");
            assert!(matches!(err, StoreError::PermissionDenied(_)), "{err}");
            let err = store
                .delete_prefix(prefix)
                .await
                .expect_err("delete_prefix through a permissive directory refused");
            assert!(matches!(err, StoreError::PermissionDenied(_)), "{err}");
        }
        assert!(dir.path().join("acct/obj").exists(), "nothing was deleted");
        // Once the operator fixes the mode, the same key works.
        std::fs::set_permissions(
            dir.path().join("acct"),
            std::fs::Permissions::from_mode(0o700),
        )
        .unwrap();
        store
            .put("acct/table/obj", chunked_body(vec![1]))
            .await
            .expect("put after chmod");
        assert_eq!(store.delete_prefix("acct").await.unwrap(), 2);
    }

    /// The root is re-checked by every operation, not only by `open` and
    /// `validate`, so widening it after startup is caught by the next call.
    #[tokio::test]
    async fn root_widened_after_open_is_refused_by_operations() {
        use std::os::unix::fs::PermissionsExt;
        let (dir, store) = fs_store().await;
        store.put("a/obj", chunked_body(vec![1])).await.unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o750)).unwrap();
        let put = store.put("obj", chunked_body(vec![1])).await;
        assert!(
            matches!(put, Err(StoreError::PermissionDenied(_))),
            "{put:?}"
        );
        let get = store.get("a/obj").await.err().expect("get refused");
        assert!(matches!(get, StoreError::PermissionDenied(_)), "{get}");
        let head = store.head("a/obj").await;
        assert!(
            matches!(head, Err(StoreError::PermissionDenied(_))),
            "{head:?}"
        );
        let list = store.list("").try_collect::<Vec<_>>().await;
        assert!(
            matches!(list, Err(StoreError::PermissionDenied(_))),
            "{list:?}"
        );
        let del = store.delete_prefix("a").await;
        assert!(
            matches!(del, Err(StoreError::PermissionDenied(_))),
            "{del:?}"
        );
        assert!(dir.path().join("a/obj").exists());
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        assert_eq!(store.delete_prefix("a").await.unwrap(), 1);
    }

    /// A root under a directory that other users can write to (without the
    /// sticky bit) is refused: they could rename the root away and plant a
    /// symlink at its path. A sticky world-writable ancestor (like /tmp) is
    /// accepted. The other ancestor cell, a directory owned by a third user,
    /// needs a second uid and is not exercised here.
    #[tokio::test]
    async fn root_under_a_world_writable_parent_is_refused() {
        use std::os::unix::fs::PermissionsExt;
        let parent = tempfile::tempdir().expect("parent");
        let root = parent.path().join("backups");
        std::fs::create_dir(&root).unwrap();
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
        for (mode, accepted) in [
            (0o777u32, false),
            (0o770, false),
            (0o1777, true),
            (0o755, true),
        ] {
            std::fs::set_permissions(parent.path(), std::fs::Permissions::from_mode(mode)).unwrap();
            let result = FilesystemStore::open(&root).await;
            assert_eq!(
                result.is_ok(),
                accepted,
                "parent mode {mode:04o}: {:?}",
                result.as_ref().err().map(ToString::to_string)
            );
            if let Err(err) = result {
                assert!(err.to_string().contains("ancestor"), "{err}");
            }
        }
    }

    #[tokio::test]
    async fn validate_refuses_a_root_that_became_writable_by_others() {
        use std::os::unix::fs::PermissionsExt;
        let (dir, store) = fs_store().await;
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o770)).unwrap();
        let err = store.validate().await.expect_err("refused");
        assert!(matches!(err, StoreError::PermissionDenied(_)), "{err}");
    }

    /// Owner-only modes are accepted; the owner's own bits do not matter.
    #[tokio::test]
    async fn open_accepts_owner_only_roots() {
        use std::os::unix::fs::PermissionsExt;
        for mode in [0o700u32, 0o500] {
            let dir = tempfile::tempdir().expect("tempdir");
            std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(mode)).unwrap();
            FilesystemStore::open(dir.path())
                .await
                .unwrap_or_else(|e| panic!("mode {mode:o} must be accepted: {e}"));
        }
    }

    /// Symlink input space beyond the existing-target cases: a dangling link
    /// at the destination, a dangling link in the parent chain, a relative
    /// link that escapes, and a link that points at another link.
    #[tokio::test]
    async fn dangling_symlink_at_destination_is_refused() {
        let (dir, store) = fs_store().await;
        private_dir(&dir.path().join("a"));
        let missing = dir.path().join("nowhere-outside");
        std::os::unix::fs::symlink(&missing, dir.path().join("a/obj")).unwrap();
        let err = store
            .put("a/obj", chunked_body(vec![1]))
            .await
            .expect_err("put refused");
        assert!(matches!(err, StoreError::PermissionDenied(_)), "{err}");
        assert!(
            !missing.exists(),
            "dangling link target must not be created"
        );
        let err = store.head("a/obj").await.expect_err("head refused");
        assert!(err.to_string().contains("symlink"), "{err}");
        let err = store.get("a/obj").await.err().expect("get refused");
        assert!(matches!(err, StoreError::PermissionDenied(_)), "{err}");
    }

    #[tokio::test]
    async fn dangling_symlink_in_parent_chain_is_refused() {
        let outside = tempfile::tempdir().expect("outside dir");
        let missing_dir = outside.path().join("not-yet");
        let (dir, store) = fs_store().await;
        std::os::unix::fs::symlink(&missing_dir, dir.path().join("link")).unwrap();
        let err = store
            .put("link/sub/obj", chunked_body(vec![1]))
            .await
            .expect_err("put refused");
        assert!(matches!(err, StoreError::PermissionDenied(_)), "{err}");
        assert!(
            !missing_dir.exists(),
            "nothing may be created through a dangling link"
        );
    }

    #[tokio::test]
    async fn relative_symlink_escape_is_refused() {
        let (dir, store) = fs_store().await;
        // Sibling of the root under the same parent, so the relative link
        // resolves (this cell is "relative link that escapes", not "dangling").
        let outside = tempfile::tempdir_in(dir.path().parent().unwrap()).expect("outside dir");
        std::fs::write(outside.path().join("secret.txt"), b"x").unwrap();
        let rel = std::path::Path::new("..").join(outside.path().file_name().unwrap());
        std::os::unix::fs::symlink(&rel, dir.path().join("rel")).unwrap();
        assert!(
            dir.path().join("rel/secret.txt").exists(),
            "the link must resolve for this cell to mean anything"
        );
        let err = store
            .get("rel/secret.txt")
            .await
            .err()
            .expect("get refused");
        assert!(matches!(err, StoreError::PermissionDenied(_)), "{err}");
        let err = store
            .put("rel/new", chunked_body(vec![1]))
            .await
            .expect_err("put refused");
        assert!(matches!(err, StoreError::PermissionDenied(_)), "{err}");
        assert!(!outside.path().join("new").exists());
    }

    #[tokio::test]
    async fn symlink_chain_is_refused() {
        let outside = tempfile::tempdir().expect("outside dir");
        let (dir, store) = fs_store().await;
        private_dir(&dir.path().join("real"));
        std::os::unix::fs::symlink(dir.path().join("real"), dir.path().join("hop1")).unwrap();
        std::os::unix::fs::symlink(dir.path().join("hop1"), dir.path().join("hop2")).unwrap();
        std::os::unix::fs::symlink(outside.path(), dir.path().join("real/out")).unwrap();
        for key in ["hop2/obj", "hop1/obj", "real/out/obj"] {
            let err = store.put(key, chunked_body(vec![1])).await.expect_err(key);
            assert!(
                matches!(err, StoreError::PermissionDenied(_)),
                "{key}: {err}"
            );
        }
        assert!(!dir.path().join("real/obj").exists());
        assert!(!outside.path().join("obj").exists());
    }

    /// A root given as a symlink is canonicalized; the target's owner and
    /// mode are what is checked, and keys resolve under the target.
    #[tokio::test]
    async fn symlinked_root_is_canonicalized() {
        let real = private_tempdir();
        let link_parent = tempfile::tempdir().expect("link parent");
        let link = link_parent.path().join("backups");
        std::os::unix::fs::symlink(real.path(), &link).unwrap();
        let store = FilesystemStore::open(&link)
            .await
            .expect("open through a root symlink");
        store.put("a/obj", chunked_body(vec![1])).await.unwrap();
        assert!(real.path().join("a/obj").is_file());
        // Repointing the link after open changes nothing: the canonical path
        // is what the store holds.
        let other = private_tempdir();
        std::fs::remove_file(&link).unwrap();
        std::os::unix::fs::symlink(other.path(), &link).unwrap();
        store.put("b/obj", chunked_body(vec![2])).await.unwrap();
        assert!(real.path().join("b/obj").is_file());
        assert!(!other.path().join("b").exists());
    }

    /// A key that names an existing directory cannot be written; the error
    /// is an I/O error and the directory and its contents survive.
    #[tokio::test]
    async fn put_over_an_existing_directory_is_refused() {
        let (dir, store) = fs_store().await;
        store.put("a/b", chunked_body(vec![1])).await.unwrap();
        let err = store
            .put("a", chunked_body(vec![2]))
            .await
            .expect_err("refused");
        assert!(matches!(err, StoreError::Io { .. }), "{err}");
        assert_eq!(read_all(store.get("a/b").await.unwrap()).await, vec![1]);
        let temps: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".put-")
            })
            .collect();
        assert!(temps.is_empty(), "no temp file left behind");
    }

    /// A failed put removes the directories it created, so the failure is
    /// invisible to `list()` as it is on S3. Directories that existed before
    /// the put stay.
    #[tokio::test]
    async fn failed_put_removes_its_directory_skeleton() {
        let (dir, store) = fs_store().await;
        store.put("keep/one", chunked_body(vec![1])).await.unwrap();
        let err = store
            .put("keep/new/deep/obj", failing_body(vec![1]))
            .await
            .expect_err("failed put");
        assert!(matches!(err, StoreError::Other(_)), "{err}");
        assert!(
            !dir.path().join("keep/new").exists(),
            "created skeleton removed"
        );
        assert!(
            dir.path().join("keep/one").is_file(),
            "pre-existing content untouched"
        );
        assert_eq!(list_keys(&store, "").await, vec!["keep/one"]);
    }

    #[tokio::test]
    async fn validate_probes_the_root() {
        let (_dir, store) = fs_store().await;
        store.validate().await.expect("writable root validates");
    }

    #[tokio::test]
    async fn validate_rejects_an_unwritable_root() {
        use std::os::unix::fs::PermissionsExt;
        // SAFETY: geteuid has no preconditions.
        if unsafe { libc::geteuid() } == 0 {
            eprintln!("skipping: root bypasses file permission checks");
            return;
        }
        let dir = private_tempdir();
        let store = FilesystemStore::open(dir.path()).await.expect("open");
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o500))
            .expect("chmod");
        let result = store.validate().await;
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700))
            .expect("chmod back");
        let err = result.expect_err("unwritable root fails validation");
        assert!(matches!(err, StoreError::PermissionDenied(_)), "{err}");
    }

    #[tokio::test]
    async fn open_refuses_a_missing_root() {
        let dir = tempfile::tempdir().expect("tempdir");
        let missing = dir.path().join("does-not-exist");
        assert!(FilesystemStore::open(&missing).await.is_err());
    }

    #[tokio::test]
    async fn symlinked_directory_escape_is_refused() {
        let outside = tempfile::tempdir().expect("outside dir");
        std::fs::write(outside.path().join("secret.txt"), b"outside data").unwrap();

        let (dir, store) = fs_store().await;
        std::os::unix::fs::symlink(outside.path(), dir.path().join("esc")).unwrap();

        let err = store
            .get("esc/secret.txt")
            .await
            .err()
            .expect("get refused");
        assert!(matches!(err, StoreError::PermissionDenied(_)), "{err}");

        let err = store
            .head("esc/secret.txt")
            .await
            .expect_err("head refused");
        assert!(matches!(err, StoreError::PermissionDenied(_)), "{err}");

        let err = store
            .put("esc/new-file", chunked_body(vec![1]))
            .await
            .expect_err("put refused");
        assert!(matches!(err, StoreError::PermissionDenied(_)), "{err}");
        assert!(
            !outside.path().join("new-file").exists(),
            "nothing may be written outside the root"
        );
    }

    #[tokio::test]
    async fn symlinked_file_escape_is_refused() {
        let outside = tempfile::tempdir().expect("outside dir");
        let target = outside.path().join("secret.txt");
        std::fs::write(&target, b"outside data").unwrap();

        let (dir, store) = fs_store().await;
        std::os::unix::fs::symlink(&target, dir.path().join("leak.json")).unwrap();

        let err = store.get("leak.json").await.err().expect("get refused");
        assert!(matches!(err, StoreError::PermissionDenied(_)), "{err}");

        let err = store
            .put("leak.json", chunked_body(vec![1]))
            .await
            .expect_err("put through an escaping symlink refused");
        assert!(matches!(err, StoreError::PermissionDenied(_)), "{err}");
        assert_eq!(
            std::fs::read(&target).unwrap(),
            b"outside data",
            "the symlink target must be untouched"
        );
    }

    /// An in-root directory symlink aliases one key onto another: a put
    /// through the alias would land under the victim's prefix while list and
    /// `delete_prefix` on the alias see nothing. The store refuses the alias
    /// on every key-resolving operation, so all operations agree on what a
    /// key names.
    #[tokio::test]
    async fn in_root_directory_symlink_is_refused() {
        let (dir, store) = fs_store().await;
        store
            .put("victim/sub/obj", chunked_body(vec![1]))
            .await
            .unwrap();
        std::os::unix::fs::symlink(dir.path().join("victim"), dir.path().join("alias")).unwrap();

        let err = store
            .put("alias/sub/obj2", chunked_body(vec![2]))
            .await
            .expect_err("put through an in-root symlink refused");
        assert!(matches!(err, StoreError::PermissionDenied(_)), "{err}");
        assert!(
            !dir.path().join("victim/sub/obj2").exists(),
            "nothing may land under the aliased prefix"
        );

        let err = store
            .get("alias/sub/obj")
            .await
            .err()
            .expect("get through an in-root symlink refused");
        assert!(matches!(err, StoreError::PermissionDenied(_)), "{err}");

        let err = store
            .head("alias/sub/obj")
            .await
            .expect_err("head through an in-root symlink refused");
        assert!(matches!(err, StoreError::PermissionDenied(_)), "{err}");

        assert!(
            list_keys(&store, "alias").await.is_empty(),
            "the aliased prefix lists nothing"
        );

        let deleted = store.delete_prefix("alias").await.unwrap();
        assert_eq!(deleted, 0, "delete through the alias removes nothing");
        assert_eq!(
            read_all(store.get("victim/sub/obj").await.unwrap()).await,
            vec![1],
            "the object under its real key survives"
        );
    }

    /// An in-root file symlink planted at a key would redirect get and head
    /// to another key's content, and a put over it would resolve through the
    /// link. All three are refused.
    #[tokio::test]
    async fn in_root_file_symlink_is_refused() {
        let (dir, store) = fs_store().await;
        store
            .put("victim/data", chunked_body(vec![7]))
            .await
            .unwrap();
        std::os::unix::fs::symlink(dir.path().join("victim/data"), dir.path().join("planted"))
            .unwrap();

        let err = store.get("planted").await.err().expect("get refused");
        assert!(matches!(err, StoreError::PermissionDenied(_)), "{err}");

        let err = store.head("planted").await.expect_err("head refused");
        assert!(matches!(err, StoreError::PermissionDenied(_)), "{err}");

        let err = store
            .put("planted", chunked_body(vec![8]))
            .await
            .expect_err("put over an in-root file symlink refused");
        assert!(matches!(err, StoreError::PermissionDenied(_)), "{err}");
        assert_eq!(
            read_all(store.get("victim/data").await.unwrap()).await,
            vec![7],
            "the symlink target must be untouched"
        );
    }

    #[tokio::test]
    async fn symlinks_are_invisible_to_list() {
        let outside = tempfile::tempdir().expect("outside dir");
        std::fs::write(outside.path().join("secret.txt"), b"x").unwrap();

        let (dir, store) = fs_store().await;
        store
            .put("real/object", chunked_body(vec![1]))
            .await
            .unwrap();
        std::os::unix::fs::symlink(outside.path(), dir.path().join("esc")).unwrap();

        assert_eq!(list_keys(&store, "").await, vec!["real/object"]);
    }

    /// A put through a planted directory symlink: `create_dir_all` follows
    /// symlinks in existing components, so a naive implementation creates
    /// parent directories outside the root before its escape check refuses
    /// the put. The store must check the deepest existing ancestor before
    /// creating anything.
    #[tokio::test]
    async fn multi_level_put_under_directory_symlink_creates_nothing_outside() {
        let outside = tempfile::tempdir().expect("outside dir");
        let (dir, store) = fs_store().await;
        std::os::unix::fs::symlink(outside.path(), dir.path().join("esc")).unwrap();

        let err = store
            .put("esc/sub/deeper/file", chunked_body(vec![1]))
            .await
            .expect_err("put through an escaping symlink refused");
        assert!(matches!(err, StoreError::PermissionDenied(_)), "{err}");
        assert!(
            !outside.path().join("sub").exists(),
            "no directory may be created outside the root"
        );
        assert_eq!(
            std::fs::read_dir(outside.path()).unwrap().count(),
            0,
            "the symlink target must be untouched"
        );
    }

    /// Same shape with a file symlink at the parent position: the walk stops
    /// at the symlink, which resolves outside the root, and nothing is
    /// created or modified on the far side.
    #[tokio::test]
    async fn multi_level_put_under_file_symlink_creates_nothing_outside() {
        let outside = tempfile::tempdir().expect("outside dir");
        let target = outside.path().join("secret.txt");
        std::fs::write(&target, b"outside data").unwrap();

        let (dir, store) = fs_store().await;
        std::os::unix::fs::symlink(&target, dir.path().join("leak")).unwrap();

        let err = store
            .put("leak/sub/file", chunked_body(vec![1]))
            .await
            .expect_err("put through an escaping file symlink refused");
        assert!(matches!(err, StoreError::PermissionDenied(_)), "{err}");
        assert_eq!(
            std::fs::read(&target).unwrap(),
            b"outside data",
            "the symlink target must be untouched"
        );
        assert_eq!(
            std::fs::read_dir(outside.path()).unwrap().count(),
            1,
            "nothing may be created outside the root"
        );
    }

    /// An in-root regular file at an ancestor of the key is a collision, not
    /// an escape: the put fails with an I/O error and creates nothing.
    #[tokio::test]
    async fn put_under_an_existing_object_is_refused() {
        let (_dir, store) = fs_store().await;
        store.put("plain", chunked_body(vec![1])).await.unwrap();
        let err = store
            .put("plain/sub/file", chunked_body(vec![2]))
            .await
            .expect_err("ancestor collision refused");
        assert!(matches!(err, StoreError::Io { .. }), "{err}");
        assert_eq!(read_all(store.get("plain").await.unwrap()).await, vec![1]);
    }
}

mod s3 {
    use super::*;
    use extenddb_backup_store::{S3Store, S3StoreConfig};

    const SKIP: &str =
        "skipping: EXTENDDB_TEST_S3_ENDPOINT is not set; see crates/backup-store/README.md";

    /// The test harness captures `eprintln!`, so a skipped S3 suite would be
    /// invisible in default output. Writing to the raw stderr handle bypasses
    /// capture and keeps the skip visible.
    fn announce_skip() {
        use std::io::Write;
        let _ = writeln!(std::io::stderr(), "{SKIP}");
    }

    /// The one place the skip/require decision is made. Every S3 test gets
    /// its endpoint from here, so no test can skip through a private path.
    /// Returns `None` after announcing the skip; panics when
    /// `EXTENDDB_TEST_S3_REQUIRED` is set and no endpoint is configured.
    fn endpoint_or_skip() -> Option<String> {
        match std::env::var("EXTENDDB_TEST_S3_ENDPOINT") {
            Ok(endpoint) if !endpoint.trim().is_empty() => Some(endpoint),
            _ => {
                assert!(
                    std::env::var_os("EXTENDDB_TEST_S3_REQUIRED").is_none(),
                    "EXTENDDB_TEST_S3_REQUIRED is set but EXTENDDB_TEST_S3_ENDPOINT is not; \
                     the S3 conformance suite must run in this environment"
                );
                announce_skip();
                None
            }
        }
    }

    fn test_region() -> String {
        std::env::var("EXTENDDB_TEST_S3_REGION").unwrap_or_else(|_| "us-east-1".to_owned())
    }

    fn test_bucket() -> String {
        std::env::var("EXTENDDB_TEST_S3_BUCKET")
            .unwrap_or_else(|_| "extenddb-backup-store-tests".to_owned())
    }

    /// Config for one test: a bucket, a prefix, path-style addressing for
    /// local endpoints.
    fn test_config(endpoint: String, bucket: String, prefix: String) -> S3StoreConfig {
        S3StoreConfig {
            bucket,
            prefix,
            region: Some(test_region()),
            endpoint: Some(endpoint),
            force_path_style: true,
            upload_part_size_bytes: PART_SIZE,
            max_concurrent_uploads: 4,
        }
    }

    /// A unique prefix per test so runs never collide.
    fn unique_prefix() -> String {
        format!("conformance-{}/", uuid::Uuid::new_v4())
    }

    /// Build a store against the local endpoint, or `None` (with a printed
    /// reason) when the environment does not provide one.
    async fn store() -> Option<S3Store> {
        let config = test_config(endpoint_or_skip()?, test_bucket(), unique_prefix());
        let store = S3Store::open(&config).await.expect("S3Store::open");
        ensure_bucket(&config).await;
        Some(store)
    }

    /// Create the test bucket when it does not exist yet, with a raw client
    /// so the check stays independent of the store under test.
    async fn ensure_bucket(config: &S3StoreConfig) {
        let client = raw_client(config).await;
        let mut request = client.create_bucket().bucket(&config.bucket);
        // S3 requires a location constraint for every region but us-east-1,
        // and rejects one that names us-east-1.
        if let Some(region) = config.region.as_deref().filter(|r| *r != "us-east-1") {
            use aws_sdk_s3::types::{BucketLocationConstraint, CreateBucketConfiguration};
            request = request.create_bucket_configuration(
                CreateBucketConfiguration::builder()
                    .location_constraint(BucketLocationConstraint::from(region))
                    .build(),
            );
        }
        match request.send().await {
            Ok(_) => {}
            Err(err) => {
                let service = err.as_service_error();
                let exists = service.is_some_and(|e| {
                    e.is_bucket_already_owned_by_you() || e.is_bucket_already_exists()
                });
                assert!(exists, "create_bucket failed: {err}");
            }
        }
    }

    async fn raw_client(config: &S3StoreConfig) -> aws_sdk_s3::Client {
        let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
        if let Some(region) = &config.region {
            loader = loader.region(aws_config::Region::new(region.clone()));
        }
        if let Some(endpoint) = &config.endpoint {
            loader = loader.endpoint_url(endpoint.clone());
        }
        let shared = loader.load().await;
        let builder = aws_sdk_s3::config::Builder::from(&shared).force_path_style(true);
        aws_sdk_s3::Client::from_conf(builder.build())
    }

    #[tokio::test]
    async fn put_metadata_matches_head_and_list() {
        let Some(store) = store().await else { return };
        suite::put_metadata_matches_head_and_list(&store).await;
        // S3 always assigns an ETag.
        let meta = store.put("etag/x", chunked_body(vec![9])).await.unwrap();
        assert!(meta.etag.is_some(), "S3 put must report an ETag");
    }

    #[tokio::test]
    async fn round_trip() {
        let Some(store) = store().await else { return };
        suite::round_trip(&store).await;
    }

    #[tokio::test]
    async fn overwrite_replaces_content() {
        let Some(store) = store().await else { return };
        suite::overwrite_replaces_content(&store).await;
    }

    #[tokio::test]
    async fn empty_body_round_trips() {
        let Some(store) = store().await else { return };
        suite::empty_body_round_trips(&store).await;
    }

    #[tokio::test]
    async fn list_with_and_without_prefix() {
        let Some(store) = store().await else { return };
        suite::list_with_and_without_prefix(&store).await;
    }

    #[tokio::test]
    async fn prefix_matching_is_component_aligned() {
        let Some(store) = store().await else { return };
        suite::prefix_matching_is_component_aligned(&store).await;
    }

    #[tokio::test]
    async fn delete_prefix_removes_and_counts() {
        let Some(store) = store().await else { return };
        suite::delete_prefix_removes_and_counts(&store).await;
    }

    #[tokio::test]
    async fn missing_object_is_not_found() {
        let Some(store) = store().await else { return };
        suite::missing_object_is_not_found(&store).await;
    }

    #[tokio::test]
    async fn invalid_keys_are_rejected() {
        let Some(store) = store().await else { return };
        suite::invalid_keys_are_rejected(&store).await;
    }

    #[tokio::test]
    async fn body_larger_than_part_size() {
        let Some(store) = store().await else { return };
        suite::body_larger_than_part_size(&store).await;
    }

    #[tokio::test]
    async fn failed_put_aborts_the_multipart_upload() {
        let Some(endpoint) = endpoint_or_skip() else {
            return;
        };
        let prefix = unique_prefix();
        let config = test_config(endpoint, test_bucket(), prefix.clone());
        let store = S3Store::open(&config).await.expect("S3Store::open");
        ensure_bucket(&config).await;

        suite::failed_put_leaves_nothing(&store).await;

        // The store-level check proved no object landed; this proves the
        // multipart upload itself was aborted rather than left in progress.
        let client = raw_client(&config).await;
        let uploads = client
            .list_multipart_uploads()
            .bucket(&config.bucket)
            .prefix(&prefix)
            .send()
            .await
            .expect("list_multipart_uploads");
        assert!(
            uploads.uploads().is_empty(),
            "a failed put must abort its multipart upload: {:?}",
            uploads.uploads()
        );
    }

    /// An interceptor that fails `CompleteMultipartUpload`, so the abort path
    /// after a complete-time failure is exercised against a real endpoint.
    #[derive(Debug)]
    struct FailCompleteMultipartUpload;

    impl aws_sdk_s3::config::Intercept for FailCompleteMultipartUpload {
        fn name(&self) -> &'static str {
            "FailCompleteMultipartUpload"
        }

        fn read_before_execution(
            &self,
            context: &aws_sdk_s3::config::interceptors::BeforeSerializationInterceptorContextRef<
                '_,
            >,
            _cfg: &mut aws_sdk_s3::config::ConfigBag,
        ) -> Result<(), aws_sdk_s3::error::BoxError> {
            let is_complete = context
                .input()
                .downcast_ref::<aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadInput>()
                .is_some();
            if is_complete {
                return Err("injected CompleteMultipartUpload failure".into());
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn failed_complete_aborts_the_multipart_upload() {
        let Some(endpoint) = endpoint_or_skip() else {
            return;
        };
        let prefix = unique_prefix();
        let config = test_config(endpoint, test_bucket(), prefix.clone());
        let store = S3Store::open_with_interceptor(&config, FailCompleteMultipartUpload)
            .await
            .expect("S3Store::open_with_interceptor");
        ensure_bucket(&config).await;

        let body = patterned_bytes(usize::try_from(PART_SIZE).unwrap() * 2 + 4096);
        let err = store
            .put("complete/fails.bin", chunked_body(body))
            .await
            .expect_err("injected complete failure surfaces");
        assert!(
            err.to_string().contains("injected") || matches!(err, StoreError::Transport { .. }),
            "the injected failure must propagate: {err}"
        );
        assert!(
            store.head("complete/fails.bin").await.unwrap().is_none(),
            "no object may exist after a failed complete"
        );

        let client = raw_client(&config).await;
        let uploads = client
            .list_multipart_uploads()
            .bucket(&config.bucket)
            .prefix(&prefix)
            .send()
            .await
            .expect("list_multipart_uploads");
        assert!(
            uploads.uploads().is_empty(),
            "a failed complete must abort its multipart upload: {:?}",
            uploads.uploads()
        );
    }

    #[tokio::test]
    async fn validate_calls_head_bucket() {
        let Some(store) = store().await else { return };
        store.validate().await.expect("existing bucket validates");
    }

    #[tokio::test]
    async fn validate_names_a_missing_bucket() {
        let Some(endpoint) = endpoint_or_skip() else {
            return;
        };
        let missing = format!("extenddb-no-such-bucket-{}", uuid::Uuid::new_v4());
        let config = test_config(endpoint, missing.clone(), String::new());
        let store = S3Store::open(&config).await.expect("S3Store::open");
        let err = store.validate().await.expect_err("missing bucket fails");
        assert!(
            err.to_string().contains(&missing) && err.to_string().contains("does not exist"),
            "the message must name the bucket and say it does not exist: {err}"
        );
    }
}
