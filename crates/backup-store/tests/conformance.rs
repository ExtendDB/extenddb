// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Conformance suite run against both store implementations.
//!
//! The `suite` module holds store-agnostic checks; the `filesystem` and `s3`
//! modules bind them to a tempdir-backed [`FilesystemStore`] and an
//! [`S3Store`] against a local S3-compatible endpoint. S3 tests run only when
//! `EXTENDDB_TEST_S3_ENDPOINT` is set and skip with a printed reason
//! otherwise, except when `EXTENDDB_TEST_S3_REQUIRED` is also set (CI), where
//! a missing endpoint is a test failure. The five tests that put a TCP proxy
//! in front of the endpoint also need it to be `http://`, and skip with a
//! printed reason otherwise, `EXTENDDB_TEST_S3_REQUIRED` or not. See the
//! crate README for how to run them.
//!
//! The endpoint in CI is moto, which cannot produce some of what the store
//! handles; those cases are synthesised, each at the place named: a 301 and a
//! 403 with its error body (`RewriteStatus`), a `NoSuchUpload` or
//! `AccessDenied` answer to one operation (`RewriteOperation`), a per-key `DeleteObjects`
//! error (`DenyOneDelete`), `ListMultipartUploads` truncation (`PageUploads`;
//! moto never truncates), a response held or swallowed in flight
//! (`delaying_proxy`, `swallowing_proxy`), and a `GetObject` body that stops
//! after the headers (`stalling_proxy`).

use bytes::Bytes;
use extenddb_backup_store::{BackupStore, ByteStream, StoreError, byte_stream_from};
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

/// Names the test a child process was started for (see `run_in_child`).
const CHILD_TEST: &str = "EXTENDDB_TEST_CHILD";

/// Whether this process is the child `run_in_child` started for `name`.
fn is_child(name: &str) -> bool {
    std::env::var(CHILD_TEST).is_ok_and(|n| n == name)
}

/// Proxy variables the SDK's HTTP client reads. A child starts without them,
/// so a proxy in the parent's environment cannot answer for the endpoint a
/// test expects to reach (or to find closed); a test that needs one sets it
/// in `set`.
const PROXY_VARIABLES: [&str; 8] = [
    "HTTP_PROXY",
    "http_proxy",
    "HTTPS_PROXY",
    "https_proxy",
    "ALL_PROXY",
    "all_proxy",
    "NO_PROXY",
    "no_proxy",
];

/// Run the test `name` again in a child process (this test binary with
/// `--exact`), with `set` added to and `remove` taken from its environment,
/// and assert that exactly that test ran and passed. For tests that change
/// process-wide state (a resource limit, the AWS environment) which must not
/// leak into the tests running beside them. The child never inherits the
/// parent's `PROXY_VARIABLES`. Returns the child's standard output.
fn run_in_child(name: &str, set: &[(&str, &std::ffi::OsStr)], remove: &[&str]) -> String {
    let mut command = std::process::Command::new(std::env::current_exe().expect("test binary"));
    command
        .args([name, "--exact", "--nocapture", "--test-threads=1"])
        .env(CHILD_TEST, name);
    for key in PROXY_VARIABLES {
        command.env_remove(key);
    }
    for (key, value) in set {
        command.env(key, value);
    }
    for key in remove {
        command.env_remove(key);
    }
    let output = command.output().expect("spawn the child test process");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success() && stdout.contains("test result: ok. 1 passed"),
        "child {name} failed ({}):\n--- stdout\n{stdout}\n--- stderr\n{stderr}",
        output.status
    );
    stdout.into_owned()
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

    /// `list` orders by the key's bytes, not by component: `-` (0x2d) and
    /// `.` (0x2e) sort before `/` (0x2f), so `a-c` and `a.c` come before
    /// `a/b` even though the component `a` sorts before `a-c`.
    pub async fn list_order_is_key_bytes(store: &dyn BackupStore) {
        for key in ["a/b", "a.c", "a-c"] {
            store.put(key, chunked_body(vec![1])).await.unwrap();
        }
        assert_eq!(list_keys(store, "").await, vec!["a-c", "a.c", "a/b"]);
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

        // Keys one store could hold and the other could not: a component
        // over the filesystem name limit (S3 stores it), and U+FFFE / U+FFFF
        // (S3 stores them but cannot list them).
        let long_component = format!("long/{}", "c".repeat(256));
        for key in [
            long_component.as_str(),
            "nonchar/a\u{fffe}",
            "nonchar/\u{ffff}b",
        ] {
            let err = store
                .put(key, chunked_body(vec![1]))
                .await
                .expect_err("refused by the key rule");
            assert!(
                matches!(err, StoreError::InvalidKey(_)),
                "put {key:?}: {err}"
            );
            let err = store.get(key).await.err().expect("get refused");
            assert!(
                matches!(err, StoreError::InvalidKey(_)),
                "get {key:?}: {err}"
            );
        }
        assert!(
            list_keys(store, "").await.is_empty(),
            "nothing was written by a refused put"
        );

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
    async fn list_order_is_key_bytes() {
        let (_dir, store) = fs_store().await;
        suite::list_order_is_key_bytes(&store).await;
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

    /// Every `.put-*` entry under `root`, at any depth.
    fn temporaries_under(root: &std::path::Path) -> Vec<std::path::PathBuf> {
        let mut found = Vec::new();
        let mut stack = vec![root.to_path_buf()];
        while let Some(current) = stack.pop() {
            for entry in std::fs::read_dir(&current).expect("read_dir") {
                let entry = entry.expect("entry");
                if entry.file_type().expect("file_type").is_dir() {
                    stack.push(entry.path());
                } else if entry.file_name().to_string_lossy().starts_with(".put-") {
                    found.push(entry.path());
                }
            }
        }
        found
    }

    /// A put whose future is dropped mid-body (a caller's timeout, a
    /// cancelled job) leaves no temporary behind and the previous object
    /// untouched: the guard removes the temporary from `Drop`, as the S3
    /// store's guard aborts its upload. The put is dropped only once its
    /// temporary exists, so the test sees the guard remove a file that was
    /// there.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dropped_put_leaves_no_temporary() {
        use futures::StreamExt as _;
        let (dir, store) = fs_store().await;
        let store = std::sync::Arc::new(store);
        store.put("d/obj", chunked_body(b"old")).await.unwrap();
        // 1 MiB that is written, then a body that never ends.
        let stalled: ByteStream = Box::pin(
            futures::stream::iter([Ok(Bytes::from(patterned_bytes(1024 * 1024)))])
                .chain(futures::stream::pending()),
        );
        let put = {
            let store = store.clone();
            tokio::spawn(async move { store.put("d/obj", stalled).await })
        };
        let mut existed = Vec::new();
        for _ in 0..1000 {
            existed = temporaries_under(dir.path());
            if !existed.is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(
            existed.len(),
            1,
            "the put's temporary exists before the drop"
        );
        put.abort();
        assert!(
            put.await
                .expect_err("the put must not complete")
                .is_cancelled(),
            "the put was dropped"
        );
        let left = temporaries_under(dir.path());
        assert!(
            left.is_empty(),
            "temporaries left by the dropped put: {left:?}"
        );
        assert_eq!(read_all(store.get("d/obj").await.unwrap()).await, b"old");
        assert_eq!(list_keys(&*store, "").await, vec!["d/obj"]);
    }

    /// A `delete_prefix` whose future is dropped (a caller's timeout) stops
    /// before its next removal, a file's or a directory's: at most the one
    /// removal already past its check goes through. The walk is held at a
    /// chosen removal through the store's test hook, which signals the test
    /// and then blocks; the test drops the call while the walk is held,
    /// counts the entries (files and directories) under the prefix at once,
    /// and again after the hold has ended, and asserts the second count is
    /// at least the first minus one. The cells hold the walk at a file with
    /// a sibling file after it, at the last file of a directory and at a
    /// directory (the layout is three directories of two files, so removal
    /// 0 is the first of those and 1 the second, 2 the directory), the
    /// three places a missing check would let the walk go on from. An
    /// object put under the prefix after the drop is still there afterwards.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn dropped_delete_prefix_stops_at_its_next_entry() {
        use std::os::unix::fs::OpenOptionsExt as _;
        const HOLD: std::time::Duration = std::time::Duration::from_millis(400);
        fn count_entries(dir: &std::path::Path) -> usize {
            std::fs::read_dir(dir)
                .expect("read the prefix")
                .map(|entry| {
                    let entry = entry.expect("entry");
                    if entry.file_type().expect("file type").is_dir() {
                        1 + count_entries(&entry.path())
                    } else {
                        1
                    }
                })
                .sum()
        }
        let (dir, mut store) = fs_store().await;
        let held_at: std::sync::Arc<
            std::sync::Mutex<(u64, Option<tokio::sync::oneshot::Sender<()>>)>,
        > = std::sync::Arc::new(std::sync::Mutex::new((u64::MAX, None)));
        let hook_state = held_at.clone();
        store.set_removal_hook(std::sync::Arc::new(move |removals| {
            let sender = {
                let mut state = hook_state.lock().unwrap();
                (removals == state.0).then(|| state.1.take()).flatten()
            };
            if let Some(sender) = sender {
                let _ = sender.send(());
                std::thread::sleep(HOLD);
            }
        }));
        let store = std::sync::Arc::new(store);
        for (cell, hold_at) in [
            ("a file with a sibling file after it", 0u64),
            ("the last file of a directory", 1),
            ("a directory", 2),
        ] {
            let prefix = format!("held{hold_at}");
            private_dir(&dir.path().join(&prefix));
            for d in 0..3 {
                let sub = dir.path().join(&prefix).join(format!("d{d}"));
                private_dir(&sub);
                for f in 0..2 {
                    std::fs::OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .mode(0o600)
                        .open(sub.join(format!("f{f}")))
                        .expect("create an object");
                }
            }
            let (held_tx, held_rx) = tokio::sync::oneshot::channel();
            *held_at.lock().unwrap() = (hold_at, Some(held_tx));
            // The hook signals when the walk reaches the chosen removal;
            // ending the select drops the deletion there. The timeout is a
            // hang guard only, wide enough for a loaded host.
            tokio::time::timeout(std::time::Duration::from_secs(60), async {
                tokio::select! {
                    biased;
                    held = held_rx => held.expect("the walk reached the chosen removal"),
                    deleted = store.delete_prefix(&prefix) => {
                        panic!("{cell}: delete_prefix finished first: {deleted:?}")
                    }
                }
            })
            .await
            .expect("the walk reaches the chosen removal");
            let at_drop = count_entries(&dir.path().join(&prefix));
            // The caller believes the deletion stopped and writes again.
            let key = format!("{prefix}/d2/new");
            store
                .put(&key, chunked_body(b"new"))
                .await
                .expect("put after the dropped deletion");
            tokio::time::sleep(HOLD + std::time::Duration::from_millis(500)).await;
            let later = count_entries(&dir.path().join(&prefix));
            println!(
                "{cell}: {at_drop} entries under {prefix} at the drop, {later} afterwards (one \
                 is the new object)"
            );
            // `later` counts the new object; `at_drop` does not.
            let removed_after_drop = (at_drop + 1).saturating_sub(later);
            assert!(
                removed_after_drop <= 1,
                "{cell}: the dropped delete_prefix removed {removed_after_drop} entries after \
                 the drop"
            );
            assert!(
                store.head(&key).await.unwrap().is_some(),
                "{cell}: the dropped delete_prefix went on and removed {key}"
            );
        }
    }

    /// `validate` beside a `delete_prefix("")`, which removes temporaries
    /// and so the probe too: every `validate` succeeds. A round in which
    /// the deletion removed something (the probe is the only file under the
    /// root) is a round where `validate`'s own removal found the probe gone;
    /// the test runs until that happened in several rounds.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn validate_beside_delete_all_succeeds() {
        let (_dir, store) = fs_store().await;
        let store = std::sync::Arc::new(store);
        let (mut rounds, mut raced) = (0u32, 0u32);
        while raced < 20 && rounds < 20_000 {
            rounds += 1;
            let validate = {
                let store = store.clone();
                tokio::spawn(async move { store.validate().await })
            };
            let deleter = {
                let store = store.clone();
                tokio::spawn(async move {
                    let mut removed = 0;
                    for _ in 0..4 {
                        removed += store.delete_prefix("").await.expect("delete_prefix");
                    }
                    removed
                })
            };
            let outcome = validate.await.unwrap();
            if deleter.await.unwrap() > 0 {
                raced += 1;
            }
            outcome.unwrap_or_else(|e| panic!("round {rounds}: validate failed: {e}"));
        }
        println!("{rounds} rounds, the deletion removed the probe in {raced}");
        assert!(raced > 0, "no round removed the probe under validate");
    }

    /// A put or a `validate` dropped at any moment leaves no temporary, the
    /// moment its temporary's open is in flight on the blocking pool
    /// included: the guard is created with the file inside the blocking
    /// task, so a result nobody receives still removes it. Each round
    /// aborts a put (every fourth round a `validate`) after 0 to 400 µs, so
    /// many are dropped during their open; once the blocking pool has
    /// finished, no `.put-*` entry is left.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn puts_dropped_at_any_moment_leave_no_temporary() {
        const ROUNDS: u64 = 200;
        let (dir, store) = fs_store().await;
        let store = std::sync::Arc::new(store);
        // The directory exists, so the rounds spend their time in the open
        // and the write rather than in creating it.
        store.put("drop/seed", chunked_body(b"seed")).await.unwrap();
        let (mut aborted, mut finished) = (0u64, 0u64);
        for round in 0..ROUNDS {
            let task = {
                let store = store.clone();
                tokio::spawn(async move {
                    if round % 4 == 3 {
                        store.validate().await
                    } else {
                        let key = format!("drop/obj-{round}");
                        store
                            .put(&key, byte_stream_from(vec![1u8]))
                            .await
                            .map(|_| ())
                    }
                })
            };
            // Blocks this worker only; the task runs on another.
            std::thread::sleep(std::time::Duration::from_micros(round * 37 % 401));
            task.abort();
            match task.await {
                Err(e) if e.is_cancelled() => aborted += 1,
                Ok(outcome) => {
                    outcome.expect("a round that ran to the end succeeds");
                    finished += 1;
                }
                Err(e) => panic!("round {round}: {e}"),
            }
        }
        println!("{aborted} rounds aborted, {finished} finished");
        assert!(aborted > 0, "no round was dropped before it finished");
        // An open still in flight finishes on the blocking pool, and its
        // guard removes the file then.
        let mut left = Vec::new();
        for _ in 0..50 {
            left = temporaries_under(dir.path());
            if left.is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        assert!(
            left.is_empty(),
            "{} temporaries left by dropped rounds, {} of them puts': {:?}",
            left.len(),
            left.iter()
                .filter(|path| path.parent() != Some(dir.path()))
                .count(),
            left.iter().take(5).collect::<Vec<_>>()
        );
    }

    /// A body that panics after its first chunk unwinds through the put; the
    /// guard still removes the temporary.
    #[tokio::test]
    async fn panicking_body_leaves_no_temporary() {
        use futures::StreamExt as _;
        let (dir, store) = fs_store().await;
        let store = std::sync::Arc::new(store);
        let panicking: ByteStream = Box::pin(
            futures::stream::iter([Ok(Bytes::from(patterned_bytes(64 * 1024)))]).chain(
                futures::stream::poll_fn(
                    |_| -> std::task::Poll<Option<Result<Bytes, StoreError>>> {
                        panic!("injected body panic")
                    },
                ),
            ),
        );
        let put = {
            let store = store.clone();
            tokio::spawn(async move { store.put("p/obj", panicking).await })
        };
        let joined = put.await;
        assert!(
            joined.as_ref().is_err_and(tokio::task::JoinError::is_panic),
            "the body's panic reaches the task: {joined:?}"
        );
        let left = temporaries_under(dir.path());
        assert!(
            left.is_empty(),
            "temporaries left by the panicking put: {left:?}"
        );
        assert!(store.head("p/obj").await.unwrap().is_none());
        // A left-over temporary would be counted here as an object.
        assert_eq!(store.delete_prefix("p").await.unwrap(), 0);
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
        // 0740 and 0704 grant only a read bit, so the rule must look at the
        // whole 0o077 mask and not only at write and execute.
        for mode in [
            0o720u32, 0o702, 0o777, 0o1777, 0o750, 0o755, 0o705, 0o701, 0o740, 0o704,
        ] {
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
    /// operation descends into or through it, and the put creates nothing
    /// beneath it.
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
        // list and delete_prefix apply the same rule on their descent: from
        // the root, at the prefix directory, and on every directory between
        // the root and the prefix (a private directory below the permissive
        // one must not make a prefix under it usable).
        private_dir(&dir.path().join("acct/table"));
        std::fs::write(dir.path().join("acct/table/obj"), b"y").unwrap();
        for prefix in ["acct/table/obj", "acct/table", "acct", ""] {
            let err = store
                .list(prefix)
                .try_collect::<Vec<_>>()
                .await
                .err()
                .unwrap_or_else(|| panic!("list({prefix:?}) through a permissive directory"));
            assert!(
                matches!(err, StoreError::PermissionDenied(_)),
                "list({prefix:?}): {err}"
            );
            let err = store.delete_prefix(prefix).await.err().unwrap_or_else(|| {
                panic!("delete_prefix({prefix:?}) through a permissive directory")
            });
            assert!(
                matches!(err, StoreError::PermissionDenied(_)),
                "delete_prefix({prefix:?}): {err}"
            );
        }
        assert!(dir.path().join("acct/obj").exists(), "nothing was deleted");
        assert!(
            dir.path().join("acct/table/obj").exists(),
            "nothing was deleted"
        );
        // Once the operator fixes the mode, the same keys work.
        std::fs::set_permissions(
            dir.path().join("acct"),
            std::fs::Permissions::from_mode(0o700),
        )
        .unwrap();
        store
            .put("acct/table/obj2", chunked_body(vec![1]))
            .await
            .expect("put after chmod");
        assert_eq!(
            list_keys(&store, "acct/table").await,
            vec!["acct/table/obj", "acct/table/obj2"]
        );
        assert_eq!(store.delete_prefix("acct").await.unwrap(), 3);
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
        // A non-empty prefix too: the walk from a subdirectory does not pass
        // through the root, so the refusal has to come from the explicit
        // root re-check.
        let list = store.list("a").try_collect::<Vec<_>>().await;
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

    /// A root under a directory that group or others can write to (without
    /// the sticky bit) is refused: they could rename the root away and plant
    /// a symlink at its path. A sticky world-writable ancestor (like /tmp) is
    /// accepted.
    ///
    /// The two uid cells, an ancestor owned by a third user and a root or
    /// subdirectory owned by another user (`check_owner_only`'s uid test),
    /// need a second uid: they are the `foreign_owned_*` tests below, which
    /// chown and so run only as root, and are run as root in CI (a separate
    /// step of `.github/workflows/test.yml`).
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
        // `validate` repeats the ancestor rule, so a parent widened after
        // `open` is reported by the next validation.
        let store = FilesystemStore::open(&root).await.expect("open under 0755");
        std::fs::set_permissions(parent.path(), std::fs::Permissions::from_mode(0o777)).unwrap();
        let err = store
            .validate()
            .await
            .expect_err("validate re-checks ancestors");
        assert!(matches!(err, StoreError::PermissionDenied(_)), "{err}");
        assert!(err.to_string().contains("ancestor"), "{err}");
    }

    /// The ancestor rule walks to the filesystem root, not only to the
    /// immediate parent.
    #[tokio::test]
    async fn root_under_a_world_writable_grandparent_is_refused() {
        use std::os::unix::fs::PermissionsExt;
        let grandparent = tempfile::tempdir().expect("grandparent");
        let parent = grandparent.path().join("parent");
        std::fs::create_dir(&parent).unwrap();
        std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o755)).unwrap();
        let root = parent.join("backups");
        std::fs::create_dir(&root).unwrap();
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
        std::fs::set_permissions(grandparent.path(), std::fs::Permissions::from_mode(0o777))
            .unwrap();
        let err = FilesystemStore::open(&root)
            .await
            .err()
            .expect("world-writable grandparent refused");
        assert!(matches!(err, StoreError::PermissionDenied(_)), "{err}");
        assert!(
            err.to_string()
                .contains(&grandparent.path().display().to_string()),
            "the message names the offending ancestor: {err}"
        );
        std::fs::set_permissions(grandparent.path(), std::fs::Permissions::from_mode(0o755))
            .unwrap();
        FilesystemStore::open(&root)
            .await
            .expect("open after chmod");
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

    /// A failed put leaves no file under the key and no temporary; the
    /// directories it created stay (empty, invisible to `list`, removed by
    /// `delete_prefix` of the prefix). A component over `NAME_MAX`, which
    /// used to fail inside the directory creation walk, is now refused by
    /// the key rule before anything is created.
    #[tokio::test]
    async fn failed_put_leaves_only_empty_directories() {
        let (dir, store) = fs_store().await;
        store.put("keep/one", chunked_body(vec![1])).await.unwrap();
        let err = store
            .put("keep/new/deep/obj", failing_body(vec![1]))
            .await
            .expect_err("failed put");
        assert!(matches!(err, StoreError::Other(_)), "{err}");
        assert!(
            dir.path().join("keep/new/deep").is_dir(),
            "the created directories stay"
        );
        assert_eq!(
            std::fs::read_dir(dir.path().join("keep/new/deep"))
                .unwrap()
                .count(),
            0,
            "and are empty"
        );
        assert!(
            dir.path().join("keep/one").is_file(),
            "pre-existing content untouched"
        );
        assert_eq!(list_keys(&store, "").await, vec!["keep/one"]);

        let long = "b".repeat(256);
        let err = store
            .put(&format!("n1/{long}/obj"), chunked_body(vec![1]))
            .await
            .expect_err("over-long component refused");
        assert!(matches!(err, StoreError::InvalidKey(_)), "{err}");
        assert!(
            !dir.path().join("n1").exists(),
            "nothing is created for a key the rule refuses"
        );
        // delete_prefix removes the empty skeletons with the prefix.
        assert_eq!(store.delete_prefix("keep/new").await.unwrap(), 0);
        assert!(!dir.path().join("keep/new").exists());
    }

    /// A directory-sync failure after the rename leaves the new, complete
    /// object (the only owner-only mode cell where that sync fails is a
    /// parent without read permission, `0300`: `create_new` and `rename`
    /// need write and search, `open(dir)` for the fsync needs read).
    #[tokio::test]
    async fn post_rename_sync_failure_keeps_the_new_object() {
        use std::os::unix::fs::PermissionsExt;
        if effective_uid_is_root() {
            eprintln!("SKIP post_rename_sync_failure_keeps_the_new_object: running as root");
            return;
        }
        let (dir, store) = fs_store().await;
        store.put("k/obj", chunked_body(b"old")).await.unwrap();
        std::fs::set_permissions(dir.path().join("k"), std::fs::Permissions::from_mode(0o300))
            .unwrap();
        let err = store
            .put("k/obj", chunked_body(b"new-complete"))
            .await
            .expect_err("the directory sync fails");
        assert!(
            matches!(&err, StoreError::Io { context, .. } if context.starts_with("open directory")),
            "{err}"
        );
        std::fs::set_permissions(dir.path().join("k"), std::fs::Permissions::from_mode(0o700))
            .unwrap();
        assert_eq!(
            read_all(store.get("k/obj").await.unwrap()).await,
            b"new-complete",
            "the renamed object stays"
        );
        assert_eq!(
            std::fs::read_dir(dir.path().join("k")).unwrap().count(),
            1,
            "no temporary beside it"
        );
    }

    fn effective_uid_is_root() -> bool {
        // SAFETY: geteuid has no preconditions.
        unsafe { libc::geteuid() == 0 }
    }

    /// Names the store never writes (a backslash, a control character, a
    /// file or a directory whose name is not UTF-8) are not objects: `list`
    /// hides them, and what is under a non-UTF-8 directory, and
    /// `delete_prefix` removes them.
    #[tokio::test]
    async fn names_failing_the_key_rule_are_hidden_and_deleted() {
        use std::os::unix::ffi::OsStrExt;
        let (dir, store) = fs_store().await;
        store.put("a/obj", chunked_body(vec![1])).await.unwrap();
        std::fs::write(dir.path().join("a/back\\slash"), b"x").unwrap();
        std::fs::write(dir.path().join("a/ctl\u{1}"), b"x").unwrap();
        let a = dir.path().join("a");
        std::fs::write(a.join(std::ffi::OsStr::from_bytes(b"bad\xff")), b"x").unwrap();
        let bad_dir = a.join(std::ffi::OsStr::from_bytes(b"dir\xfe"));
        private_dir(&bad_dir);
        std::fs::write(bad_dir.join("ok"), b"x").unwrap();
        // Only the full `.put-*.tmp` pattern marks a temporary; this name is
        // an ordinary key and is listed.
        std::fs::write(dir.path().join("a/.put-nosuffix"), b"x").unwrap();
        assert_eq!(
            list_keys(&store, "").await,
            vec!["a/.put-nosuffix", "a/obj"]
        );
        assert_eq!(
            list_keys(&store, "a").await,
            vec!["a/.put-nosuffix", "a/obj"]
        );
        // obj, the backslash, the control character, .put-nosuffix, the
        // non-UTF-8 file and the file under the non-UTF-8 directory.
        assert_eq!(store.delete_prefix("a").await.unwrap(), 6);
        assert!(!dir.path().join("a").exists());
    }

    /// `delete_prefix` removes the prefix directory and leaves the
    /// directories above it.
    #[tokio::test]
    async fn delete_prefix_keeps_the_directories_above_the_prefix() {
        let (dir, store) = fs_store().await;
        store.put("a/b/obj", chunked_body(vec![1])).await.unwrap();
        assert_eq!(store.delete_prefix("a/b").await.unwrap(), 1);
        assert!(
            !dir.path().join("a/b").exists(),
            "the prefix directory is removed"
        );
        assert!(
            dir.path().join("a").is_dir(),
            "the directory above the prefix stays"
        );
    }

    /// An ancestor of the root renamed, with a symlink left at its old path
    /// (`mv a b; ln -s b a`): the root path still reaches the same tree, but
    /// its canonical form has changed. Every operation and `validate` refuse
    /// it with the same message, rather than `get`, `head`, `list` and
    /// `delete_prefix` working through the moved ancestor while `put`
    /// refuses.
    #[tokio::test]
    async fn moved_ancestor_is_refused_by_every_operation() {
        const MOVED: &str = "an ancestor of the backup root was moved or replaced by a symlink";
        let base = private_tempdir();
        let a = base.path().join("a");
        private_dir(&a);
        let root = a.join("root");
        private_dir(&root);
        let store = FilesystemStore::open(&root).await.expect("open");
        store.put("x/obj", chunked_body(vec![1])).await.unwrap();
        let b = base.path().join("b");
        std::fs::rename(&a, &b).unwrap();
        std::os::unix::fs::symlink(&b, &a).unwrap();
        let refused = |what: &str, result: Result<(), StoreError>| match result {
            Err(StoreError::PermissionDenied(msg)) if msg.contains(MOVED) => {}
            other => panic!("{what}: expected the moved-ancestor refusal, got {other:?}"),
        };
        refused(
            "put below",
            store.put("x/new", chunked_body(vec![2])).await.map(|_| ()),
        );
        refused(
            "put at the top",
            store.put("top", chunked_body(vec![2])).await.map(|_| ()),
        );
        refused("get", store.get("x/obj").await.map(|_| ()));
        refused("head", store.head("x/obj").await.map(|_| ()));
        for prefix in ["", "x", "x/obj"] {
            refused(
                &format!("list({prefix:?})"),
                store.list(prefix).try_collect::<Vec<_>>().await.map(|_| ()),
            );
            refused(
                &format!("delete_prefix({prefix:?})"),
                store.delete_prefix(prefix).await.map(|_| ()),
            );
        }
        refused("validate", store.validate().await);
        assert!(b.join("root/x/obj").is_file(), "nothing was deleted");
        assert!(!b.join("root/x/new").exists() && !b.join("root/top").exists());
        // With the ancestor back in place the same store works again.
        std::fs::remove_file(&a).unwrap();
        std::fs::rename(&b, &a).unwrap();
        assert_eq!(list_keys(&store, "").await, vec!["x/obj"]);
        store
            .validate()
            .await
            .expect("validate after the move is undone");
    }

    /// The root replaced by a symlink after `open` (the owner removed the
    /// directory and planted a link) is refused with a message that says so.
    #[tokio::test]
    async fn root_replaced_by_a_symlink_is_refused() {
        let other = private_tempdir();
        let (dir, store) = fs_store().await;
        let root = dir.path().to_path_buf();
        std::fs::remove_dir(&root).unwrap();
        std::os::unix::fs::symlink(other.path(), &root).unwrap();
        let err = store
            .put("obj", chunked_body(vec![1]))
            .await
            .expect_err("refused");
        assert!(matches!(err, StoreError::PermissionDenied(_)), "{err}");
        assert!(err.to_string().contains("is a symlink"), "{err}");
        let err = store.validate().await.expect_err("refused");
        assert!(err.to_string().contains("is a symlink"), "{err}");
        assert_eq!(std::fs::read_dir(other.path()).unwrap().count(), 0);
        // Put the directory back so TempDir's cleanup finds one.
        std::fs::remove_file(&root).unwrap();
        std::fs::create_dir(&root).unwrap();
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

    /// `list` and `delete_prefix` resolve the prefix with the same no-follow
    /// walk as `get`: a symlink in an intermediate component is refused
    /// whether it aliases a directory inside the root or points outside it,
    /// and nothing on the far side is listed or removed.
    #[tokio::test]
    async fn list_and_delete_through_an_intermediate_symlink_are_refused() {
        let outside = tempfile::tempdir().expect("outside dir");
        std::fs::create_dir(outside.path().join("sub")).unwrap();
        std::fs::write(outside.path().join("sub/file"), b"outside").unwrap();

        let (dir, store) = fs_store().await;
        store
            .put("victim/sub/obj", chunked_body(vec![1]))
            .await
            .unwrap();
        std::os::unix::fs::symlink(dir.path().join("victim"), dir.path().join("alias")).unwrap();
        std::os::unix::fs::symlink(outside.path(), dir.path().join("esc")).unwrap();

        // Prefixes one component below the link, and a prefix naming the
        // object itself through the link.
        for prefix in ["alias/sub", "alias/sub/obj", "esc/sub", "esc/sub/file"] {
            let err = store
                .list(prefix)
                .try_collect::<Vec<_>>()
                .await
                .err()
                .unwrap_or_else(|| panic!("list({prefix:?}) through a symlink refused"));
            assert!(
                matches!(err, StoreError::PermissionDenied(_)),
                "list({prefix:?}): {err}"
            );
            let err =
                store.delete_prefix(prefix).await.err().unwrap_or_else(|| {
                    panic!("delete_prefix({prefix:?}) through a symlink refused")
                });
            assert!(
                matches!(err, StoreError::PermissionDenied(_)),
                "delete_prefix({prefix:?}): {err}"
            );
        }
        assert_eq!(
            read_all(store.get("victim/sub/obj").await.unwrap()).await,
            vec![1],
            "the aliased object survives"
        );
        assert_eq!(
            std::fs::read(outside.path().join("sub/file")).unwrap(),
            b"outside",
            "nothing outside the root is touched"
        );
    }

    /// Below the prefix, `delete_prefix` removes a symlink as a link and
    /// never descends into its target, inside or outside the root.
    #[tokio::test]
    async fn delete_prefix_removes_symlinks_below_the_prefix_without_following() {
        let outside = tempfile::tempdir().expect("outside dir");
        std::fs::create_dir(outside.path().join("sub")).unwrap();
        std::fs::write(outside.path().join("sub/file"), b"outside").unwrap();

        let (dir, store) = fs_store().await;
        store.put("bk/real", chunked_body(vec![1])).await.unwrap();
        store.put("keep/obj", chunked_body(vec![2])).await.unwrap();
        std::os::unix::fs::symlink(outside.path(), dir.path().join("bk/esc")).unwrap();
        std::os::unix::fs::symlink(
            outside.path().join("sub/file"),
            dir.path().join("bk/filelink"),
        )
        .unwrap();
        std::os::unix::fs::symlink(dir.path().join("keep"), dir.path().join("bk/alias")).unwrap();

        assert_eq!(list_keys(&store, "bk").await, vec!["bk/real"]);
        // One object and three links: the count includes hidden entries, as
        // the trait documents.
        assert_eq!(store.delete_prefix("bk").await.unwrap(), 4);
        assert!(
            !dir.path().join("bk").exists(),
            "the prefix directory is gone"
        );
        assert_eq!(
            std::fs::read(outside.path().join("sub/file")).unwrap(),
            b"outside",
            "the escaping links' targets survive"
        );
        assert!(outside.path().join("sub").is_dir());
        assert_eq!(
            read_all(store.get("keep/obj").await.unwrap()).await,
            vec![2],
            "the in-root alias target survives"
        );
    }

    /// Two deleters on the same prefix do not fail each other: an entry the
    /// other one removed first counts as already gone. Repeated so the race
    /// is actually hit; the result must be the same every time.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_delete_prefix_calls_do_not_fail_each_other() {
        let (_dir, store) = fs_store().await;
        let store = std::sync::Arc::new(store);
        for round in 0..5 {
            let total: u64 = 200;
            for n in 0..total {
                store
                    .put(&format!("bk/{}/f{n}", n % 10), chunked_body(vec![1]))
                    .await
                    .unwrap();
            }
            let a = {
                let store = store.clone();
                tokio::spawn(async move { store.delete_prefix("bk").await })
            };
            let b = {
                let store = store.clone();
                tokio::spawn(async move { store.delete_prefix("bk").await })
            };
            let a = a.await.unwrap();
            let b = b.await.unwrap();
            let (a, b) = match (a, b) {
                (Ok(a), Ok(b)) => (a, b),
                other => panic!("round {round}: a concurrent delete failed: {other:?}"),
            };
            assert_eq!(a + b, total, "round {round}: every object counted once");
            assert!(list_keys(&*store, "bk").await.is_empty());
        }
    }

    /// Temporary files from an in-flight put are not objects: `list` hides
    /// them under any prefix, `delete_prefix` still removes them, and
    /// deleting everything leaves the root itself in place and usable.
    #[tokio::test]
    async fn temporaries_are_hidden_and_delete_all_keeps_the_root() {
        let (dir, store) = fs_store().await;
        store.put("a/obj", chunked_body(vec![1])).await.unwrap();
        std::fs::write(dir.path().join(".put-x.tmp"), b"t").unwrap();
        std::fs::write(dir.path().join("a/.put-y.tmp"), b"t").unwrap();

        assert_eq!(list_keys(&store, "").await, vec!["a/obj"]);
        assert_eq!(list_keys(&store, "a").await, vec!["a/obj"]);
        assert!(
            list_keys(&store, "a/.put-y.tmp").await.is_empty(),
            "a prefix naming a temporary lists nothing"
        );
        assert!(
            store.head("a/.put-y.tmp").await.unwrap().is_some(),
            "the key space still allows the name; only listings hide it"
        );

        assert_eq!(store.delete_prefix("").await.unwrap(), 3);
        assert!(dir.path().is_dir(), "the root survives delete_prefix(\"\")");
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
        store
            .put("after/obj", chunked_body(vec![2]))
            .await
            .expect("the store is usable after deleting everything");
        assert_eq!(store.delete_prefix("").await.unwrap(), 1);
    }

    /// A key that names a directory, or that passes through an object, is
    /// not an object: `get` is `NotFound` and `head` is `None`.
    #[tokio::test]
    async fn directory_and_through_object_keys_are_not_found() {
        let (_dir, store) = fs_store().await;
        store.put("a/b", chunked_body(vec![1])).await.unwrap();
        store.put("d", chunked_body(vec![2])).await.unwrap();
        for key in ["a", "d/x", "d/x/y"] {
            let err = store
                .get(key)
                .await
                .err()
                .unwrap_or_else(|| panic!("get({key:?}) is not an object"));
            assert!(matches!(err, StoreError::NotFound), "get({key:?}): {err}");
            assert!(
                store.head(key).await.unwrap().is_none(),
                "head({key:?}) is None"
            );
        }
    }

    /// A put that fails before the rename leaves the previous object under
    /// the key, complete and unchanged.
    #[tokio::test]
    async fn failed_put_keeps_the_previous_object() {
        let (dir, store) = fs_store().await;
        store
            .put("keep/obj", chunked_body(b"previous"))
            .await
            .unwrap();
        let err = store
            .put("keep/obj", failing_body(patterned_bytes(2 * 1024 * 1024)))
            .await
            .expect_err("mid-body failure surfaces");
        assert!(matches!(err, StoreError::Other(_)), "{err}");
        assert_eq!(
            read_all(store.get("keep/obj").await.unwrap()).await,
            b"previous"
        );
        assert_eq!(
            std::fs::read_dir(dir.path().join("keep")).unwrap().count(),
            1,
            "no temporary is left beside the object"
        );
    }

    /// A key that names an existing directory is refused before the body is
    /// read: the body here fails as soon as it is polled, so a store that
    /// read it first would report the injected error instead.
    #[tokio::test]
    async fn put_onto_a_directory_is_refused_before_the_body_is_read() {
        let (_dir, store) = fs_store().await;
        store.put("a/b", chunked_body(vec![1])).await.unwrap();
        let err = store
            .put("a", failing_body(Vec::<u8>::new()))
            .await
            .expect_err("refused");
        assert!(
            matches!(&err, StoreError::Io { source, .. }
                if source.kind() == std::io::ErrorKind::IsADirectory),
            "the directory refusal, not the body's error: {err}"
        );
        assert_eq!(read_all(store.get("a/b").await.unwrap()).await, vec![1]);
    }

    /// `delete_prefix` checks every directory below the prefix before it
    /// removes anything, so a refusal leaves the whole prefix in place. Many
    /// siblings make it all but certain that a delete-as-you-go walk would
    /// reach some of them before the permissive directory.
    #[tokio::test]
    async fn delete_prefix_refuses_before_deleting_anything() {
        use std::os::unix::fs::PermissionsExt;
        let (dir, store) = fs_store().await;
        for n in 0..40 {
            store
                .put(&format!("x/f{n:02}"), chunked_body(vec![1]))
                .await
                .unwrap();
            store
                .put(&format!("top{n:02}"), chunked_body(vec![1]))
                .await
                .unwrap();
        }
        store
            .put("x/deep/bad/obj", chunked_body(vec![1]))
            .await
            .unwrap();
        std::fs::set_permissions(
            dir.path().join("x/deep/bad"),
            std::fs::Permissions::from_mode(0o777),
        )
        .unwrap();
        for prefix in ["x", "x/deep", ""] {
            let err = store
                .delete_prefix(prefix)
                .await
                .expect_err("a permissive directory below the prefix");
            assert!(
                matches!(&err, StoreError::PermissionDenied(msg) if msg.contains("x/deep/bad")),
                "delete_prefix({prefix:?}): {err}"
            );
        }
        let count = |sub: &str| std::fs::read_dir(dir.path().join(sub)).unwrap().count();
        assert_eq!(count("x"), 41, "x/f00..x/f39 and x/deep remain");
        assert_eq!(count(""), 41, "top00..top39 and x remain");
        assert!(dir.path().join("x/deep/bad/obj").is_file());
        // Once the mode is fixed, the same call succeeds.
        std::fs::set_permissions(
            dir.path().join("x/deep/bad"),
            std::fs::Permissions::from_mode(0o700),
        )
        .unwrap();
        assert_eq!(store.delete_prefix("").await.unwrap(), 81);
    }

    /// The `RLIMIT_FSIZE` tests run their body in a child process, because
    /// the limit applies to the whole process; returns `true` in the child.
    fn run_in_fsize_child(name: &str) -> bool {
        if is_child(name) {
            return true;
        }
        run_in_child(name, &[], &[]);
        false
    }

    /// Lower this process's soft file size limit and ignore `SIGXFSZ`, so a
    /// write past the limit fails with `EFBIG` instead of killing the
    /// process. The hard limit is kept.
    fn limit_file_size(bytes: u64) {
        // SAFETY: plain libc calls on a valid, initialised struct; ignoring
        // SIGXFSZ has no other effect on this test process.
        unsafe {
            libc::signal(libc::SIGXFSZ, libc::SIG_IGN);
            let mut limit: libc::rlimit = std::mem::zeroed();
            assert_eq!(libc::getrlimit(libc::RLIMIT_FSIZE, &raw mut limit), 0);
            limit.rlim_cur = bytes as libc::rlim_t;
            assert_eq!(
                libc::setrlimit(libc::RLIMIT_FSIZE, &raw const limit),
                0,
                "setrlimit: {}",
                std::io::Error::last_os_error()
            );
        }
    }

    fn is_efbig(err: &StoreError) -> bool {
        matches!(err, StoreError::Io { source, .. } if source.raw_os_error() == Some(libc::EFBIG))
    }

    /// A full disk, without root or a mount: past `RLIMIT_FSIZE` a write
    /// fails with `EFBIG`, as on a full filesystem with `ENOSPC`. The body is
    /// one chunk, so the failing write is the last one: tokio reports its
    /// error only to the next write or flush on the file, and `sync_all`
    /// waits for that write but does not return its error, so without a
    /// flush before the drop a short file would be renamed into place. The
    /// put must fail and the previous object must survive.
    #[tokio::test]
    async fn full_disk_put_fails_and_keeps_the_previous_object() {
        if !run_in_fsize_child("filesystem::full_disk_put_fails_and_keeps_the_previous_object") {
            return;
        }
        let (dir, store) = fs_store().await;
        limit_file_size(64 * 1024);
        store
            .put("k/obj", chunked_body(b"previous"))
            .await
            .expect("a small put fits under the limit");
        let err = store
            .put("k/obj", byte_stream_from(vec![7u8; 256 * 1024]))
            .await
            .expect_err("a body past the limit fails");
        assert!(is_efbig(&err), "{err}");
        // The same with a body of several chunks, where a later write
        // reports the failure.
        let err = store
            .put("k/obj", chunked_body(vec![8u8; 3 * 1024 * 1024]))
            .await
            .expect_err("a body past the limit fails");
        assert!(is_efbig(&err), "{err}");
        assert_eq!(
            read_all(store.get("k/obj").await.unwrap()).await,
            b"previous",
            "the previous object survives"
        );
        assert_eq!(
            std::fs::read_dir(dir.path().join("k")).unwrap().count(),
            1,
            "no temporary is left beside it"
        );
    }

    /// `validate`'s probe write has the same trap: past the limit the probe
    /// write fails and validation must report it.
    #[tokio::test]
    async fn full_disk_validate_fails() {
        if !run_in_fsize_child("filesystem::full_disk_validate_fails") {
            return;
        }
        let (dir, store) = fs_store().await;
        limit_file_size(0);
        let err = store.validate().await.expect_err("the probe write fails");
        assert!(is_efbig(&err), "{err}");
        assert_eq!(
            std::fs::read_dir(dir.path()).unwrap().count(),
            0,
            "the probe is removed"
        );
    }

    /// The uid cells of the owner-only rule need a second user; these tests
    /// chown to uid 65534 and so run only as root (CI runs them in a
    /// separate step). Returns `false` with a printed reason otherwise, and
    /// panics instead when `EXTENDDB_TEST_REQUIRE_ROOT` is set (the CI root
    /// step sets it, so a run that is not root after all fails rather than
    /// skips).
    fn running_as_root(name: &str) -> bool {
        use std::io::Write;
        if effective_uid_is_root() {
            return true;
        }
        assert!(
            std::env::var_os("EXTENDDB_TEST_REQUIRE_ROOT").is_none(),
            "{name}: EXTENDDB_TEST_REQUIRE_ROOT is set but the suite is not running as root; \
             the uid cells must run in this environment"
        );
        let _ = writeln!(
            std::io::stderr(),
            "SKIP {name}: needs root to chown to another uid (run as root in CI)"
        );
        false
    }

    const FOREIGN_UID: u32 = 65534;

    fn chown(path: &std::path::Path, uid: u32) {
        std::os::unix::fs::chown(path, Some(uid), Some(uid))
            .unwrap_or_else(|e| panic!("chown {}: {e}", path.display()));
    }

    fn assert_foreign_owner(result: Result<impl std::fmt::Debug, StoreError>, what: &str) {
        match result {
            Err(StoreError::PermissionDenied(msg)) if msg.contains("owned by uid 65534") => {}
            other => panic!("{what}: expected a refusal naming uid 65534, got {other:?}"),
        }
    }

    /// A root owned by another user: `open` and `validate` refuse it, and so
    /// does every operation of a store opened before the chown.
    #[tokio::test]
    async fn foreign_owned_root_is_refused() {
        if !running_as_root("foreign_owned_root_is_refused") {
            return;
        }
        let (dir, store) = fs_store().await;
        store.put("a/obj", chunked_body(vec![1])).await.unwrap();
        chown(dir.path(), FOREIGN_UID);
        assert_foreign_owner(FilesystemStore::open(dir.path()).await.map(|_| ()), "open");
        assert_foreign_owner(store.validate().await, "validate");
        assert_foreign_owner(store.put("b/obj", chunked_body(vec![1])).await, "put");
        assert_foreign_owner(store.get("a/obj").await.map(|_| ()), "get");
        assert_foreign_owner(store.head("a/obj").await, "head");
        for prefix in ["", "a"] {
            assert_foreign_owner(
                store.list(prefix).try_collect::<Vec<_>>().await,
                &format!("list({prefix:?})"),
            );
            assert_foreign_owner(
                store.delete_prefix(prefix).await,
                &format!("delete_prefix({prefix:?})"),
            );
        }
        assert!(dir.path().join("a/obj").is_file(), "nothing was deleted");
        assert!(!dir.path().join("b").exists(), "nothing was created");
        chown(dir.path(), 0);
        assert_eq!(store.delete_prefix("").await.unwrap(), 1);
    }

    /// A subdirectory owned by another user, at the prefix and above it: put,
    /// get, head, list and `delete_prefix` through it are refused and
    /// nothing is created or deleted. `validate` checks only the root and
    /// its ancestors, so it passes.
    #[tokio::test]
    async fn foreign_owned_subdirectory_is_refused() {
        if !running_as_root("foreign_owned_subdirectory_is_refused") {
            return;
        }
        let (dir, store) = fs_store().await;
        store
            .put("acct/table/obj", chunked_body(vec![1]))
            .await
            .unwrap();
        store.put("other/obj", chunked_body(vec![2])).await.unwrap();
        chown(&dir.path().join("acct"), FOREIGN_UID);
        assert_foreign_owner(
            store.put("acct/table/obj2", chunked_body(vec![1])).await,
            "put below",
        );
        assert_foreign_owner(store.put("acct/new", chunked_body(vec![1])).await, "put");
        assert_foreign_owner(store.get("acct/table/obj").await.map(|_| ()), "get");
        assert_foreign_owner(store.head("acct/table/obj").await, "head");
        for prefix in ["", "acct", "acct/table", "acct/table/obj"] {
            assert_foreign_owner(
                store.list(prefix).try_collect::<Vec<_>>().await,
                &format!("list({prefix:?})"),
            );
            assert_foreign_owner(
                store.delete_prefix(prefix).await,
                &format!("delete_prefix({prefix:?})"),
            );
        }
        assert!(dir.path().join("acct/table/obj").is_file());
        assert!(
            dir.path().join("other/obj").is_file(),
            "delete_prefix(\"\") deleted nothing"
        );
        assert!(!dir.path().join("acct/new").exists());
        assert!(!dir.path().join("acct/table/obj2").exists());
        store
            .validate()
            .await
            .expect("validate checks the root only");
        assert_eq!(list_keys(&store, "other").await, vec!["other/obj"]);
        chown(&dir.path().join("acct"), 0);
        assert_eq!(store.delete_prefix("").await.unwrap(), 2);
    }

    /// An ancestor of the root owned by a third user (neither the store's
    /// user nor root) could rename the root away: `open` and `validate`
    /// refuse it. Operations do not repeat the ancestor rule.
    #[tokio::test]
    async fn foreign_owned_ancestor_is_refused() {
        if !running_as_root("foreign_owned_ancestor_is_refused") {
            return;
        }
        let parent = tempfile::tempdir().expect("parent");
        let root = parent.path().join("backups");
        private_dir(&root);
        let store = FilesystemStore::open(&root).await.expect("open");
        chown(parent.path(), FOREIGN_UID);
        let err = FilesystemStore::open(&root)
            .await
            .err()
            .expect("an ancestor owned by uid 65534");
        assert!(
            err.to_string().contains("ancestor") && err.to_string().contains("owned by uid 65534"),
            "{err}"
        );
        let err = store
            .validate()
            .await
            .expect_err("validate re-checks ancestors");
        assert!(
            matches!(&err, StoreError::PermissionDenied(msg)
                if msg.contains("ancestor") && msg.contains("owned by uid 65534")),
            "{err}"
        );
        store
            .put("a/obj", chunked_body(vec![1]))
            .await
            .expect("operations check the root, not its ancestors");
        chown(parent.path(), 0);
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

    /// The endpoint for a test that puts one of the suite's TCP proxies
    /// (`delaying_proxy`, `swallowing_proxy`, `stalling_proxy`) in front of
    /// it. They read request lines, which TLS hides, and connect upstream
    /// in plain TCP, so they need an `http://` endpoint; against an
    /// `https://` one (Amazon S3) the test is skipped with a printed reason.
    /// `EXTENDDB_TEST_S3_REQUIRED` does not turn this skip into a failure:
    /// it is the switch for a missing endpoint, as `EXTENDDB_TEST_REQUIRE_ROOT`
    /// is for the root-only tests, and here an endpoint is set.
    fn http_endpoint_or_skip(test: &str) -> Option<String> {
        use std::io::Write;
        let endpoint = endpoint_or_skip()?;
        if endpoint.starts_with("http://") {
            return Some(endpoint);
        }
        let _ = writeln!(
            std::io::stderr(),
            "skipping {test}: it puts a TCP proxy in front of the S3 endpoint, which needs an \
             http:// EXTENDDB_TEST_S3_ENDPOINT"
        );
        None
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
            allow_insecure_endpoint: false,
            force_path_style: true,
            upload_part_size_bytes: PART_SIZE,
            max_concurrent_uploads: 4,
        }
    }

    /// A unique prefix per test so runs never collide.
    fn unique_prefix() -> String {
        format!("conformance-{}/", uuid::Uuid::new_v4())
    }

    /// Run the test `name` in a child whose environment carries the suite's
    /// own credentials and region and no other credential source (no session
    /// token, profile or IMDS), for the S3 tests that send a request without
    /// an endpoint: the SDK resolves credentials before it builds the
    /// request, so without these the test would fail on "no providers in
    /// chain provided credentials" on a machine without AWS credentials, and
    /// pass on one with real credentials in `~/.aws` by using them. Like
    /// every child, it starts without the parent's proxy variables.
    fn run_with_test_credentials(name: &str) {
        run_with_test_credentials_and(name, &[]);
    }

    /// `run_with_test_credentials` with `extra` added to the child's
    /// environment; returns the child's standard output.
    fn run_with_test_credentials_and(name: &str, extra: &[(&str, &std::ffi::OsStr)]) -> String {
        let mut set: Vec<(&str, &std::ffi::OsStr)> = vec![
            ("AWS_ACCESS_KEY_ID", std::ffi::OsStr::new("testing")),
            ("AWS_SECRET_ACCESS_KEY", std::ffi::OsStr::new("testing")),
            ("AWS_REGION", std::ffi::OsStr::new("us-east-1")),
            ("AWS_EC2_METADATA_DISABLED", std::ffi::OsStr::new("true")),
        ];
        set.extend_from_slice(extra);
        run_in_child(
            name,
            &set,
            &[
                "AWS_SESSION_TOKEN",
                "AWS_PROFILE",
                "AWS_WEB_IDENTITY_TOKEN_FILE",
                "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
                "AWS_CONTAINER_CREDENTIALS_FULL_URI",
            ],
        )
    }

    /// Build a store against the local endpoint, or `None` (with a printed
    /// reason) when the environment does not provide one.
    async fn store() -> Option<S3Store> {
        let config = test_config(endpoint_or_skip()?, test_bucket(), unique_prefix());
        let store = S3Store::open(&config).await.expect("S3Store::open");
        ensure_bucket(&config).await;
        Some(store)
    }

    /// Make sure the suite bucket exists, once per test process, with a raw
    /// client so the check stays independent of the store under test.
    /// `HeadBucket` first and `CreateBucket` only when it is missing: Amazon
    /// S3 answers parallel `CreateBucket` calls on an existing bucket with
    /// `409 OperationAborted`, which failed most of the suite there.
    async fn ensure_bucket(config: &S3StoreConfig) {
        static READY: tokio::sync::OnceCell<()> = tokio::sync::OnceCell::const_new();
        assert_eq!(
            config.bucket,
            test_bucket(),
            "ensure_bucket prepares the suite bucket only"
        );
        READY.get_or_init(|| create_bucket_if_missing(config)).await;
    }

    async fn create_bucket_if_missing(config: &S3StoreConfig) {
        use aws_sdk_s3::error::ProvideErrorMetadata as _;
        const ATTEMPTS: u32 = 5;
        let client = raw_client(config).await;
        match client.head_bucket().bucket(&config.bucket).send().await {
            Ok(_) => return,
            Err(err)
                if err.as_service_error().is_some_and(
                    aws_sdk_s3::operation::head_bucket::HeadBucketError::is_not_found,
                ) => {}
            Err(err) => panic!(
                "head_bucket {} failed: {}",
                config.bucket,
                aws_sdk_s3::error::DisplayErrorContext(&err)
            ),
        }
        for attempt in 1..=ATTEMPTS {
            let mut request = client.create_bucket().bucket(&config.bucket);
            // S3 requires a location constraint for every region but
            // us-east-1, and rejects one that names us-east-1.
            if let Some(region) = config.region.as_deref().filter(|r| *r != "us-east-1") {
                use aws_sdk_s3::types::{BucketLocationConstraint, CreateBucketConfiguration};
                request = request.create_bucket_configuration(
                    CreateBucketConfiguration::builder()
                        .location_constraint(BucketLocationConstraint::from(region))
                        .build(),
                );
            }
            let Err(err) = request.send().await else {
                return;
            };
            let service = err.as_service_error();
            if service
                .is_some_and(|e| e.is_bucket_already_owned_by_you() || e.is_bucket_already_exists())
            {
                return;
            }
            // A conflicting operation on the bucket is in progress (another
            // process creating it); retry after a pause.
            if service.and_then(|e| e.code()) == Some("OperationAborted") && attempt < ATTEMPTS {
                tokio::time::sleep(std::time::Duration::from_millis(500 * u64::from(attempt)))
                    .await;
                continue;
            }
            panic!(
                "create_bucket {} failed: {}",
                config.bucket,
                aws_sdk_s3::error::DisplayErrorContext(&err)
            );
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
        let mut builder = aws_sdk_s3::config::Builder::from(&shared).force_path_style(true);
        // As the store does for an `http://` endpoint: a proxy variable in
        // the environment must not carry the suite's own requests.
        if config.endpoint.as_deref().is_some_and(|e| {
            e.get(..7)
                .is_some_and(|s| s.eq_ignore_ascii_case("http://"))
        }) {
            builder = builder.http_client(S3Store::http_client_without_proxy());
        }
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
    async fn list_order_is_key_bytes() {
        let Some(store) = store().await else { return };
        suite::list_order_is_key_bytes(&store).await;
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

    /// Counts `CreateMultipartUpload` calls, so a test about the abort path
    /// can prove a multipart upload was actually started.
    #[derive(Debug, Default)]
    struct CountCreateMultipartUpload(std::sync::Arc<std::sync::atomic::AtomicUsize>);

    impl aws_sdk_s3::config::Intercept for CountCreateMultipartUpload {
        fn name(&self) -> &'static str {
            "CountCreateMultipartUpload"
        }

        fn read_before_execution(
            &self,
            context: &aws_sdk_s3::config::interceptors::BeforeSerializationInterceptorContextRef<
                '_,
            >,
            _cfg: &mut aws_sdk_s3::config::ConfigBag,
        ) -> Result<(), aws_sdk_s3::error::BoxError> {
            let is_create = context
                .input()
                .downcast_ref::<aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadInput>()
                .is_some();
            if is_create {
                self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            Ok(())
        }
    }

    /// A body that fails after more than one full part has been read: the
    /// first part is already uploading when the error arrives, so the put
    /// has a multipart upload to abort. (A body that fails inside the second
    /// part is refused before `CreateMultipartUpload`, which is the
    /// filesystem-friendly shape `suite::failed_put_leaves_nothing` uses.)
    #[tokio::test]
    async fn failed_put_aborts_the_multipart_upload() {
        let Some(endpoint) = endpoint_or_skip() else {
            return;
        };
        let prefix = unique_prefix();
        let config = test_config(endpoint, test_bucket(), prefix.clone());
        let creates = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let store =
            S3Store::open_with_interceptor(&config, CountCreateMultipartUpload(creates.clone()))
                .await
                .expect("S3Store::open_with_interceptor");
        ensure_bucket(&config).await;

        let good = patterned_bytes(usize::try_from(PART_SIZE).unwrap() * 2 + 4096);
        let err = store
            .put("aborted/upload.bin", failing_body(good))
            .await
            .expect_err("mid-body failure surfaces");
        assert!(
            matches!(&err, StoreError::Other(msg) if msg.contains("injected")),
            "the injected error must propagate, got: {err}"
        );
        assert_eq!(
            creates.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the body was large enough to start a multipart upload"
        );
        assert!(
            store.head("aborted/upload.bin").await.unwrap().is_none(),
            "a failed put must leave nothing under the key"
        );
        assert!(list_keys(&store, "aborted").await.is_empty());

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
    async fn validate_passes_on_an_existing_bucket() {
        let Some(store) = store().await else { return };
        store.validate().await.expect("existing bucket validates");
    }

    /// The body-failure shape shared with the filesystem store: the error
    /// arrives inside the second part, before any multipart upload exists.
    #[tokio::test]
    async fn failed_put_leaves_nothing() {
        let Some(store) = store().await else { return };
        suite::failed_put_leaves_nothing(&store).await;
    }

    /// Rewrites every response to a chosen status (plus optional header)
    /// before the SDK deserializes it. moto never answers `ListObjectsV2`
    /// with 301 or 403, so the two `validate` branches that explain those are
    /// pinned through this seam.
    #[derive(Debug)]
    struct RewriteStatus {
        status: u16,
        header: Option<(&'static str, &'static str)>,
        body: Option<&'static str>,
    }

    impl aws_sdk_s3::config::Intercept for RewriteStatus {
        fn name(&self) -> &'static str {
            "RewriteStatus"
        }

        fn modify_before_deserialization(
            &self,
            context: &mut aws_sdk_s3::config::interceptors::BeforeDeserializationInterceptorContextMut<'_>,
            _runtime_components: &aws_sdk_s3::config::RuntimeComponents,
            _cfg: &mut aws_sdk_s3::config::ConfigBag,
        ) -> Result<(), aws_sdk_s3::error::BoxError> {
            let response = context.response_mut();
            *response.status_mut() = TryFrom::try_from(self.status)?;
            if let Some((name, value)) = self.header {
                response.headers_mut().insert(name, value);
            }
            if let Some(body) = self.body {
                *response.body_mut() = aws_sdk_s3::primitives::SdkBody::from(body);
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn validate_names_the_region_of_a_redirected_bucket() {
        let Some(endpoint) = endpoint_or_skip() else {
            return;
        };
        let config = test_config(endpoint, test_bucket(), unique_prefix());
        ensure_bucket(&config).await;
        let store = S3Store::open_with_interceptor(
            &config,
            RewriteStatus {
                status: 301,
                header: Some(("x-amz-bucket-region", "eu-west-1")),
                body: None,
            },
        )
        .await
        .expect("S3Store::open_with_interceptor");
        let err = store.validate().await.expect_err("301 fails validation");
        assert!(matches!(err, StoreError::Other(_)), "{err}");
        assert!(
            err.to_string().contains(&config.bucket)
                && err.to_string().contains("eu-west-1")
                && err.to_string().contains("not the configured region"),
            "the message names the bucket and its region: {err}"
        );
    }

    /// A denied `validate` names S3's error code, so an operator can tell a
    /// wrong key from a missing bucket policy. The rewrite supplies the body
    /// S3 would send with the 403.
    #[tokio::test]
    async fn validate_reports_a_denied_bucket_as_permission_denied() {
        let Some(endpoint) = endpoint_or_skip() else {
            return;
        };
        let config = test_config(endpoint, test_bucket(), unique_prefix());
        ensure_bucket(&config).await;
        let store = S3Store::open_with_interceptor(
            &config,
            RewriteStatus {
                status: 403,
                header: None,
                body: Some(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>InvalidAccessKeyId</Code>\
                     <Message>The AWS Access Key Id you provided does not exist in our records.</Message>\
                     <RequestId>r</RequestId></Error>",
                ),
            },
        )
        .await
        .expect("S3Store::open_with_interceptor");
        let err = store.validate().await.expect_err("403 fails validation");
        assert!(matches!(err, StoreError::PermissionDenied(_)), "{err}");
        assert!(
            err.to_string().contains(&config.bucket)
                && err.to_string().contains("denied")
                && err.to_string().contains("InvalidAccessKeyId"),
            "the message names the bucket, says access is denied, and keeps the code: {err}"
        );
    }

    /// Misconfigurations that never reach S3 (no credentials, an unreachable
    /// endpoint) are reported with their cause, not as a bare "dispatch
    /// failure". Runs in a child with the suite's own credentials
    /// (`run_with_test_credentials`), so the failure it sees is the refused
    /// connection and not the parent environment's missing credentials.
    #[tokio::test]
    async fn validate_names_the_cause_of_a_transport_failure() {
        const NAME: &str = "s3::validate_names_the_cause_of_a_transport_failure";
        if !is_child(NAME) {
            run_with_test_credentials(NAME);
            return;
        }
        let config = test_config(
            "http://127.0.0.1:9".to_owned(),
            "unused".to_owned(),
            String::new(),
        );
        let store = S3Store::open(&config).await.expect("S3Store::open");
        let err = store
            .validate()
            .await
            .expect_err("nothing listens on port 9");
        let text = err.to_string();
        assert!(text.contains("unused"), "names the bucket: {text}");
        assert!(
            text.contains("refused") || text.contains("connect"),
            "names the connection failure, not only the category: {text}"
        );
    }

    /// A put whose future is dropped mid-upload (a cancelled job) aborts its
    /// multipart upload from the drop guard, and `delete_prefix` aborts any
    /// upload a crashed process left, so no parts accrue storage.
    #[tokio::test]
    async fn dropped_put_and_delete_prefix_abort_multipart_uploads() {
        use futures::StreamExt as _;
        let Some(endpoint) = endpoint_or_skip() else {
            return;
        };
        let prefix = unique_prefix();
        let config = test_config(endpoint, test_bucket(), prefix.clone());
        let store = S3Store::open(&config).await.expect("S3Store::open");
        ensure_bucket(&config).await;
        let client = raw_client(&config).await;
        let list_uploads = || async {
            client
                .list_multipart_uploads()
                .bucket(&config.bucket)
                .prefix(&prefix)
                .send()
                .await
                .expect("list_multipart_uploads")
                .uploads()
                .len()
        };

        // Two full parts then a body that never ends: the upload is open and
        // the put is stuck when the timeout drops it.
        let stalled: ByteStream = Box::pin(
            futures::stream::iter(
                patterned_bytes(usize::try_from(PART_SIZE).unwrap() * 2)
                    .chunks(1024 * 1024)
                    .map(|c| Ok(Bytes::copy_from_slice(c)))
                    .collect::<Vec<_>>(),
            )
            .chain(futures::stream::pending()),
        );
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            store.put("stalled", stalled),
        )
        .await;
        assert!(result.is_err(), "the put must not complete");
        // The guard spawned the abort; let it run.
        for _ in 0..50 {
            if list_uploads().await == 0 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        assert_eq!(
            list_uploads().await,
            0,
            "the dropped put's upload was aborted"
        );

        // An upload nobody owns (a crashed process): delete_prefix sweeps it.
        client
            .create_multipart_upload()
            .bucket(&config.bucket)
            .key(format!("{prefix}orphan/part.bin"))
            .send()
            .await
            .expect("create orphan upload");
        assert_eq!(list_uploads().await, 1);
        assert_eq!(store.delete_prefix("orphan").await.unwrap(), 0);
        assert_eq!(list_uploads().await, 0, "delete_prefix aborted the orphan");
    }

    /// Listing and deleting paginate: more than one `ListObjectsV2` page
    /// (1000 keys) under a prefix, with a string-prefix sibling that must
    /// stay untouched.
    #[tokio::test]
    async fn list_and_delete_paginate_past_one_page() {
        use futures::StreamExt as _;
        let Some(store) = store().await else { return };
        let total = 1005u32;
        let puts = futures::stream::iter(
            (0..total).map(|n| store.put(format!("pg/x/{n:05}").leak(), chunked_body(vec![1]))),
        )
        .buffer_unordered(16)
        .try_collect::<Vec<_>>()
        .await
        .expect("puts");
        assert_eq!(puts.len(), total as usize);
        for n in 0..3 {
            store
                .put(&format!("pg/xy/{n}"), chunked_body(vec![2]))
                .await
                .unwrap();
        }
        let keys = list_keys(&store, "pg/x").await;
        assert_eq!(keys.len(), total as usize, "every key across pages");
        assert!(keys.windows(2).all(|w| w[0] < w[1]), "in key order");
        assert_eq!(store.delete_prefix("pg/x").await.unwrap(), u64::from(total));
        assert!(list_keys(&store, "pg/x").await.is_empty());
        assert_eq!(
            list_keys(&store, "pg/xy").await.len(),
            3,
            "sibling untouched"
        );
    }

    /// Rewrites `DeleteObjects` responses to report the request's first key
    /// denied; the store surfaces it as a denial naming the store-side key.
    /// The deserialization hook cannot see the input, so the key is taken
    /// from the `DeleteObjectsInput` before execution and remembered (the
    /// client issues one request at a time in this test). moto has no IAM,
    /// so it never produces a per-key error itself.
    #[derive(Debug, Default)]
    struct DenyOneDelete(std::sync::Mutex<Option<String>>);

    impl aws_sdk_s3::config::Intercept for DenyOneDelete {
        fn name(&self) -> &'static str {
            "DenyOneDelete"
        }

        fn read_before_execution(
            &self,
            context: &aws_sdk_s3::config::interceptors::BeforeSerializationInterceptorContextRef<
                '_,
            >,
            _cfg: &mut aws_sdk_s3::config::ConfigBag,
        ) -> Result<(), aws_sdk_s3::error::BoxError> {
            let first_key = context
                .input()
                .downcast_ref::<aws_sdk_s3::operation::delete_objects::DeleteObjectsInput>()
                .and_then(|input| input.delete())
                .and_then(|delete| delete.objects().first())
                .map(|object| object.key().to_owned());
            *self.0.lock().unwrap() = first_key;
            Ok(())
        }

        fn modify_before_deserialization(
            &self,
            context: &mut aws_sdk_s3::config::interceptors::BeforeDeserializationInterceptorContextMut<'_>,
            _runtime_components: &aws_sdk_s3::config::RuntimeComponents,
            _cfg: &mut aws_sdk_s3::config::ConfigBag,
        ) -> Result<(), aws_sdk_s3::error::BoxError> {
            if let Some(key) = self.0.lock().unwrap().as_deref() {
                let body = format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
                     <DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
                     <Error><Key>{key}</Key><Code>AccessDenied</Code>\
                     <Message>Access Denied</Message></Error></DeleteResult>"
                );
                *context.response_mut().body_mut() = aws_sdk_s3::primitives::SdkBody::from(body);
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn per_key_delete_errors_are_reported() {
        let Some(endpoint) = endpoint_or_skip() else {
            return;
        };
        let prefix = unique_prefix();
        let config = test_config(endpoint, test_bucket(), prefix.clone());
        ensure_bucket(&config).await;
        let store = S3Store::open_with_interceptor(&config, DenyOneDelete::default())
            .await
            .expect("S3Store::open_with_interceptor");
        store.put("deny/k1", chunked_body(vec![1])).await.unwrap();
        let err = store.delete_prefix("deny").await.expect_err("denied key");
        assert!(matches!(err, StoreError::PermissionDenied(_)), "{err}");
        let text = err.to_string();
        assert!(
            text.contains("deny/k1") && !text.contains(&format!("{prefix}deny")),
            "names the store-side key: {err}"
        );
        // Clean up for real.
        let plain = S3Store::open(&config).await.unwrap();
        plain.delete_prefix("deny").await.unwrap();
    }

    /// Objects this store never wrote and cannot address (console folder
    /// markers, a backslash in the name) are hidden from `list` and still
    /// removed by `delete_prefix`.
    #[tokio::test]
    async fn foreign_keys_are_hidden_and_deleted() {
        let Some(endpoint) = endpoint_or_skip() else {
            return;
        };
        let prefix = unique_prefix();
        let config = test_config(endpoint, test_bucket(), prefix.clone());
        let store = S3Store::open(&config).await.expect("S3Store::open");
        ensure_bucket(&config).await;
        let client = raw_client(&config).await;
        // (A control character would be the third case, but S3's XML listing
        // cannot carry one without `encoding-type=url`, and moto answers 500.)
        for raw in ["acct/", "acct/back\\slash"] {
            client
                .put_object()
                .bucket(&config.bucket)
                .key(format!("{prefix}{raw}"))
                .body(Vec::new().into())
                .send()
                .await
                .expect("plant foreign object");
        }
        store.put("acct/obj", chunked_body(vec![1])).await.unwrap();
        assert_eq!(list_keys(&store, "").await, vec!["acct/obj"]);
        assert_eq!(list_keys(&store, "acct").await, vec!["acct/obj"]);
        assert_eq!(store.delete_prefix("acct").await.unwrap(), 3);
        assert!(list_keys(&store, "").await.is_empty());
    }

    /// A store prefix of exactly `len` bytes made of 100-byte components, so
    /// it passes the key rule's 255-byte component limit.
    fn long_prefix(len: usize) -> String {
        let mut prefix: String = format!("{}/", "p".repeat(100))
            .chars()
            .cycle()
            .take(len)
            .collect();
        if prefix.ends_with('/') {
            prefix.pop();
            prefix.push('p');
        }
        assert_eq!(prefix.len(), len);
        prefix
    }

    /// `open` refuses a store prefix so long that no backup key could fit
    /// under it, instead of accepting the configuration and failing the
    /// first put.
    #[tokio::test]
    async fn over_long_store_prefix_is_refused_at_open() {
        let long = long_prefix(extenddb_backup_store::MAX_STORE_PREFIX_LEN);
        let config = test_config("http://127.0.0.1:9".to_owned(), "unused".to_owned(), long);
        let err = S3Store::open(&config).await.err().expect("refused");
        assert!(
            err.to_string()
                .contains("prefix is 513 bytes; the maximum is 512"),
            "refused for its length: {err}"
        );
        let fits = long_prefix(extenddb_backup_store::MAX_STORE_PREFIX_LEN - 1);
        S3Store::open(&test_config(
            "http://127.0.0.1:9".to_owned(),
            "unused".to_owned(),
            fits,
        ))
        .await
        .expect("one under the limit opens");
    }

    /// A bucket that has gone away is not a missing object: `get`, `list`
    /// and `put` report the bucket rather than `NotFound` or an empty
    /// listing. `head` cannot tell: S3 answers `HeadObject` with a bare 404
    /// either way, so it reports the key absent and `validate` is the check
    /// that names a missing bucket.
    #[tokio::test]
    async fn missing_bucket_is_not_a_missing_object() {
        let Some(endpoint) = endpoint_or_skip() else {
            return;
        };
        let missing = format!("extenddb-no-such-bucket-{}", uuid::Uuid::new_v4());
        let config = test_config(endpoint, missing.clone(), String::new());
        let store = S3Store::open(&config).await.expect("S3Store::open");
        let err = store.get("some/key").await.err().expect("get fails");
        assert!(
            matches!(&err, StoreError::Other(msg) if msg.contains("NoSuchBucket")),
            "get on a missing bucket must not be NotFound: {err}"
        );
        let err = store
            .list("some")
            .try_collect::<Vec<_>>()
            .await
            .expect_err("list fails");
        assert!(
            matches!(&err, StoreError::Other(msg) if msg.contains("NoSuchBucket")),
            "list on a missing bucket must not be empty: {err}"
        );
        let err = store
            .put("some/key", chunked_body(vec![1]))
            .await
            .expect_err("put fails");
        assert!(
            matches!(&err, StoreError::Other(msg) if msg.contains("NoSuchBucket")),
            "put on a missing bucket must name the bucket problem: {err}"
        );
    }

    /// S3 bounds the whole object key, store prefix included, so a key that
    /// passes the shared rule can still be too long under a long prefix. The
    /// store refuses it as an invalid key before any request is sent; no
    /// endpoint is needed.
    #[tokio::test]
    async fn key_too_long_under_the_store_prefix_is_invalid() {
        // The longest prefix `open` accepts; a key of 512 bytes fits the
        // shared rule but not under it.
        let config = test_config(
            "http://127.0.0.1:9".to_owned(),
            "unused".to_owned(),
            long_prefix(extenddb_backup_store::MAX_STORE_PREFIX_LEN - 1),
        );
        let store = S3Store::open(&config).await.expect("S3Store::open");
        // 512-byte normalized prefix plus a 513-byte key: valid alone (no
        // component over 255 bytes), 1025 bytes together.
        let key = format!("{0}/{0}/x", "k".repeat(255));
        assert_eq!(key.len(), 513);
        let err = store
            .put(&key, chunked_body(vec![1]))
            .await
            .expect_err("prefix plus key exceeds the S3 limit");
        assert!(matches!(err, StoreError::InvalidKey(_)), "{err}");
        assert!(
            err.to_string().contains("store prefix"),
            "the message blames the prefix, not the key alone: {err}"
        );
        let err = store.get(&key).await.err().expect("get rejects");
        assert!(matches!(err, StoreError::InvalidKey(_)), "{err}");
        let err = store.head(&key).await.expect_err("head rejects");
        assert!(matches!(err, StoreError::InvalidKey(_)), "{err}");
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

    use aws_sdk_s3::config::interceptors::{
        BeforeDeserializationInterceptorContextMut, BeforeSerializationInterceptorContextRef,
        BeforeTransmitInterceptorContextRef, FinalizerInterceptorContextRef,
    };
    use aws_sdk_s3::config::{ConfigBag, Intercept, RuntimeComponents};
    use aws_sdk_s3::error::BoxError;
    use aws_sdk_s3::operation::{
        abort_multipart_upload::AbortMultipartUploadInput,
        complete_multipart_upload::CompleteMultipartUploadInput,
        create_multipart_upload::CreateMultipartUploadInput, delete_object::DeleteObjectInput,
        get_object::GetObjectInput, head_bucket::HeadBucketInput,
        list_multipart_uploads::ListMultipartUploadsInput,
        list_multipart_uploads::ListMultipartUploadsOutput, list_objects_v2::ListObjectsV2Input,
        upload_part::UploadPartInput, upload_part::UploadPartOutput,
    };
    use std::marker::PhantomData;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    const ACCESS_DENIED_BODY: &str = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error>\
        <Code>AccessDenied</Code><Message>Access Denied</Message><RequestId>r</RequestId></Error>";

    const NO_SUCH_UPLOAD_BODY: &str = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error>\
        <Code>NoSuchUpload</Code><Message>The specified upload does not exist.</Message>\
        <RequestId>r</RequestId></Error>";

    /// Rewrites the response of one operation, chosen by its input type `T`,
    /// to `status` with an S3 error `body`, and counts the rewrites. Like
    /// `DenyOneDelete`, it remembers the operation from the last
    /// `read_before_execution`, so it is used only where the store issues
    /// that operation with nothing else in flight.
    #[derive(Debug)]
    struct RewriteOperation<T> {
        status: u16,
        body: &'static str,
        active: AtomicBool,
        rewrites: Arc<AtomicUsize>,
        _operation: PhantomData<fn() -> T>,
    }

    impl<T> RewriteOperation<T> {
        fn new(status: u16, body: &'static str, rewrites: Arc<AtomicUsize>) -> Self {
            Self {
                status,
                body,
                active: AtomicBool::new(false),
                rewrites,
                _operation: PhantomData,
            }
        }
    }

    impl<T: std::fmt::Debug + Send + Sync + 'static> Intercept for RewriteOperation<T> {
        fn name(&self) -> &'static str {
            "RewriteOperation"
        }

        fn read_before_execution(
            &self,
            context: &BeforeSerializationInterceptorContextRef<'_>,
            _cfg: &mut ConfigBag,
        ) -> Result<(), BoxError> {
            let matches = context.input().downcast_ref::<T>().is_some();
            self.active.store(matches, Ordering::SeqCst);
            Ok(())
        }

        fn modify_before_deserialization(
            &self,
            context: &mut BeforeDeserializationInterceptorContextMut<'_>,
            _runtime_components: &RuntimeComponents,
            _cfg: &mut ConfigBag,
        ) -> Result<(), BoxError> {
            if self.active.load(Ordering::SeqCst) {
                let response = context.response_mut();
                *response.status_mut() = TryFrom::try_from(self.status)?;
                *response.body_mut() = aws_sdk_s3::primitives::SdkBody::from(self.body);
                self.rewrites.fetch_add(1, Ordering::SeqCst);
            }
            Ok(())
        }
    }

    /// Records the operations a store sends, by input type, and the request
    /// URIs.
    #[derive(Debug, Default)]
    struct RecordOperations {
        operations: Arc<std::sync::Mutex<Vec<&'static str>>>,
        uris: Arc<std::sync::Mutex<Vec<String>>>,
    }

    impl Intercept for RecordOperations {
        fn name(&self) -> &'static str {
            "RecordOperations"
        }

        fn read_before_execution(
            &self,
            context: &BeforeSerializationInterceptorContextRef<'_>,
            _cfg: &mut ConfigBag,
        ) -> Result<(), BoxError> {
            let input = context.input();
            let name = if input.downcast_ref::<ListObjectsV2Input>().is_some() {
                "ListObjectsV2"
            } else if input.downcast_ref::<HeadBucketInput>().is_some() {
                "HeadBucket"
            } else if input.downcast_ref::<DeleteObjectInput>().is_some() {
                "DeleteObject"
            } else if input.downcast_ref::<GetObjectInput>().is_some() {
                "GetObject"
            } else if input.downcast_ref::<CreateMultipartUploadInput>().is_some() {
                "CreateMultipartUpload"
            } else if input.downcast_ref::<AbortMultipartUploadInput>().is_some() {
                "AbortMultipartUpload"
            } else if input.downcast_ref::<ListMultipartUploadsInput>().is_some() {
                "ListMultipartUploads"
            } else {
                "other"
            };
            self.operations.lock().unwrap().push(name);
            Ok(())
        }

        fn read_before_transmit(
            &self,
            context: &BeforeTransmitInterceptorContextRef<'_>,
            _runtime_components: &RuntimeComponents,
            _cfg: &mut ConfigBag,
        ) -> Result<(), BoxError> {
            self.uris
                .lock()
                .unwrap()
                .push(context.request().uri().to_owned());
            Ok(())
        }
    }

    fn count_of(operations: &std::sync::Mutex<Vec<&'static str>>, name: &str) -> usize {
        operations
            .lock()
            .unwrap()
            .iter()
            .filter(|op| **op == name)
            .count()
    }

    async fn open_uploads(client: &aws_sdk_s3::Client, bucket: &str, prefix: &str) -> Vec<String> {
        client
            .list_multipart_uploads()
            .bucket(bucket)
            .prefix(prefix)
            .send()
            .await
            .expect("list_multipart_uploads")
            .uploads()
            .iter()
            .filter_map(|u| u.key().map(str::to_owned))
            .collect()
    }

    async fn create_upload(client: &aws_sdk_s3::Client, bucket: &str, key: &str) {
        client
            .create_multipart_upload()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .expect("create an orphan upload");
    }

    /// The upload sweep uses the same component-aligned rule as the object
    /// deletion: string-prefix siblings keep their uploads.
    #[tokio::test]
    async fn delete_prefix_aborts_only_uploads_under_the_prefix() {
        let Some(endpoint) = endpoint_or_skip() else {
            return;
        };
        let prefix = unique_prefix();
        let config = test_config(endpoint, test_bucket(), prefix.clone());
        let store = S3Store::open(&config).await.expect("S3Store::open");
        ensure_bucket(&config).await;
        let client = raw_client(&config).await;
        for key in ["sib/x/obj", "sib/xy/obj", "sib/x-other", "sib/x"] {
            create_upload(&client, &config.bucket, &format!("{prefix}{key}")).await;
        }
        assert_eq!(store.delete_prefix("sib/x").await.unwrap(), 0);
        let mut left = open_uploads(&client, &config.bucket, &prefix).await;
        left.sort();
        assert_eq!(
            left,
            vec![
                format!("{prefix}sib/x-other"),
                format!("{prefix}sib/xy/obj")
            ],
            "only sib/x and sib/x/obj were aborted"
        );
        assert_eq!(store.delete_prefix("sib").await.unwrap(), 0);
        assert!(
            open_uploads(&client, &config.bucket, &prefix)
                .await
                .is_empty()
        );
    }

    /// Opens a store whose responses to operation `T` are rewritten to 403
    /// `AccessDenied`, runs `validate`, and checks that it fails naming
    /// `grant` after the rewrite actually happened. Returns the message.
    /// Aborts any upload the run left under its prefix (the rewritten
    /// `CreateMultipartUpload` was carried out by the server).
    async fn validate_with_denied<T: std::fmt::Debug + Send + Sync + 'static>(
        endpoint: String,
        grant: &str,
    ) -> String {
        let prefix = unique_prefix();
        let config = test_config(endpoint, test_bucket(), prefix.clone());
        ensure_bucket(&config).await;
        let rewrites = Arc::new(AtomicUsize::new(0));
        let store = S3Store::open_with_interceptor(
            &config,
            RewriteOperation::<T>::new(403, ACCESS_DENIED_BODY, rewrites.clone()),
        )
        .await
        .expect("S3Store::open_with_interceptor");
        let err = store.validate().await.expect_err(grant);
        let client = raw_client(&config).await;
        let left = client
            .list_multipart_uploads()
            .bucket(&config.bucket)
            .prefix(&prefix)
            .send()
            .await
            .expect("list_multipart_uploads");
        for upload in left.uploads() {
            client
                .abort_multipart_upload()
                .bucket(&config.bucket)
                .key(upload.key().unwrap_or_default())
                .upload_id(upload.upload_id().unwrap_or_default())
                .send()
                .await
                .expect("abort the upload the denied probe left");
        }
        assert_eq!(rewrites.load(Ordering::SeqCst), 1, "{grant}: the probe ran");
        match err {
            StoreError::PermissionDenied(msg)
                if msg.contains(grant)
                    && msg.contains(&config.bucket)
                    && msg.contains("AccessDenied: Access Denied") =>
            {
                msg
            }
            other => panic!("{grant}: {other}"),
        }
    }

    /// `validate` probes each S3 grant the store uses, and a denial names the
    /// missing one (with the KMS action for the create, and the
    /// `s3:ListBucket` caveat for the read).
    #[tokio::test]
    async fn validate_names_each_missing_grant() {
        let Some(endpoint) = endpoint_or_skip() else {
            return;
        };
        validate_with_denied::<ListObjectsV2Input>(endpoint.clone(), "s3:ListBucket").await;
        validate_with_denied::<DeleteObjectInput>(endpoint.clone(), "s3:DeleteObject").await;
        let msg = validate_with_denied::<GetObjectInput>(endpoint.clone(), "s3:GetObject").await;
        assert!(
            msg.contains("when s3:ListBucket does not cover the key"),
            "{msg}"
        );
        let msg =
            validate_with_denied::<CreateMultipartUploadInput>(endpoint.clone(), "s3:PutObject")
                .await;
        assert!(msg.contains("kms:GenerateDataKey"), "{msg}");
        validate_with_denied::<AbortMultipartUploadInput>(
            endpoint.clone(),
            "s3:AbortMultipartUpload",
        )
        .await;
        validate_with_denied::<ListMultipartUploadsInput>(
            endpoint,
            "s3:ListBucketMultipartUploads",
        )
        .await;
    }

    /// `validate` lists one key rather than calling `HeadBucket`, runs every
    /// probe, and leaves no object and no upload behind. (On a versioned
    /// bucket the `DeleteObject` probe leaves a delete marker; moto creates
    /// none for an absent key, so that is not observable here.)
    #[tokio::test]
    async fn validate_probes_every_grant_and_leaves_no_object_or_upload() {
        let Some(endpoint) = endpoint_or_skip() else {
            return;
        };
        let prefix = unique_prefix();
        let config = test_config(endpoint, test_bucket(), prefix.clone());
        ensure_bucket(&config).await;
        let recorder = RecordOperations::default();
        let operations = recorder.operations.clone();
        let store = S3Store::open_with_interceptor(&config, recorder)
            .await
            .expect("S3Store::open_with_interceptor");
        store.validate().await.expect("validate");
        assert_eq!(
            *operations.lock().unwrap(),
            vec![
                "ListObjectsV2",
                "DeleteObject",
                "GetObject",
                "CreateMultipartUpload",
                "AbortMultipartUpload",
                "ListMultipartUploads"
            ]
        );
        assert_eq!(count_of(&operations, "HeadBucket"), 0);
        let client = raw_client(&config).await;
        let objects = client
            .list_objects_v2()
            .bucket(&config.bucket)
            .prefix(&prefix)
            .send()
            .await
            .expect("list_objects_v2");
        assert!(objects.contents().is_empty(), "{:?}", objects.contents());
        assert!(
            open_uploads(&client, &config.bucket, &prefix)
                .await
                .is_empty()
        );
    }

    /// Without `s3:ListBucketMultipartUploads`, `delete_prefix` still deletes
    /// the objects and returns `Ok`, so a retry does not fail forever.
    #[tokio::test]
    async fn delete_prefix_with_a_denied_upload_listing_succeeds() {
        let Some(endpoint) = endpoint_or_skip() else {
            return;
        };
        let config = test_config(endpoint, test_bucket(), unique_prefix());
        ensure_bucket(&config).await;
        let rewrites = Arc::new(AtomicUsize::new(0));
        let store = S3Store::open_with_interceptor(
            &config,
            RewriteOperation::<ListMultipartUploadsInput>::new(
                403,
                ACCESS_DENIED_BODY,
                rewrites.clone(),
            ),
        )
        .await
        .expect("S3Store::open_with_interceptor");
        for key in ["bk/a", "bk/b"] {
            store.put(key, chunked_body(vec![1])).await.unwrap();
        }
        assert_eq!(store.delete_prefix("bk").await.expect("Ok"), 2);
        assert_eq!(rewrites.load(Ordering::SeqCst), 1, "the listing was denied");
        assert!(
            list_keys(&store, "bk").await.is_empty(),
            "the objects are gone"
        );
        assert_eq!(store.delete_prefix("bk").await.expect("a retry is Ok"), 0);
    }

    /// Without `s3:AbortMultipartUpload`, `delete_prefix` still deletes the
    /// objects and returns `Ok`, and the sweep goes on to the next upload
    /// after a denial; a retry with no objects left returns `Ok(0)`. Any
    /// other abort failure (a server error, an expired token) still fails
    /// the call.
    #[tokio::test]
    async fn delete_prefix_with_a_denied_upload_abort_succeeds() {
        const INTERNAL_ERROR_BODY: &str = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error>\
            <Code>InternalError</Code><Message>We encountered an internal error.</Message>\
            <RequestId>r</RequestId></Error>";
        const EXPIRED_TOKEN_BODY: &str = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error>\
            <Code>ExpiredToken</Code><Message>The provided token has expired.</Message>\
            <RequestId>r</RequestId></Error>";
        let Some(endpoint) = endpoint_or_skip() else {
            return;
        };
        let prefix = unique_prefix();
        let config = test_config(endpoint, test_bucket(), prefix.clone());
        ensure_bucket(&config).await;
        let client = raw_client(&config).await;
        let rewrites = Arc::new(AtomicUsize::new(0));
        let store = S3Store::open_with_interceptor(
            &config,
            RewriteOperation::<AbortMultipartUploadInput>::new(
                403,
                ACCESS_DENIED_BODY,
                rewrites.clone(),
            ),
        )
        .await
        .expect("S3Store::open_with_interceptor");
        for key in ["bk/a", "bk/b", "bk/c", "bk/d", "bk/e"] {
            store.put(key, chunked_body(vec![1])).await.unwrap();
        }
        for key in ["bk/orphan-1", "bk/orphan-2"] {
            create_upload(&client, &config.bucket, &format!("{prefix}{key}")).await;
        }
        assert_eq!(store.delete_prefix("bk").await.expect("Ok"), 5);
        assert_eq!(
            rewrites.load(Ordering::SeqCst),
            2,
            "both aborts were sent and denied"
        );
        assert!(
            list_keys(&store, "bk").await.is_empty(),
            "the objects are gone"
        );
        // The rewrite replaces the response, so moto did abort those two.
        // Two more orphans face each retry, which has no object left.
        for retry in 1..=2 {
            for key in ["bk/orphan-1", "bk/orphan-2"] {
                create_upload(&client, &config.bucket, &format!("{prefix}{key}")).await;
            }
            assert_eq!(store.delete_prefix("bk").await.expect("a retry is Ok"), 0);
            assert_eq!(
                rewrites.load(Ordering::SeqCst),
                2 + 2 * retry,
                "retry {retry} swept both uploads and both aborts were denied"
            );
        }

        for (status, body, code) in [
            (500, INTERNAL_ERROR_BODY, "InternalError"),
            (400, EXPIRED_TOKEN_BODY, "ExpiredToken"),
        ] {
            create_upload(&client, &config.bucket, &format!("{prefix}bk/orphan-1")).await;
            let failing = S3Store::open_with_interceptor(
                &config,
                RewriteOperation::<AbortMultipartUploadInput>::new(
                    status,
                    body,
                    Arc::new(AtomicUsize::new(0)),
                ),
            )
            .await
            .expect("S3Store::open_with_interceptor");
            let err = failing
                .delete_prefix("bk")
                .await
                .expect_err("an abort failure other than a denial fails the call");
            assert!(err.to_string().contains(code), "{code}: {err}");
        }
        assert_eq!(
            S3Store::open(&config)
                .await
                .unwrap()
                .delete_prefix("bk")
                .await
                .unwrap(),
            0
        );
        assert!(
            open_uploads(&client, &config.bucket, &prefix)
                .await
                .is_empty()
        );
    }

    /// The upload sweep tolerates only a real denial of
    /// `ListMultipartUploads`: a server error or an expired token fails
    /// `delete_prefix`, so the caller retries rather than leaving uploads
    /// behind.
    #[tokio::test]
    async fn delete_prefix_fails_when_the_upload_listing_fails_otherwise() {
        const INTERNAL_ERROR_BODY: &str = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error>\
            <Code>InternalError</Code><Message>We encountered an internal error.</Message>\
            <RequestId>r</RequestId></Error>";
        const EXPIRED_TOKEN_BODY: &str = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error>\
            <Code>ExpiredToken</Code><Message>The provided token has expired.</Message>\
            <RequestId>r</RequestId></Error>";
        let Some(endpoint) = endpoint_or_skip() else {
            return;
        };
        for (status, body, code) in [
            (500, INTERNAL_ERROR_BODY, "InternalError"),
            (400, EXPIRED_TOKEN_BODY, "ExpiredToken"),
        ] {
            let config = test_config(endpoint.clone(), test_bucket(), unique_prefix());
            ensure_bucket(&config).await;
            let rewrites = Arc::new(AtomicUsize::new(0));
            let store = S3Store::open_with_interceptor(
                &config,
                RewriteOperation::<ListMultipartUploadsInput>::new(status, body, rewrites.clone()),
            )
            .await
            .expect("S3Store::open_with_interceptor");
            store.put("bk/a", chunked_body(vec![1])).await.unwrap();
            let err = store
                .delete_prefix("bk")
                .await
                .expect_err("a listing failure other than a denial fails the call");
            assert!(
                rewrites.load(Ordering::SeqCst) >= 1,
                "{code}: the listing was rewritten"
            );
            assert!(err.to_string().contains(code), "{code}: {err}");
            S3Store::open(&config)
                .await
                .unwrap()
                .delete_prefix("bk")
                .await
                .unwrap();
        }
    }

    /// `validate`'s probe abort answered `NoSuchUpload` (a concurrent
    /// `delete_prefix("")` swept the probe upload first) passes: the
    /// request reached the bucket, and the upload is gone. The real abort
    /// runs on moto and its answer is rewritten.
    #[tokio::test]
    async fn validate_passes_when_its_probe_upload_is_gone() {
        let Some(endpoint) = endpoint_or_skip() else {
            return;
        };
        let prefix = unique_prefix();
        let config = test_config(endpoint, test_bucket(), prefix.clone());
        ensure_bucket(&config).await;
        let rewrites = Arc::new(AtomicUsize::new(0));
        let store = S3Store::open_with_interceptor(
            &config,
            RewriteOperation::<AbortMultipartUploadInput>::new(
                404,
                NO_SUCH_UPLOAD_BODY,
                rewrites.clone(),
            ),
        )
        .await
        .expect("S3Store::open_with_interceptor");
        store
            .validate()
            .await
            .expect("an upload already gone is not a failed probe");
        assert_eq!(
            rewrites.load(Ordering::SeqCst),
            1,
            "the probe abort was answered"
        );
        let client = raw_client(&config).await;
        assert!(
            open_uploads(&client, &config.bucket, &prefix)
                .await
                .is_empty()
        );
    }

    /// Runtime shutdown sends no abort for a put in flight: tokio drops the
    /// put's future and the guard's spawned abort without polling it. The
    /// put has uploaded two parts and waits for the rest of its body when
    /// its runtime shuts down, in each of the three ways a runtime ends.
    /// Afterwards, from a fresh runtime, each upload (and so its two parts)
    /// is still open, and a `delete_prefix` covering the keys aborts them.
    /// (moto's `ListParts` answer does not parse in this SDK, so the parts
    /// are counted as they are uploaded.) This
    /// pins what the docs state; a change that made shutdown abort the
    /// uploads fails here and must update them.
    #[test]
    fn runtime_shutdown_leaves_in_flight_uploads_for_delete_prefix() {
        use futures::StreamExt as _;
        let Some(endpoint) = endpoint_or_skip() else {
            return;
        };
        let prefix = unique_prefix();
        let config = test_config(endpoint, test_bucket(), prefix.clone());
        let part = usize::try_from(PART_SIZE).unwrap();
        let mut keys = Vec::new();
        for mode in [
            "shutdown-timeout",
            "shutdown-background",
            "current-thread-dropped",
        ] {
            let runtime = if mode == "current-thread-dropped" {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
            } else {
                tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .build()
                    .unwrap()
            };
            let key = format!("shutdown/{mode}/obj");
            runtime.block_on(async {
                ensure_bucket(&config).await;
                let uploaded = Arc::new(AtomicUsize::new(0));
                let store = Arc::new(
                    S3Store::open_with_interceptor(&config, CountUploadedParts(uploaded.clone()))
                        .await
                        .expect("S3Store::open_with_interceptor"),
                );
                let client = raw_client(&config).await;
                // Two whole parts, then a body that never ends.
                let body: ByteStream = Box::pin(
                    futures::stream::iter([
                        Ok(Bytes::from(patterned_bytes(part))),
                        Ok(Bytes::from(patterned_bytes(part))),
                    ])
                    .chain(futures::stream::pending()),
                );
                tokio::spawn({
                    let (store, key) = (store.clone(), key.clone());
                    async move {
                        let _ = store.put(&key, body).await;
                    }
                });
                let (uploaded, client, bucket, full_key) = (
                    &uploaded,
                    &client,
                    &config.bucket,
                    &format!("{prefix}{key}"),
                );
                wait_until("the put has uploaded both parts", || async move {
                    uploaded.load(Ordering::SeqCst) == 2
                        && open_uploads(client, bucket, full_key).await.len() == 1
                })
                .await;
            });
            match mode {
                "shutdown-timeout" => {
                    runtime.shutdown_timeout(std::time::Duration::from_millis(100));
                }
                "shutdown-background" => runtime.shutdown_background(),
                _ => drop(runtime),
            }
            keys.push(format!("{prefix}{key}"));
        }
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            // Room for an abort, had any been sent.
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            let client = raw_client(&config).await;
            let mut left = open_uploads(&client, &config.bucket, &prefix).await;
            left.sort();
            keys.sort();
            assert_eq!(left, keys, "runtime shutdown sent no abort");
            let store = S3Store::open(&config).await.expect("S3Store::open");
            assert_eq!(store.delete_prefix("shutdown").await.unwrap(), 0);
            assert!(
                open_uploads(&client, &config.bucket, &prefix)
                    .await
                    .is_empty(),
                "delete_prefix swept the uploads"
            );
        });
    }

    /// Counts `UploadPart` requests that succeeded.
    #[derive(Debug)]
    struct CountUploadedParts(Arc<AtomicUsize>);

    impl Intercept for CountUploadedParts {
        fn name(&self) -> &'static str {
            "CountUploadedParts"
        }

        fn read_after_execution(
            &self,
            context: &FinalizerInterceptorContextRef<'_>,
            _runtime_components: &RuntimeComponents,
            _cfg: &mut ConfigBag,
        ) -> Result<(), BoxError> {
            let uploaded = context
                .output_or_error()
                .and_then(Result::ok)
                .is_some_and(|output| output.downcast_ref::<UploadPartOutput>().is_some());
            if uploaded {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
            Ok(())
        }
    }

    /// `CompleteMultipartUpload` answering `NoSuchUpload` (an upload aborted
    /// elsewhere) is a failed put, not a missing object.
    #[tokio::test]
    async fn no_such_upload_at_complete_is_not_not_found() {
        let Some(endpoint) = endpoint_or_skip() else {
            return;
        };
        let prefix = unique_prefix();
        let config = test_config(endpoint, test_bucket(), prefix.clone());
        ensure_bucket(&config).await;
        let rewrites = Arc::new(AtomicUsize::new(0));
        let store = S3Store::open_with_interceptor(
            &config,
            RewriteOperation::<CompleteMultipartUploadInput>::new(
                404,
                NO_SUCH_UPLOAD_BODY,
                rewrites.clone(),
            ),
        )
        .await
        .expect("S3Store::open_with_interceptor");
        let body = patterned_bytes(usize::try_from(PART_SIZE).unwrap() + 1);
        let err = store
            .put("gone/obj", chunked_body(body))
            .await
            .expect_err("complete fails");
        assert_eq!(rewrites.load(Ordering::SeqCst), 1);
        assert!(
            matches!(&err, StoreError::Other(msg) if msg.contains("NoSuchUpload")),
            "{err}"
        );
        let client = raw_client(&config).await;
        assert!(
            open_uploads(&client, &config.bucket, &prefix)
                .await
                .is_empty()
        );
    }

    /// The timeouts, region and addressing style the store configures take
    /// effect. No endpoint is needed: the configuration is read back, and the
    /// addressing style is read from the URI of a request that is refused at
    /// connect. Runs in a child with the suite's own credentials, since the
    /// SDK resolves credentials before it builds the request this test
    /// reads.
    #[tokio::test]
    async fn client_configuration_is_applied() {
        const NAME: &str = "s3::client_configuration_is_applied";
        if !is_child(NAME) {
            run_with_test_credentials(NAME);
            return;
        }
        let mut config = test_config(
            "http://localhost:9".to_owned(),
            "cfg-bucket".to_owned(),
            String::new(),
        );
        config.region = Some("eu-central-1".to_owned());
        assert_ne!(
            std::env::var("AWS_REGION").ok().as_deref(),
            Some("eu-central-1"),
            "the configured region must differ from the environment's"
        );
        for path_style in [true, false] {
            config.force_path_style = path_style;
            // `open` refuses http://localhost without path-style addressing
            // (the request goes to `<bucket>.localhost`); the override lets
            // this test observe that URI.
            config.allow_insecure_endpoint = !path_style;
            let recorder = RecordOperations::default();
            let uris = recorder.uris.clone();
            let store = S3Store::open_with_interceptor(&config, recorder)
                .await
                .expect("S3Store::open_with_interceptor");
            let client_config = store.client_config();
            assert_eq!(
                client_config.region().map(ToString::to_string).as_deref(),
                Some("eu-central-1")
            );
            let timeouts = client_config.timeout_config().expect("timeouts are set");
            assert_eq!(
                timeouts.operation_attempt_timeout(),
                Some(std::time::Duration::from_secs(60))
            );
            assert_eq!(
                timeouts.operation_timeout(),
                Some(std::time::Duration::from_secs(300))
            );
            let _ = store
                .head("k")
                .await
                .expect_err("nothing listens on port 9");
            let uri = uris
                .lock()
                .unwrap()
                .first()
                .cloned()
                .expect("a request was built");
            let expected = if path_style {
                "http://localhost:9/cfg-bucket/k"
            } else {
                "http://cfg-bucket.localhost:9/k"
            };
            assert_eq!(uri, expected, "force_path_style = {path_style}");
        }
    }

    /// Records the largest number of `UploadPart` requests in flight at
    /// once. A request is in flight from its `read_before_execution` to its
    /// `read_after_execution`.
    #[derive(Debug, Default)]
    struct CountInFlightParts {
        in_flight: AtomicUsize,
        max: Arc<AtomicUsize>,
        parts: Arc<AtomicUsize>,
    }

    impl Intercept for CountInFlightParts {
        fn name(&self) -> &'static str {
            "CountInFlightParts"
        }

        fn read_before_execution(
            &self,
            context: &BeforeSerializationInterceptorContextRef<'_>,
            _cfg: &mut ConfigBag,
        ) -> Result<(), BoxError> {
            if context.input().downcast_ref::<UploadPartInput>().is_some() {
                let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                self.max.fetch_max(now, Ordering::SeqCst);
                self.parts.fetch_add(1, Ordering::SeqCst);
            }
            Ok(())
        }

        fn read_after_execution(
            &self,
            context: &FinalizerInterceptorContextRef<'_>,
            _runtime_components: &RuntimeComponents,
            _cfg: &mut ConfigBag,
        ) -> Result<(), BoxError> {
            let is_part = context
                .output_or_error()
                .and_then(Result::ok)
                .is_some_and(|output| output.downcast_ref::<UploadPartOutput>().is_some());
            if is_part {
                self.in_flight.fetch_sub(1, Ordering::SeqCst);
            }
            Ok(())
        }
    }

    /// No more than `max_concurrent_uploads` parts are in flight at once,
    /// and with five parts and a bound of two, two are (the bound is reached,
    /// so the check is not vacuous).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn part_uploads_respect_the_concurrency_bound() {
        let Some(endpoint) = endpoint_or_skip() else {
            return;
        };
        let mut config = test_config(endpoint, test_bucket(), unique_prefix());
        config.max_concurrent_uploads = 2;
        ensure_bucket(&config).await;
        let counter = CountInFlightParts::default();
        let (max, parts) = (counter.max.clone(), counter.parts.clone());
        let store = S3Store::open_with_interceptor(&config, counter)
            .await
            .expect("S3Store::open_with_interceptor");
        let body = patterned_bytes(usize::try_from(PART_SIZE).unwrap() * 5);
        store
            .put("conc/obj", chunked_body(&body))
            .await
            .expect("put");
        assert_eq!(parts.load(Ordering::SeqCst), 5);
        assert_eq!(
            max.load(Ordering::SeqCst),
            2,
            "at most, and at some point exactly, max_concurrent_uploads parts in flight"
        );
        assert_eq!(read_all(store.get("conc/obj").await.unwrap()).await, body);
    }

    /// A body that needs more parts than the limit is refused before the
    /// part past the limit is sent, and its upload is aborted.
    #[tokio::test]
    async fn a_body_over_the_part_limit_is_refused_and_aborted() {
        let Some(endpoint) = endpoint_or_skip() else {
            return;
        };
        let prefix = unique_prefix();
        let config = test_config(endpoint, test_bucket(), prefix.clone());
        ensure_bucket(&config).await;
        let counter = CountInFlightParts::default();
        let parts = counter.parts.clone();
        let mut store = S3Store::open_with_interceptor(&config, counter)
            .await
            .expect("S3Store::open_with_interceptor");
        store.set_max_parts(2);
        let body = patterned_bytes(usize::try_from(PART_SIZE).unwrap() * 3);
        let err = store
            .put("limit/obj", chunked_body(body))
            .await
            .expect_err("three parts over a limit of two");
        assert!(
            matches!(&err, StoreError::Other(msg) if msg.contains("exceeds 2 parts")),
            "{err}"
        );
        assert!(parts.load(Ordering::SeqCst) <= 2, "part 3 was never sent");
        assert!(store.head("limit/obj").await.unwrap().is_none());
        let client = raw_client(&config).await;
        assert!(
            open_uploads(&client, &config.bucket, &prefix)
                .await
                .is_empty()
        );
        // Two parts fit.
        let body = patterned_bytes(usize::try_from(PART_SIZE).unwrap() * 2);
        store
            .put("limit/obj", chunked_body(body))
            .await
            .expect("two parts fit");
    }

    /// Pages `ListMultipartUploads` responses: moto ignores `max-uploads`
    /// and returns every upload in one response, so this hook cuts each
    /// response to `page` uploads after the request's markers, sets
    /// `IsTruncated` and the next markers, as S3 does. The markers come from
    /// the last request's input (the sweep sends one listing at a time).
    #[derive(Debug)]
    struct PageUploads {
        page: usize,
        markers: std::sync::Mutex<(String, String)>,
        pages: Arc<AtomicUsize>,
    }

    impl Intercept for PageUploads {
        fn name(&self) -> &'static str {
            "PageUploads"
        }

        fn read_before_execution(
            &self,
            context: &BeforeSerializationInterceptorContextRef<'_>,
            _cfg: &mut ConfigBag,
        ) -> Result<(), BoxError> {
            if let Some(input) = context.input().downcast_ref::<ListMultipartUploadsInput>() {
                *self.markers.lock().unwrap() = (
                    input.key_marker().unwrap_or_default().to_owned(),
                    input.upload_id_marker().unwrap_or_default().to_owned(),
                );
            }
            Ok(())
        }

        fn modify_before_completion(
            &self,
            context: &mut aws_sdk_s3::config::interceptors::FinalizerInterceptorContextMut<'_>,
            _runtime_components: &RuntimeComponents,
            _cfg: &mut ConfigBag,
        ) -> Result<(), BoxError> {
            let Some(Ok(output)) = context.output_or_error_mut() else {
                return Ok(());
            };
            let Some(output) = output.downcast_mut::<ListMultipartUploadsOutput>() else {
                return Ok(());
            };
            let after = self.markers.lock().unwrap().clone();
            let id = |u: &aws_sdk_s3::types::MultipartUpload| {
                (
                    u.key().unwrap_or_default().to_owned(),
                    u.upload_id().unwrap_or_default().to_owned(),
                )
            };
            let mut uploads = output.uploads.take().unwrap_or_default();
            uploads.sort_by_key(id);
            uploads.retain(|u| after.0.is_empty() || id(u) > after);
            let truncated = uploads.len() > self.page;
            uploads.truncate(self.page);
            let last = uploads.last().map(id);
            output.is_truncated = Some(truncated);
            output.next_key_marker = last.clone().filter(|_| truncated).map(|l| l.0);
            output.next_upload_id_marker = last.filter(|_| truncated).map(|l| l.1);
            output.uploads = Some(uploads);
            self.pages.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    /// The upload sweep reads every page of `ListMultipartUploads`: five
    /// orphan uploads in pages of two take three pages, and all five are
    /// aborted (a sweep that read only the first page would leave three).
    #[tokio::test]
    async fn upload_sweep_reads_every_page() {
        let Some(endpoint) = endpoint_or_skip() else {
            return;
        };
        let prefix = unique_prefix();
        let config = test_config(endpoint, test_bucket(), prefix.clone());
        ensure_bucket(&config).await;
        let pages = Arc::new(AtomicUsize::new(0));
        let store = S3Store::open_with_interceptor(
            &config,
            PageUploads {
                page: 2,
                markers: std::sync::Mutex::default(),
                pages: pages.clone(),
            },
        )
        .await
        .expect("S3Store::open_with_interceptor");
        let client = raw_client(&config).await;
        for n in 0..5 {
            create_upload(&client, &config.bucket, &format!("{prefix}orphans/u{n}")).await;
        }
        assert_eq!(
            open_uploads(&client, &config.bucket, &prefix).await.len(),
            5
        );
        assert_eq!(store.delete_prefix("orphans").await.unwrap(), 0);
        assert_eq!(pages.load(Ordering::SeqCst), 3, "5 uploads in pages of 2");
        assert!(
            open_uploads(&client, &config.bucket, &prefix)
                .await
                .is_empty(),
            "every page was swept"
        );
    }

    /// Counts `ListObjectsV2` requests.
    #[derive(Debug, Default)]
    struct CountObjectListings(Arc<AtomicUsize>);

    impl Intercept for CountObjectListings {
        fn name(&self) -> &'static str {
            "CountObjectListings"
        }

        fn read_before_execution(
            &self,
            context: &BeforeSerializationInterceptorContextRef<'_>,
            _cfg: &mut ConfigBag,
        ) -> Result<(), BoxError> {
            if context
                .input()
                .downcast_ref::<ListObjectsV2Input>()
                .is_some()
            {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
            Ok(())
        }
    }

    /// The test page size reaches `ListObjectsV2` (moto honours
    /// `max-keys`): five objects in pages of two take three requests for
    /// `list` and for `delete_prefix`, and every object is seen.
    #[tokio::test]
    async fn object_listing_pages_at_the_test_page_size() {
        let Some(endpoint) = endpoint_or_skip() else {
            return;
        };
        let config = test_config(endpoint, test_bucket(), unique_prefix());
        ensure_bucket(&config).await;
        let counter = CountObjectListings::default();
        let requests = counter.0.clone();
        let mut store = S3Store::open_with_interceptor(&config, counter)
            .await
            .expect("S3Store::open_with_interceptor");
        store.set_page_size(2);
        for n in 0..5 {
            store
                .put(&format!("pages/o{n}"), chunked_body(vec![1]))
                .await
                .unwrap();
        }
        assert_eq!(list_keys(&store, "pages").await.len(), 5);
        assert_eq!(
            requests.swap(0, Ordering::SeqCst),
            3,
            "list: 5 keys in pages of 2"
        );
        assert_eq!(store.delete_prefix("pages").await.unwrap(), 5);
        assert_eq!(
            requests.load(Ordering::SeqCst),
            3,
            "delete_prefix pages too"
        );
        assert!(list_keys(&store, "pages").await.is_empty());
    }

    /// A TCP proxy in front of the S3 endpoint that holds the response to
    /// every `CreateMultipartUpload` (`POST ...?uploads`) for `delay`, so the
    /// server has created the upload while the client is still waiting.
    /// The delay has to be asynchronous: a `std::thread::sleep` in an
    /// interceptor hook blocks the put's own poll, and the pre-fix code
    /// (`CreateMultipartUpload` inline in the put) then finished the create
    /// and armed its guard before the timeout could drop it, so that mutant
    /// passed. Requests on one connection are sequential (HTTP/1.1 without
    /// pipelining), so the next response after a create request is its own.
    /// Returns the proxy's endpoint and the number of held responses it has
    /// released to the client.
    async fn delaying_proxy(
        upstream: &str,
        delay: std::time::Duration,
    ) -> (String, Arc<AtomicUsize>) {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let target = upstream
            .trim_start_matches("http://")
            .trim_end_matches('/')
            .to_owned();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind the proxy");
        let address = listener.local_addr().expect("proxy address");
        let released = Arc::new(AtomicUsize::new(0));
        let release_count = released.clone();
        tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                let target = target.clone();
                let release_count = release_count.clone();
                tokio::spawn(async move {
                    let Ok(server) = tokio::net::TcpStream::connect(&target).await else {
                        return;
                    };
                    let (mut client_read, mut client_write) = client.into_split();
                    let (mut server_read, mut server_write) = server.into_split();
                    let hold = Arc::new(AtomicBool::new(false));
                    let hold_next = hold.clone();
                    let upstream = async move {
                        let mut buf = vec![0u8; 64 * 1024];
                        while let Ok(n) = client_read.read(&mut buf).await {
                            if n == 0 {
                                break;
                            }
                            let data = &buf[..n];
                            let line_end = data.iter().position(|b| *b == b'\n').unwrap_or(n);
                            let line = &data[..line_end];
                            if line.starts_with(b"POST ")
                                && line.windows(8).any(|w| w == b"?uploads")
                            {
                                hold_next.store(true, Ordering::SeqCst);
                            }
                            if server_write.write_all(data).await.is_err() {
                                break;
                            }
                        }
                        let _ = server_write.shutdown().await;
                    };
                    let downstream = async move {
                        let mut buf = vec![0u8; 64 * 1024];
                        while let Ok(n) = server_read.read(&mut buf).await {
                            if n == 0 {
                                break;
                            }
                            if hold.swap(false, Ordering::SeqCst) {
                                tokio::time::sleep(delay).await;
                                release_count.fetch_add(1, Ordering::SeqCst);
                            }
                            if client_write.write_all(&buf[..n]).await.is_err() {
                                break;
                            }
                        }
                        let _ = client_write.shutdown().await;
                    };
                    tokio::join!(upstream, downstream);
                });
            }
        });
        (format!("http://{address}"), released)
    }

    /// Polls `ready` every 20 ms until it returns true; panics after
    /// `HANG_GUARD`, which only a hung test reaches. The drop tests use it to
    /// drop their put or `validate` at a state the proxy reports rather than
    /// after a fixed time.
    async fn wait_until<F, Fut>(what: &str, mut ready: F)
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        const HANG_GUARD: std::time::Duration = std::time::Duration::from_secs(30);
        let started = std::time::Instant::now();
        while !ready().await {
            assert!(
                started.elapsed() < HANG_GUARD,
                "timed out waiting until {what}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }

    /// The create response the proxy holds; long enough that the drop below
    /// lands inside it on a slow runner, and checked by the tests.
    const CREATE_HOLD: std::time::Duration = std::time::Duration::from_secs(3);

    /// A put dropped while `CreateMultipartUpload` is in flight (S3 has
    /// created the upload, the response is not back yet) leaves no open
    /// upload: the guard the create task arms aborts it. The put is dropped
    /// once the create has been sent and moto lists the upload, while the
    /// proxy still holds the response.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn put_dropped_during_create_leaves_no_upload() {
        let Some(endpoint) = http_endpoint_or_skip("put_dropped_during_create_leaves_no_upload")
        else {
            return;
        };
        let prefix = unique_prefix();
        let direct = test_config(endpoint.clone(), test_bucket(), prefix.clone());
        ensure_bucket(&direct).await;
        let (proxy, released) = delaying_proxy(&endpoint, CREATE_HOLD).await;
        let proxied = test_config(proxy, test_bucket(), prefix.clone());
        let creates = Arc::new(AtomicUsize::new(0));
        let store = Arc::new(
            S3Store::open_with_interceptor(&proxied, CountCreateMultipartUpload(creates.clone()))
                .await
                .expect("S3Store::open_with_interceptor"),
        );
        let client = raw_client(&direct).await;
        let body = chunked_body(patterned_bytes(usize::try_from(PART_SIZE).unwrap() * 2));
        let put = {
            let store = store.clone();
            tokio::spawn(async move { store.put("dropped/obj", body).await })
        };
        let (counted, raw, bucket, under) = (&creates, &client, &direct.bucket, &prefix);
        wait_until(
            "the create was sent and moto has the upload",
            || async move {
                counted.load(Ordering::SeqCst) == 1
                    && open_uploads(raw, bucket, under).await.len() == 1
            },
        )
        .await;
        put.abort();
        assert!(put.await.expect_err("dropped").is_cancelled());
        assert_eq!(
            released.load(Ordering::SeqCst),
            0,
            "the put was dropped while its create response was held"
        );
        let left = uploads_left_after_cleanup(&client, &direct.bucket, &prefix).await;
        assert!(
            left.is_empty(),
            "the dropped put's upload was aborted: {left:?}"
        );
        assert_eq!(creates.load(Ordering::SeqCst), 1);
    }

    /// `validate` dropped while its probe `CreateMultipartUpload` is in
    /// flight leaves no upload: the create runs in a spawned task that arms
    /// the guard, as a put's does. Dropped as in
    /// `put_dropped_during_create_leaves_no_upload`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn validate_dropped_during_its_create_leaves_no_upload() {
        let Some(endpoint) =
            http_endpoint_or_skip("validate_dropped_during_its_create_leaves_no_upload")
        else {
            return;
        };
        let prefix = unique_prefix();
        let direct = test_config(endpoint.clone(), test_bucket(), prefix.clone());
        ensure_bucket(&direct).await;
        let (proxy, released) = delaying_proxy(&endpoint, CREATE_HOLD).await;
        let proxied = test_config(proxy, test_bucket(), prefix.clone());
        let creates = Arc::new(AtomicUsize::new(0));
        let store = Arc::new(
            S3Store::open_with_interceptor(&proxied, CountCreateMultipartUpload(creates.clone()))
                .await
                .expect("S3Store::open_with_interceptor"),
        );
        let client = raw_client(&direct).await;
        let validate = {
            let store = store.clone();
            tokio::spawn(async move { store.validate().await })
        };
        let (counted, raw, bucket, under) = (&creates, &client, &direct.bucket, &prefix);
        wait_until(
            "the probe create was sent and moto has the upload",
            || async move {
                counted.load(Ordering::SeqCst) == 1
                    && open_uploads(raw, bucket, under).await.len() == 1
            },
        )
        .await;
        validate.abort();
        assert!(validate.await.expect_err("dropped").is_cancelled());
        assert_eq!(
            released.load(Ordering::SeqCst),
            0,
            "validate was dropped while its create response was held"
        );
        let left = uploads_left_after_cleanup(&client, &direct.bucket, &prefix).await;
        assert!(left.is_empty(), "the probe upload was aborted: {left:?}");
    }

    /// A TCP proxy in front of the S3 endpoint that swallows the first
    /// `AbortMultipartUpload` (`DELETE ...uploadId=`): the request is read and
    /// never forwarded, so the client waits for a response that does not
    /// come. Every later abort is forwarded. Returns the proxy's endpoint and
    /// the number of aborts seen (swallowed or forwarded). As in
    /// `delaying_proxy`, a request line is recognised at the start of a read.
    async fn swallowing_proxy(upstream: &str) -> (String, Arc<AtomicUsize>) {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let target = upstream
            .trim_start_matches("http://")
            .trim_end_matches('/')
            .to_owned();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind the proxy");
        let address = listener.local_addr().expect("proxy address");
        let aborts = Arc::new(AtomicUsize::new(0));
        let seen = aborts.clone();
        tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                let target = target.clone();
                let seen = seen.clone();
                tokio::spawn(async move {
                    let Ok(server) = tokio::net::TcpStream::connect(&target).await else {
                        return;
                    };
                    let (mut client_read, mut client_write) = client.into_split();
                    let (mut server_read, mut server_write) = server.into_split();
                    let upstream = async move {
                        let mut buf = vec![0u8; 64 * 1024];
                        let mut swallowing = false;
                        while let Ok(n) = client_read.read(&mut buf).await {
                            if n == 0 {
                                break;
                            }
                            let data = &buf[..n];
                            let line_end = data.iter().position(|b| *b == b'\n').unwrap_or(n);
                            let line = &data[..line_end];
                            if line.starts_with(b"DELETE ")
                                && line.windows(9).any(|w| w == b"uploadId=")
                                && seen.fetch_add(1, Ordering::SeqCst) == 0
                            {
                                swallowing = true;
                            }
                            if swallowing {
                                continue;
                            }
                            if server_write.write_all(data).await.is_err() {
                                break;
                            }
                        }
                        let _ = server_write.shutdown().await;
                    };
                    let downstream = async move {
                        let mut buf = vec![0u8; 64 * 1024];
                        while let Ok(n) = server_read.read(&mut buf).await {
                            if n == 0 || client_write.write_all(&buf[..n]).await.is_err() {
                                break;
                            }
                        }
                        let _ = client_write.shutdown().await;
                    };
                    tokio::join!(upstream, downstream);
                });
            }
        });
        (format!("http://{address}"), aborts)
    }

    /// A TCP proxy in front of the S3 endpoint that passes every request and
    /// response through, except that the response to a `GET` whose request
    /// line contains `marker` (an object key) is cut after its headers: the
    /// headers reach the client, the body never does, and the connection is
    /// held open. That is the shape of a stalled `GetObject`, which moto
    /// never produces; the SDK returns the output once the headers are in,
    /// and the body stream is what must time out.
    async fn stalling_proxy(upstream: &str, marker: &'static [u8]) -> String {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let target = upstream
            .trim_start_matches("http://")
            .trim_end_matches('/')
            .to_owned();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind the proxy");
        let address = listener.local_addr().expect("proxy address");
        tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                let target = target.clone();
                tokio::spawn(async move {
                    let Ok(server) = tokio::net::TcpStream::connect(&target).await else {
                        return;
                    };
                    let (mut client_read, mut client_write) = client.into_split();
                    let (mut server_read, mut server_write) = server.into_split();
                    let stall = Arc::new(AtomicBool::new(false));
                    let stall_next = stall.clone();
                    let upstream = async move {
                        let mut buf = vec![0u8; 64 * 1024];
                        while let Ok(n) = client_read.read(&mut buf).await {
                            if n == 0 {
                                break;
                            }
                            let data = &buf[..n];
                            let line_end = data.iter().position(|b| *b == b'\n').unwrap_or(n);
                            let line = &data[..line_end];
                            if line.starts_with(b"GET ")
                                && line.windows(marker.len()).any(|w| w == marker)
                            {
                                stall_next.store(true, Ordering::SeqCst);
                            }
                            if server_write.write_all(data).await.is_err() {
                                break;
                            }
                        }
                        let _ = server_write.shutdown().await;
                    };
                    let downstream = async move {
                        let mut buf = vec![0u8; 64 * 1024];
                        while let Ok(n) = server_read.read(&mut buf).await {
                            if n == 0 {
                                break;
                            }
                            let data = &buf[..n];
                            if stall.load(Ordering::SeqCst) {
                                // Forward the headers (through the blank
                                // line) and then nothing more, ever.
                                let header_end = data
                                    .windows(4)
                                    .position(|w| w == b"\r\n\r\n")
                                    .map(|at| at + 4);
                                let Some(header_end) = header_end else {
                                    // Headers spanning reads: forward this
                                    // part and look again.
                                    if client_write.write_all(data).await.is_err() {
                                        break;
                                    }
                                    continue;
                                };
                                let _ = client_write.write_all(&data[..header_end]).await;
                                let _ = client_write.flush().await;
                                std::future::pending::<()>().await;
                            }
                            if client_write.write_all(data).await.is_err() {
                                break;
                            }
                        }
                        let _ = client_write.shutdown().await;
                    };
                    tokio::join!(upstream, downstream);
                });
            }
        });
        format!("http://{address}")
    }

    /// A `GetObject` whose body stops arriving after the headers fails the
    /// read within the store's body idle timeout instead of hanging the
    /// reader: the SDK's request timeouts end with the headers, so the store
    /// bounds each body chunk itself. The error is a transport error naming
    /// the key. The SDK's stalled-stream protection would fail the same read
    /// after its 5 s grace period with its own message; the store's bound is
    /// set to 1 s here and the elapsed time pins which of the two fired.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn stalled_get_body_fails_within_the_idle_timeout() {
        let Some(endpoint) =
            http_endpoint_or_skip("stalled_get_body_fails_within_the_idle_timeout")
        else {
            return;
        };
        let prefix = unique_prefix();
        let direct = test_config(endpoint.clone(), test_bucket(), prefix.clone());
        ensure_bucket(&direct).await;
        let writer = S3Store::open(&direct).await.expect("S3Store::open");
        writer
            .put("stall-body/obj", chunked_body(patterned_bytes(64 * 1024)))
            .await
            .unwrap();
        let proxied = test_config(
            stalling_proxy(&endpoint, b"stall-body").await,
            test_bucket(),
            prefix.clone(),
        );
        let mut store = S3Store::open(&proxied).await.expect("S3Store::open");
        store.set_body_idle_timeout(std::time::Duration::from_secs(1));
        let stream = store
            .get("stall-body/obj")
            .await
            .expect("the headers arrive, so get() returns a stream");
        let started = std::time::Instant::now();
        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            stream.try_collect::<Vec<Bytes>>(),
        )
        .await;
        let Ok(Err(err)) = outcome else {
            panic!("the body read must fail, not hang or complete: {outcome:?}");
        };
        let elapsed = started.elapsed();
        assert!(
            elapsed >= std::time::Duration::from_secs(1)
                && elapsed < std::time::Duration::from_secs(4),
            "failed after the store's 1 s bound, not the SDK's 5 s grace or the test's 10 s: \
             {elapsed:?}"
        );
        let text = err.to_string();
        assert!(matches!(err, StoreError::Transport { .. }), "{text}");
        assert!(
            text.contains("stall-body/obj") && text.contains("stalled"),
            "names the key and the stall: {text}"
        );
        // The direct path still reads the object whole.
        assert_eq!(
            read_all(writer.get("stall-body/obj").await.unwrap()).await,
            patterned_bytes(64 * 1024)
        );
    }

    /// Polls the uploads under `prefix` for up to twenty seconds until none
    /// is left; returns what is left.
    async fn uploads_left_after_cleanup(
        client: &aws_sdk_s3::Client,
        bucket: &str,
        prefix: &str,
    ) -> Vec<String> {
        let mut left = Vec::new();
        for _ in 0..200 {
            left = open_uploads(client, bucket, prefix).await;
            if left.is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        left
    }

    /// A put whose body fails in the third part waits on its inline
    /// `AbortMultipartUpload`; the proxy swallows that abort, and the put is
    /// dropped once the proxy has seen it, while the put waits. The drop
    /// guard is still armed, so it sends a second abort and no upload is
    /// left.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn put_dropped_during_its_inline_abort_leaves_no_upload() {
        let Some(endpoint) =
            http_endpoint_or_skip("put_dropped_during_its_inline_abort_leaves_no_upload")
        else {
            return;
        };
        let prefix = unique_prefix();
        let direct = test_config(endpoint.clone(), test_bucket(), prefix.clone());
        ensure_bucket(&direct).await;
        let (proxy, aborts) = swallowing_proxy(&endpoint).await;
        let proxied = test_config(proxy, test_bucket(), prefix.clone());
        let creates = Arc::new(AtomicUsize::new(0));
        let store = Arc::new(
            S3Store::open_with_interceptor(&proxied, CountCreateMultipartUpload(creates.clone()))
                .await
                .expect("S3Store::open_with_interceptor"),
        );
        let client = raw_client(&direct).await;
        let body = failing_body(patterned_bytes(
            usize::try_from(PART_SIZE).unwrap() * 2 + 4096,
        ));
        let put = {
            let store = store.clone();
            tokio::spawn(async move { store.put("inline-abort/obj", body).await })
        };
        let seen = &aborts;
        wait_until("the proxy swallowed the inline abort", || async move {
            seen.load(Ordering::SeqCst) == 1
        })
        .await;
        assert!(!put.is_finished(), "the put waits on its swallowed abort");
        put.abort();
        assert!(put.await.expect_err("dropped").is_cancelled());
        assert_eq!(creates.load(Ordering::SeqCst), 1, "a multipart upload ran");
        let left = uploads_left_after_cleanup(&client, &direct.bucket, &prefix).await;
        assert!(left.is_empty(), "uploads left: {left:?}");
        assert_eq!(
            aborts.load(Ordering::SeqCst),
            2,
            "the swallowed inline abort and the guard's"
        );
    }

    /// `validate` dropped while its probe `AbortMultipartUpload` is held
    /// (swallowed by the proxy; dropped once the proxy has seen it): the
    /// guard around the probe upload aborts it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn validate_dropped_during_its_probe_abort_leaves_no_upload() {
        let Some(endpoint) =
            http_endpoint_or_skip("validate_dropped_during_its_probe_abort_leaves_no_upload")
        else {
            return;
        };
        let prefix = unique_prefix();
        let direct = test_config(endpoint.clone(), test_bucket(), prefix.clone());
        ensure_bucket(&direct).await;
        let (proxy, aborts) = swallowing_proxy(&endpoint).await;
        let store = Arc::new(
            S3Store::open(&test_config(proxy, test_bucket(), prefix.clone()))
                .await
                .expect("S3Store::open"),
        );
        let client = raw_client(&direct).await;
        let validate = {
            let store = store.clone();
            tokio::spawn(async move { store.validate().await })
        };
        let seen = &aborts;
        wait_until("the proxy swallowed the probe abort", || async move {
            seen.load(Ordering::SeqCst) == 1
        })
        .await;
        assert!(
            !validate.is_finished(),
            "validate waits on its swallowed abort"
        );
        validate.abort();
        assert!(validate.await.expect_err("dropped").is_cancelled());
        let left = uploads_left_after_cleanup(&client, &direct.bucket, &prefix).await;
        assert!(left.is_empty(), "uploads left: {left:?}");
        assert_eq!(
            aborts.load(Ordering::SeqCst),
            2,
            "the swallowed probe abort and the guard's"
        );
    }

    /// Records the URI of the first request and stops it before it is sent,
    /// so a test can see where a request would go without a connection.
    #[derive(Debug, Default)]
    struct StopBeforeTransmit(Arc<std::sync::Mutex<Option<String>>>);

    impl Intercept for StopBeforeTransmit {
        fn name(&self) -> &'static str {
            "StopBeforeTransmit"
        }

        fn read_before_transmit(
            &self,
            context: &BeforeTransmitInterceptorContextRef<'_>,
            _runtime_components: &RuntimeComponents,
            _cfg: &mut ConfigBag,
        ) -> Result<(), BoxError> {
            self.0
                .lock()
                .unwrap()
                .get_or_insert_with(|| context.request().uri().to_owned());
            Err("stopped before transmit".into())
        }
    }

    /// The endpoint rules apply to an endpoint the SDK reads itself, not
    /// only to the config file's `endpoint`: `AWS_ENDPOINT_URL`,
    /// `AWS_ENDPOINT_URL_S3` (which wins over `AWS_ENDPOINT_URL`), and the
    /// shared config's `endpoint_url` and `services` section. Each cell runs
    /// in a child with only its sources set (and, like every child, no proxy
    /// variable):
    ///
    /// - an `http://` endpoint to `10.0.0.5` from each SDK source is refused
    ///   at `open`, naming `allow_insecure_endpoint` and the source, and with
    ///   the override the request goes to that host (so the refusal was
    ///   about the endpoint the client uses); a loopback one opens;
    /// - `endpoint` set together with an SDK source wins: the request goes
    ///   to the configured endpoint, and an `http://` non-loopback value in
    ///   `AWS_ENDPOINT_URL_S3` does not make `open` refuse a loopback
    ///   `endpoint`;
    /// - `AWS_IGNORE_CONFIGURED_ENDPOINT_URLS` changes neither outcome: the
    ///   `10.0.0.5` value in `AWS_ENDPOINT_URL` is refused under it, and a
    ///   loopback `AWS_ENDPOINT_URL_S3` is used under it;
    /// - every shape the syntax rule refuses, through `endpoint` and each
    ///   SDK source, is refused at `open` with its problem class and its
    ///   source named and without the secret it carries.
    ///
    /// No endpoint is needed.
    #[tokio::test]
    async fn endpoint_from_the_sdk_environment_follows_the_http_rule() {
        if is_child(ENDPOINT_CELL_TEST) {
            endpoint_cell_in_this_process().await;
        } else {
            endpoint_cells_in_children();
        }
    }

    const ENDPOINT_CELL_TEST: &str = "s3::endpoint_from_the_sdk_environment_follows_the_http_rule";

    /// Set in a cell's child to the endpoint its request must go to when
    /// `open` must accept the endpoint; empty when `open` must refuse
    /// `10.0.0.5` or (with `EXPECT_REFUSAL`) refuse the syntax.
    const EXPECT: &str = "EXTENDDB_TEST_EXPECT_ENDPOINT";
    /// The source a refusal must name, as the message spells it.
    const EXPECT_SOURCE: &str = "EXTENDDB_TEST_EXPECT_SOURCE";
    /// The problem class a syntax refusal must name; unset for the other
    /// cells.
    const EXPECT_REFUSAL: &str = "EXTENDDB_TEST_EXPECT_REFUSAL";
    /// `[backup.store.s3] endpoint` for the cell; unset leaves it `None`.
    const CONFIG_ENDPOINT: &str = "EXTENDDB_TEST_CONFIG_ENDPOINT";

    /// The source names as the refusal messages spell them.
    const FROM_CONFIG: &str = "[backup.store.s3] endpoint";
    const FROM_PROFILE: &str = "endpoint_url in the shared AWS config file's profile";
    const FROM_SERVICES: &str =
        "endpoint_url under s3 in the shared AWS config file's services section";

    /// Endpoints the syntax rule refuses, each with the problem class its
    /// refusal names. Each carries `S3cretPW` where its class would carry a
    /// credential or a signature, so a message that repeats any part of it
    /// is seen.
    const REFUSED_SHAPES: [(&str, &str); 8] = [
        (
            "https://u:S3cretPW@127.0.0.1:19219/?a=b",
            "user information",
        ),
        (
            "http://u:S3cretPW@127.0.0.1:19219/?X-Amz-Signature=abc",
            "user information",
        ),
        ("https://u:S3cretPW@[::1%25lo]:19219", "user information"),
        ("https://u:S3cretPW@127.0.0.1:19219", "user information"),
        ("https://127.0.0.1:19219/?a=S3cretPW", "query component"),
        ("https://127.0.0.1:19219/#S3cretPW", "fragment"),
        ("https://[::1%25S3cretPW]:19219", "IPv6 zone id"),
        ("https://256.0.0.1:19219/S3cretPW", "not a valid URL"),
    ];

    /// A shared config file whose profile `ep` sets `endpoint_url` to
    /// `endpoint`, either directly or through its `services` section.
    fn profile_file(
        dir: &std::path::Path,
        name: &str,
        endpoint: &str,
        services: bool,
    ) -> std::path::PathBuf {
        let path = dir.join(name);
        let text = if services {
            format!(
                "[profile ep]\nservices = ep-services\nregion = us-east-1\n\n\
                 [services ep-services]\ns3 =\n  endpoint_url = {endpoint}\n"
            )
        } else {
            format!("[profile ep]\nendpoint_url = {endpoint}\nregion = us-east-1\n")
        };
        std::fs::write(&path, text).unwrap();
        path
    }

    /// One endpoint cell: its name and the variables its child gets.
    type EndpointCell = (String, Vec<(&'static str, std::ffi::OsString)>);

    fn os(value: &str) -> std::ffi::OsString {
        std::ffi::OsString::from(value)
    }

    /// The `http://` rule through each SDK source, and the precedence of
    /// `endpoint` over them.
    fn http_rule_cells(dir: &std::path::Path) -> Vec<EndpointCell> {
        let general = profile_file(dir, "general-config", "http://10.0.0.5:9", false);
        let services = profile_file(dir, "services-config", "http://10.0.0.5:9", true);
        let other_loopback = profile_file(dir, "loopback-config", "http://127.0.0.1:8", true);
        vec![
            (
                "AWS_ENDPOINT_URL".to_owned(),
                vec![
                    ("AWS_ENDPOINT_URL", os("http://10.0.0.5:9")),
                    (EXPECT_SOURCE, os("AWS_ENDPOINT_URL")),
                ],
            ),
            (
                "AWS_ENDPOINT_URL_S3".to_owned(),
                vec![
                    ("AWS_ENDPOINT_URL_S3", os("http://10.0.0.5:9")),
                    (EXPECT_SOURCE, os("AWS_ENDPOINT_URL_S3")),
                ],
            ),
            (
                "AWS_ENDPOINT_URL_S3 over an https AWS_ENDPOINT_URL".to_owned(),
                vec![
                    ("AWS_ENDPOINT_URL", os("https://s3.example.com")),
                    ("AWS_ENDPOINT_URL_S3", os("http://10.0.0.5:9")),
                    (EXPECT_SOURCE, os("AWS_ENDPOINT_URL_S3")),
                ],
            ),
            // `AWS_IGNORE_CONFIGURED_ENDPOINT_URLS` clears only the general
            // `endpoint_url` the loader copies into the shared config; the
            // S3 client's own lookup, and the store's, read the variables
            // through the service key all the same, so the flag neither
            // lifts a refusal nor drops a loopback setting.
            (
                "AWS_ENDPOINT_URL under AWS_IGNORE_CONFIGURED_ENDPOINT_URLS".to_owned(),
                vec![
                    ("AWS_IGNORE_CONFIGURED_ENDPOINT_URLS", os("true")),
                    ("AWS_ENDPOINT_URL", os("http://10.0.0.5:9")),
                    (EXPECT_SOURCE, os("AWS_ENDPOINT_URL")),
                ],
            ),
            (
                "loopback AWS_ENDPOINT_URL_S3 under AWS_IGNORE_CONFIGURED_ENDPOINT_URLS".to_owned(),
                vec![
                    ("AWS_IGNORE_CONFIGURED_ENDPOINT_URLS", os("true")),
                    ("AWS_ENDPOINT_URL_S3", os("http://127.0.0.1:9")),
                    (EXPECT, os("http://127.0.0.1:9/")),
                ],
            ),
            (
                "profile endpoint_url".to_owned(),
                vec![
                    ("AWS_CONFIG_FILE", general.into_os_string()),
                    ("AWS_PROFILE", os("ep")),
                    (EXPECT_SOURCE, os(FROM_PROFILE)),
                ],
            ),
            (
                "profile services section".to_owned(),
                vec![
                    ("AWS_CONFIG_FILE", services.into_os_string()),
                    ("AWS_PROFILE", os("ep")),
                    (EXPECT_SOURCE, os(FROM_SERVICES)),
                ],
            ),
            (
                "loopback AWS_ENDPOINT_URL_S3".to_owned(),
                vec![
                    ("AWS_ENDPOINT_URL_S3", os("http://127.0.0.1:9")),
                    (EXPECT, os("http://127.0.0.1:9/")),
                ],
            ),
            // `endpoint` wins over every SDK source, whatever their values.
            (
                "endpoint over a loopback AWS_ENDPOINT_URL_S3".to_owned(),
                vec![
                    (CONFIG_ENDPOINT, os("http://127.0.0.1:9")),
                    ("AWS_ENDPOINT_URL_S3", os("http://127.0.0.1:8")),
                    (EXPECT, os("http://127.0.0.1:9/")),
                ],
            ),
            (
                "endpoint over a non-loopback AWS_ENDPOINT_URL_S3".to_owned(),
                vec![
                    (CONFIG_ENDPOINT, os("http://127.0.0.1:9")),
                    ("AWS_ENDPOINT_URL_S3", os("http://10.0.0.5:9")),
                    (EXPECT, os("http://127.0.0.1:9/")),
                ],
            ),
            (
                "endpoint over a loopback AWS_ENDPOINT_URL and profile services section".to_owned(),
                vec![
                    (CONFIG_ENDPOINT, os("http://127.0.0.1:9")),
                    ("AWS_ENDPOINT_URL", os("http://127.0.0.1:7")),
                    ("AWS_CONFIG_FILE", other_loopback.into_os_string()),
                    ("AWS_PROFILE", os("ep")),
                    (EXPECT, os("http://127.0.0.1:9/")),
                ],
            ),
        ]
    }

    /// Every `REFUSED_SHAPES` entry through `endpoint` and each SDK source.
    fn syntax_cells(dir: &std::path::Path) -> Vec<EndpointCell> {
        let mut cells = Vec::new();
        for (index, (shape, class)) in REFUSED_SHAPES.into_iter().enumerate() {
            let general = profile_file(dir, &format!("shape-{index}-general"), shape, false);
            let services = profile_file(dir, &format!("shape-{index}-services"), shape, true);
            let sources: [(&str, Vec<(&'static str, std::ffi::OsString)>); 5] = [
                (FROM_CONFIG, vec![(CONFIG_ENDPOINT, os(shape))]),
                ("AWS_ENDPOINT_URL", vec![("AWS_ENDPOINT_URL", os(shape))]),
                (
                    "AWS_ENDPOINT_URL_S3",
                    vec![("AWS_ENDPOINT_URL_S3", os(shape))],
                ),
                (
                    FROM_PROFILE,
                    vec![
                        ("AWS_CONFIG_FILE", general.into_os_string()),
                        ("AWS_PROFILE", os("ep")),
                    ],
                ),
                (
                    FROM_SERVICES,
                    vec![
                        ("AWS_CONFIG_FILE", services.into_os_string()),
                        ("AWS_PROFILE", os("ep")),
                    ],
                ),
            ];
            for (source_index, (source, mut vars)) in sources.into_iter().enumerate() {
                vars.push((EXPECT_SOURCE, os(source)));
                vars.push((EXPECT_REFUSAL, os(class)));
                cells.push((
                    format!("shape {index} through source {source_index} ({source})"),
                    vars,
                ));
            }
        }
        cells
    }

    fn endpoint_cells_in_children() {
        let home = tempfile::tempdir().expect("home");
        let credentials = home.path().join("credentials");
        std::fs::write(&credentials, "").unwrap();
        let empty = home.path().join("empty-config");
        std::fs::write(&empty, "").unwrap();
        let mut cells = http_rule_cells(home.path());
        cells.extend(syntax_cells(home.path()));
        for (cell, vars) in &cells {
            println!("cell: {cell}");
            let mut set: Vec<(&str, &std::ffi::OsStr)> = vec![
                ("HOME", home.path().as_os_str()),
                ("AWS_CONFIG_FILE", empty.as_os_str()),
                ("AWS_SHARED_CREDENTIALS_FILE", credentials.as_os_str()),
                ("AWS_EC2_METADATA_DISABLED", "true".as_ref()),
                ("AWS_ACCESS_KEY_ID", "testing".as_ref()),
                ("AWS_SECRET_ACCESS_KEY", "testing".as_ref()),
            ];
            for (key, value) in vars {
                set.retain(|(k, _)| k != key);
                set.push((key, value.as_os_str()));
            }
            let remove: Vec<&str> = [
                "AWS_ENDPOINT_URL",
                "AWS_ENDPOINT_URL_S3",
                "AWS_IGNORE_CONFIGURED_ENDPOINT_URLS",
                "AWS_PROFILE",
                EXPECT,
                EXPECT_SOURCE,
                EXPECT_REFUSAL,
                CONFIG_ENDPOINT,
            ]
            .into_iter()
            .filter(|key| !vars.iter().any(|(k, _)| k == key))
            .collect();
            run_in_child(ENDPOINT_CELL_TEST, &set, &remove);
        }
    }

    async fn endpoint_cell_in_this_process() {
        let var = |name: &str| std::env::var(name).unwrap_or_default();
        let expected = var(EXPECT);
        let source = format!("(from {})", var(EXPECT_SOURCE));
        let refusal = var(EXPECT_REFUSAL);
        let mut config = S3StoreConfig {
            bucket: "env-bucket".to_owned(),
            prefix: String::new(),
            region: Some("us-east-1".to_owned()),
            endpoint: std::env::var(CONFIG_ENDPOINT).ok(),
            allow_insecure_endpoint: false,
            force_path_style: true,
            upload_part_size_bytes: PART_SIZE,
            max_concurrent_uploads: 4,
        };
        if !refusal.is_empty() {
            for allow_insecure in [false, true] {
                config.allow_insecure_endpoint = allow_insecure;
                let text = match S3Store::open(&config).await {
                    Ok(_) => panic!("open accepted an endpoint the syntax rule refuses"),
                    Err(e) => e.to_string(),
                };
                println!("refusal: {text}");
                assert!(
                    text.contains(&refusal) && text.contains(&source),
                    "expected {refusal:?} and {source:?}: {text}"
                );
                assert!(
                    !text.contains("S3cretPW"),
                    "the message carries the secret: {text}"
                );
            }
            return;
        }
        let refused = expected.is_empty();
        match S3Store::open(&config).await {
            Err(e) if refused => {
                let text = e.to_string();
                println!("refusal: {text}");
                assert!(
                    text.contains("allow_insecure_endpoint")
                        && text.contains("10.0.0.5")
                        && text.contains(&source),
                    "expected the override, the host and {source:?}: {text}"
                );
            }
            Ok(_) if !refused => {}
            Err(e) => panic!("a loopback endpoint must open: {e}"),
            Ok(_) => panic!("open accepted an http:// endpoint to 10.0.0.5 from the environment"),
        }
        config.allow_insecure_endpoint = refused;
        let recorder = StopBeforeTransmit::default();
        let uri = recorder.0.clone();
        let store = S3Store::open_with_interceptor(&config, recorder)
            .await
            .expect("S3Store::open_with_interceptor");
        let _ = store.head("k").await.expect_err("stopped before transmit");
        let uri = uri.lock().unwrap().clone().expect("a request was built");
        let expected = if refused {
            "http://10.0.0.5:9/"
        } else {
            expected.as_str()
        };
        println!("request URI: {uri}");
        assert!(uri.starts_with(expected), "{uri} does not go to {expected}");
    }

    /// Rewrites every request's URI, after the endpoint is resolved and
    /// before signing, to an `http://` URI on a non-loopback host: the shape
    /// a request would have if the SDK sent it somewhere `open` did not
    /// check.
    #[derive(Debug)]
    struct RedirectRequest;

    impl Intercept for RedirectRequest {
        fn name(&self) -> &'static str {
            "RedirectRequest"
        }

        fn modify_before_signing(
            &self,
            context: &mut aws_sdk_s3::config::interceptors::BeforeTransmitInterceptorContextMut<'_>,
            _runtime_components: &RuntimeComponents,
            _cfg: &mut ConfigBag,
        ) -> Result<(), BoxError> {
            context
                .request_mut()
                .set_uri("http://10.0.0.5:9/unused/k")
                .map_err(|e| BoxError::from(e.to_string()))?;
            Ok(())
        }
    }

    /// Without `allow_insecure_endpoint`, a request is checked before it is
    /// sent: an `http://` request to a host that is not loopback is refused
    /// with the override's name, without a connection attempt (10.0.0.5:9
    /// would otherwise time out). No endpoint is needed; the child carries
    /// the suite's own credentials, which the SDK resolves before the check
    /// runs.
    #[tokio::test]
    async fn cleartext_request_to_another_host_is_refused_before_it_is_sent() {
        const NAME: &str = "s3::cleartext_request_to_another_host_is_refused_before_it_is_sent";
        if !is_child(NAME) {
            run_with_test_credentials(NAME);
            return;
        }
        let config = test_config(
            "http://127.0.0.1:9".to_owned(),
            "unused".to_owned(),
            String::new(),
        );
        let store = S3Store::open_with_interceptor(&config, RedirectRequest)
            .await
            .expect("S3Store::open_with_interceptor");
        let started = std::time::Instant::now();
        let err = store.head("k").await.expect_err("refused");
        let text = err.to_string();
        assert!(
            text.contains("refusing to send a plain http:// request to 10.0.0.5")
                && text.contains("allow_insecure_endpoint"),
            "{text}"
        );
        assert!(
            started.elapsed() < std::time::Duration::from_secs(2),
            "refused before any connection attempt"
        );
    }

    /// A TCP listener standing in for a proxy: it accepts connections, keeps
    /// the first bytes each sends, and closes it.
    struct RecordingListener {
        address: std::net::SocketAddr,
        received: Arc<std::sync::Mutex<Vec<u8>>>,
        stop: Arc<AtomicBool>,
        thread: std::thread::JoinHandle<()>,
    }

    impl RecordingListener {
        fn start() -> Self {
            Self::serve(std::net::TcpListener::bind("127.0.0.1:0").expect("bind the listener"))
        }

        /// Record what `listener`'s connections send.
        fn serve(listener: std::net::TcpListener) -> Self {
            use std::io::Read as _;
            listener.set_nonblocking(true).expect("non-blocking accept");
            let address = listener.local_addr().expect("listener address");
            let received = Arc::new(std::sync::Mutex::new(Vec::new()));
            let stop = Arc::new(AtomicBool::new(false));
            let (thread_received, thread_stop) = (received.clone(), stop.clone());
            let thread = std::thread::spawn(move || {
                while !thread_stop.load(Ordering::SeqCst) {
                    match listener.accept() {
                        Ok((mut stream, _)) => {
                            let _ = stream.set_nonblocking(false);
                            let _ = stream
                                .set_read_timeout(Some(std::time::Duration::from_millis(500)));
                            let mut buf = [0u8; 4096];
                            if let Ok(n) = stream.read(&mut buf) {
                                thread_received.lock().unwrap().extend_from_slice(&buf[..n]);
                            }
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            std::thread::sleep(std::time::Duration::from_millis(10));
                        }
                        Err(_) => break,
                    }
                }
            });
            Self {
                address,
                received,
                stop,
                thread,
            }
        }

        /// Stop accepting and return everything received.
        fn finish(self) -> String {
            self.stop.store(true, Ordering::SeqCst);
            self.thread.join().expect("the listener thread");
            String::from_utf8_lossy(&self.received.lock().unwrap()).into_owned()
        }
    }

    /// An `http://` endpoint is reached directly, whatever proxy the
    /// environment names, so a signed clear-text request never goes to a
    /// proxy. The child's `HTTP_PROXY`, `http_proxy`, `ALL_PROXY` and
    /// `all_proxy` name a local listener and the endpoint is a closed
    /// loopback port: the request fails to connect and the listener
    /// receives nothing. With an `https://` endpoint and `HTTPS_PROXY`
    /// naming the same kind of listener, the request does reach it (a
    /// `CONNECT`): the SDK's proxy behaviour for `https://` is unchanged,
    /// and the listener would have seen a proxied request. No endpoint is
    /// needed.
    #[tokio::test]
    async fn http_endpoint_is_reached_without_the_environment_proxy() {
        const NAME: &str = "s3::http_endpoint_is_reached_without_the_environment_proxy";
        const CELL_ENDPOINT: &str = "EXTENDDB_TEST_PROXY_CELL_ENDPOINT";
        if is_child(NAME) {
            let endpoint = std::env::var(CELL_ENDPOINT).expect("the cell's endpoint");
            let config = test_config(endpoint, "proxy-bucket".to_owned(), String::new());
            let store = S3Store::open(&config).await.expect("S3Store::open");
            let text = store
                .head("k")
                .await
                .expect_err("nothing answers")
                .to_string();
            println!("head error: {text}");
            return;
        }
        let closed = std::net::TcpListener::bind("127.0.0.1:0")
            .and_then(|listener| listener.local_addr())
            .expect("a free port")
            .port();
        let cells: [(String, &[&str], bool); 2] = [
            (
                format!("http://127.0.0.1:{closed}"),
                &["HTTP_PROXY", "http_proxy", "ALL_PROXY", "all_proxy"],
                false,
            ),
            (
                format!("https://127.0.0.1:{closed}"),
                &["HTTPS_PROXY", "https_proxy"],
                true,
            ),
        ];
        for (endpoint, variables, proxied) in cells {
            let listener = RecordingListener::start();
            let proxy = std::ffi::OsString::from(format!("http://{}", listener.address));
            let mut extra: Vec<(&str, &std::ffi::OsStr)> = vec![(CELL_ENDPOINT, endpoint.as_ref())];
            extra.extend(variables.iter().map(|name| (*name, proxy.as_os_str())));
            let stdout = run_with_test_credentials_and(NAME, &extra);
            let received = listener.finish();
            println!("{endpoint}: the listener received {} bytes", received.len());
            if proxied {
                assert!(
                    received.starts_with("CONNECT 127.0.0.1:"),
                    "{endpoint} goes through HTTPS_PROXY: {received:?}"
                );
            } else {
                assert!(
                    received.is_empty(),
                    "{endpoint} went through the proxy: {received:?}"
                );
                // The request was sent, to the closed port itself.
                assert!(
                    stdout.contains("head error:") && stdout.contains("refused"),
                    "{endpoint}: the request went to the closed port: {stdout}"
                );
            }
        }
    }

    /// A listener on `::1` and one on `127.0.0.1` at the same port, so
    /// `http://localhost:<port>` (and `<name>.localhost:<port>`) reaches one
    /// of them whichever address the resolver returns first.
    fn localhost_listeners() -> (RecordingListener, RecordingListener) {
        loop {
            let v6 = std::net::TcpListener::bind("[::1]:0").expect("bind ::1");
            let port = v6.local_addr().expect("listener address").port();
            if let Ok(v4) = std::net::TcpListener::bind(("127.0.0.1", port)) {
                return (RecordingListener::serve(v6), RecordingListener::serve(v4));
            }
        }
    }

    /// The two bucket names the SDK routes to the S3 Express backend, an S3
    /// Express One Zone directory bucket (`--x-s3`) and a directory-bucket
    /// access point (`--xa-s3`), are refused at `open`: the store targets
    /// general-purpose buckets. The endpoint is a pair of local listeners,
    /// which must receive nothing (S3 Express session authentication sends
    /// `CreateSession` from a client the SDK builds without the store's
    /// interceptors, to `<bucket>.localhost` here, whatever
    /// `force_path_style` says). The child carries the suite's own
    /// credentials, which a request would need. No endpoint is needed.
    #[tokio::test]
    async fn directory_bucket_is_refused_at_open() {
        const NAME: &str = "s3::directory_bucket_is_refused_at_open";
        const CELL_ENDPOINT: &str = "EXTENDDB_TEST_EXPRESS_ENDPOINT";
        const CELL_BUCKET: &str = "EXTENDDB_TEST_EXPRESS_BUCKET";
        if is_child(NAME) {
            let endpoint = std::env::var(CELL_ENDPOINT).expect("the cell's endpoint");
            let bucket = std::env::var(CELL_BUCKET).expect("the cell's bucket");
            let suffix = &bucket[bucket.rfind("--").expect("an S3 Express suffix")..];
            let config = test_config(endpoint, bucket.clone(), String::new());
            match S3Store::open(&config).await {
                Err(e) => {
                    let text = e.to_string();
                    println!("refusal: {text}");
                    assert!(
                        text.contains(&format!("(the {suffix} suffix)"))
                            && text.contains("directory bucket or a directory-bucket access point")
                            && text.contains("general-purpose buckets only"),
                        "{text}"
                    );
                }
                Ok(store) => {
                    let outcome =
                        tokio::time::timeout(std::time::Duration::from_secs(10), store.head("k"))
                            .await;
                    panic!("open accepted {bucket}; head: {outcome:?}");
                }
            }
            return;
        }
        for bucket in ["foo--use1-az4--x-s3", "foo--use1-az4--xa-s3"] {
            let (v6, v4) = localhost_listeners();
            let endpoint = format!("http://localhost:{}", v6.address.port());
            run_with_test_credentials_and(
                NAME,
                &[
                    (CELL_ENDPOINT, endpoint.as_ref()),
                    (CELL_BUCKET, bucket.as_ref()),
                ],
            );
            let received = format!("{}{}", v6.finish(), v4.finish());
            assert!(
                received.is_empty(),
                "{bucket}: a request was sent: {received:?}"
            );
        }
    }

    /// The client is built with S3 Express session authentication disabled,
    /// so a client that does reach an S3 Express name sends the operation
    /// itself and no `CreateSession` (which the SDK would send first, from
    /// a client rebuilt without the store's interceptors). The refusal of
    /// such names at `open` is bypassed through the test-only `open`, the
    /// endpoint is a local listener, and the child sends one `head`: the
    /// listener must receive that `HEAD` and no `?session` request. The
    /// child carries the suite's own credentials. No endpoint is needed.
    #[tokio::test]
    async fn directory_bucket_client_sends_no_create_session() {
        const NAME: &str = "s3::directory_bucket_client_sends_no_create_session";
        const CELL_ENDPOINT: &str = "EXTENDDB_TEST_EXPRESS_ENDPOINT";
        const CELL_BUCKET: &str = "EXTENDDB_TEST_EXPRESS_BUCKET";
        if is_child(NAME) {
            let endpoint = std::env::var(CELL_ENDPOINT).expect("the cell's endpoint");
            let bucket = std::env::var(CELL_BUCKET).expect("the cell's bucket");
            let config = test_config(endpoint, bucket, String::new());
            let store = S3Store::open_accepting_s3_express_names(&config)
                .await
                .expect("the test-only open accepts the name");
            let outcome =
                tokio::time::timeout(std::time::Duration::from_secs(10), store.head("k")).await;
            println!("head: {outcome:?}");
            return;
        }
        for bucket in ["foo--use1-az4--x-s3", "foo--use1-az4--xa-s3"] {
            let listener = RecordingListener::start();
            let endpoint = format!("http://{}", listener.address);
            run_with_test_credentials_and(
                NAME,
                &[
                    (CELL_ENDPOINT, endpoint.as_ref()),
                    (CELL_BUCKET, bucket.as_ref()),
                ],
            );
            let received = listener.finish();
            println!("{bucket}: the listener received {received:?}");
            assert!(
                !received.contains("?session"),
                "{bucket}: the client sent CreateSession: {received:?}"
            );
            assert!(
                received.contains(&format!("HEAD /{bucket}/k")),
                "{bucket}: the listener did not receive the head: {received:?}"
            );
        }
    }

    /// The SDK's credential and region providers send through clients they
    /// build themselves, which take the HTTP client the shared config was
    /// loaded with; the default one sends an `http://` request to the proxy
    /// named in `HTTP_PROXY` or `ALL_PROXY` whatever the endpoint's host
    /// is. So when any of their endpoints is `http://` (a loopback emulator
    /// passes `open`), `open` gives the chain the proxy-less client. Each
    /// cell runs in a child with one credential endpoint `http://` to a
    /// local listener, a proxy listener in the proxy variables, and an
    /// `https://` S3 endpoint on a closed port, and sends one `head`: the
    /// proxy must receive nothing and the credential listener must receive
    /// the provider's request (the web identity token, the container
    /// authorization token, the SSO access token, the SSO OIDC refresh and
    /// the IMDS token request never reach the proxy). The last cell has an
    /// `http://` STS endpoint beside an `https://` S3 endpoint and
    /// `HTTPS_PROXY`: the S3 client keeps the SDK's default client, so the
    /// proxy receives its `CONNECT`. No endpoint is needed.
    #[tokio::test]
    async fn credential_provider_requests_bypass_the_environment_proxy() {
        if is_child(PROXY_CELL_TEST) {
            let endpoint = std::env::var(PROXY_CELL_ENDPOINT).expect("the cell's endpoint");
            let config = test_config(endpoint, "proxy-bucket".to_owned(), String::new());
            let store = S3Store::open(&config).await.expect("S3Store::open");
            let outcome =
                tokio::time::timeout(std::time::Duration::from_secs(15), store.head("k")).await;
            println!("head: {outcome:?}");
            return;
        }
        let closed = std::net::TcpListener::bind("127.0.0.1:0")
            .and_then(|listener| listener.local_addr())
            .expect("a free port")
            .port();
        let https_closed = format!("https://127.0.0.1:{closed}");
        let home = tempfile::tempdir().expect("home");
        for cell in proxy_cells(home.path(), &https_closed) {
            println!("cell: {}", cell.name);
            run_proxy_cell(&cell, home.path(), &https_closed);
        }
    }

    const PROXY_CELL_TEST: &str = "s3::credential_provider_requests_bypass_the_environment_proxy";
    const PROXY_CELL_ENDPOINT: &str = "EXTENDDB_TEST_PROXY_CELL_ENDPOINT";

    /// One cell of `credential_provider_requests_bypass_the_environment_proxy`.
    struct ProxyCell {
        name: &'static str,
        /// The variable carrying the credential listener's `http://`
        /// endpoint, and the path appended to it (the container URI has one).
        endpoint_var: &'static str,
        endpoint_path: &'static str,
        /// The proxy variables naming the proxy listener.
        proxy_vars: &'static [&'static str],
        /// The other variables the child gets.
        vars: Vec<(&'static str, std::ffi::OsString)>,
        /// What the credential listener must receive; empty for the
        /// `HTTPS_PROXY` cell, whose proxy must receive the `CONNECT`.
        expect: &'static [&'static str],
        /// Whether the credential endpoint is spelt `localhost` (served by
        /// the localhost pair) rather than `127.0.0.1`.
        on_localhost: bool,
        /// The SSO token file to cache before the cell, or `None`.
        sso_cache: Option<String>,
    }

    /// A shared config file under `home` with an `sso_session` profile
    /// named `cell`, for the SSO cells.
    fn sso_cell_profile(home: &std::path::Path) -> std::ffi::OsString {
        let sso_config = home.join("sso-config");
        std::fs::write(
            &sso_config,
            "[profile cell]\nsso_session = extenddb-proxy-cell\n\
             sso_account_id = 123456789012\nsso_role_name = Backup\nregion = us-east-1\n\n\
             [sso-session extenddb-proxy-cell]\nsso_region = us-east-1\n\
             sso_start_url = https://extenddb-proxy-cell.awsapps.com/start\n",
        )
        .unwrap();
        sso_config.into_os_string()
    }

    /// The SSO token cache file for that session: a token that is still
    /// valid goes to SSO, an expired one with a refresh token goes to SSO
    /// OIDC first.
    fn sso_cached_token(expires_at: &str) -> String {
        format!(
            "{{\"accessToken\":\"SSO-ACCESS-TOKEN-SECRET\",\"expiresAt\":\"{expires_at}\",\
             \"refreshToken\":\"SSO-REFRESH-TOKEN-SECRET\",\"clientId\":\"cell-client\",\
             \"clientSecret\":\"SSO-CLIENT-SECRET\",\
             \"registrationExpiresAt\":\"2099-01-01T00:00:00Z\",\
             \"startUrl\":\"https://extenddb-proxy-cell.awsapps.com/start\",\
             \"region\":\"us-east-1\"}}"
        )
    }

    /// The cells, with their fixtures written under `home`.
    fn proxy_cells(home: &std::path::Path, https_closed: &str) -> Vec<ProxyCell> {
        let token = home.join("token");
        std::fs::write(&token, "WEB-IDENTITY-TOKEN-SECRET").unwrap();
        let web_identity = [
            ("AWS_WEB_IDENTITY_TOKEN_FILE", token.into_os_string()),
            ("AWS_ROLE_ARN", os("arn:aws:iam::123456789012:role/backup")),
        ];
        let sso = [
            ("AWS_CONFIG_FILE", sso_cell_profile(home)),
            ("AWS_PROFILE", os("cell")),
        ];
        vec![
            ProxyCell {
                name: "STS through AWS_ENDPOINT_URL_STS, HTTP_PROXY",
                endpoint_var: "AWS_ENDPOINT_URL_STS",
                endpoint_path: "",
                proxy_vars: &["HTTP_PROXY"],
                vars: web_identity.to_vec(),
                expect: &["POST / ", "AssumeRoleWithWebIdentity"],
                on_localhost: false,
                sso_cache: None,
            },
            ProxyCell {
                name: "STS through AWS_ENDPOINT_URL on localhost, ALL_PROXY",
                endpoint_var: "AWS_ENDPOINT_URL",
                endpoint_path: "",
                proxy_vars: &["ALL_PROXY", "all_proxy"],
                vars: web_identity.to_vec(),
                expect: &["POST / ", "AssumeRoleWithWebIdentity"],
                on_localhost: true,
                sso_cache: None,
            },
            ProxyCell {
                name: "container credentials through AWS_CONTAINER_CREDENTIALS_FULL_URI, HTTP_PROXY",
                endpoint_var: "AWS_CONTAINER_CREDENTIALS_FULL_URI",
                endpoint_path: "/creds",
                proxy_vars: &["HTTP_PROXY", "http_proxy"],
                vars: vec![(
                    "AWS_CONTAINER_AUTHORIZATION_TOKEN",
                    os("CONTAINER-AUTH-TOKEN-SECRET"),
                )],
                expect: &["GET /creds ", "CONTAINER-AUTH-TOKEN-SECRET"],
                on_localhost: false,
                sso_cache: None,
            },
            ProxyCell {
                name: "SSO through AWS_ENDPOINT_URL_SSO, HTTP_PROXY",
                endpoint_var: "AWS_ENDPOINT_URL_SSO",
                endpoint_path: "",
                proxy_vars: &["HTTP_PROXY"],
                vars: sso
                    .iter()
                    .cloned()
                    .chain([("AWS_ENDPOINT_URL_SSO_OIDC", os(https_closed))])
                    .collect(),
                expect: &["GET /federation/credentials", "SSO-ACCESS-TOKEN-SECRET"],
                on_localhost: false,
                sso_cache: Some(sso_cached_token("2099-01-01T00:00:00Z")),
            },
            ProxyCell {
                name: "SSO OIDC refresh through AWS_ENDPOINT_URL_SSO_OIDC, HTTP_PROXY",
                endpoint_var: "AWS_ENDPOINT_URL_SSO_OIDC",
                endpoint_path: "",
                proxy_vars: &["HTTP_PROXY"],
                vars: sso
                    .iter()
                    .cloned()
                    .chain([("AWS_ENDPOINT_URL_SSO", os(https_closed))])
                    .collect(),
                expect: &["POST /token "],
                on_localhost: false,
                sso_cache: Some(sso_cached_token("2020-01-01T00:00:00Z")),
            },
            ProxyCell {
                name: "IMDS through AWS_EC2_METADATA_SERVICE_ENDPOINT, HTTP_PROXY",
                endpoint_var: "AWS_EC2_METADATA_SERVICE_ENDPOINT",
                endpoint_path: "",
                proxy_vars: &["HTTP_PROXY"],
                vars: vec![],
                expect: &["PUT /latest/api/token "],
                on_localhost: false,
                sso_cache: None,
            },
            ProxyCell {
                name: "https S3 endpoint through HTTPS_PROXY beside an http STS endpoint",
                endpoint_var: "AWS_ENDPOINT_URL_STS",
                endpoint_path: "",
                proxy_vars: &["HTTPS_PROXY"],
                vars: vec![
                    ("AWS_ACCESS_KEY_ID", os("testing")),
                    ("AWS_SECRET_ACCESS_KEY", os("testing")),
                ],
                expect: &[],
                on_localhost: false,
                sso_cache: None,
            },
        ]
    }

    /// Run one cell in a child and check what its two listeners received.
    fn run_proxy_cell(cell: &ProxyCell, home: &std::path::Path, https_closed: &str) {
        let credentials = home.join("credentials");
        std::fs::write(&credentials, "").unwrap();
        let empty = home.join("empty-config");
        std::fs::write(&empty, "").unwrap();
        // The SSO provider reads `$HOME/.aws/sso/cache/<sha1 of the session
        // name>.json`; this is the hash of `extenddb-proxy-cell`.
        let cache = home.join(".aws/sso/cache");
        std::fs::create_dir_all(&cache).unwrap();
        let cache_file = cache.join("93252d8a890975da4aeadaa1b27aa1096c184105.json");
        match &cell.sso_cache {
            Some(token) => std::fs::write(&cache_file, token).unwrap(),
            None => {
                let _ = std::fs::remove_file(&cache_file);
            }
        }
        let proxy = RecordingListener::start();
        // The credential listener: one on `127.0.0.1`, or the localhost
        // pair for an endpoint spelt `localhost`.
        let (credential_listener, localhost_pair, credential) = if cell.on_localhost {
            let (v6, v4) = localhost_listeners();
            let url = format!("http://localhost:{}", v6.address.port());
            (None, Some((v6, v4)), url)
        } else {
            let listener = RecordingListener::start();
            let url = format!("http://{}", listener.address);
            (Some(listener), None, url)
        };
        let credential_endpoint = format!("{credential}{}", cell.endpoint_path);
        let proxy_url = format!("http://{}", proxy.address);
        let mut set: Vec<(&str, &std::ffi::OsStr)> = vec![
            ("HOME", home.as_os_str()),
            ("AWS_CONFIG_FILE", empty.as_os_str()),
            ("AWS_SHARED_CREDENTIALS_FILE", credentials.as_os_str()),
            ("AWS_REGION", "us-east-1".as_ref()),
            (PROXY_CELL_ENDPOINT, https_closed.as_ref()),
            (cell.endpoint_var, credential_endpoint.as_ref()),
        ];
        if cell.endpoint_var != "AWS_EC2_METADATA_SERVICE_ENDPOINT" {
            set.push(("AWS_EC2_METADATA_DISABLED", "true".as_ref()));
        }
        for name in cell.proxy_vars {
            set.push((name, proxy_url.as_ref()));
        }
        for (key, value) in &cell.vars {
            set.retain(|(k, _)| k != key);
            set.push((key, value.as_os_str()));
        }
        let remove: Vec<&str> = [
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
            "AWS_PROFILE",
            "AWS_WEB_IDENTITY_TOKEN_FILE",
            "AWS_ROLE_ARN",
            "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
            "AWS_CONTAINER_CREDENTIALS_FULL_URI",
            "AWS_CONTAINER_AUTHORIZATION_TOKEN",
            "AWS_EC2_METADATA_SERVICE_ENDPOINT",
            "AWS_EC2_METADATA_DISABLED",
            "AWS_ENDPOINT_URL",
            "AWS_ENDPOINT_URL_S3",
            "AWS_ENDPOINT_URL_STS",
            "AWS_ENDPOINT_URL_SSO",
            "AWS_ENDPOINT_URL_SSO_OIDC",
            CONFIG_ENDPOINT,
        ]
        .into_iter()
        .filter(|key| !set.iter().any(|(k, _)| k == key))
        .collect();
        let stdout = run_in_child(PROXY_CELL_TEST, &set, &remove);
        let received_by_proxy = proxy.finish();
        let received_by_credential = match (credential_listener, localhost_pair) {
            (Some(listener), _) => listener.finish(),
            (None, Some((v6, v4))) => format!("{}{}", v6.finish(), v4.finish()),
            (None, None) => unreachable!("one listener per cell"),
        };
        check_proxy_cell(cell, &received_by_proxy, &received_by_credential, &stdout);
    }

    /// What the two listeners of a cell must have received.
    fn check_proxy_cell(
        cell: &ProxyCell,
        received_by_proxy: &str,
        received_by_credential: &str,
        stdout: &str,
    ) {
        println!(
            "{}: the proxy received {} bytes, the credential listener {} bytes",
            cell.name,
            received_by_proxy.len(),
            received_by_credential.len()
        );
        if cell.expect.is_empty() {
            assert!(
                received_by_proxy.starts_with("CONNECT 127.0.0.1:"),
                "{}: the https S3 request did not go through HTTPS_PROXY: {received_by_proxy:?}",
                cell.name
            );
            assert!(
                received_by_credential.is_empty(),
                "{}: static credentials sent to STS: {received_by_credential:?}",
                cell.name
            );
            return;
        }
        assert!(
            received_by_proxy.is_empty(),
            "{}: the proxy received a credential request: {received_by_proxy:?}",
            cell.name
        );
        for expected in cell.expect {
            assert!(
                received_by_credential.contains(expected),
                "{}: the credential listener did not receive {expected:?}: \
                 {received_by_credential:?}\n--- child stdout\n{stdout}",
                cell.name
            );
        }
    }

    /// The IMDS endpoint setting (`AWS_EC2_METADATA_SERVICE_ENDPOINT`, or
    /// the profile's `ec2_metadata_service_endpoint`) is held at `open` to
    /// the syntax rule and to IMDS's own `http://` rule: a loopback host or
    /// one of the two IMDS addresses, `169.254.169.254` and
    /// `[fd00:ec2::254]`. Each cell runs in a child with no region and no
    /// credentials in its environment, so the SDK would query IMDS at `open`
    /// for the region: an `http://` endpoint to a non-loopback address of
    /// this host is refused, naming the client, the setting and the host,
    /// and the listener there must receive nothing; with
    /// `allow_insecure_endpoint` it opens. The refusal holds when a variable
    /// of the form `AWS_EC2_METADATA_SERVICE_ENDPOINT_` (a trailing
    /// underscore, the shape a shell loop over `AWS_..._${SERVICE}` with an
    /// empty service produces), which the SDK gives no meaning to, holds an
    /// `https://` value beside it. A loopback endpoint and the two
    /// IMDS addresses open (the child then sets `region`, so no request is
    /// sent), and a value carrying user information is refused without the
    /// secret. On a host without a non-loopback address the cells use
    /// 10.0.0.5 and only the refusals are checked (the test prints which).
    /// No endpoint is needed.
    #[tokio::test]
    async fn imds_endpoint_follows_the_http_rule() {
        if is_child(IMDS_CELL_TEST) {
            imds_cell_in_this_process().await;
            return;
        }
        let address = non_loopback_address();
        let listener = address.map(|ip| {
            RecordingListener::serve(std::net::TcpListener::bind((ip, 0)).expect("bind"))
        });
        let (host, non_loopback) = if let Some(listener) = &listener {
            println!("listener on {}", listener.address);
            (
                listener.address.ip().to_string(),
                format!("http://{}", listener.address),
            )
        } else {
            println!("no non-loopback address on this host: refusals only");
            ("10.0.0.5".to_owned(), "http://10.0.0.5:9".to_owned())
        };
        let home = tempfile::tempdir().expect("home");
        let credentials = home.path().join("credentials");
        std::fs::write(&credentials, "").unwrap();
        let empty = home.path().join("empty-config");
        std::fs::write(&empty, "").unwrap();
        for (cell, vars) in imds_cells(home.path(), &non_loopback) {
            println!("cell: {cell}");
            let mut set: Vec<(&str, &std::ffi::OsStr)> = vec![
                ("HOME", home.path().as_os_str()),
                ("AWS_CONFIG_FILE", empty.as_os_str()),
                ("AWS_SHARED_CREDENTIALS_FILE", credentials.as_os_str()),
                (EXPECT_HOST, host.as_ref()),
            ];
            for (key, value) in &vars {
                set.retain(|(k, _)| k != key);
                set.push((key, value.as_os_str()));
            }
            let remove: Vec<&str> = [
                "AWS_ACCESS_KEY_ID",
                "AWS_SECRET_ACCESS_KEY",
                "AWS_SESSION_TOKEN",
                "AWS_PROFILE",
                "AWS_REGION",
                "AWS_DEFAULT_REGION",
                "AWS_EC2_METADATA_DISABLED",
                "AWS_EC2_METADATA_SERVICE_ENDPOINT",
                "AWS_EC2_METADATA_SERVICE_ENDPOINT_",
                "AWS_EC2_METADATA_SERVICE_ENDPOINT_MODE",
                "AWS_WEB_IDENTITY_TOKEN_FILE",
                "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
                "AWS_CONTAINER_CREDENTIALS_FULL_URI",
                "AWS_ENDPOINT_URL",
                "AWS_ENDPOINT_URL_S3",
                "AWS_ENDPOINT_URL_STS",
                "AWS_ENDPOINT_URL_SSO",
                "AWS_ENDPOINT_URL_SSO_OIDC",
            ]
            .into_iter()
            .filter(|key| !vars.iter().any(|(k, _)| k == key))
            .collect();
            run_in_child(IMDS_CELL_TEST, &set, &remove);
        }
        if let Some(listener) = listener {
            let received = listener.finish();
            assert!(
                received.is_empty(),
                "an IMDS request reached the listener: {received:?}"
            );
        }
    }

    const IMDS_CELL_TEST: &str = "s3::imds_endpoint_follows_the_http_rule";
    /// `refused:<source text>`, `refused:syntax:<source text>` or `accepted`.
    const IMDS_CELL_EXPECT: &str = "EXTENDDB_TEST_IMDS_EXPECT";
    const FROM_IMDS_PROFILE: &str =
        "ec2_metadata_service_endpoint in the shared AWS config file's profile";

    /// The cells of `imds_endpoint_follows_the_http_rule`: each one's name
    /// and the variables its child gets, with `non_loopback` the listener's
    /// `http://` endpoint.
    fn imds_cells(
        dir: &std::path::Path,
        non_loopback: &str,
    ) -> Vec<(&'static str, Vec<(&'static str, std::ffi::OsString)>)> {
        let profile = dir.join("imds-config");
        std::fs::write(
            &profile,
            format!("[default]\nec2_metadata_service_endpoint = {non_loopback}\n"),
        )
        .unwrap();
        let env_var = "AWS_EC2_METADATA_SERVICE_ENDPOINT";
        vec![
            (
                "non-loopback AWS_EC2_METADATA_SERVICE_ENDPOINT",
                vec![
                    (env_var, os(non_loopback)),
                    (IMDS_CELL_EXPECT, os(&format!("refused:{env_var}"))),
                ],
            ),
            // The SDK reads the variable itself; a lookup through a service
            // key with an empty service id would read this one first.
            (
                "non-loopback AWS_EC2_METADATA_SERVICE_ENDPOINT beside an https \
                 AWS_EC2_METADATA_SERVICE_ENDPOINT_",
                vec![
                    (env_var, os(non_loopback)),
                    (
                        "AWS_EC2_METADATA_SERVICE_ENDPOINT_",
                        os("https://127.0.0.1:1"),
                    ),
                    (IMDS_CELL_EXPECT, os(&format!("refused:{env_var}"))),
                ],
            ),
            (
                "non-loopback profile ec2_metadata_service_endpoint",
                vec![
                    ("AWS_CONFIG_FILE", profile.into_os_string()),
                    (
                        IMDS_CELL_EXPECT,
                        os(&format!("refused:{FROM_IMDS_PROFILE}")),
                    ),
                ],
            ),
            (
                "user information in AWS_EC2_METADATA_SERVICE_ENDPOINT",
                vec![
                    (env_var, os("http://u:S3cretPW@169.254.169.254")),
                    (IMDS_CELL_EXPECT, os(&format!("refused:syntax:{env_var}"))),
                ],
            ),
            (
                "loopback AWS_EC2_METADATA_SERVICE_ENDPOINT",
                vec![
                    (env_var, os("http://127.0.0.1:9")),
                    (IMDS_CELL_EXPECT, os("accepted")),
                ],
            ),
            (
                "IMDS IPv4 address",
                vec![
                    (env_var, os("http://169.254.169.254")),
                    (IMDS_CELL_EXPECT, os("accepted")),
                ],
            ),
            (
                "IMDS IPv6 address",
                vec![
                    (env_var, os("http://[fd00:ec2::254]")),
                    (IMDS_CELL_EXPECT, os("accepted")),
                ],
            ),
        ]
    }

    async fn imds_cell_in_this_process() {
        let expect = std::env::var(IMDS_CELL_EXPECT).expect("the cell's expectation");
        let host = std::env::var(EXPECT_HOST).unwrap_or_default();
        let mut config = S3StoreConfig {
            bucket: "imds-bucket".to_owned(),
            prefix: String::new(),
            region: None,
            endpoint: Some("https://127.0.0.1:1".to_owned()),
            allow_insecure_endpoint: false,
            force_path_style: true,
            upload_part_size_bytes: PART_SIZE,
            max_concurrent_uploads: 4,
        };
        if expect == "accepted" {
            config.region = Some("us-east-1".to_owned());
            S3Store::open(&config)
                .await
                .expect("a loopback or IMDS-address endpoint opens");
            return;
        }
        let source = expect.strip_prefix("refused:").expect("refused:<source>");
        let syntax = source.strip_prefix("syntax:");
        match S3Store::open(&config).await {
            Err(e) => {
                let text = e.to_string();
                println!("refusal: {text}");
                match syntax {
                    Some(source) => assert!(
                        text.starts_with(&format!("the IMDS endpoint (from {source})"))
                            && text.contains("user information")
                            && !text.contains("S3cretPW"),
                        "{text}"
                    ),
                    None => assert!(
                        text.starts_with(&format!("the IMDS endpoint (from {source})"))
                            && text.contains(&format!("uses http:// to {host},"))
                            && text.contains("169.254.169.254")
                            && text.contains("allow_insecure_endpoint")
                            && text.contains("whether or not"),
                        "expected {source:?} and {host}: {text}"
                    ),
                }
            }
            Ok(store) => {
                let outcome =
                    tokio::time::timeout(std::time::Duration::from_secs(10), store.head("k")).await;
                panic!("open accepted the IMDS endpoint; head: {outcome:?}");
            }
        }
        if syntax.is_none() {
            config.allow_insecure_endpoint = true;
            config.region = Some("us-east-1".to_owned());
            S3Store::open(&config)
                .await
                .expect("the override accepts the IMDS endpoint too");
        }
    }

    /// An address of this host that is not loopback (the source address of
    /// the default route), for a listener a clear-text request must not
    /// reach. `None` on a host without one.
    fn non_loopback_address() -> Option<std::net::IpAddr> {
        let socket = std::net::UdpSocket::bind("0.0.0.0:0").ok()?;
        // Connecting a UDP socket sends nothing; it picks the route.
        socket.connect("192.0.2.1:9").ok()?;
        let ip = socket.local_addr().ok()?.ip();
        (!ip.is_loopback() && !ip.is_unspecified()).then_some(ip)
    }

    const CREDENTIAL_CELL_TEST: &str = "s3::credential_provider_endpoints_follow_the_http_rule";
    /// The client and source the refusal must name, as the message spells
    /// them (`the STS endpoint (from AWS_ENDPOINT_URL)`).
    const EXPECT_CREDENTIAL_REFUSAL: &str = "EXTENDDB_TEST_EXPECT_CREDENTIAL_REFUSAL";
    /// The host the refusal must name.
    const EXPECT_HOST: &str = "EXTENDDB_TEST_EXPECT_HOST";

    /// The SDK's credential providers send through STS, SSO and SSO OIDC
    /// clients they build themselves, which read `AWS_ENDPOINT_URL`, their
    /// own `AWS_ENDPOINT_URL_<SERVICE>`, and the profile's `endpoint_url`
    /// and `services` entries, but never `endpoint`, and have neither the
    /// store's request check nor its proxy-less client. So `open` holds
    /// those endpoints to the same rules whether or not the configured
    /// credentials use them. Each cell runs in a child whose credentials
    /// come from a web identity token (the provider that sends the token to
    /// STS in its request body), with the S3 endpoint `https://` and one
    /// credential endpoint `http://` to a listener on a non-loopback
    /// address of this host. `open` must refuse, naming the client, the
    /// setting and the host and never the token; with
    /// `allow_insecure_endpoint` it opens. The listener must receive
    /// nothing. On a host without a non-loopback address the cells use
    /// 10.0.0.5 and only the refusals are checked (the test prints which).
    /// No endpoint is needed.
    #[tokio::test]
    async fn credential_provider_endpoints_follow_the_http_rule() {
        if is_child(CREDENTIAL_CELL_TEST) {
            credential_cell_in_this_process().await;
            return;
        }
        let address = non_loopback_address();
        let listener = address.map(|ip| {
            RecordingListener::serve(std::net::TcpListener::bind((ip, 0)).expect("bind"))
        });
        let (host, general) = if let Some(listener) = &listener {
            println!("listener on {}", listener.address);
            (
                listener.address.ip().to_string(),
                format!("http://{}", listener.address),
            )
        } else {
            println!("no non-loopback address on this host: refusals only");
            ("10.0.0.5".to_owned(), "http://10.0.0.5:9".to_owned())
        };
        let home = tempfile::tempdir().expect("home");
        let token = home.path().join("token");
        std::fs::write(&token, "JWT-SECRET-TOKEN-1234").unwrap();
        let credentials = home.path().join("credentials");
        std::fs::write(&credentials, "").unwrap();
        let empty = home.path().join("empty-config");
        std::fs::write(&empty, "").unwrap();
        let mut cells = credential_env_cells(&general);
        cells.extend(credential_file_cells(home.path(), &general));
        for (cell, vars) in &cells {
            println!("cell: {cell}");
            let mut set: Vec<(&str, &std::ffi::OsStr)> = vec![
                ("HOME", home.path().as_os_str()),
                ("AWS_CONFIG_FILE", empty.as_os_str()),
                ("AWS_SHARED_CREDENTIALS_FILE", credentials.as_os_str()),
                ("AWS_EC2_METADATA_DISABLED", "true".as_ref()),
                ("AWS_REGION", "us-east-1".as_ref()),
                ("AWS_WEB_IDENTITY_TOKEN_FILE", token.as_os_str()),
                (
                    "AWS_ROLE_ARN",
                    "arn:aws:iam::123456789012:role/backup".as_ref(),
                ),
                (EXPECT_HOST, host.as_ref()),
            ];
            for (key, value) in vars {
                set.retain(|(k, _)| k != key);
                set.push((key, value.as_os_str()));
            }
            let remove: Vec<&str> = [
                "AWS_ACCESS_KEY_ID",
                "AWS_SECRET_ACCESS_KEY",
                "AWS_SESSION_TOKEN",
                "AWS_PROFILE",
                "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
                "AWS_CONTAINER_CREDENTIALS_FULL_URI",
                "AWS_ENDPOINT_URL",
                "AWS_ENDPOINT_URL_",
                "AWS_ENDPOINT_URL_S3",
                "AWS_ENDPOINT_URL_STS",
                "AWS_ENDPOINT_URL_SSO",
                "AWS_ENDPOINT_URL_SSO_OIDC",
                CONFIG_ENDPOINT,
            ]
            .into_iter()
            .filter(|key| !vars.iter().any(|(k, _)| k == key))
            .collect();
            run_in_child(CREDENTIAL_CELL_TEST, &set, &remove);
        }
        if let Some(listener) = listener {
            let received = listener.finish();
            assert!(
                received.is_empty(),
                "a credential request reached the listener: {received:?}"
            );
        }
    }

    /// The cells of `credential_provider_endpoints_follow_the_http_rule` set
    /// in the environment: each one's name and the variables its child
    /// gets, with `general` the `http://` endpoint of the listener.
    fn credential_env_cells(
        general: &str,
    ) -> Vec<(&'static str, Vec<(&'static str, std::ffi::OsString)>)> {
        let https = os("https://127.0.0.1:1");
        let sts = |source: &str| os(&format!("the STS endpoint (from {source})"));
        vec![
            (
                "AWS_ENDPOINT_URL beside an https AWS_ENDPOINT_URL_S3",
                vec![
                    ("AWS_ENDPOINT_URL_S3", https.clone()),
                    ("AWS_ENDPOINT_URL", os(general)),
                    (EXPECT_CREDENTIAL_REFUSAL, sts("AWS_ENDPOINT_URL")),
                ],
            ),
            (
                "AWS_ENDPOINT_URL beside an https endpoint",
                vec![
                    (CONFIG_ENDPOINT, https.clone()),
                    ("AWS_ENDPOINT_URL", os(general)),
                    (EXPECT_CREDENTIAL_REFUSAL, sts("AWS_ENDPOINT_URL")),
                ],
            ),
            (
                "AWS_ENDPOINT_URL_STS",
                vec![
                    (CONFIG_ENDPOINT, https.clone()),
                    ("AWS_ENDPOINT_URL_STS", os(general)),
                    (EXPECT_CREDENTIAL_REFUSAL, sts("AWS_ENDPOINT_URL_STS")),
                ],
            ),
            (
                "AWS_ENDPOINT_URL_SSO",
                vec![
                    (CONFIG_ENDPOINT, https.clone()),
                    // STS gets its own https endpoint, so a build without
                    // this check sends its request nowhere.
                    ("AWS_ENDPOINT_URL_STS", https.clone()),
                    ("AWS_ENDPOINT_URL_SSO", os(general)),
                    (
                        EXPECT_CREDENTIAL_REFUSAL,
                        os("the SSO endpoint (from AWS_ENDPOINT_URL_SSO)"),
                    ),
                ],
            ),
            (
                "AWS_ENDPOINT_URL_SSO_OIDC",
                vec![
                    (CONFIG_ENDPOINT, https.clone()),
                    // STS gets its own https endpoint, so a build without
                    // this check sends its request nowhere.
                    ("AWS_ENDPOINT_URL_STS", https.clone()),
                    ("AWS_ENDPOINT_URL_SSO_OIDC", os(general)),
                    (
                        EXPECT_CREDENTIAL_REFUSAL,
                        os("the SSO OIDC endpoint (from AWS_ENDPOINT_URL_SSO_OIDC)"),
                    ),
                ],
            ),
            // Every credential client has its own https variable, so only
            // the general check sees AWS_ENDPOINT_URL; it must read that
            // variable itself, not a service key whose empty service id
            // would read `AWS_ENDPOINT_URL_` (a trailing underscore, which
            // the SDK gives no meaning to) first.
            (
                "AWS_ENDPOINT_URL beside an https AWS_ENDPOINT_URL_ and https variables for \
                 every credential client",
                vec![
                    (CONFIG_ENDPOINT, https.clone()),
                    ("AWS_ENDPOINT_URL_STS", https.clone()),
                    ("AWS_ENDPOINT_URL_SSO", https.clone()),
                    ("AWS_ENDPOINT_URL_SSO_OIDC", https.clone()),
                    ("AWS_ENDPOINT_URL_", https.clone()),
                    ("AWS_ENDPOINT_URL", os(general)),
                    (
                        EXPECT_CREDENTIAL_REFUSAL,
                        os("the general AWS endpoint (from AWS_ENDPOINT_URL)"),
                    ),
                ],
            ),
        ]
    }

    /// The cells of `credential_provider_endpoints_follow_the_http_rule` set
    /// in a shared config file written under `dir`.
    fn credential_file_cells(
        dir: &std::path::Path,
        general: &str,
    ) -> Vec<(&'static str, Vec<(&'static str, std::ffi::OsString)>)> {
        let config_file = |name: &str, text: String| {
            let path = dir.join(name);
            std::fs::write(&path, text).unwrap();
            path.into_os_string()
        };
        let https = os("https://127.0.0.1:1");
        let sts = |source: &str| os(&format!("the STS endpoint (from {source})"));
        vec![
            (
                "profile endpoint_url beside an https endpoint",
                vec![
                    (CONFIG_ENDPOINT, https.clone()),
                    (
                        "AWS_CONFIG_FILE",
                        config_file(
                            "general",
                            format!("[default]\nendpoint_url = {general}\nregion = us-east-1\n"),
                        ),
                    ),
                    (EXPECT_CREDENTIAL_REFUSAL, sts(FROM_PROFILE)),
                ],
            ),
            (
                "services section sts entry",
                vec![
                    (CONFIG_ENDPOINT, https.clone()),
                    (
                        "AWS_CONFIG_FILE",
                        config_file(
                            "sts-entry",
                            format!(
                                "[default]\nservices = svc\nregion = us-east-1\n\n\
                                 [services svc]\nsts =\n  endpoint_url = {general}\n"
                            ),
                        ),
                    ),
                    (
                        EXPECT_CREDENTIAL_REFUSAL,
                        sts(
                            "endpoint_url under sts in the shared AWS config file's services \
                             section",
                        ),
                    ),
                ],
            ),
            // Every credential client has its own https entry, so only the
            // general setting is http://; any other SDK client would use it.
            (
                "profile endpoint_url under https entries for every credential client",
                vec![
                    (CONFIG_ENDPOINT, https.clone()),
                    (
                        "AWS_CONFIG_FILE",
                        config_file(
                            "general-under-entries",
                            format!(
                                "[default]\nendpoint_url = {general}\nservices = svc\n\
                                 region = us-east-1\n\n[services svc]\n\
                                 sts =\n  endpoint_url = https://127.0.0.1:1\n\
                                 sso =\n  endpoint_url = https://127.0.0.1:1\n\
                                 sso-oidc =\n  endpoint_url = https://127.0.0.1:1\n"
                            ),
                        ),
                    ),
                    (
                        EXPECT_CREDENTIAL_REFUSAL,
                        os(&format!("the general AWS endpoint (from {FROM_PROFILE})")),
                    ),
                ],
            ),
        ]
    }

    async fn credential_cell_in_this_process() {
        let var = |name: &str| std::env::var(name).unwrap_or_default();
        let expected = var(EXPECT_CREDENTIAL_REFUSAL);
        let host = var(EXPECT_HOST);
        let mut config = S3StoreConfig {
            bucket: "cred-bucket".to_owned(),
            prefix: String::new(),
            region: Some("us-east-1".to_owned()),
            endpoint: std::env::var(CONFIG_ENDPOINT).ok(),
            allow_insecure_endpoint: false,
            force_path_style: true,
            upload_part_size_bytes: PART_SIZE,
            max_concurrent_uploads: 4,
        };
        match S3Store::open(&config).await {
            Err(e) => {
                let text = e.to_string();
                println!("refusal: {text}");
                assert!(
                    text.starts_with(&expected)
                        && text.contains(&format!("uses http:// to {host},"))
                        && text.contains("allow_insecure_endpoint")
                        && text.contains("whether or not"),
                    "expected {expected:?} and {host}: {text}"
                );
                assert!(!text.contains("JWT-SECRET"), "{text}");
            }
            Ok(store) => {
                // Show where the credential request goes, then fail.
                let outcome =
                    tokio::time::timeout(std::time::Duration::from_secs(10), store.head("k")).await;
                panic!("open accepted an http:// credential endpoint; head: {outcome:?}");
            }
        }
        config.allow_insecure_endpoint = true;
        S3Store::open(&config)
            .await
            .expect("the override accepts a credential endpoint too");
    }

    /// An SSO profile resolves through the SSO provider, which needs
    /// aws-config's `sso` feature; without it the SDK answers every request
    /// with "requires following cargo feature(s) enabled: sso". Runs in a
    /// child with only the SSO profile in its environment. No token is
    /// cached for the profile, so the provider fails, but with an SSO token
    /// error rather than the missing-feature error. No endpoint is needed.
    #[tokio::test]
    async fn sso_profile_is_supported() {
        const NAME: &str = "s3::sso_profile_is_supported";
        if !is_child(NAME) {
            let home = tempfile::tempdir().expect("home");
            let config = home.path().join("config");
            std::fs::write(
                &config,
                "[profile extenddb-sso]\n\
                 sso_session = extenddb\n\
                 sso_account_id = 123456789012\n\
                 sso_role_name = Backup\n\
                 region = us-east-1\n\
                 \n\
                 [sso-session extenddb]\n\
                 sso_region = us-east-1\n\
                 sso_start_url = https://extenddb-test.awsapps.com/start\n",
            )
            .unwrap();
            let credentials = home.path().join("credentials");
            std::fs::write(&credentials, "").unwrap();
            run_in_child(
                NAME,
                &[
                    ("HOME", home.path().as_os_str()),
                    ("AWS_CONFIG_FILE", config.as_os_str()),
                    ("AWS_SHARED_CREDENTIALS_FILE", credentials.as_os_str()),
                    ("AWS_PROFILE", std::ffi::OsStr::new("extenddb-sso")),
                    ("AWS_EC2_METADATA_DISABLED", std::ffi::OsStr::new("true")),
                ],
                &[
                    "AWS_ACCESS_KEY_ID",
                    "AWS_SECRET_ACCESS_KEY",
                    "AWS_SESSION_TOKEN",
                    "AWS_WEB_IDENTITY_TOKEN_FILE",
                    "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
                    "AWS_CONTAINER_CREDENTIALS_FULL_URI",
                ],
            );
            return;
        }
        let config = test_config(
            "http://127.0.0.1:9".to_owned(),
            "unused".to_owned(),
            String::new(),
        );
        let store = S3Store::open(&config).await.expect("S3Store::open");
        // Credentials are resolved before the request is sent, so the
        // failure is the provider's, reported with its cause chain.
        let err = store.head("k").await.expect_err("no SSO token is cached");
        let text = err.to_string();
        println!("SSO provider error: {text}");
        assert!(matches!(err, StoreError::Transport { .. }), "{text}");
        assert!(!text.contains("cargo feature"), "{text}");
        assert!(text.contains("SSO") || text.contains("sso"), "{text}");
    }
}
