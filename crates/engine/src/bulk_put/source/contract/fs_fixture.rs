// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The filesystem store's fixture: a fresh store root under the system temp
//! directory, removed when the fixture drops. A bucket is a directory under
//! the caller's account directory and an object a file beneath it, at the
//! key read as a relative path.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

use super::super::tests::Root;
use super::super::{FsSink, FsSource};
use super::{ACCOUNT, BulkSink, BulkSource, StoreFixture};

pub(super) struct FsFixture {
    root: Root,
    source: FsSource,
    sink: FsSink,
    buckets: AtomicUsize,
}

impl FsFixture {
    pub(super) fn new() -> Self {
        let root = Root::new();
        Self {
            source: FsSource { root: root.root() },
            sink: FsSink { root: root.root() },
            root,
            buckets: AtomicUsize::new(0),
        }
    }

    /// The path of `key` in `bucket`. A key is a relative path here; the
    /// cases give keys with a leading `/` to the S3 store only.
    fn path(&self, bucket: &str, key: &str) -> PathBuf {
        assert!(!key.starts_with('/'), "{key:?} is not a relative path");
        self.root.root().join(ACCOUNT).join(bucket).join(key)
    }
}

/// The regular files beneath `dir`, as paths relative to `base` joined with
/// `/`.
fn walk(base: &Path, dir: &Path, out: &mut Vec<String>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries {
        let path = entry.unwrap().path();
        let kind = std::fs::symlink_metadata(&path).unwrap().file_type();
        if kind.is_dir() {
            walk(base, &path, out);
        } else if kind.is_file() {
            let relative = path.strip_prefix(base).unwrap();
            let parts: Vec<String> = relative
                .components()
                .map(|part| part.as_os_str().to_string_lossy().into_owned())
                .collect();
            out.push(parts.join("/"));
        }
    }
}

impl StoreFixture for FsFixture {
    const S3: bool = false;

    fn source(&self) -> &dyn BulkSource {
        &self.source
    }

    fn sink(&self) -> &dyn BulkSink {
        &self.sink
    }

    async fn bucket(&self) -> String {
        let name = format!("bucket-{}", self.buckets.fetch_add(1, Ordering::Relaxed));
        self.root.bucket_dir(ACCOUNT, &name);
        name
    }

    fn missing_bucket(&self) -> String {
        "bucket-missing".to_owned()
    }

    async fn put(&self, bucket: &str, key: &str, body: &[u8]) {
        let path = self.path(bucket, key);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, body).unwrap();
    }

    async fn get(&self, bucket: &str, key: &str) -> Option<Vec<u8>> {
        match std::fs::read(self.path(bucket, key)) {
            Ok(body) => Some(body),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => panic!("reading {key:?}: {error}"),
        }
    }

    async fn keys(&self, bucket: &str) -> Vec<String> {
        let base = self.root.root().join(ACCOUNT).join(bucket);
        let mut keys = Vec::new();
        walk(&base, &base, &mut keys);
        keys.sort();
        keys
    }

    async fn pending(&self, bucket: &str) -> Vec<String> {
        // The report writer and the manifest write render under
        // `{name}.tmp-{owner}` and rename at the end.
        self.keys(bucket)
            .await
            .into_iter()
            .filter(|key| {
                key.rsplit('/')
                    .next()
                    .is_some_and(|name| name.contains(".tmp-"))
            })
            .collect()
    }

    async fn cleanup(self) {
        // The root is removed when the fixture drops.
    }
}
