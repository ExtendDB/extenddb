// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Tests for the bulk put source and sink, split by subject.

use std::io::Write;
use std::path::Path;

use super::*;

mod codec;
mod dropped_fill;
mod jail;
mod reader;
mod rotation;
mod sink;

pub(super) const OWNER: &str = "111122223333";

pub(super) const OTHER: &str = "999999999999";

pub(super) const OP_ID: &str = "01789060589767-da9ddd4f";

pub(super) const LEASE_OWNER: &str = "worker-1";

/// A fresh directory acting as a configured bulk store root.
pub(super) struct Root(PathBuf);

impl Root {
    pub(super) fn new() -> Self {
        let dir = std::env::temp_dir().join(format!("eb-bulk-src-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir.canonicalize().unwrap())
    }

    pub(super) fn root(&self) -> Arc<PathBuf> {
        Arc::new(self.0.clone())
    }

    pub(super) fn bucket_dir(&self, account: &str, bucket: &str) -> PathBuf {
        let dir = self.0.join(account).join(bucket);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// Create `<root>/<account>/<bucket>/<rel>` with content.
    fn file(&self, account: &str, bucket: &str, rel: &str, contents: &[u8]) -> PathBuf {
        let path = self.0.join(account).join(bucket).join(rel);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, contents).unwrap();
        path
    }
}

impl Drop for Root {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub(super) fn source(root: &Root) -> FsSource {
    FsSource { root: root.root() }
}

pub(super) fn sink(root: &Root) -> FsSink {
    FsSink { root: root.root() }
}

/// The bucket `bucket` of the caller `OWNER`, with no expected owner.
pub(super) fn at(bucket: &str) -> BucketRef<'_> {
    BucketRef::new(OWNER, bucket, None)
}

/// An enumerated input file of `size` bytes, as the record stores it.
pub(super) fn input_file(path: &str, size: u64) -> InputFile {
    InputFile {
        path: path.to_owned(),
        size: i64::try_from(size).unwrap(),
        rows: None,
        etag: None,
        abandon: None,
    }
}

pub(super) fn gzip_bytes(content: &[u8]) -> Vec<u8> {
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    enc.write_all(content).unwrap();
    enc.finish().unwrap()
}

/// One zstd frame. `Fastest` emits compressed blocks, so a frame of a
/// few hundred rows spans several blocks and a cut lands mid-block.
pub(super) fn zstd_bytes(content: &[u8]) -> Vec<u8> {
    ruzstd::encoding::compress_to_vec(content, ruzstd::encoding::CompressionLevel::Fastest)
}

pub(super) async fn read_all(
    reader: &mut Box<dyn LineReader>,
    max_line_bytes: usize,
) -> Vec<String> {
    let mut lines = Vec::new();
    while let Some(line) = reader.next_line(max_line_bytes).await.unwrap() {
        lines.push(line);
    }
    lines
}

/// Open one report writer, write the records as a single page, and
/// finish it, the way the runner renders one kind.
pub(super) async fn render_one(
    sink: &FsSink,
    prefix: &str,
    records: Vec<Value>,
) -> Result<String, SinkError> {
    let mut writer = sink
        .open(
            &at("loads"),
            prefix,
            OP_ID,
            ReportKind::Error,
            LEASE_OWNER,
            &OutputSse::None,
        )
        .await?;
    writer.write_page(records).await?;
    writer.finish().await
}

/// Open a directory handle the way the enumeration does, for tests
/// that drive the walk directly.
#[cfg(unix)]
pub(super) fn open_dir_handle(path: &Path) -> std::fs::File {
    use rustix::fs::{CWD, Mode, OFlags, openat};
    let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
    std::fs::File::from(openat(CWD, path, flags, Mode::empty()).unwrap())
}

pub(super) fn decode_report(path: &Path) -> Vec<String> {
    let compressed = std::fs::read(path).unwrap();
    let mut decoder = flate2::read::GzDecoder::new(compressed.as_slice());
    let mut plain = String::new();
    std::io::Read::read_to_string(&mut decoder, &mut plain).unwrap();
    plain.lines().map(str::to_owned).collect()
}
