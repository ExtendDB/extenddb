// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The store contract: the cases every bulk store passes, run once against
//! the filesystem store and once against the S3 store, the way the catalog
//! contract runs once per backend. A [`StoreFixture`] makes buckets and
//! objects with the store's own primitives, beside the source and sink the
//! cases drive, and reads back what the sink wrote.
//!
//! The filesystem fixture works in a fresh store root under the system temp
//! directory. The S3 fixture works against the service that
//! `EXTENDDB_TEST_S3_ENDPOINT` names (for example `http://127.0.0.1:7070`),
//! in region us-east-1 with path-style addressing, with credentials from the
//! standard AWS variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.
//! Without those variables every S3 case prints a SKIP line and passes; with
//! `EXTENDDB_TEST_S3_REQUIRED` set, a missing variable fails the case
//! instead. Each S3 case works in buckets of its own and removes them when
//! it passes, so a failed case leaves its buckets for inspection.
//!
//! A case that holds on one store only names that store and prints a SKIP
//! line on the other.

mod fs_fixture;
mod list;
mod owner;
mod read;
mod s3_fixture;
mod sink;

use extenddb_storage::InputFile;

use super::tests::{LEASE_OWNER, OP_ID, OWNER};
use super::{BucketRef, BulkSink, BulkSource, SinkError, SourceError, SourceFile, StoreFault};
use fs_fixture::FsFixture;
use s3_fixture::S3Fixture;

/// The caller's account in every case.
const ACCOUNT: &str = OWNER;

/// The operation every report and manifest in the cases belongs to.
const OPERATION: &str = OP_ID;

/// The lease owner the runner passes to the sink.
const LEASE: &str = LEASE_OWNER;

/// The line cap every read in the cases uses.
const MAX_LINE: usize = 1 << 20;

/// The head of every missing-bucket message, S3's own and the filesystem
/// store's.
const NO_SUCH_BUCKET: &str = "The specified bucket does not exist";

/// What a case needs from a store beside its source and sink: buckets, and
/// objects written and read with the store's own primitives.
trait StoreFixture: Sync {
    /// Whether this is the S3 store, whose faults carry S3's message and the
    /// tail of the call that met them.
    const S3: bool;

    fn source(&self) -> &dyn BulkSource;

    fn sink(&self) -> &dyn BulkSink;

    /// A new, empty bucket of the caller's.
    async fn bucket(&self) -> String;

    /// A bucket name that names no bucket.
    fn missing_bucket(&self) -> String;

    async fn put(&self, bucket: &str, key: &str, body: &[u8]);

    /// The object at `key`, or `None` when there is none.
    async fn get(&self, bucket: &str, key: &str) -> Option<Vec<u8>>;

    /// Every object in `bucket`, in byte order, keyed as the store holds it.
    async fn keys(&self, bucket: &str) -> Vec<String>;

    /// The writes started in `bucket` and not completed: multipart uploads
    /// in progress on S3, temp files on the filesystem.
    async fn pending(&self, bucket: &str) -> Vec<String>;

    /// Remove what the case made.
    async fn cleanup(self)
    where
        Self: Sized;
}

/// The tail S3 faults carry, by the call that met them (`s3::error`).
enum Tail {
    /// Input reads, the Start-time listing and the manifest write.
    AmazonS3 { status: u16, code: &'static str },
    /// Report writes and the write check of a report key.
    S3 { status: u16 },
}

/// `fault` is a location fault with `code` and the message `head`: exactly
/// `head` on the filesystem store; on S3, S3's own message, which starts
/// with `head`, followed by `tail`.
fn assert_location_fault<F: StoreFixture>(fault: &StoreFault, code: &str, head: &str, tail: Tail) {
    assert_eq!(fault.code, code, "{fault:?}");
    assert!(!fault.after_first_byte, "{fault:?}");
    if !F::S3 {
        assert_eq!(fault.message, head);
        return;
    }
    assert!(fault.message.starts_with(head), "{}", fault.message);
    let (opening, closing) = match tail {
        Tail::AmazonS3 { status, code } => (
            format!(
                " (Service: Amazon S3; Status Code: {status}; Error Code: {code}; Request ID: "
            ),
            "; Proxy: null)",
        ),
        Tail::S3 { status } => (
            format!(" (Service: S3, Status Code: {status}, Request ID: "),
            ") (SDK Attempt Count: 1)",
        ),
    };
    assert!(fault.message.contains(&opening), "{}", fault.message);
    assert!(fault.message.ends_with(closing), "{}", fault.message);
}

/// The bucket `bucket` of the caller, with no expected owner.
fn at(bucket: &str) -> BucketRef<'_> {
    BucketRef::new(ACCOUNT, bucket, None)
}

/// Input row `index` as one JSON line, newline excluded.
fn row(index: usize) -> String {
    format!("{{\"Item\":{{\"pk\":{{\"S\":\"row-{index:06}\"}}}}}}")
}

/// Rows `first` to `first + count - 1`, one per line.
fn rows(first: usize, count: usize) -> Vec<u8> {
    (first..first + count)
        .flat_map(|index| format!("{}\n", row(index)).into_bytes())
        .collect()
}

/// Rows `first` to `first + count - 1` as a reader yields them.
fn lines(first: usize, count: usize) -> Vec<String> {
    (first..first + count).map(row).collect()
}

/// The lowercase hex MD5 of `bytes`, the entity tag of a single-part
/// object.
fn md5_hex(bytes: &[u8]) -> String {
    format!("{:x}", <md5::Md5 as md5::Digest>::digest(bytes))
}

/// The record an enumerated file becomes at Start.
fn recorded(file: &SourceFile) -> InputFile {
    InputFile {
        path: file.path.clone(),
        size: i64::try_from(file.size).unwrap(),
        rows: None,
        etag: file.etag.clone(),
        abandon: None,
    }
}

fn paths(files: &[SourceFile]) -> Vec<&str> {
    files.iter().map(|file| file.path.as_str()).collect()
}

/// The one file the listing of `prefix` gives.
async fn only_file<F: StoreFixture>(f: &F, bucket: &str, prefix: &str) -> SourceFile {
    let mut files = f.source().list(&at(bucket), prefix, 10).await.unwrap();
    assert_eq!(files.len(), 1, "{files:?}");
    files.remove(0)
}

fn source_fault(error: SourceError) -> StoreFault {
    match error {
        SourceError::Store(fault) => fault,
        other => panic!("expected a store fault, got {other:?}"),
    }
}

fn sink_fault(error: SinkError) -> StoreFault {
    match error {
        SinkError::Store(fault) => fault,
        other => panic!("expected a store fault, got {other:?}"),
    }
}

/// One test per case and store: `both` cases on each store; a case that
/// holds on one store only runs there and prints a SKIP line on the other.
macro_rules! store_contract {
    (
        both: [$($bm:ident::$both:ident),* $(,)?],
        filesystem_only: [$($fm:ident::$fs:ident),* $(,)?],
        s3_only: [$($sm:ident::$s3:ident),* $(,)?] $(,)?
    ) => {
        mod filesystem {
            $(
                #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
                async fn $both() {
                    let fixture = super::FsFixture::new();
                    super::$bm::$both(&fixture).await;
                    super::StoreFixture::cleanup(fixture).await;
                }
            )*
            $(
                #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
                async fn $fs() {
                    let fixture = super::FsFixture::new();
                    super::$fm::$fs(&fixture).await;
                    super::StoreFixture::cleanup(fixture).await;
                }
            )*
            $(
                #[test]
                fn $s3() {
                    eprintln!("SKIP {}: the case holds on the S3 store only", stringify!($s3));
                }
            )*
        }

        mod s3 {
            $(
                #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
                async fn $both() {
                    let Some(fixture) = super::S3Fixture::connect(stringify!($both)).await else {
                        return;
                    };
                    super::$bm::$both(&fixture).await;
                    super::StoreFixture::cleanup(fixture).await;
                }
            )*
            $(
                #[test]
                fn $fs() {
                    eprintln!(
                        "SKIP {}: the case holds on the filesystem store only",
                        stringify!($fs)
                    );
                }
            )*
            $(
                #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
                async fn $s3() {
                    let Some(fixture) = super::S3Fixture::connect(stringify!($s3)).await else {
                        return;
                    };
                    super::$sm::$s3(&fixture).await;
                    super::StoreFixture::cleanup(fixture).await;
                }
            )*
        }
    };
}

store_contract! {
    both: [
        list::the_bucket_check_passes_a_bucket_that_exists,
        list::the_bucket_check_answers_a_missing_bucket_with_no_such_bucket,
        list::the_listing_matches_the_prefix_byte_wise,
        list::the_listing_is_in_byte_order_of_the_whole_key,
        list::the_listing_stops_one_past_max_files,
        list::an_empty_prefix_lists_every_object_with_its_size,
        list::a_prefix_that_matches_nothing_lists_nothing,
        read::every_gzip_member_is_read,
        read::every_zstd_frame_is_read,
        read::an_object_without_the_codecs_magic_is_not_compressed,
        read::a_read_to_the_end_reports_its_bytes_and_the_objects_etag,
        read::an_object_larger_than_one_read_part_is_read_whole,
        sink::a_report_is_installed_under_the_service_key_for_each_prefix_form,
        sink::rotation_installs_two_objects_and_neither_is_visible_before_finish,
        sink::a_writer_dropped_without_finish_leaves_nothing_behind,
        sink::a_report_larger_than_one_upload_part_is_one_object,
        sink::a_second_render_replaces_the_first,
        sink::the_manifest_is_installed_under_the_service_key_and_replaced,
        sink::the_location_check_passes_a_bucket_that_exists_and_writes_nothing,
        sink::the_location_check_answers_a_missing_bucket_with_no_such_bucket,
    ],
    filesystem_only: [
        list::on_the_filesystem_store_the_listing_carries_no_etag,
        owner::on_the_filesystem_store_an_expected_owner_is_access_denied,
        read::on_the_filesystem_store_a_read_stops_at_the_listed_size_after_the_file_grew,
        read::on_the_filesystem_store_a_read_ends_early_on_a_file_that_shrank,
    ],
    s3_only: [
        list::on_s3_the_listing_carries_each_objects_etag,
        list::on_s3_keys_with_spaces_backslashes_and_tabs_are_listed_and_read_back,
        list::on_s3_path_like_keys_are_listed_and_read_back_where_the_service_keeps_them,
        owner::on_s3_every_request_carries_the_expected_owner_when_one_is_set,
        read::on_s3_an_object_written_again_after_its_listing_is_overwritten,
    ],
}
