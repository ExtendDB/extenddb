// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Cases for the input side at Start: the bucket check and the listing.

use super::super::InputCodec;
use super::super::tests::read_all;
use super::*;

/// A bucket that exists passes the check, with no prefix, with a prefix
/// that holds objects, and with one that holds none.
pub(super) async fn the_bucket_check_passes_a_bucket_that_exists<F: StoreFixture>(f: &F) {
    let bucket = f.bucket().await;
    f.source().check_bucket(&at(&bucket), "").await.unwrap();
    f.put(&bucket, "in/part-0.json", &rows(0, 1)).await;
    f.source().check_bucket(&at(&bucket), "in/").await.unwrap();
    f.source()
        .check_bucket(&at(&bucket), "nothing/")
        .await
        .unwrap();
}

/// A bucket that does not exist is `S3NoSuchBucket`.
pub(super) async fn the_bucket_check_answers_a_missing_bucket_with_no_such_bucket<
    F: StoreFixture,
>(
    f: &F,
) {
    let missing = f.missing_bucket();
    let error = f
        .source()
        .check_bucket(&at(&missing), "in/")
        .await
        .expect_err("the bucket does not exist");
    assert_location_fault::<F>(
        &source_fault(error),
        "S3NoSuchBucket",
        NO_SUCH_BUCKET,
        Tail::AmazonS3 {
            status: 404,
            code: "NoSuchBucket",
        },
    );
}

/// The prefix matches keys byte by byte: `orders/2026` matches
/// `orders/2026-09-10.json` and everything under `orders/2026/`, and nothing
/// that only shares the first bytes up to a different one.
pub(super) async fn the_listing_matches_the_prefix_byte_wise<F: StoreFixture>(f: &F) {
    let bucket = f.bucket().await;
    for key in [
        "orders/2026-09-10.json",
        "orders/2026/nested/part.json",
        "orders/2025-12-31.json",
        "orders2026.json",
        "other/orders/2026-09-10.json",
    ] {
        f.put(&bucket, key, &rows(0, 1)).await;
    }
    let files = f
        .source()
        .list(&at(&bucket), "orders/2026", 100)
        .await
        .unwrap();
    assert_eq!(
        paths(&files),
        ["orders/2026-09-10.json", "orders/2026/nested/part.json"]
    );
}

/// The listing is in byte order of the whole key, whatever the nesting:
/// `a-b.json`, `a.json` and `a/b.json` sort by their second byte.
pub(super) async fn the_listing_is_in_byte_order_of_the_whole_key<F: StoreFixture>(f: &F) {
    let bucket = f.bucket().await;
    for key in [
        "b.json", "a/z.json", "B.json", "a.json", "a/b.json", "a-b.json",
    ] {
        f.put(&bucket, key, &rows(0, 1)).await;
    }
    let files = f.source().list(&at(&bucket), "", 100).await.unwrap();
    assert_eq!(
        paths(&files),
        [
            "B.json", "a-b.json", "a.json", "a/b.json", "a/z.json", "b.json"
        ]
    );
}

/// Collecting more than `max_files` ends the listing with `max_files + 1`
/// entries, so the caller can tell the bound was passed; which of the
/// matches they are is the store's (the filesystem walk stops in directory
/// order), and they come in byte order. A listing within the bound is whole.
pub(super) async fn the_listing_stops_one_past_max_files<F: StoreFixture>(f: &F) {
    let bucket = f.bucket().await;
    let keys: Vec<String> = (0..5)
        .map(|index| format!("in/part-{index}.json"))
        .collect();
    for (index, key) in keys.iter().enumerate() {
        f.put(&bucket, key, &rows(index, 1)).await;
    }
    let files = f.source().list(&at(&bucket), "in/", 2).await.unwrap();
    let listed = paths(&files);
    assert_eq!(listed.len(), 3, "{listed:?}");
    assert!(
        listed.windows(2).all(|pair| pair[0] < pair[1]),
        "{listed:?}"
    );
    assert!(
        listed.iter().all(|path| keys.iter().any(|key| key == path)),
        "{listed:?}"
    );
    let files = f.source().list(&at(&bucket), "in/", 4).await.unwrap();
    assert_eq!(files.len(), 5);
    let files = f.source().list(&at(&bucket), "in/", 5).await.unwrap();
    assert_eq!(paths(&files), keys);
}

/// An empty prefix lists every object in the bucket, each with its size.
pub(super) async fn an_empty_prefix_lists_every_object_with_its_size<F: StoreFixture>(f: &F) {
    let bucket = f.bucket().await;
    let objects = [
        ("deep/er/part.json", rows(0, 3)),
        ("in/part.json", rows(3, 1)),
        ("top.json", rows(4, 2)),
    ];
    for (key, body) in &objects {
        f.put(&bucket, key, body).await;
    }
    let files = f.source().list(&at(&bucket), "", 100).await.unwrap();
    let listed: Vec<(&str, u64)> = files.iter().map(|f| (f.path.as_str(), f.size)).collect();
    let written: Vec<(&str, u64)> = objects
        .iter()
        .map(|(key, body)| (*key, body.len() as u64))
        .collect();
    assert_eq!(listed, written);
}

/// A prefix nothing lies under lists nothing, and is not a fault.
pub(super) async fn a_prefix_that_matches_nothing_lists_nothing<F: StoreFixture>(f: &F) {
    let bucket = f.bucket().await;
    f.put(&bucket, "in/part.json", &rows(0, 1)).await;
    for prefix in ["nothing/", "in/part.json.gz", "in/q"] {
        let files = f.source().list(&at(&bucket), prefix, 100).await.unwrap();
        assert!(files.is_empty(), "{prefix:?} listed {files:?}");
    }
}

/// On S3 the listing carries each object's entity tag, unquoted: for an
/// object written with one PUT, the hex MD5 of its bytes.
pub(super) async fn on_s3_the_listing_carries_each_objects_etag<F: StoreFixture>(f: &F) {
    let bucket = f.bucket().await;
    let bodies = [rows(0, 2), rows(2, 3)];
    for (index, body) in bodies.iter().enumerate() {
        f.put(&bucket, &format!("in/part-{index}.json"), body).await;
    }
    let files = f.source().list(&at(&bucket), "in/", 10).await.unwrap();
    let etags: Vec<Option<String>> = files.iter().map(|file| file.etag.clone()).collect();
    let digests: Vec<Option<String>> = bodies.iter().map(|body| Some(md5_hex(body))).collect();
    assert_eq!(etags, digests);
}

/// The filesystem store lists no entity tag; the reader computes it from
/// the bytes it reads.
pub(super) async fn on_the_filesystem_store_the_listing_carries_no_etag<F: StoreFixture>(f: &F) {
    let bucket = f.bucket().await;
    f.put(&bucket, "in/part.json", &rows(0, 2)).await;
    let file = only_file(f, &bucket, "in/").await;
    assert_eq!(file.etag, None);
}

/// Reads `file` whole and returns its lines.
async fn read_back<F: StoreFixture>(f: &F, bucket: &str, file: &SourceFile) -> Vec<String> {
    let mut reader = f
        .source()
        .open(&at(bucket), &recorded(file), InputCodec::None)
        .await
        .unwrap();
    read_all(&mut reader, MAX_LINE).await
}

/// On S3 a key may hold characters no path rule of the filesystem store
/// allows: a space, `+` and `%`, a backslash, a tab, non-ASCII letters, and
/// a zero-byte key ending in `/`. Each is listed under its own key and read
/// back from it.
pub(super) async fn on_s3_keys_with_spaces_backslashes_and_tabs_are_listed_and_read_back<
    F: StoreFixture,
>(
    f: &F,
) {
    let bucket = f.bucket().await;
    let objects = [
        ("in/a b+c%25.json", rows(0, 1)),
        ("in/back\\slash.json", rows(1, 1)),
        ("in/dir/", Vec::new()),
        ("in/tab\tkey.json", rows(2, 1)),
        ("in/\u{fc}nicode.json", rows(3, 1)),
    ];
    for (key, body) in &objects {
        f.put(&bucket, key, body).await;
    }
    let files = f.source().list(&at(&bucket), "in/", 100).await.unwrap();
    let mut expected: Vec<&str> = objects.iter().map(|(key, _)| *key).collect();
    expected.sort_unstable();
    assert_eq!(paths(&files), expected);
    for file in &files {
        let (_, body) = objects.iter().find(|(key, _)| *key == file.path).unwrap();
        let text = String::from_utf8(body.clone()).unwrap();
        let want: Vec<&str> = text.lines().collect();
        assert_eq!(read_back(f, &bucket, file).await, want, "{:?}", file.path);
    }
}

/// On S3 a key may hold a doubled `/`, a leading `/`, and `.` and `..`
/// segments. The listing gives the keys the service holds, and each is read
/// back from it; a key the service kept as written is listed and read under
/// that key. A service that stores objects as files may rewrite such keys,
/// and a key it rewrote is reported as skipped.
pub(super) async fn on_s3_path_like_keys_are_listed_and_read_back_where_the_service_keeps_them<
    F: StoreFixture,
>(
    f: &F,
) {
    let bucket = f.bucket().await;
    let objects = [
        ("a3//b//c/part.json", rows(0, 1)),
        ("/lead/part.json", rows(1, 1)),
        ("x/../y/part.json", rows(2, 1)),
        ("dot/./part.json", rows(3, 1)),
    ];
    for (key, body) in &objects {
        f.put(&bucket, key, body).await;
    }
    let held = f.keys(&bucket).await;
    let files = f.source().list(&at(&bucket), "", 100).await.unwrap();
    assert_eq!(paths(&files), held);
    for file in &files {
        assert_eq!(
            read_back(f, &bucket, file).await.len(),
            1,
            "{:?}",
            file.path
        );
    }
    for (index, (key, _)) in objects.iter().enumerate() {
        if !held.iter().any(|held| held == key) {
            eprintln!("SKIP {key:?}: the service holds the object under another key");
            continue;
        }
        let file = files.iter().find(|file| file.path == *key).unwrap();
        assert_eq!(
            read_back(f, &bucket, file).await,
            lines(index, 1),
            "{key:?}"
        );
    }
}
