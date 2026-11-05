// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Cases for reading one enumerated object: the size ceiling, the codecs,
//! and what a read to the end reports.

use super::super::tests::{gzip_bytes, read_all, zstd_bytes};
use super::super::{InputCodec, LineReader};
use super::*;

async fn open<F: StoreFixture>(
    f: &F,
    bucket: &str,
    file: &SourceFile,
    codec: InputCodec,
) -> Box<dyn LineReader> {
    f.source()
        .open(&at(bucket), &recorded(file), codec)
        .await
        .unwrap()
}

/// On the filesystem store the reader reads no further than the size the
/// listing gave: rows added to the file after the listing are not read.
pub(super) async fn on_the_filesystem_store_a_read_stops_at_the_listed_size_after_the_file_grew<
    F: StoreFixture,
>(
    f: &F,
) {
    let bucket = f.bucket().await;
    let listed = rows(0, 3);
    f.put(&bucket, "in/part.json", &listed).await;
    let file = only_file(f, &bucket, "in/").await;
    let mut grown = listed.clone();
    grown.extend(rows(3, 2));
    f.put(&bucket, "in/part.json", &grown).await;
    let mut reader = open(f, &bucket, &file, InputCodec::None).await;
    assert_eq!(read_all(&mut reader, MAX_LINE).await, lines(0, 3));
    assert_eq!(reader.bytes_consumed(), listed.len() as u64);
}

/// On the filesystem store a file that shrank after the listing reads to
/// its new end and no further, without a fault.
pub(super) async fn on_the_filesystem_store_a_read_ends_early_on_a_file_that_shrank<
    F: StoreFixture,
>(
    f: &F,
) {
    let bucket = f.bucket().await;
    f.put(&bucket, "in/part.json", &rows(0, 4)).await;
    let file = only_file(f, &bucket, "in/").await;
    let shrunk = rows(0, 2);
    f.put(&bucket, "in/part.json", &shrunk).await;
    let mut reader = open(f, &bucket, &file, InputCodec::None).await;
    assert_eq!(read_all(&mut reader, MAX_LINE).await, lines(0, 2));
    assert_eq!(reader.bytes_consumed(), shrunk.len() as u64);
}

/// On S3 an object written again after the listing, larger or smaller, is
/// not the object listed: every GET carries the listed ETag in `If-Match`,
/// so the open answers `Overwritten` and no byte of the new object is read.
pub(super) async fn on_s3_an_object_written_again_after_its_listing_is_overwritten<
    F: StoreFixture,
>(
    f: &F,
) {
    let bucket = f.bucket().await;
    for (prefix, replacement) in [("grew/", rows(0, 5)), ("shrank/", rows(0, 2))] {
        let key = format!("{prefix}part.json");
        f.put(&bucket, &key, &rows(0, 3)).await;
        let file = only_file(f, &bucket, prefix).await;
        f.put(&bucket, &key, &replacement).await;
        let opened = f
            .source()
            .open(&at(&bucket), &recorded(&file), InputCodec::None)
            .await;
        assert!(
            matches!(opened, Err(SourceError::Overwritten)),
            "{prefix}: {:?}",
            opened.err()
        );
    }
}

/// Reads `body`, stored under `codec`, to its end and checks the lines, the
/// compressed bytes consumed and the entity tag the reader reports.
async fn read_encoded<F: StoreFixture>(f: &F, body: &[u8], codec: InputCodec, count: usize) {
    let bucket = f.bucket().await;
    f.put(&bucket, "in/part.bin", body).await;
    let file = only_file(f, &bucket, "in/").await;
    let mut reader = open(f, &bucket, &file, codec).await;
    assert_eq!(read_all(&mut reader, MAX_LINE).await, lines(0, count));
    assert_eq!(reader.bytes_consumed(), body.len() as u64);
    assert_eq!(reader.etag(), Some(md5_hex(body)));
}

/// A gzip object of several members is read whole, every member included.
pub(super) async fn every_gzip_member_is_read<F: StoreFixture>(f: &F) {
    let mut body = gzip_bytes(&rows(0, 3));
    body.extend(gzip_bytes(&rows(3, 2)));
    body.extend(gzip_bytes(&rows(5, 4)));
    read_encoded(f, &body, InputCodec::Gzip, 9).await;
}

/// A zstd object of several frames is read whole, every frame included.
pub(super) async fn every_zstd_frame_is_read<F: StoreFixture>(f: &F) {
    let mut body = zstd_bytes(&rows(0, 300));
    body.extend(zstd_bytes(&rows(300, 2)));
    read_encoded(f, &body, InputCodec::Zstd, 302).await;
}

/// Under a codec, an object that does not start with the codec's magic is
/// `NotCompressed`, refused at open.
pub(super) async fn an_object_without_the_codecs_magic_is_not_compressed<F: StoreFixture>(f: &F) {
    let bucket = f.bucket().await;
    f.put(&bucket, "in/part.json", &rows(0, 2)).await;
    let file = only_file(f, &bucket, "in/").await;
    for codec in [InputCodec::Gzip, InputCodec::Zstd] {
        let opened = f.source().open(&at(&bucket), &recorded(&file), codec).await;
        assert!(
            matches!(opened, Err(SourceError::NotCompressed)),
            "{codec:?}: {:?}",
            opened.err()
        );
    }
}

/// A read to the end has consumed the listed size and reports the object's
/// entity tag: the listed one on S3, the MD5 of the bytes on the filesystem
/// store, which for an object written with one PUT are the same. The
/// store's `etag` and `line_count`, which the manifest falls back on, agree
/// with the read.
pub(super) async fn a_read_to_the_end_reports_its_bytes_and_the_objects_etag<F: StoreFixture>(
    f: &F,
) {
    let bucket = f.bucket().await;
    let body = rows(0, 10);
    f.put(&bucket, "in/part.json", &body).await;
    let file = only_file(f, &bucket, "in/").await;
    let expected = file.etag.clone().unwrap_or_else(|| md5_hex(&body));
    assert_eq!(expected, md5_hex(&body));
    let mut reader = open(f, &bucket, &file, InputCodec::None).await;
    assert_eq!(reader.etag(), None, "no tag before the end of the object");
    assert_eq!(read_all(&mut reader, MAX_LINE).await, lines(0, 10));
    assert_eq!(reader.bytes_consumed(), body.len() as u64);
    assert_eq!(reader.etag(), Some(expected.clone()));
    let input = recorded(&file);
    assert_eq!(
        f.source().etag(&at(&bucket), &input).await.unwrap(),
        expected
    );
    let counted = f
        .source()
        .line_count(&at(&bucket), &input, InputCodec::None)
        .await
        .unwrap();
    assert_eq!(counted, 10);
}

/// An object larger than one read part (8 MiB on S3, where each later part
/// is a ranged GET conditional on the first part's entity tag) is read
/// whole, with the tag of the object.
pub(super) async fn an_object_larger_than_one_read_part_is_read_whole<F: StoreFixture>(f: &F) {
    let bucket = f.bucket().await;
    // 10,488,000 bytes in rows of 114.
    let count = 92_000;
    let pad = "p".repeat(66);
    let body: Vec<u8> = (0..count)
        .flat_map(|index| {
            format!(
                "{{\"Item\":{{\"pk\":{{\"S\":\"{index:08}\"}},\"pad\":{{\"S\":\"{pad}\"}}}}}}\n"
            )
            .into_bytes()
        })
        .collect();
    assert!(body.len() > 9 * 1024 * 1024);
    f.put(&bucket, "in/big.json", &body).await;
    let file = only_file(f, &bucket, "in/").await;
    let mut reader = open(f, &bucket, &file, InputCodec::None).await;
    let mut read = 0;
    while let Some(line) = reader.next_line(MAX_LINE).await.unwrap() {
        assert!(
            line.contains(&format!("\"{read:08}\"")),
            "row {read}: {line}"
        );
        read += 1;
    }
    assert_eq!(read, count);
    assert_eq!(reader.bytes_consumed(), body.len() as u64);
    assert_eq!(reader.etag(), Some(md5_hex(&body)));
}
