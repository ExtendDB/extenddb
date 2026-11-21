// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Bulk put source/sink codec tests.

use super::*;

#[tokio::test]
async fn zstd_round_trip_counts_compressed_bytes_and_reports_the_compressed_etag() {
    let root = Root::new();
    let mut plain = Vec::new();
    for i in 0..400 {
        plain.extend_from_slice(format!("{{\"Item\":{{\"pk\":{{\"S\":\"k{i}\"}}}}}}\n").as_bytes());
    }
    let compressed = zstd_bytes(&plain);
    let size = u64::try_from(compressed.len()).unwrap();
    root.file(OWNER, "loads", "part-000.json.zst", &compressed);
    let src = source(&root);
    let mut reader = src
        .open(
            &at("loads"),
            &input_file("part-000.json.zst", size),
            InputCodec::Zstd,
        )
        .await
        .unwrap();
    let lines = read_all(&mut reader, 4096).await;
    assert_eq!(lines.len(), 400);
    assert_eq!(lines[399], "{\"Item\":{\"pk\":{\"S\":\"k399\"}}}");
    assert_eq!(
        reader.bytes_consumed(),
        size,
        "consumed bytes are compressed bytes"
    );
    let hashed = src
        .etag(&at("loads"), &input_file("part-000.json.zst", size))
        .await
        .unwrap();
    assert_eq!(reader.etag(), Some(hashed));
    assert_eq!(
        src.line_count(
            &at("loads"),
            &input_file("part-000.json.zst", size),
            InputCodec::Zstd
        )
        .await
        .unwrap(),
        400
    );
}

#[tokio::test]
async fn two_concatenated_zstd_frames_are_read_whole() {
    let root = Root::new();
    let mut bytes = zstd_bytes(b"{\"a\":1}\n{\"a\":2}\n");
    bytes.extend(zstd_bytes(b"{\"a\":3}\n"));
    let size = u64::try_from(bytes.len()).unwrap();
    root.file(OWNER, "loads", "part-000.json.zst", &bytes);
    let mut reader = source(&root)
        .open(
            &at("loads"),
            &input_file("part-000.json.zst", size),
            InputCodec::Zstd,
        )
        .await
        .unwrap();
    let lines = read_all(&mut reader, 4096).await;
    assert_eq!(lines, vec!["{\"a\":1}", "{\"a\":2}", "{\"a\":3}"]);
}

/// A complete frame followed by a truncated one reads as the first
/// frame's lines and a clean end of file, with every byte of the object
/// consumed so the byte count and the tag cover it (measured on the
/// service: the operation ends COMPLETED with the first frame's rows, no
/// failed row, and ProcessedInputBytes equal to the size).
#[tokio::test]
async fn a_truncated_frame_after_a_complete_frame_ends_the_file_cleanly() {
    let root = Root::new();
    let mut bytes = zstd_bytes(b"{\"a\":1}\n{\"a\":2}\n");
    let second = zstd_bytes(b"{\"a\":3}\n{\"a\":4}\n");
    bytes.extend_from_slice(&second[..second.len() / 2]);
    let size = u64::try_from(bytes.len()).unwrap();
    root.file(OWNER, "loads", "part-000.json.zst", &bytes);
    let src = source(&root);
    let mut reader = src
        .open(
            &at("loads"),
            &input_file("part-000.json.zst", size),
            InputCodec::Zstd,
        )
        .await
        .unwrap();
    let lines = read_all(&mut reader, 4096).await;
    assert_eq!(lines, vec!["{\"a\":1}", "{\"a\":2}"]);
    assert_eq!(reader.bytes_consumed(), size);
    let hashed = src
        .etag(&at("loads"), &input_file("part-000.json.zst", size))
        .await
        .unwrap();
    assert_eq!(reader.etag(), Some(hashed));
}

/// A zstd frame cut short is a failed decompression, its own class
/// (the hosted service reports the file as "could not be decompressed"
/// and counts its bytes, measured on the service), not the gzip path's
/// corrupt-stream class. Whatever the decoder yielded before the cut is
/// served first; the fault follows.
#[tokio::test]
async fn a_truncated_zstd_frame_is_a_decode_failure() {
    let root = Root::new();
    let mut plain = Vec::new();
    // 20,000 rows compress to about ten 128 KiB blocks, so a cut at 90
    // percent of the bytes lands after several complete blocks.
    for i in 0..20000 {
        plain.extend_from_slice(format!("{{\"i\":{i},\"pad\":\"{:0>40}\"}}\n", i).as_bytes());
    }
    let whole = zstd_bytes(&plain);
    let truncated = &whole[..whole.len() * 9 / 10];
    let size = u64::try_from(truncated.len()).unwrap();
    root.file(OWNER, "loads", "part-000.json.zst", truncated);
    let mut reader = source(&root)
        .open(
            &at("loads"),
            &input_file("part-000.json.zst", size),
            InputCodec::Zstd,
        )
        .await
        .unwrap();
    let mut served = 0usize;
    let err = loop {
        match reader.next_line(4096).await {
            Ok(Some(_)) => served += 1,
            Ok(None) => panic!("a truncated zstd frame must not read as clean end of file"),
            Err(e) => break e,
        }
    };
    assert!(matches!(err, SourceError::DecodeFailed), "got {err:?}");
    assert!(
        served > 0 && served < 20000,
        "the complete blocks before the cut are served ({served} lines), the rest are not"
    );
    assert_eq!(reader.etag(), None);
}

/// Under `ZSTD`, a file that does not begin with the zstd magic is not a
/// compressed object: gzip bytes and plain text both answer
/// `NotCompressed` at open, the class the gzip path answers for the
/// mirror case (measured on the service: both end `FAILED` with the
/// validation pair).
#[tokio::test]
async fn bytes_without_the_zstd_magic_under_zstd_are_not_compressed() {
    let root = Root::new();
    let plain = b"{\"a\":1}\n".to_vec();
    root.file(OWNER, "loads", "plain.json", &plain);
    root.file(OWNER, "loads", "part.json.gz", &gzip_bytes(&plain));
    root.file(OWNER, "loads", "part.json.zst", &zstd_bytes(&plain));
    let src = source(&root);
    for (path, codec) in [
        ("plain.json", InputCodec::Zstd),
        ("part.json.gz", InputCodec::Zstd),
        ("part.json.zst", InputCodec::Gzip),
    ] {
        let size = std::fs::metadata(root.0.join(OWNER).join("loads").join(path))
            .unwrap()
            .len();
        let err = src
            .open(&at("loads"), &input_file(path, size), codec)
            .await
            .err()
            .unwrap_or_else(|| panic!("{path} under {codec:?} must be refused"));
        assert!(
            matches!(err, SourceError::NotCompressed),
            "{path} under {codec:?}: got {err:?}"
        );
    }
}

#[tokio::test]
async fn gzip_round_trip() {
    let root = Root::new();
    let plain = b"{\"Item\":{\"pk\":{\"S\":\"a\"}}}\n{\"Item\":{\"pk\":{\"S\":\"b\"}}}\n";
    let compressed = gzip_bytes(plain);
    let compressed_len = u64::try_from(compressed.len()).unwrap();
    root.file(OWNER, "loads", "part-000.json.gz", &compressed);

    let mut reader = source(&root)
        .open(
            &at("loads"),
            &input_file("part-000.json.gz", compressed_len),
            InputCodec::Gzip,
        )
        .await
        .unwrap();
    let lines = read_all(&mut reader, 4096).await;
    assert_eq!(
        lines,
        vec![
            "{\"Item\":{\"pk\":{\"S\":\"a\"}}}",
            "{\"Item\":{\"pk\":{\"S\":\"b\"}}}"
        ]
    );
    assert_eq!(
        reader.bytes_consumed(),
        compressed_len,
        "consumed bytes are compressed bytes"
    );
}

#[tokio::test]
async fn two_concatenated_gzip_members_are_read_whole() {
    let root = Root::new();
    let mut compressed = gzip_bytes(b"{\"Item\":{\"pk\":{\"S\":\"a\"}}}\n");
    compressed.extend_from_slice(&gzip_bytes(b"{\"Item\":{\"pk\":{\"S\":\"b\"}}}\n"));
    let size = u64::try_from(compressed.len()).unwrap();
    root.file(OWNER, "loads", "part-000.json.gz", &compressed);

    let mut reader = source(&root)
        .open(
            &at("loads"),
            &input_file("part-000.json.gz", size),
            InputCodec::Gzip,
        )
        .await
        .unwrap();
    let lines = read_all(&mut reader, 4096).await;
    assert_eq!(
        lines,
        vec![
            "{\"Item\":{\"pk\":{\"S\":\"a\"}}}",
            "{\"Item\":{\"pk\":{\"S\":\"b\"}}}"
        ],
        "rows in the second member are read"
    );
}

#[tokio::test]
async fn a_truncated_gzip_stream_is_a_corrupt_stream_fault() {
    let root = Root::new();
    let mut plain = Vec::new();
    for i in 0..50 {
        plain.extend_from_slice(format!("{{\"i\":{i}}}\n").as_bytes());
    }
    let whole = gzip_bytes(&plain);
    // Cut inside the deflate data, before the gzip trailer.
    let truncated = &whole[..whole.len() - 12];
    let size = u64::try_from(truncated.len()).unwrap();
    root.file(OWNER, "loads", "part-000.json.gz", truncated);

    let mut reader = source(&root)
        .open(
            &at("loads"),
            &input_file("part-000.json.gz", size),
            InputCodec::Gzip,
        )
        .await
        .unwrap();
    let err = loop {
        match reader.next_line(4096).await {
            Ok(Some(_)) => {}
            Ok(None) => panic!("a truncated gzip stream must not read as clean end of file"),
            Err(e) => break e,
        }
    };
    assert!(matches!(err, SourceError::Corrupt), "got {err:?}");
    assert_eq!(
        reader.etag(),
        None,
        "a reader that faulted did not consume the file whole and reports no tag"
    );
}

/// The reader's entity tag is the MD5 of the raw bytes it consumed: absent
/// until end of file, then equal to what `etag` computes by reading the
/// file again, for a plain file and for a gzip file (the compressed
/// bytes, matching the object's ETag).
#[tokio::test]
async fn the_reader_reports_the_etag_only_once_the_file_is_consumed_whole() {
    let root = Root::new();
    let mut plain = Vec::new();
    for i in 0..300 {
        plain.extend_from_slice(format!("{{\"i\":{i}}}\n").as_bytes());
    }
    let compressed = gzip_bytes(&plain);
    root.file(OWNER, "loads", "plain.json", &plain);
    root.file(OWNER, "loads", "part.json.gz", &compressed);
    let src = source(&root);
    for (path, compression, bytes) in [
        ("plain.json", InputCodec::None, plain.as_slice()),
        ("part.json.gz", InputCodec::Gzip, compressed.as_slice()),
    ] {
        let size = u64::try_from(bytes.len()).unwrap();
        let mut reader = src
            .open(&at("loads"), &input_file(path, size), compression)
            .await
            .unwrap();
        let first = reader.next_line(4096).await.unwrap();
        assert!(first.is_some());
        assert_eq!(
            reader.etag(),
            None,
            "{path}: no tag while lines remain to be read"
        );
        let mut lines = 1;
        while reader.next_line(4096).await.unwrap().is_some() {
            lines += 1;
        }
        assert_eq!(lines, 300);
        let hashed = src
            .etag(&at("loads"), &input_file(path, size))
            .await
            .unwrap();
        assert_eq!(
            reader.etag(),
            Some(hashed),
            "{path}: the streamed tag equals the hashed tag"
        );
    }
}

/// The zstd decoder loads a frame declaring a window up
/// to 128 MiB and refuses one declaring more. The bound is the crate
/// default and matches the service's acceptance edge: frames declaring a
/// 64 MiB and a 128 MiB window load and complete on the service, a 256 MiB
/// window does not. A frame header carrying the window descriptor alone
/// decides it: 0x88 is a 128 MiB window, 0x90 a 256 MiB window.
#[test]
fn the_zstd_decoder_loads_a_128_mib_window_and_refuses_a_256_mib_window() {
    use super::super::codec::ZstdFrames;
    // zstd magic, a frame header descriptor with no content-size and no
    // single-segment flag so a window descriptor follows, then the window
    // descriptor byte.
    let frame = |descriptor: u8| vec![0x28u8, 0xb5, 0x2f, 0xfd, 0x00, descriptor];
    assert!(
        ZstdFrames::new(std::io::Cursor::new(frame(0x88))).is_ok(),
        "a 128 MiB window loads"
    );
    let refused = ZstdFrames::new(std::io::Cursor::new(frame(0x90)));
    assert!(refused.is_err(), "a 256 MiB window is refused");
    assert_eq!(
        refused.err().unwrap().kind(),
        std::io::ErrorKind::InvalidData,
        "an over-large window reads as corrupt data"
    );
}
