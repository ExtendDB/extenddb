// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Bulk put source/sink reader tests.

use super::*;

#[tokio::test]
async fn line_one_byte_over_the_cap_is_too_long_and_stops_the_file() {
    let root = Root::new();
    let cap = 16;
    let mut content = vec![b'a'; cap + 1];
    content.push(b'\n');
    content.extend_from_slice(b"{\"Item\":{}}\n");
    let size = u64::try_from(content.len()).unwrap();
    root.file(OWNER, "loads", "part-000.json", &content);

    let mut reader = source(&root)
        .open(
            &at("loads"),
            &input_file("part-000.json", size),
            InputCodec::None,
        )
        .await
        .unwrap();
    let err = reader.next_line(cap).await.expect_err("over the cap");
    assert!(matches!(err, SourceError::LineTooLong), "got {err:?}");
    // The rest of the file is not read: the valid line after the fault
    // is never yielded.
    assert!(reader.next_line(cap).await.unwrap().is_none());
}

#[tokio::test]
async fn line_exactly_at_the_cap_is_returned() {
    let root = Root::new();
    let cap = 16;
    let mut content = vec![b'b'; cap];
    content.push(b'\n');
    let size = u64::try_from(content.len()).unwrap();
    root.file(OWNER, "loads", "part-000.json", &content);

    let mut reader = source(&root)
        .open(
            &at("loads"),
            &input_file("part-000.json", size),
            InputCodec::None,
        )
        .await
        .unwrap();
    let line = reader.next_line(cap).await.unwrap().unwrap();
    assert_eq!(line.len(), cap);
    assert!(reader.next_line(cap).await.unwrap().is_none());
}

#[tokio::test]
async fn reading_stops_at_the_enumerated_size_when_the_file_grew() {
    let root = Root::new();
    let enumerated = b"{\"Item\":{\"pk\":{\"S\":\"a\"}}}\n{\"Item\":{\"pk\":{\"S\":\"b\"}}}\n";
    let size = u64::try_from(enumerated.len()).unwrap();
    let path = root.file(OWNER, "loads", "part-000.json", enumerated);

    // The file grows after enumeration; the appended rows are past the
    // enumerated size and must never be read.
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap();
    file.write_all(b"{\"Item\":{\"pk\":{\"S\":\"appended\"}}}\n")
        .unwrap();
    drop(file);

    let mut reader = source(&root)
        .open(
            &at("loads"),
            &input_file("part-000.json", size),
            InputCodec::None,
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
        "only the enumerated rows are read"
    );
    assert_eq!(reader.bytes_consumed(), size);
}

#[tokio::test]
async fn a_200_line_file_is_served_in_order_across_blocking_hops() {
    let root = Root::new();
    let mut content = Vec::new();
    for i in 0..200 {
        content.extend_from_slice(format!("{{\"i\":{i}}}\n").as_bytes());
    }
    let size = u64::try_from(content.len()).unwrap();
    root.file(OWNER, "loads", "part-000.json", &content);

    let mut reader = source(&root)
        .open(
            &at("loads"),
            &input_file("part-000.json", size),
            InputCodec::None,
        )
        .await
        .unwrap();
    let lines = read_all(&mut reader, 4096).await;
    assert_eq!(lines.len(), 200);
    for (i, line) in lines.iter().enumerate() {
        assert_eq!(line, &format!("{{\"i\":{i}}}"));
    }
}

#[tokio::test]
async fn the_line_cap_is_enforced_per_line_within_a_batch() {
    let root = Root::new();
    let cap = 32;
    let mut content = Vec::new();
    // 150 short lines cross two batch boundaries before the fault.
    for i in 0..150 {
        content.extend_from_slice(format!("{{\"i\":{i}}}\n").as_bytes());
    }
    content.extend_from_slice(&vec![b'x'; cap + 1]);
    content.push(b'\n');
    content.extend_from_slice(b"{\"i\":151}\n");
    let size = u64::try_from(content.len()).unwrap();
    root.file(OWNER, "loads", "part-000.json", &content);

    let mut reader = source(&root)
        .open(
            &at("loads"),
            &input_file("part-000.json", size),
            InputCodec::None,
        )
        .await
        .unwrap();
    let mut lines = Vec::new();
    let err = loop {
        match reader.next_line(cap).await {
            Ok(Some(line)) => lines.push(line),
            Ok(None) => panic!(
                "the over-cap line must fault, got EOF after {}",
                lines.len()
            ),
            Err(e) => break e,
        }
    };
    assert!(matches!(err, SourceError::LineTooLong), "got {err:?}");
    assert_eq!(lines.len(), 150, "every line before the fault is served");
    assert_eq!(lines[149], "{\"i\":149}");
    // Nothing past the fault point is served.
    assert!(reader.next_line(cap).await.unwrap().is_none());
}

/// The fill loop is bounded by bytes as well as lines: lines totalling
/// more than [`LINE_BATCH_BYTES`] within one [`LINE_BATCH`] stop the
/// batch early, observable as the first blocking hop consuming about
/// the bound rather than the whole file, while every line is still
/// served in order.
#[tokio::test]
async fn the_line_batch_is_bounded_by_bytes_as_well_as_lines() {
    let root = Root::new();
    let line_len = 1024 * 1024;
    let line_count = 12;
    let mut content = Vec::new();
    for i in 0..line_count {
        let mut line = format!("{i:02}").into_bytes();
        line.resize(line_len, b'x');
        content.extend_from_slice(&line);
        content.push(b'\n');
    }
    let size = u64::try_from(content.len()).unwrap();
    root.file(OWNER, "loads", "part-000.json", &content);

    let cap = 2 * 1024 * 1024;
    let mut reader = source(&root)
        .open(
            &at("loads"),
            &input_file("part-000.json", size),
            InputCodec::None,
        )
        .await
        .unwrap();
    let first = reader.next_line(cap).await.unwrap().unwrap();
    // The first fill stopped at the byte bound: eight lines of one MiB
    // reach it, so the raw bytes consumed stay near eight MiB instead
    // of the twelve the line bound alone would have buffered.
    let consumed = reader.bytes_consumed();
    assert!(
        (8 * 1024 * 1024..9 * 1024 * 1024).contains(&consumed),
        "the fill must stop at the byte bound, consumed {consumed}"
    );

    let mut lines = vec![first];
    lines.extend(read_all(&mut reader, cap).await);
    assert_eq!(lines.len(), line_count);
    for (i, line) in lines.iter().enumerate() {
        assert!(
            line.starts_with(&format!("{i:02}")),
            "line {i} served out of order"
        );
        assert_eq!(line.len(), line_len);
    }
}
