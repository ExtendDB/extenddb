// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! An abandon that cut its file short ends the operation the same way
//! after a kill as without one, however far the run had read into the next
//! file when it was killed.

use super::*;

/// One row of exactly `bytes` bytes, line terminator excluded; `fault`
/// gives its attribute an unknown type tag, a parse-level fault that
/// abandons the rest of the file.
fn padded_row(index: usize, bytes: usize, fault: bool) -> String {
    let tag = if fault { "X" } else { "S" };
    let head = format!(r#"{{"Item":{{"pk":{{"S":"k{index}"}},"sk":{{"S":"s"}},"p":{{"{tag}":""#);
    let tail = r#""}}}"#;
    let pad = bytes - head.len() - tail.len();
    format!("{head}{}{tail}", "x".repeat(pad))
}

/// `lines` rows of 160 bytes, terminator included, the first `faults` of
/// them carrying the parse-level fault.
fn file_of_160_byte_rows(lines: usize, first: usize, faults: usize) -> Vec<u8> {
    let mut out = String::new();
    for line in 0..lines {
        out.push_str(&padded_row(first + line, 159, line < faults));
        out.push('\n');
    }
    out.into_bytes()
}

/// File 0, 23,840 bytes, is abandoned at its first line, a cut at the end
/// of the 8,000-byte read that held the fault (measured on the service);
/// file 1 holds 600 rows of 160 bytes.
fn stage(h: &Harness) {
    h.write_input("part-000.json", &file_of_160_byte_rows(149, 0, 1));
    h.write_input("part-001.json", &file_of_160_byte_rows(600, 1000, 0));
}

async fn a_cut_file_still_fails_the_operation_after_a_kill_inside_the_next_file_case(
    backend: Backend,
) {
    let fresh = Harness::on(backend).await;
    stage(&fresh);
    let files = fresh.enumerate().await;
    assert_eq!(files[0].size, 23_840);
    let expected = fresh
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;
    assert_eq!(expected.status, "FAILED");
    assert_eq!(
        expected.failure_code.as_deref(),
        Some("ValidationException")
    );
    assert_eq!(expected.processed_input_bytes, 8_000 + 600 * 160);
    fresh.cleanup().await;

    let h = Harness::on(backend).await;
    stage(&h);
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    let record = h.claim("runner-1", Duration::from_millis(100)).await;
    // The faulting line of file 0, then 300 rows of file 1: the checkpoint
    // at row 300 of file 1 has persisted more bytes than file 0's size.
    let notify = Arc::new(tokio::sync::Notify::new());
    let source = Arc::new(PendAfterSource {
        inner: h.fs_source(),
        after: 301,
        handed: Arc::new(AtomicU64::new(0)),
        notify: Arc::clone(&notify),
    });
    let deps =
        h.with_short_store_lease(h.deps_with("runner-1", config(), source, Arc::new(h.fs_sink())));
    tokio::select! {
        _ = runner::run(&deps, record) => panic!("the runner must not finish while the reader pends"),
        () = notify.notified() => {}
    }
    let mid = h.record(&op).await;
    assert_eq!((mid.cursor_file_index, mid.cursor_row_index), (1, 300));
    assert!(
        mid.processed_input_bytes > 23_840,
        "the persisted figure has passed file 0's size: {}",
        mid.processed_input_bytes
    );

    tokio::time::sleep(Duration::from_millis(250)).await;
    let record = h.claim("runner-2", Duration::from_secs(60)).await;
    runner::run(&h.deps("runner-2", config()), record)
        .await
        .expect("resumed run");

    let record = h.record(&op).await;
    assert_eq!(record.status, expected.status);
    assert_eq!(record.failure_code, expected.failure_code);
    assert_eq!(record.failure_message, expected.failure_message);
    assert_eq!(record.processed_item_count, expected.processed_item_count);
    assert_eq!(record.failed_item_count, expected.failed_item_count);
    assert_eq!(
        record.processed_input_bytes, expected.processed_input_bytes,
        "file 0 counts its cut, not its size"
    );
    h.cleanup().await;
}

on_both_backends!(
    a_cut_file_still_fails_the_operation_after_a_kill_inside_the_next_file_case,
    a_cut_file_still_fails_the_operation_after_a_kill_inside_the_next_file,
    a_cut_file_still_fails_the_operation_after_a_kill_inside_the_next_file_on_postgres
);
