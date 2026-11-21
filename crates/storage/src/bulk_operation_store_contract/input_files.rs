// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The input files the runner records on an operation's first claim
//! (`record_input_files`): once, under the lease, with the total.

use super::*;

/// A new operation as the Start handler inserts it: no files, no total.
fn unlisted(fx: &Fixture<impl StorageEngine>, table: &TableDescription) -> NewBulkOperation {
    let mut req = new_op(fx, &fx.account_a, table, None, "fp-unlisted", 1_000);
    req.input_files = Vec::new();
    req.total_input_bytes = None;
    req
}

fn listed(path: &str, size: i64) -> InputFile {
    InputFile {
        path: path.to_owned(),
        size,
        etag: Some(format!("etag-{path}")),
        ..InputFile::default()
    }
}

/// An operation started with no files reads back no files and no total.
/// The lease holder records the files with their total in one call: the
/// record then carries them in list order with the tags the listing gave,
/// and the lease runs from the call. A second call writes nothing.
pub async fn the_lease_holder_records_the_input_files_once<E: StorageEngine>(fx: &Fixture<E>) {
    let table = create_table(fx, &fx.account_a, "input_files_once").await;
    let started = created(start(fx, &fx.account_a, unlisted(fx, &table), 50).await);
    assert!(started.input_files.is_empty());
    assert_eq!(started.total_input_bytes, None);
    let claimed = fx
        .engine
        .claim_runnable("owner-1", Duration::from_secs(1), 10, &[], &[])
        .await
        .expect("claim");
    let lease_before = claimed[0]
        .lease_until_ms
        .expect("a claimed row has a lease");

    let files = vec![listed("in/a.json", 10), listed("in/b.json", 20)];
    let state = fx
        .engine
        .record_input_files(
            &fx.account_a,
            &started.operation_id,
            "owner-1",
            Duration::from_secs(600),
            files,
            30,
        )
        .await
        .expect("record the input files");
    assert!(matches!(state, LeaseState::Held), "{state:?}");
    let record = get(fx, &fx.account_a, &started.operation_id)
        .await
        .expect("the record exists");
    assert_eq!(record.total_input_bytes, Some(30));
    let stored: Vec<(&str, i64, Option<&str>)> = record
        .input_files
        .iter()
        .map(|f| (f.path.as_str(), f.size, f.etag.as_deref()))
        .collect();
    assert_eq!(
        stored,
        vec![
            ("in/a.json", 10, Some("etag-in/a.json")),
            ("in/b.json", 20, Some("etag-in/b.json")),
        ]
    );
    assert!(
        record.lease_until_ms.expect("still leased") > lease_before + 300_000,
        "the call renews the lease"
    );

    let again = fx
        .engine
        .record_input_files(
            &fx.account_a,
            &started.operation_id,
            "owner-1",
            Duration::from_secs(60),
            vec![listed("in/c.json", 5)],
            5,
        )
        .await
        .expect("a second record answers");
    assert!(matches!(again, LeaseState::Lost), "{again:?}");
    let record = get(fx, &fx.account_a, &started.operation_id)
        .await
        .expect("the record exists");
    assert_eq!(record.total_input_bytes, Some(30));
    assert_eq!(record.input_files.len(), 2, "the second call wrote nothing");
}

/// Only the lease holder records the files: a caller that holds no lease on
/// the row, and the runner after the terminal transition cleared its lease,
/// both answer `Lost` and write nothing.
pub async fn recording_the_input_files_needs_the_lease<E: StorageEngine>(fx: &Fixture<E>) {
    for (index, case) in ["another owner", "after finish"].into_iter().enumerate() {
        let table = create_table(fx, &fx.account_a, &format!("input_files_lease_{index}")).await;
        let started = created(start(fx, &fx.account_a, unlisted(fx, &table), 50).await);
        fx.engine
            .claim_runnable("owner-1", Duration::from_secs(60), 10, &[], &[])
            .await
            .expect("claim");
        let caller = if case == "after finish" {
            let finished = fx
                .engine
                .finish(
                    &fx.account_a,
                    &started.operation_id,
                    "owner-1",
                    Terminal {
                        status: "FAILED".to_owned(),
                        end_time_ms: 2_000,
                        failure_code: Some("S3NoSuchBucket".to_owned()),
                        failure_message: Some("The specified bucket does not exist".to_owned()),
                        usage: json!({}),
                    },
                )
                .await
                .expect("finish");
            assert!(matches!(finished, FinishState::Held(_)), "{case}");
            "owner-1"
        } else {
            "owner-2"
        };
        let state = fx
            .engine
            .record_input_files(
                &fx.account_a,
                &started.operation_id,
                caller,
                Duration::from_secs(60),
                vec![listed("in/a.json", 10)],
                10,
            )
            .await
            .expect("record answers");
        assert!(matches!(state, LeaseState::Lost), "{case}: {state:?}");
        let record = get(fx, &fx.account_a, &started.operation_id)
            .await
            .expect("the record exists");
        assert_eq!(record.total_input_bytes, None, "{case}");
        assert!(record.input_files.is_empty(), "{case}");
    }
}
