// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Tests of the filesystem report sink.

#[cfg(unix)]
use super::super::sink::FINAL_NAME;
use super::*;

/// The render opens the bucket directory, never creates it: a report
/// bucket removed after Start fails the render with `NotFound`, and no
/// directory is created at the bucket name.
#[tokio::test]
async fn render_fails_when_the_report_bucket_directory_is_missing() {
    let root = Root::new();
    // The account directory exists; the bucket does not.
    std::fs::create_dir_all(root.0.join(OWNER)).unwrap();
    let records = vec![serde_json::json!({"errorMessages": ["x"]})];
    let err = render_one(&sink(&root), "", records)
        .await
        .expect_err("a missing bucket directory must fail the render");
    match err {
        SinkError::Io(e) => assert_eq!(e.kind(), std::io::ErrorKind::NotFound, "{e}"),
        other => panic!("expected an Io NotFound, got {other:?}"),
    }
    assert!(
        !root.0.join(OWNER).join("loads").exists(),
        "the render must not create the bucket directory"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn render_fails_on_a_planted_symlink_named_awsdynamodb() {
    let root = Root::new();
    let victim_dir = root.bucket_dir(OTHER, "loads");
    let own_bucket = root.bucket_dir(OWNER, "loads");
    std::os::unix::fs::symlink(&victim_dir, own_bucket.join("AWSDynamoDB")).unwrap();

    let records = vec![serde_json::json!({"errorMessages": ["x"]})];
    let err = render_one(&sink(&root), "", records)
        .await
        .expect_err("a planted symlink must fail the render");
    assert!(matches!(err, SinkError::Symlink(_)), "got {err:?}");
    // Nothing was created inside the victim's subtree.
    assert_eq!(std::fs::read_dir(&victim_dir).unwrap().count(), 0);
}

/// The R-1 write-side window: an ancestor of the report directory is
/// swapped for a symlink to another account's completed report
/// directory before the sink opens. The walk from the verified account
/// handle fails at the swapped component, and the victim's rendered
/// report is untouched.
#[cfg(unix)]
#[tokio::test]
async fn render_refuses_an_ancestor_swapped_for_a_foreign_report_directory() {
    let root = Root::new();
    root.bucket_dir(OWNER, "loads");
    // The victim's completed report, the file the redirected rename
    // would otherwise replace.
    let victim_operation = root
        .0
        .join(OTHER)
        .join("loads")
        .join("AWSDynamoDB")
        .join("BulkOperation")
        .join(format!("{OTHER}:{OP_ID}-0"));
    let victim_errors = victim_operation.join("output").join("errors");
    std::fs::create_dir_all(&victim_errors).unwrap();
    let victim_report = victim_errors.join(FINAL_NAME);
    std::fs::write(&victim_report, b"victim").unwrap();

    // The owner's chain exists down to BulkOperation; the operation
    // directory is a symlink whose target holds the remaining trailing
    // components.
    let own_bulk = root
        .0
        .join(OWNER)
        .join("loads")
        .join("AWSDynamoDB")
        .join("BulkOperation");
    std::fs::create_dir_all(&own_bulk).unwrap();
    std::os::unix::fs::symlink(
        &victim_operation,
        own_bulk.join(format!("{OWNER}:{OP_ID}-0")),
    )
    .unwrap();

    let records = vec![serde_json::json!({"errorMessages": ["x"]})];
    let err = render_one(&sink(&root), "", records)
        .await
        .expect_err("a swapped ancestor must fail the render");
    assert!(matches!(err, SinkError::Symlink(_)), "got {err:?}");
    assert_eq!(std::fs::read(&victim_report).unwrap(), b"victim");
}

#[tokio::test]
async fn render_rejects_a_prefix_that_escapes_the_account_subtree() {
    let root = Root::new();
    root.bucket_dir(OWNER, "loads");
    let victim_dir = root.bucket_dir(OTHER, "loads");

    for prefix in [format!("../../{OTHER}/loads"), "/etc/cron.d".to_owned()] {
        let records = vec![serde_json::json!({"errorMessages": ["x"]})];
        let err = render_one(&sink(&root), &prefix, records)
            .await
            .expect_err("an escaping prefix must be refused");
        assert!(matches!(err, SinkError::Jail(_)), "got {err:?}");
    }
    assert_eq!(std::fs::read_dir(&victim_dir).unwrap().count(), 0);
}

#[tokio::test]
async fn render_creates_the_chain_and_twice_replaces_the_file() {
    let root = Root::new();
    root.bucket_dir(OWNER, "loads");
    let first = vec![serde_json::json!({"errorMessages": ["first"]})];
    let second = vec![
        serde_json::json!({"errorMessages": ["second"]}),
        serde_json::json!({"errorMessages": ["third"]}),
    ];

    let key = render_one(&sink(&root), "reports", first).await.unwrap();
    assert_eq!(
        key,
        format!(
            "reports/AWSDynamoDB/BulkOperation/{OWNER}:{OP_ID}-0/output/errors/output-00000.json.gz"
        )
    );

    let final_path = root
        .0
        .join(OWNER)
        .join("loads")
        .join("reports")
        .join("AWSDynamoDB")
        .join("BulkOperation")
        .join(format!("{OWNER}:{OP_ID}-0"))
        .join("output")
        .join("errors")
        .join("output-00000.json.gz");
    assert_eq!(
        decode_report(&final_path),
        vec!["{\"errorMessages\":[\"first\"]}"]
    );

    // Every component of the chain is a real directory, not a symlink.
    let mut cursor = root.0.join(OWNER).join("loads");
    for component in [
        "reports",
        "AWSDynamoDB",
        "BulkOperation",
        &format!("{OWNER}:{OP_ID}-0"),
        "output",
        "errors",
    ] {
        cursor = cursor.join(component);
        let meta = std::fs::symlink_metadata(&cursor).unwrap();
        assert!(meta.is_dir() && !meta.file_type().is_symlink());
    }

    // A leftover temp file of the same owner from an earlier crash does
    // not wedge the retry: it is removed before create_new.
    let dir = final_path.parent().unwrap();
    std::fs::write(
        dir.join(format!("output-00000.json.gz.tmp-{LEASE_OWNER}")),
        b"stale",
    )
    .unwrap();

    // A second render replaces the file through the rename.
    render_one(&sink(&root), "reports", second).await.unwrap();
    assert_eq!(
        decode_report(&final_path),
        vec![
            "{\"errorMessages\":[\"second\"]}",
            "{\"errorMessages\":[\"third\"]}"
        ]
    );
    // No temporary file is left behind.
    assert_eq!(std::fs::read_dir(dir).unwrap().count(), 1);
}

#[tokio::test]
async fn two_pages_then_finish_produce_one_gzip_with_all_records_in_order() {
    let root = Root::new();
    root.bucket_dir(OWNER, "loads");
    let sink = sink(&root);
    let mut writer = sink
        .open(
            &at("loads"),
            "",
            OP_ID,
            ReportKind::Error,
            LEASE_OWNER,
            &OutputSse::None,
        )
        .await
        .unwrap();
    writer
        .write_page(vec![
            serde_json::json!({"errorMessages": ["a"]}),
            serde_json::json!({"errorMessages": ["b"]}),
        ])
        .await
        .unwrap();
    writer
        .write_page(vec![serde_json::json!({"errorMessages": ["c"]})])
        .await
        .unwrap();
    writer.finish().await.unwrap();

    let final_path = root
        .0
        .join(OWNER)
        .join("loads")
        .join("AWSDynamoDB")
        .join("BulkOperation")
        .join(format!("{OWNER}:{OP_ID}-0"))
        .join("output")
        .join("errors")
        .join("output-00000.json.gz");
    assert_eq!(
        decode_report(&final_path),
        vec![
            "{\"errorMessages\":[\"a\"]}",
            "{\"errorMessages\":[\"b\"]}",
            "{\"errorMessages\":[\"c\"]}"
        ]
    );
}

#[tokio::test]
async fn dropping_a_writer_without_finish_leaves_no_temp_file() {
    let root = Root::new();
    root.bucket_dir(OWNER, "loads");
    let sink = sink(&root);
    let mut writer = sink
        .open(
            &at("loads"),
            "",
            OP_ID,
            ReportKind::Error,
            LEASE_OWNER,
            &OutputSse::None,
        )
        .await
        .unwrap();
    writer
        .write_page(vec![serde_json::json!({"errorMessages": ["a"]})])
        .await
        .unwrap();
    drop(writer);

    let dir = root
        .0
        .join(OWNER)
        .join("loads")
        .join("AWSDynamoDB")
        .join("BulkOperation")
        .join(format!("{OWNER}:{OP_ID}-0"))
        .join("output")
        .join("errors");
    assert_eq!(
        std::fs::read_dir(&dir).unwrap().count(),
        0,
        "the abandoned temp file is removed"
    );
}

/// A hard-killed runner leaves a temp file whose name embeds an owner
/// id no successor recomputes. The next open of the report directory
/// sweeps it, and only it: the final report file stays.
#[tokio::test]
async fn opening_the_report_directory_removes_a_dead_owners_temp_and_keeps_the_final_file() {
    let root = Root::new();
    root.bucket_dir(OWNER, "loads");
    let sink = sink(&root);

    // A finished report and a dead incarnation's leftover temp.
    render_one(&sink, "", vec![serde_json::json!({"errorMessages": ["a"]})])
        .await
        .unwrap();
    let dir = root
        .0
        .join(OWNER)
        .join("loads")
        .join("AWSDynamoDB")
        .join("BulkOperation")
        .join(format!("{OWNER}:{OP_ID}-0"))
        .join("output")
        .join("errors");
    let dead_temp = dir.join("output-00000.json.gz.tmp-dead-owner");
    std::fs::write(&dead_temp, b"partial").unwrap();
    let final_file = dir.join("output-00000.json.gz");
    let final_before = std::fs::read(&final_file).unwrap();

    let writer = sink
        .open(
            &at("loads"),
            "",
            OP_ID,
            ReportKind::Error,
            LEASE_OWNER,
            &OutputSse::None,
        )
        .await
        .unwrap();
    assert!(
        !dead_temp.exists(),
        "the dead owner's temp file is gone after open"
    );
    assert_eq!(
        std::fs::read(&final_file).unwrap(),
        final_before,
        "the final file is untouched"
    );
    drop(writer);
}

/// The handle-relative discipline: once the directory handle is taken,
/// swapping the validated parent for a symlink cannot redirect the
/// write, because the create, write, and rename all address the handle.
#[cfg(unix)]
#[tokio::test]
async fn a_parent_swapped_after_the_handle_is_taken_cannot_redirect_the_write() {
    let root = Root::new();
    root.bucket_dir(OWNER, "loads");
    let victim_dir = root.bucket_dir(OTHER, "loads");
    let sink = sink(&root);
    let mut writer = sink
        .open(
            &at("loads"),
            "",
            OP_ID,
            ReportKind::Error,
            LEASE_OWNER,
            &OutputSse::None,
        )
        .await
        .unwrap();

    // The validated parent exists now; swap it for a symlink into the
    // victim's subtree.
    let errors_dir = root
        .0
        .join(OWNER)
        .join("loads")
        .join("AWSDynamoDB")
        .join("BulkOperation")
        .join(format!("{OWNER}:{OP_ID}-0"))
        .join("output")
        .join("errors");
    let moved = errors_dir.with_file_name("errors-moved");
    std::fs::rename(&errors_dir, &moved).unwrap();
    std::os::unix::fs::symlink(&victim_dir, &errors_dir).unwrap();

    writer
        .write_page(vec![serde_json::json!({"errorMessages": ["x"]})])
        .await
        .unwrap();
    writer.finish().await.unwrap();

    // The report landed in the original directory, wherever its name
    // went, and nothing reached the victim's subtree.
    assert_eq!(
        decode_report(&moved.join("output-00000.json.gz")),
        vec!["{\"errorMessages\":[\"x\"]}"]
    );
    assert_eq!(std::fs::read_dir(&victim_dir).unwrap().count(), 0);
}

/// The final install step: a symlink planted at the final report name
/// before the render must be replaced by the rename, not written
/// through. The tenant controls its subtree, so the whole report chain
/// and the planted link are in place before the sink opens; the foreign
/// target keeps its content, the link is gone, and the final name is a
/// regular file holding the report.
#[cfg(unix)]
#[tokio::test]
async fn a_symlink_planted_at_the_final_report_name_is_replaced_not_followed() {
    let root = Root::new();
    root.bucket_dir(OWNER, "loads");
    let victim_dir = root.bucket_dir(OTHER, "loads");
    let victim_file = victim_dir.join("precious.json");
    std::fs::write(&victim_file, b"victim").unwrap();

    let errors_dir = root
        .0
        .join(OWNER)
        .join("loads")
        .join("AWSDynamoDB")
        .join("BulkOperation")
        .join(format!("{OWNER}:{OP_ID}-0"))
        .join("output")
        .join("errors");
    std::fs::create_dir_all(&errors_dir).unwrap();
    std::os::unix::fs::symlink(&victim_file, errors_dir.join(FINAL_NAME)).unwrap();

    let records = vec![serde_json::json!({"errorMessages": ["x"]})];
    render_one(&sink(&root), "", records)
        .await
        .expect("the render must succeed by replacing the planted link");

    assert_eq!(
        std::fs::read(&victim_file).unwrap(),
        b"victim",
        "the planted link's target must be untouched"
    );
    let final_path = errors_dir.join(FINAL_NAME);
    let meta = std::fs::symlink_metadata(&final_path).unwrap();
    assert!(
        meta.file_type().is_file(),
        "the final name must be a regular file, not the planted link"
    );
    assert_eq!(
        decode_report(&final_path),
        vec!["{\"errorMessages\":[\"x\"]}"]
    );
}

/// Report objects and the manifest hold item values, so they are created
/// readable and writable by the server's own user only (0600), and every
/// directory the render creates beneath the bucket is 0700. The bucket
/// directory is the caller's and keeps its own mode.
#[cfg(unix)]
#[tokio::test]
async fn report_files_and_the_directories_the_render_creates_are_private_to_the_server_user() {
    use std::os::unix::fs::PermissionsExt;
    let mode = |path: &Path| {
        std::fs::symlink_metadata(path)
            .unwrap()
            .permissions()
            .mode()
            & 0o777
    };
    let root = Root::new();
    let bucket = root.bucket_dir(OWNER, "loads");
    let bucket_mode = mode(&bucket);

    let records = vec![serde_json::json!({"errorMessages": ["x"]})];
    render_one(&sink(&root), "reports/daily", records)
        .await
        .unwrap();
    sink(&root)
        .write_manifest(
            &at("loads"),
            "reports/daily",
            OP_ID,
            LEASE_OWNER,
            &OutputSse::None,
            vec![serde_json::json!({"fileProcessingStatus": "FULLY_PROCESSED"})],
        )
        .await
        .unwrap();

    let operations = bucket
        .join("reports")
        .join("daily")
        .join("AWSDynamoDB")
        .join("BulkOperation");
    let errors = operations
        .join(format!("{OWNER}:{OP_ID}-0"))
        .join("output")
        .join("errors");
    let manifest = operations
        .join(format!("{OWNER}:{OP_ID}"))
        .join("manifest-files.json");
    for file in [errors.join(FINAL_NAME), manifest] {
        assert_eq!(mode(&file), 0o600, "{}", file.display());
    }
    let mut created = vec![bucket.join("reports")];
    for component in [
        "daily".to_owned(),
        "AWSDynamoDB".to_owned(),
        "BulkOperation".to_owned(),
        format!("{OWNER}:{OP_ID}-0"),
        "output".to_owned(),
        "errors".to_owned(),
    ] {
        let next = created.last().unwrap().join(component);
        created.push(next);
    }
    created.push(operations.join(format!("{OWNER}:{OP_ID}")));
    for dir in &created {
        assert_eq!(mode(dir), 0o700, "{}", dir.display());
    }
    assert_eq!(mode(&bucket), bucket_mode, "the bucket keeps its mode");
}
