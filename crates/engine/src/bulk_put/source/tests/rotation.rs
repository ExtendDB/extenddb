// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Report object rotation: the writer rolls to `output-00001.json.gz` and
//! onward once the compressed bytes of the current object reach the
//! threshold (the hosted service closed its first object at
//! 201.73 MiB and wrote the remainder to the second).

use super::*;

/// The report directory of `OP_ID`'s error report under `bucket`.
fn errors_dir(root: &Root, bucket: &str) -> PathBuf {
    root.0
        .join(OWNER)
        .join(bucket)
        .join("AWSDynamoDB")
        .join("BulkOperation")
        .join(format!("{OWNER}:{OP_ID}-0"))
        .join("output")
        .join("errors")
}

/// Every entry name in `dir`, sorted.
fn names_in(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

/// A hex payload of `len` characters from a fixed-seed xorshift, so the
/// gzip stream gains little from compression and bytes reach the file
/// as pages are written.
fn noise(seed: u64, len: usize) -> String {
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut out = String::with_capacity(len);
    while out.len() < len {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        out.push_str(&format!("{state:016x}"));
    }
    out.truncate(len);
    out
}

/// One page of `count` records of about 4 KiB each, numbered from
/// `first`.
fn page(first: usize, count: usize) -> Vec<Value> {
    (first..first + count)
        .map(|i| {
            serde_json::json!({
                "itemLocation": {"itemIndex": i},
                "errorMessages": [noise(i as u64, 4096)],
            })
        })
        .collect()
}

fn encoded(records: &[Value]) -> Vec<String> {
    records.iter().map(Value::to_string).collect()
}

/// The page that trips the threshold stays in the object it
/// was written to; the next page opens the next numbered object. A
/// small page under the threshold shares its object with the page that
/// trips it.
#[tokio::test]
async fn pages_past_the_compressed_threshold_roll_to_numbered_objects() {
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
    writer.set_rotation_threshold(32 * 1024);

    // One small page, then three pages of about 128 KiB raw each, every
    // one of which passes 32 KiB compressed on its own.
    let pages = [page(0, 1), page(1, 32), page(33, 32), page(65, 32)];
    for records in &pages {
        writer.write_page(records.clone()).await.unwrap();
    }
    writer.finish().await.unwrap();

    let dir = errors_dir(&root, "loads");
    assert_eq!(
        names_in(&dir),
        vec![
            "output-00000.json.gz",
            "output-00001.json.gz",
            "output-00002.json.gz"
        ],
        "three objects, no temp file"
    );
    let mut expected_first = encoded(&pages[0]);
    expected_first.extend(encoded(&pages[1]));
    assert_eq!(
        decode_report(&dir.join("output-00000.json.gz")),
        expected_first,
        "the small page and the page that tripped the threshold share the first object"
    );
    assert_eq!(
        decode_report(&dir.join("output-00001.json.gz")),
        encoded(&pages[2])
    );
    assert_eq!(
        decode_report(&dir.join("output-00002.json.gz")),
        encoded(&pages[3])
    );
    let total: usize = names_in(&dir)
        .iter()
        .map(|name| decode_report(&dir.join(name)).len())
        .sum();
    assert_eq!(total, 97, "every record lands in exactly one object");
    for name in names_in(&dir) {
        let lines = decode_report(&dir.join(&name));
        assert!(!lines.is_empty(), "{name} must not be empty");
        for line in lines {
            serde_json::from_str::<Value>(&line).expect("every line is one complete JSON record");
        }
    }
}

/// A re-render that produces fewer objects than an earlier incarnation
/// left behind removes the surplus numbered objects, and the open sweep
/// removes a dead owner's temp for any object index.
#[tokio::test]
async fn a_re_render_with_fewer_objects_removes_the_surplus_and_sweeps_every_temp() {
    let root = Root::new();
    root.bucket_dir(OWNER, "loads");
    let sink = sink(&root);

    // An earlier incarnation rendered three objects.
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
    writer.set_rotation_threshold(32 * 1024);
    for records in [page(0, 32), page(32, 32), page(64, 32)] {
        writer.write_page(records).await.unwrap();
    }
    writer.finish().await.unwrap();
    let dir = errors_dir(&root, "loads");
    assert_eq!(names_in(&dir).len(), 3);

    // A dead owner's temp for the second object sits beside them.
    let dead_temp = dir.join("output-00001.json.gz.tmp-dead-owner");
    std::fs::write(&dead_temp, b"partial").unwrap();

    // The re-render fits in one object.
    render_one(&sink, "", vec![serde_json::json!({"errorMessages": ["a"]})])
        .await
        .unwrap();
    assert_eq!(
        names_in(&dir),
        vec!["output-00000.json.gz"],
        "the surplus objects and the dead owner's temp are gone"
    );
    assert_eq!(
        decode_report(&dir.join("output-00000.json.gz")),
        vec!["{\"errorMessages\":[\"a\"]}"]
    );
}

/// Dropping a writer that has rolled removes every object's temp file.
#[tokio::test]
async fn dropping_a_rolled_writer_leaves_no_temp_file() {
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
    writer.set_rotation_threshold(32 * 1024);
    writer.write_page(page(0, 32)).await.unwrap();
    writer.write_page(page(32, 32)).await.unwrap();
    drop(writer);

    let dir = errors_dir(&root, "loads");
    assert!(
        names_in(&dir).is_empty(),
        "every temp is removed: {:?}",
        names_in(&dir)
    );
}

/// A page that takes the open object to exactly the
/// rotation threshold closes it (the comparison is `>=`, not `>`). The exact
/// edge is not measured on the service (the measured case was an overshoot); this
/// pins the code's boundary. A probe writer measures the compressed bytes
/// one page writes to a fresh object; a second writer whose threshold is
/// exactly that figure writes the same page, landing written == threshold,
/// and must roll so the next page opens the next object.
#[tokio::test]
async fn a_page_landing_exactly_on_the_threshold_rolls_to_the_next_object() {
    let root = Root::new();
    root.bucket_dir(OWNER, "loads");
    let sink = sink(&root);

    let probe_records = page(0, 64);
    let mut probe = sink
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
    probe.set_rotation_threshold(u64::MAX);
    probe.write_page(probe_records.clone()).await.unwrap();
    let exact = probe.open_object_bytes();
    assert!(exact > 0, "the probe page wrote compressed bytes");
    drop(probe);

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
    writer.set_rotation_threshold(exact);
    writer.write_page(probe_records).await.unwrap();
    writer.write_page(page(64, 32)).await.unwrap();
    writer.finish().await.unwrap();

    let dir = errors_dir(&root, "loads");
    assert_eq!(
        names_in(&dir),
        vec!["output-00000.json.gz", "output-00001.json.gz"],
        "a page exactly on the threshold rolls to the next object"
    );
}

/// The production rotation threshold is 200 MiB.
/// Every rotation test lowers it through the test hook, so the constant
/// itself is otherwise unpinned (halving it passes the suite). The hosted
/// service was measured closing its first report object at 200 MiB plus the
/// records buffered when the threshold tripped.
#[test]
fn the_production_rotation_threshold_is_two_hundred_mebibytes() {
    assert_eq!(super::super::sink::ROTATION_THRESHOLD_BYTES, 209_715_200);
    assert_eq!(
        super::super::sink::ROTATION_THRESHOLD_BYTES,
        200 * 1024 * 1024
    );
}
