// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

const TID: &str = "0865b556-c8dd-433c-bb2a-f6a8fb6b6e65";
const OLD: &str = "2026-10-24T08:54:14.037";
const NEW: &str = "2026-10-24T08:54:32.805";

fn spec(enabled: bool, view: Option<StreamViewType>) -> StreamSpecification {
    StreamSpecification {
        stream_enabled: enabled,
        stream_view_type: view,
    }
}

fn table(label: Option<&str>, spec: Option<StreamSpecification>) -> TableStreamRow {
    TableStreamRow {
        table_name: "t482".to_owned(),
        table_id: TID.to_owned(),
        table_status: "ACTIVE".to_owned(),
        stream_label: label.map(str::to_owned),
        stream_specification: spec,
    }
}

fn row(shard_id: String, end: Option<&str>) -> ShardRow {
    ShardRow {
        table_id: TID.to_owned(),
        shard_id,
        parent_shard_id: None,
        starting_sequence_number: "0".repeat(21),
        ending_sequence_number: end.map(str::to_owned),
    }
}

fn stream_rows(label: &str, view: StreamViewType, end: Option<&str>) -> Vec<ShardRow> {
    (0..SHARDS_PER_STREAM)
        .map(|i| row(shard_id(TID, label, view, i).unwrap(), end))
        .collect()
}

#[test]
fn the_two_refusals_are_the_measured_sentences() {
    let enabled = spec(true, Some(StreamViewType::NewAndOldImages));
    for requested in [
        spec(true, Some(StreamViewType::NewAndOldImages)),
        spec(true, Some(StreamViewType::KeysOnly)),
    ] {
        match stream_change("t482-1792832053", Some(&enabled), &requested) {
            Err(StorageError::Validation(m)) => assert_eq!(
                m,
                "Table already has an enabled stream: TableName: t482-1792832053"
            ),
            other => panic!("{other:?}"),
        }
    }
    let disabled = spec(false, None);
    for current in [None, Some(&disabled)] {
        match stream_change("x482-nostream-1792832053", current, &disabled) {
            Err(StorageError::Validation(m)) => assert_eq!(
                m,
                "Table has no stream to disable: TableName: x482-nostream-1792832053"
            ),
            other => panic!("{other:?}"),
        }
    }
    assert_eq!(
        stream_change("t", Some(&enabled), &disabled).unwrap(),
        StreamChange::Disable
    );
    assert_eq!(
        stream_change("t", Some(&disabled), &enabled).unwrap(),
        StreamChange::Enable(StreamViewType::NewAndOldImages)
    );
    assert_eq!(
        stream_change("t", None, &enabled).unwrap(),
        StreamChange::Enable(StreamViewType::NewAndOldImages)
    );
}

#[test]
fn labels_round_trip_through_shard_ids() {
    assert_eq!(label_millis("1970-01-01T00:00:00.001"), Some(1));
    assert_eq!(label_millis("2026-10-24T08:54:14.03"), None);
    assert_eq!(label_millis("2026-10-24 08:54:14.037"), None);
    assert_eq!(label_millis("2026-13-24T08:54:14.037"), None);
    for view in [
        StreamViewType::KeysOnly,
        StreamViewType::NewImage,
        StreamViewType::OldImage,
        StreamViewType::NewAndOldImages,
    ] {
        let id = shard_id(TID, OLD, view, 3).unwrap();
        assert!(id.len() <= 65, "{id}");
        assert_eq!(
            parse_shard_id(TID, &id),
            Some((
                ShardStream::Labelled {
                    label: OLD.to_owned(),
                    view_type: view
                },
                3
            ))
        );
    }
    // Ids written before streams were named in shard ids: 16 digits on SQLite
    // and PostgreSQL, 12 on MongoDB and Cassandra.
    for legacy in [
        format!("shardId-{TID}-{:016}", 2),
        format!("shardId-{TID}-{:012}", 2),
    ] {
        assert_eq!(parse_shard_id(TID, &legacy), Some((ShardStream::Legacy, 2)));
    }
    assert_eq!(
        parse_shard_id(
            "other",
            &shard_id(TID, OLD, StreamViewType::KeysOnly, 0).unwrap()
        ),
        None
    );
    assert!(shard_id(TID, "not a label", StreamViewType::KeysOnly, 0).is_err());
}

#[test]
fn a_new_label_is_later_than_the_previous_one() {
    let far_future = "2999-01-01T00:00:00.000";
    assert_eq!(
        next_stream_label(Some(far_future)),
        "2999-01-01T00:00:00.001"
    );
    let fresh = next_stream_label(Some(OLD));
    assert!(fresh.as_str() > OLD, "{fresh}");
    assert!(label_millis(&next_stream_label(None)).is_some());
}

#[test]
fn a_disabled_stream_stays_describable_with_its_ending_numbers() {
    let mut shards = stream_rows(OLD, StreamViewType::NewAndOldImages, Some("1"));
    shards[3].ending_sequence_number = Some(shards[3].starting_sequence_number.clone());
    let t = table(Some(OLD), None);
    let d = describe_stream(
        "arn".into(),
        &t,
        Vec::new(),
        OLD.into(),
        &shards,
        None,
        None,
    )
    .unwrap();
    assert!(matches!(d.stream_status, StreamStatus::Disabled));
    assert_eq!(d.stream_view_type, StreamViewType::NewAndOldImages);
    assert_eq!(d.shards.len(), 4);
    assert!(d.last_evaluated_shard_id.is_none());
    assert!(
        d.shards
            .iter()
            .all(|s| s.sequence_number_range.ending_sequence_number.is_some())
    );

    // Limit=2 pages through the disabled stream as through an enabled one.
    let p1 = describe_stream(
        "arn".into(),
        &t,
        Vec::new(),
        OLD.into(),
        &shards,
        Some(2),
        None,
    )
    .unwrap();
    assert_eq!(p1.shards.len(), 2);
    let start = p1.last_evaluated_shard_id.clone().unwrap();
    assert_eq!(start, p1.shards[1].shard_id);
    let p2 = describe_stream(
        "arn".into(),
        &t,
        Vec::new(),
        OLD.into(),
        &shards,
        Some(2),
        Some(&start),
    )
    .unwrap();
    assert_eq!(p2.shards.len(), 2);
    assert!(p2.last_evaluated_shard_id.is_none());
}

#[test]
fn a_re_enabled_table_has_two_streams_and_only_the_new_one_is_enabled() {
    let mut shards = stream_rows(OLD, StreamViewType::NewAndOldImages, Some("5"));
    shards.extend(stream_rows(NEW, StreamViewType::KeysOnly, None));
    let t = table(Some(NEW), Some(spec(true, Some(StreamViewType::KeysOnly))));
    let streams = table_streams(&t, &shards);
    assert_eq!(streams.len(), 2);
    assert_eq!(streams[0].label, OLD);
    assert!(matches!(stream_status(&t, OLD), StreamStatus::Disabled));
    assert!(matches!(stream_status(&t, NEW), StreamStatus::Enabled));
    let old = describe_stream("a".into(), &t, Vec::new(), OLD.into(), &shards, None, None).unwrap();
    assert_eq!(old.stream_view_type, StreamViewType::NewAndOldImages);
    let new = describe_stream("a".into(), &t, Vec::new(), NEW.into(), &shards, None, None).unwrap();
    assert!(
        new.shards
            .iter()
            .all(|s| !old.shards.iter().any(|o| o.shard_id == s.shard_id))
    );
    assert_eq!(open_shard_ids(&shards).len(), 4);

    // Each ARN accepts only its own stream's shards.
    validate_shard(Some(&t), OLD, &shards, &old.shards[0].shard_id).unwrap();
    for (label, shard) in [
        (OLD, &new.shards[0].shard_id),
        (NEW, &old.shards[0].shard_id),
    ] {
        match validate_shard(Some(&t), label, &shards, shard) {
            Err(StorageError::TableNotFound(m)) => assert_eq!(m, SHARD_NOT_FOUND),
            other => panic!("{other:?}"),
        }
    }
    match validate_shard(Some(&t), "2020-01-01T00:00:00.000", &shards, "s") {
        Err(StorageError::TableNotFound(m)) => {
            assert_eq!(m, "Requested resource not found: Stream does not exist");
        }
        other => panic!("{other:?}"),
    }
}

/// SQLite and PostgreSQL wrote `shardId-{table_name}-{index:016}` before
/// v0.1.14; those rows are this table's shards and its stream must stay
/// describable after an upgrade.
#[test]
fn name_based_shard_ids_from_before_v0_1_14_are_the_current_stream() {
    let rows: Vec<ShardRow> = (0..4)
        .map(|i| row(format!("shardId-t482-{i:016}"), None))
        .collect();
    let t = table(Some(OLD), Some(spec(true, Some(StreamViewType::NewImage))));
    let streams = table_streams(&t, &rows);
    assert_eq!(streams.len(), 1);
    assert_eq!(streams[0].label, OLD);
    assert_eq!(streams[0].view_type, StreamViewType::NewImage);
    assert_eq!(streams[0].shards.len(), 4);
    validate_shard(Some(&t), OLD, &rows, "shardId-t482-0000000000000002").unwrap();
    let d = describe_stream("arn".into(), &t, Vec::new(), OLD.into(), &rows, None, None).unwrap();
    assert_eq!(d.shards.len(), 4);
    assert!(matches!(d.stream_status, StreamStatus::Enabled));
    // A shard row of another table never reaches the grouping.
    let foreign = ShardRow {
        table_id: "other".to_owned(),
        ..row("shardId-other-0000000000000000".to_owned(), None)
    };
    assert_eq!(table_streams(&t, &[foreign]).len(), 0);
}

#[test]
fn legacy_shards_belong_to_the_current_label_until_a_new_stream_takes_it() {
    let legacy: Vec<ShardRow> = (0..4)
        .map(|i| row(format!("shardId-{TID}-{i:016}"), None))
        .collect();
    let t = table(Some(OLD), Some(spec(true, Some(StreamViewType::NewImage))));
    let streams = table_streams(&t, &legacy);
    assert_eq!(streams.len(), 1);
    assert_eq!(streams[0].label, OLD);
    assert_eq!(streams[0].view_type, StreamViewType::NewImage);

    let mut both = legacy;
    both.extend(stream_rows(NEW, StreamViewType::NewImage, None));
    let t = table(Some(NEW), Some(spec(true, Some(StreamViewType::NewImage))));
    let streams = table_streams(&t, &both);
    assert_eq!(
        streams.len(),
        1,
        "the legacy stream's label is no longer known"
    );
    assert_eq!(streams[0].label, NEW);
    assert!(streams[0].shards.iter().all(|s| s.shard_id.len() == 61));
}

#[test]
fn list_streams_orders_tables_descending_and_streams_newest_first() {
    let mut shards = stream_rows(OLD, StreamViewType::NewAndOldImages, Some("5"));
    shards.extend(stream_rows(NEW, StreamViewType::NewAndOldImages, None));
    let other_id = "11111111-2222-3333-4444-555555555555";
    shards.extend((0..4).map(|i| ShardRow {
        table_id: other_id.to_owned(),
        shard_id: shard_id(other_id, OLD, StreamViewType::KeysOnly, i).unwrap(),
        ..row(String::new(), None)
    }));
    let tables = vec![
        table(
            Some(NEW),
            Some(spec(true, Some(StreamViewType::NewAndOldImages))),
        ),
        TableStreamRow {
            table_name: "a-table".to_owned(),
            table_id: other_id.to_owned(),
            ..table(Some(OLD), Some(spec(true, Some(StreamViewType::KeysOnly))))
        },
    ];
    let arn = |t: &str, l: &str| format!("{t}/{l}");
    let (all, last) = list_streams(&tables, &shards, None, 100, None, arn).unwrap();
    let got: Vec<String> = all.iter().map(|s| s.stream_arn.clone()).collect();
    assert_eq!(
        got,
        [
            format!("t482/{NEW}"),
            format!("t482/{OLD}"),
            format!("a-table/{OLD}")
        ]
    );
    assert!(last.is_none());

    let (only, last) = list_streams(&tables, &shards, Some("t482"), 100, None, arn).unwrap();
    assert_eq!(only.len(), 2);
    assert_eq!(only[0].stream_label, NEW);
    assert!(last.is_none());

    // Limit=1 over the two streams of one table: three pages, the last ARN
    // present on every page the limit cut, the third page empty.
    let page = |start: Option<&str>| {
        list_streams(
            &tables,
            &shards,
            Some("t482"),
            1,
            start.map(|l| ("t482".to_owned(), l.to_owned())),
            arn,
        )
        .unwrap()
    };
    let (p1, last) = page(None);
    assert_eq!(p1[0].stream_label, NEW);
    assert_eq!(last.as_deref(), Some(format!("t482/{NEW}").as_str()));
    let (p2, last) = page(Some(NEW));
    assert_eq!(p2[0].stream_label, OLD);
    assert_eq!(last.as_deref(), Some(format!("t482/{OLD}").as_str()));
    let (p3, last) = page(Some(OLD));
    assert!(p3.is_empty());
    assert!(last.is_none());

    // No Limit: after the older stream nothing, after the newest the older.
    let from = |l: &str| {
        list_streams(
            &tables,
            &shards,
            Some("t482"),
            100,
            Some(("t482".to_owned(), l.to_owned())),
            arn,
        )
        .unwrap()
    };
    let (after_old, last) = from(OLD);
    assert!(after_old.is_empty());
    assert!(last.is_none());
    let (after_new, last) = from(NEW);
    assert_eq!(after_new.len(), 1);
    assert_eq!(after_new[0].stream_label, OLD);
    assert!(last.is_none());

    // A start ARN of another table, with TableName, is refused.
    match list_streams(
        &tables,
        &shards,
        Some("t482"),
        100,
        Some(("a-table".to_owned(), OLD.to_owned())),
        arn,
    ) {
        Err(StorageError::Validation(m)) => assert_eq!(
            m,
            format!(
                "One or more parameter values were invalid: ExclusiveStartStreamArn provided is invalid: a-table/{OLD}"
            )
        ),
        other => panic!("{other:?}"),
    }
}

#[test]
fn a_disabled_create_table_specification_is_not_stored() {
    let make = |spec: Option<StreamSpecification>| extenddb_core::types::CreateTableInput {
        table_name: "t".to_owned(),
        stream_specification: spec,
        ..Default::default()
    };
    assert_eq!(
        without_disabled_specification(make(Some(spec(false, None)))).stream_specification,
        None
    );
    let enabled = spec(true, Some(StreamViewType::KeysOnly));
    assert_eq!(
        without_disabled_specification(make(Some(enabled.clone()))).stream_specification,
        Some(enabled)
    );
    assert_eq!(
        without_disabled_specification(make(None)).stream_specification,
        None
    );
}

/// A disable closes the shards of the stream it decided on and no other,
/// including when an overlapping enable has already opened the next stream.
#[test]
fn a_close_selects_only_the_shards_of_its_label() {
    let mut shards = stream_rows(OLD, StreamViewType::NewAndOldImages, None);
    shards.extend(stream_rows(NEW, StreamViewType::KeysOnly, None));
    let old_ids = open_shard_ids_of(TID, OLD, &shards);
    assert_eq!(old_ids.len(), 4);
    assert!(
        old_ids
            .iter()
            .all(|id| shards[..4].iter().any(|r| &r.shard_id == id))
    );
    let new_ids = open_shard_ids_of(TID, NEW, &shards);
    assert_eq!(new_ids.len(), 4);
    assert!(old_ids.iter().all(|id| !new_ids.contains(id)));
    // A label no shard carries selects nothing.
    assert!(open_shard_ids_of(TID, "2020-01-01T00:00:00.000", &shards).is_empty());
    // Closed shards are never selected again.
    shards[0].ending_sequence_number = Some("5".to_owned());
    assert_eq!(open_shard_ids_of(TID, OLD, &shards).len(), 3);

    // Legacy rows belong to whatever label the change read, until a labelled
    // stream carries that label.
    let legacy: Vec<ShardRow> = (0..4)
        .map(|i| row(format!("shardId-t482-{i:016}"), None))
        .collect();
    assert_eq!(open_shard_ids_of(TID, OLD, &legacy).len(), 4);
    let mut both = legacy.clone();
    both.extend(stream_rows(OLD, StreamViewType::KeysOnly, None));
    assert_eq!(open_shard_ids_of(TID, OLD, &both).len(), 4);
    assert!(
        open_shard_ids_of(TID, OLD, &both)
            .iter()
            .all(|id| id.len() == 61)
    );
    // Another table's rows are never selected.
    let foreign = vec![ShardRow {
        table_id: "other".to_owned(),
        ..row(
            shard_id("other", OLD, StreamViewType::KeysOnly, 0).unwrap(),
            None,
        )
    }];
    assert!(open_shard_ids_of(TID, OLD, &foreign).is_empty());
}

#[test]
fn an_enable_re_read_sees_whether_its_stream_is_still_current() {
    let enabled = table(Some(NEW), Some(spec(true, Some(StreamViewType::NewImage))));
    assert!(enable_still_current(Some(&enabled), NEW));
    // A disable committed between the enable's catalog write and its shard
    // inserts: the label is still the enable's, the specification is not
    // enabled.
    let disabled = table(Some(NEW), Some(spec(false, Some(StreamViewType::NewImage))));
    assert!(!enable_still_current(Some(&disabled), NEW));
    // A later enable replaced the label.
    assert!(!enable_still_current(Some(&enabled), OLD));
    // No specification at all, or no table row.
    assert!(!enable_still_current(Some(&table(Some(NEW), None)), NEW));
    assert!(!enable_still_current(None, NEW));
}
