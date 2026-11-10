// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use std::sync::Mutex;

use extenddb_core::types::AttributeValue;
use extenddb_storage::pitr::ReconstructPage;
use futures::future::BoxFuture;
use serde_json::json;

use super::*;

/// A state enabled at `enabled` (epoch seconds and nanoseconds).
fn state_at(secs: u64, nanos: u32) -> PitrState {
    PitrState::new(35, UNIX_EPOCH + Duration::new(secs, nanos))
}

fn at(secs: u64, nanos: u32) -> SystemTime {
    UNIX_EPOCH + Duration::new(secs, nanos)
}

fn invalid(r: Result<RestoreTime, DynamoDbError>) -> String {
    match r {
        Err(DynamoDbError::InvalidRestoreTimeException(m)) => m,
        other => panic!("{other:?}"),
    }
}

/// The restore time of a request body, through the two checks.
fn time_of(
    body: Value,
    s: &PitrState,
    now: SystemTime,
    table: &str,
) -> Result<RestoreTime, DynamoDbError> {
    restore_time(time_members(&body)?.requested()?, s, now, table)
}

fn window_text(requested: &str, earliest: &str, latest: &str, table: &str) -> String {
    format!(
        "RestoreDateTime '{requested}' must be within EarliestRestorableDateTime '{earliest}' \
         and LatestRestorableDateTime '{latest}' for table '{table}'"
    )
}

#[test]
fn an_instant_prints_as_the_service_prints_it() {
    assert_eq!(utc_instant(1_794_275_217, 0), "2026-11-10T01:46:57Z");
    assert_eq!(utc_instant(0, 0), "1970-01-01T00:00:00Z");
    assert_eq!(utc_instant(951_782_400, 0), "2000-02-29T00:00:00Z");
    assert_eq!(utc_instant(0, 1_000_000), "1970-01-01T00:00:00.001Z");
    assert_eq!(utc_instant(0, 1_000), "1970-01-01T00:00:00.000001Z");
    assert_eq!(
        utc_instant(1_794_284_405, 708_986_387),
        "2026-11-10T04:20:05.708986387Z"
    );
    // A time past the 64-bit millisecond range prints at its end.
    assert_eq!(utc_millis(i64::MAX), "+292278994-08-17T07:12:55.807Z");
    assert_eq!(utc_millis(i64::MIN), "-292275055-05-16T16:47:04.192Z");
    assert_eq!(utc_millis(253_402_300_800_000), "+10000-01-01T00:00:00Z");
    assert_eq!(utc_millis(253_402_300_799_999), "9999-12-31T23:59:59.999Z");
    assert_eq!(utc_millis(-62_135_596_801_000), "0000-12-31T23:59:59Z");
    assert_eq!(utc_millis(-1_000), "1969-12-31T23:59:59Z");
}

/// Measured on the live service, two samples each, on a table enabled in
/// second 1794284219.
#[test]
fn the_refusal_prints_the_requested_time_to_the_millisecond() {
    let table = "src-e";
    let s = state_at(1_794_284_219, 950_000_000);
    let early = at(1_794_284_268, 157_390_000);
    let floor = "2026-11-10T04:16:59Z";
    let date =
        |v: Value, now: SystemTime| invalid(time_of(json!({"RestoreDateTime": v}), &s, now, table));
    assert_eq!(
        date(json!(1_794_284_159), early),
        window_text("2026-11-10T04:15:59Z", floor, floor, table)
    );
    assert_eq!(
        date(json!(1_794_284_298.157_383), early),
        window_text("2026-11-10T04:18:18.157Z", floor, floor, table)
    );
    assert_eq!(
        date(json!(-1), early),
        window_text("1969-12-31T23:59:59Z", floor, floor, table)
    );
    assert_eq!(
        date(json!(1e20), early),
        window_text("+292278994-08-17T07:12:55.807Z", floor, floor, table)
    );
    // Once the lag passes, Latest is the request's time less five
    // minutes, to the nanosecond.
    assert_eq!(
        date(json!(1_794_284_218.0), at(1_794_284_705, 708_986_387)),
        window_text(
            "2026-11-10T04:16:58Z",
            floor,
            "2026-11-10T04:20:05.708986387Z",
            table
        )
    );
}

/// Measured on the live service (three times refused, two samples each; two
/// accepted) on a table enabled in second 1794284900.
#[test]
fn the_window_is_checked_to_the_millisecond() {
    let table = "src-f";
    let s = state_at(1_794_284_900, 900_000_000);
    let now = at(1_794_284_904, 900_000_000);
    let floor = "2026-11-10T04:28:20Z";
    let date = |v: Value| time_of(json!({"RestoreDateTime": v}), &s, now, table);
    for (v, text) in [
        (json!(1_794_284_899.5), "2026-11-10T04:28:19.500Z"),
        (json!(1_794_284_899.999), "2026-11-10T04:28:19.999Z"),
        (json!(1_794_284_906.400_066_1), "2026-11-10T04:28:26.400Z"),
        (json!(0.001), "1970-01-01T00:00:00.001Z"),
        (json!(0.0004), "1970-01-01T00:00:00Z"),
    ] {
        assert_eq!(invalid(date(v)), window_text(text, floor, floor, table));
    }
    let a1 = date(json!(1_794_284_900.0)).expect("the floor");
    assert_eq!(a1.restore_date_time, 1_794_284_900.0);
    let a2 = date(json!(1_794_284_900.5)).expect("inside the window");
    assert_eq!(a2.restore_date_time, 1_794_284_900.5);
    assert_eq!(a2.at, at(1_794_284_900, 900_000_000));
}

/// Measured on the live service, two samples each, on a table enabled in
/// second 1794289159; the requested time rounds to the millisecond and
/// saturates at the 64-bit millisecond range.
#[test]
fn the_requested_time_rounds_and_saturates() {
    let table = "src-v";
    let s = state_at(1_794_289_159, 400_000_000);
    let now = at(1_794_289_170, 0);
    let floor = "2026-11-10T05:39:19Z";
    for (v, text) in [
        (json!(1_794_284_900.000_5), "2026-11-10T04:28:20.001Z"),
        (json!(1_794_284_900.001_5), "2026-11-10T04:28:20.002Z"),
        (json!(1e19), "+292278994-08-17T07:12:55.807Z"),
        (json!(-1e20), "-292275055-05-16T16:47:04.192Z"),
        (json!(253_402_300_800_i64), "+10000-01-01T00:00:00Z"),
        (json!(253_402_300_799.999), "9999-12-31T23:59:59.999Z"),
        (json!(-62_135_596_801_i64), "0000-12-31T23:59:59Z"),
    ] {
        assert_eq!(
            invalid(time_of(json!({"RestoreDateTime": v}), &s, now, table)),
            window_text(text, floor, floor, table)
        );
    }
}

/// Measured on the live service: RestoreSummary.RestoreDateTime
/// keeps the requested milliseconds, rounded.
#[test]
fn the_restore_runs_at_the_requested_millisecond() {
    let s = state_at(1_794_284_219, 950_000_000);
    let rs2 = time_of(
        json!({"RestoreDateTime": 1_794_284_271.294_530_2}),
        &s,
        at(1_794_284_273, 294_535_900),
        "src",
    )
    .expect("a time with milliseconds");
    assert_eq!(rs2.restore_date_time, 1_794_284_271.295);
    assert_eq!(rs2.at, at(1_794_284_271, 295_000_000));
    let re10 = time_of(
        json!({"RestoreDateTime": 1_794_284_703.024_852}),
        &s,
        at(1_794_284_705, 24_859_000),
        "src",
    )
    .expect("a time with microseconds");
    assert_eq!(re10.restore_date_time, 1_794_284_703.025);
}

/// A restore at the reported floor reconstructs at the enable instant, so
/// it holds the baseline the enable took (measured: live restores at
/// EarliestRestorableDateTime hold the items written before the enable).
#[test]
fn a_restore_at_the_floor_reconstructs_at_the_enable_instant() {
    let s = state_at(1_794_284_900, 900_000_000);
    let r = time_of(
        json!({"RestoreDateTime": 1_794_284_900}),
        &s,
        at(1_794_284_905, 0),
        "src",
    )
    .expect("the floor");
    assert_eq!(r.at, s.enabled_at);
    assert_eq!(r.restore_date_time, 1_794_284_900.0);
}

/// Measured on the live service: with UseLatestRestorableTime the summary
/// is the reported Latest, the enable second for the first five minutes.
#[test]
fn use_latest_restores_to_the_reported_latest() {
    let s = state_at(1_794_284_219, 250_000_000);
    let latest = |now: SystemTime| {
        time_of(json!({"UseLatestRestorableTime": true}), &s, now, "src")
            .expect("latest")
            .restore_date_time
    };
    assert_eq!(latest(at(1_794_284_272, 648_000_000)), 1_794_284_219.0);
    assert_eq!(latest(at(1_794_284_639, 500_000_000)), 1_794_284_339.5);
}

fn serialization_text(body: Value) -> String {
    match time_members(&body) {
        Err(DynamoDbError::SerializationException(m)) => m,
        other => panic!("{other:?}"),
    }
}

/// Measured on the live service: the deserializer's text
/// for a RestoreDateTime that is not a number.
#[test]
fn a_restore_date_time_that_is_not_a_number_is_a_serialization_error() {
    for v in [
        json!("abc"),
        json!("2026-01-01T00:00:00Z"),
        json!("1794284900"),
    ] {
        assert_eq!(
            serialization_text(json!({"RestoreDateTime": v})),
            "STRING_VALUE cannot be converted to Date"
        );
    }
    assert_eq!(
        serialization_text(json!({"RestoreDateTime": true})),
        "TRUE_VALUE cannot be converted to Date"
    );
    assert_eq!(
        serialization_text(json!({"RestoreDateTime": false})),
        "FALSE_VALUE cannot be converted to Date"
    );
    assert_eq!(serialization_text(json!({"RestoreDateTime": []})), "");
    assert_eq!(
        serialization_text(json!({"RestoreDateTime": {}})),
        "Start of structure or map found where not expected"
    );
}

/// Measured on the live service: the spellings
/// of UseLatestRestorableTime, and the deserializer's text for the rest.
#[test]
fn use_latest_takes_the_services_spellings() {
    let flag = |v: Value| {
        time_members(&json!({"UseLatestRestorableTime": v}))
            .expect("a flag")
            .use_latest
    };
    for v in ["true", "True", "TRUE", "yes", "y", "t", "1"] {
        assert!(flag(json!(v)), "{v}");
    }
    for v in ["false", "False", "FALSE", "no", "n", "f", "0"] {
        assert!(!flag(json!(v)), "{v}");
    }
    assert!(flag(json!(true)));
    assert!(!flag(Value::Null));
    for v in ["off", " true", "", "null"] {
        assert_eq!(
            serialization_text(json!({"UseLatestRestorableTime": v})),
            "Unexpected token received from parser"
        );
    }
    for v in [json!(0), json!(1)] {
        assert_eq!(
            serialization_text(json!({"UseLatestRestorableTime": v})),
            "NUMBER_VALUE cannot be converted to Boolean"
        );
    }
    assert_eq!(
        serialization_text(json!({"UseLatestRestorableTime": 1.5})),
        "DECIMAL_VALUE cannot be converted to Boolean"
    );
    assert_eq!(
        serialization_text(json!({"UseLatestRestorableTime": []})),
        ""
    );
    assert_eq!(
        serialization_text(json!({"UseLatestRestorableTime": {}})),
        "Start of structure or map found where not expected"
    );
}

/// Measured on the live service: a string true with a RestoreDateTime is the
/// two-times refusal; a string false with one restores to that time.
#[test]
fn the_spellings_count_in_the_exclusivity_check() {
    let requested = |v: &str| {
        time_members(&json!({"UseLatestRestorableTime": v, "RestoreDateTime": 1000}))
            .expect("types")
            .requested()
    };
    for v in ["TRUE", "yes"] {
        match requested(v) {
            Err(DynamoDbError::ValidationException(m)) => {
                assert_eq!(m, super::super::TWO_RESTORE_TIMES);
            }
            other => panic!("{other:?}"),
        }
    }
    assert_eq!(
        requested("false").ok(),
        Some(Requested::AtMillis(1_000_000))
    );
}

#[test]
fn the_members_are_checked_before_any_table() {
    let bounds = (3, 255);
    let msg = |body: Value| match restore_members(&body, bounds) {
        Err(DynamoDbError::ValidationException(m)) => m,
        Ok(_) => "ok".to_owned(),
        Err(other) => format!("{other:?}"),
    };
    assert_eq!(
        msg(json!({"TargetTableName": "x"})),
        "TableName must be at least 3 characters long and at most 255 characters long"
    );
    assert!(
        msg(json!({"TargetTableName": "abc", "BillingModeOverride": "NOPE"})).contains(
            "Value 'NOPE' at 'billingModeOverride' failed to satisfy constraint: Member must \
         satisfy enum value set: [PROVISIONED, PAY_PER_REQUEST]"
        )
    );
    assert_eq!(msg(json!({"TargetTableName": "abc"})), "ok");
    // The time members' types come before the other members (measured with
    // a source name the pattern refuses, and with a bad billing mode and a
    // target name too short).
    for body in [
        json!({"TargetTableName": "x", "RestoreDateTime": "abc"}),
        json!({"TargetTableName": "abc", "BillingModeOverride": "BAD", "RestoreDateTime": "abc"}),
    ] {
        assert_eq!(
            msg(body),
            "SerializationException(\"STRING_VALUE cannot be converted to Date\")"
        );
    }
    assert_eq!(
        msg(
            json!({"TargetTableName": "abc", "BillingModeOverride": "BAD",
            "UseLatestRestorableTime": "off"})
        ),
        "SerializationException(\"Unexpected token received from parser\")"
    );
}

/// A floor the retention sets is not a whole second, and the window is
/// checked against it to the millisecond.
#[test]
fn a_time_before_a_retention_floor_is_refused() {
    let s = PitrState::new(1, UNIX_EPOCH + Duration::from_secs(1_794_000_000));
    let now = at(1_794_284_900, 700_000_000);
    let floor = 1_794_284_900.7 - 86_400.0;
    let date = |t: f64| time_of(json!({"RestoreDateTime": t}), &s, now, "src");
    assert!(matches!(
        date(floor - 0.3),
        Err(DynamoDbError::InvalidRestoreTimeException(_))
    ));
    assert_eq!(date(floor).expect("the floor").restore_date_time, floor);
}

/// A store that serves fixed pages and records each call's cursor.
struct Pages {
    pages: Vec<ReconstructPage>,
    calls: Mutex<Vec<Option<i64>>>,
}

impl PitrCaptureStore for Pages {
    fn pitr_state<'a>(
        &'a self,
        _: &'a str,
    ) -> BoxFuture<'a, Result<Option<PitrState>, StorageError>> {
        Box::pin(async { Ok(None) })
    }
    fn enable_pitr<'a>(
        &'a self,
        _: &'a extenddb_core::types::TableKeyInfo,
        _: Option<u8>,
        _: SystemTime,
    ) -> BoxFuture<'a, Result<PitrState, StorageError>> {
        unimplemented!()
    }
    fn disable_pitr<'a>(
        &'a self,
        _: &'a extenddb_core::types::TableKeyInfo,
    ) -> BoxFuture<'a, Result<(), StorageError>> {
        unimplemented!()
    }
    fn reconstruct_page<'a>(
        &'a self,
        _: &'a str,
        _: SystemTime,
        after: Option<i64>,
        _: u32,
    ) -> BoxFuture<'a, Result<ReconstructPage, StorageError>> {
        Box::pin(async move {
            let mut calls = self.calls.lock().expect("calls");
            let i = calls.len();
            calls.push(after);
            match self.pages.get(i) {
                Some(p) => Ok(ReconstructPage {
                    items: p.items.clone(),
                    next: p.next,
                }),
                None => Err(StorageError::Internal("read past the last page".to_owned())),
            }
        })
    }
    fn prune_before<'a>(
        &'a self,
        _: &'a str,
        _: SystemTime,
        _: u32,
    ) -> BoxFuture<'a, Result<u64, StorageError>> {
        unimplemented!()
    }
    fn pitr_tables(&self) -> BoxFuture<'_, Result<Vec<(String, PitrState)>, StorageError>> {
        unimplemented!()
    }
    fn remove_orphaned_history(&self, _: u32) -> BoxFuture<'_, Result<u64, StorageError>> {
        unimplemented!()
    }
}

fn item(pk: &str) -> Item {
    Item::from([("pk".to_owned(), AttributeValue::S(pk.to_owned()))])
}

#[tokio::test]
async fn the_stream_follows_the_cursors_and_stops_after_the_last_page() {
    let store = Pages {
        pages: vec![
            ReconstructPage {
                items: vec![item("a"), item("b")],
                next: Some(7),
            },
            ReconstructPage {
                items: vec![],
                next: Some(9),
            },
            ReconstructPage {
                items: vec![item("c")],
                next: None,
            },
        ],
        calls: Mutex::new(Vec::new()),
    };
    let items: Vec<Item> = reconstruct_stream(&store, "t", at(1, 0), 2)
        .map(|r| r.expect("item"))
        .collect()
        .await;
    assert_eq!(items, vec![item("a"), item("b"), item("c")]);
    assert_eq!(
        *store.calls.lock().expect("calls"),
        vec![None, Some(7), Some(9)]
    );
}

#[tokio::test]
async fn a_failed_page_ends_the_stream_with_its_error() {
    let store = Pages {
        pages: vec![ReconstructPage {
            items: vec![item("a")],
            next: Some(1),
        }],
        calls: Mutex::new(Vec::new()),
    };
    let got: Vec<Result<Item, StorageError>> =
        reconstruct_stream(&store, "t", at(1, 0), 1).collect().await;
    assert_eq!(got.len(), 2);
    assert!(got[0].is_ok());
    assert!(got[1].is_err());
}

fn source() -> TableDescription {
    TableDescription {
        table_name: "src".to_owned(),
        table_id: "src-id".to_owned(),
        table_arn: "arn:aws:dynamodb:us-east-1:000000000000:table/src".to_owned(),
        key_schema: serde_json::from_value(json!([{"AttributeName": "pk", "KeyType": "HASH"}]))
            .expect("keys"),
        attribute_definitions: serde_json::from_value(json!([
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "g", "AttributeType": "S"}
        ]))
        .expect("attributes"),
        billing_mode_summary: Some(
            serde_json::from_value(json!({"BillingMode": "PAY_PER_REQUEST"})).expect("billing"),
        ),
        global_secondary_indexes: Some(vec![
            serde_json::from_value(json!({
                "IndexName": "gsi",
                "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
                "IndexStatus": "ACTIVE",
                "IndexSizeBytes": 0,
                "ItemCount": 0,
                "IndexArn": "arn:aws:dynamodb:us-east-1:000000000000:table/src/index/gsi"
            }))
            .expect("gsi"),
        ]),
        stream_specification: serde_json::from_value(
            json!({"StreamEnabled": true, "StreamViewType": "NEW_IMAGE"}),
        )
        .ok(),
        ..Default::default()
    }
}

fn members(body: Value) -> RestoreMembers {
    restore_members(&body, (3, 255)).expect("members")
}

#[tokio::test]
async fn the_target_takes_the_source_definition_with_the_overrides() {
    let source = source();
    let limits = LimitsConfig::default();
    let input = target_input(
        &source,
        &members(json!({"TargetTableName": "dst"})),
        &limits,
    )
    .expect("input");
    assert_eq!(input.table_name, "dst");
    assert_eq!(input.key_schema, source.key_schema);
    assert_eq!(
        input.billing_mode,
        Some(extenddb_core::types::BillingMode::PayPerRequest)
    );
    assert_eq!(
        input
            .global_secondary_indexes
            .as_ref()
            .map(|g| g.iter().map(|i| i.index_name.clone()).collect::<Vec<_>>()),
        Some(vec!["gsi".to_owned()])
    );
    assert!(input.stream_specification.is_none());

    let dropped = target_input(
        &source,
        &members(json!({"TargetTableName": "dst", "GlobalSecondaryIndexOverride": []})),
        &limits,
    )
    .expect("input");
    assert!(dropped.global_secondary_indexes.is_none());

    let refused = target_input(
        &source,
        &members(json!({"TargetTableName": "dst", "BillingModeOverride": "PROVISIONED"})),
        &limits,
    );
    match refused {
        Err(DynamoDbError::ValidationException(m)) => assert_eq!(
            m,
            "Invalid Request: Must override ProvisionedThroughput if BillingMode is overridden \
             to PROVISIONED"
        ),
        other => panic!("{other:?}"),
    }

    let gsi = target_input(
        &source,
        &members(json!({
            "TargetTableName": "dst",
            "BillingModeOverride": "PROVISIONED",
            "ProvisionedThroughputOverride": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}
        })),
        &limits,
    );
    match gsi {
        Err(DynamoDbError::ValidationException(m)) => assert_eq!(
            m,
            "Invalid Request: Must override ProvisionedThroughput if BillingMode is overridden \
             to PROVISIONED for index gsi"
        ),
        other => panic!("{other:?}"),
    }
}
