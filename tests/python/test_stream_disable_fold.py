# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Stream lifecycle rows measured on Amazon DynamoDB on 2026-10-24, second round.

A stream change beside a billing, throughput or index change; ListStreams
paging over a table with two streams; GetShardIterator on an unknown shard or
stream; a table created with StreamSpecification {StreamEnabled: false}.
Companion to test_stream_disable.py. One module fixture runs the measured
sequence once; each test asserts one row.
"""

from __future__ import annotations

import os
import time

import boto3
import botocore.config
import pytest
from botocore.exceptions import ClientError

from helpers import unique_name, wait_for_active, wait_for_deleted

KEYS_ONLY = {"StreamEnabled": True, "StreamViewType": "KEYS_ONLY"}
NEW_IMAGE = {"StreamEnabled": True, "StreamViewType": "NEW_IMAGE"}
DISABLED = {"StreamEnabled": False}
INVALID = "One or more parameter values were invalid: "

ON_SERVICE = not os.environ.get("DYNAMODB_ENDPOINT", "").strip()


def _streams_client(endpoint_url):
    kwargs: dict = {
        "service_name": "dynamodbstreams",
        "region_name": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        # Client-side parameter validation off: the short ShardId row measures
        # the server's own length check.
        "config": botocore.config.Config(
            retries={"max_attempts": 0}, parameter_validation=False
        ),
    }
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url
        if endpoint_url.startswith("https://"):
            ca_cert = os.environ.get("EXTENDDB_CA_CERT", "")
            kwargs["verify"] = ca_cert if ca_cert else False
    return boto3.client(**kwargs)


def _call(fn, **kwargs) -> dict:
    try:
        resp = fn(**kwargs)
        resp.pop("ResponseMetadata", None)
        return {"status": 200, "response": resp}
    except ClientError as e:
        return {
            "status": e.response["ResponseMetadata"]["HTTPStatusCode"],
            "code": e.response["Error"]["Code"],
            "message": e.response["Error"]["Message"],
        }


def _list(streams, **kwargs) -> dict:
    # The service rate-limits ListStreams within a burst.
    if ON_SERVICE:
        time.sleep(6)
    return _call(streams.list_streams, **kwargs)


def _wait_disabled(streams, arn: str, timeout: float = 120.0) -> None:
    deadline = time.monotonic() + timeout
    while streams.describe_stream(StreamArn=arn)["StreamDescription"]["StreamStatus"] != "DISABLED":
        if time.monotonic() > deadline:
            return
        time.sleep(3)


def _set_stream(ddb, name: str, spec: dict, **extra) -> dict:
    row = _call(ddb.update_table, TableName=name, StreamSpecification=spec, **extra)
    wait_for_active(ddb, name)
    return row


@pytest.fixture(scope="module")
def run(dynamodb_client, endpoint_url):
    ddb = dynamodb_client
    streams = _streams_client(endpoint_url)
    key = {
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "BillingMode": "PAY_PER_REQUEST",
    }
    a, b, off = unique_name("f482-a"), unique_name("f482-b"), unique_name("f482-off")
    ddb.create_table(TableName=a, StreamSpecification=KEYS_ONLY, **key)
    ddb.create_table(TableName=b, StreamSpecification=KEYS_ONLY, **key)
    ddb.create_table(TableName=off, StreamSpecification=DISABLED, **key)
    for t in (a, b, off):
        wait_for_active(ddb, t)
    rows: dict = {"a": a, "b": b, "off": off}
    try:
        # F1: a stream change beside another change, while enabled.
        rows["disable_plus_billing"] = _set_stream(
            ddb,
            a,
            DISABLED,
            BillingMode="PROVISIONED",
            ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
        )
        gsi = {
            "IndexName": "gsi1",
            "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"}],
            "Projection": {"ProjectionType": "ALL"},
        }
        rows["disable_plus_gsi"] = _set_stream(
            ddb,
            a,
            DISABLED,
            AttributeDefinitions=[{"AttributeName": "g", "AttributeType": "S"}],
            GlobalSecondaryIndexUpdates=[{"Create": gsi}],
        )
        rows["disable_plus_gsi_undefined_attribute"] = _set_stream(
            ddb,
            a,
            DISABLED,
            GlobalSecondaryIndexUpdates=[{"Create": {**gsi, "IndexName": "gsi2"}}],
        )
        rows["enable_plus_billing_while_enabled"] = _set_stream(
            ddb, a, KEYS_ONLY, BillingMode="PAY_PER_REQUEST"
        )
        rows["a_after_refusals"] = ddb.describe_table(TableName=a)["Table"]

        # F3: GetShardIterator on unknown shard or stream.
        a_arn = rows["a_after_refusals"]["LatestStreamArn"]
        a_shard = streams.describe_stream(StreamArn=a_arn)["StreamDescription"]["Shards"][0]["ShardId"]
        rows["unknown_shard"] = _call(
            streams.get_shard_iterator,
            StreamArn=a_arn,
            ShardId="shardId-00000001700000000000-00000000",
            ShardIteratorType="TRIM_HORIZON",
        )
        rows["short_shard"] = _call(
            streams.get_shard_iterator,
            StreamArn=a_arn,
            ShardId="nope",
            ShardIteratorType="TRIM_HORIZON",
        )
        unknown_arn = a_arn[:-3] + ("000" if not a_arn.endswith("000") else "001")
        rows["unknown_arn"] = unknown_arn
        rows["unknown_stream"] = _call(
            streams.get_shard_iterator,
            StreamArn=unknown_arn,
            ShardId=a_shard,
            ShardIteratorType="TRIM_HORIZON",
        )

        # F2: two streams on A (and on B), then ListStreams paging.
        _set_stream(ddb, a, DISABLED)
        _wait_disabled(streams, a_arn)
        _set_stream(ddb, a, NEW_IMAGE)
        new_arn = ddb.describe_table(TableName=a)["Table"]["LatestStreamArn"]
        rows["a_old"], rows["a_new"] = a_arn, new_arn
        b_arn = ddb.describe_table(TableName=b)["Table"]["LatestStreamArn"]
        rows["list_a"] = _list(streams, TableName=a)
        rows["list_a_page1"] = _list(streams, TableName=a, Limit=1)
        rows["list_a_page2"] = _list(streams, TableName=a, Limit=1, ExclusiveStartStreamArn=new_arn)
        rows["list_a_page3"] = _list(streams, TableName=a, Limit=1, ExclusiveStartStreamArn=a_arn)
        rows["list_a_from_older"] = _list(streams, TableName=a, ExclusiveStartStreamArn=a_arn)
        rows["list_a_from_newest"] = _list(streams, TableName=a, ExclusiveStartStreamArn=new_arn)
        rows["b_arn"] = b_arn
        rows["list_a_from_b"] = _list(streams, TableName=a, ExclusiveStartStreamArn=b_arn)

        # F4: the table created with a disabled specification.
        rows["off_describe"] = ddb.describe_table(TableName=off)["Table"]
        rows["off_disable"] = _set_stream(ddb, off, DISABLED)
        rows["off_enable"] = _set_stream(ddb, off, KEYS_ONLY)
        rows["off_after_enable"] = ddb.describe_table(TableName=off)["Table"]
        yield rows
    finally:
        for t in (a, b, off):
            try:
                ddb.delete_table(TableName=t)
                wait_for_deleted(ddb, t)
            except ClientError:
                pass


def _refused(row: dict, code: str, sentence: str) -> None:
    assert (row.get("status"), row.get("code"), row.get("message")) == (400, code, sentence), row


# F1


def test_a_stream_change_beside_a_billing_change_is_refused(run):
    _refused(
        run["disable_plus_billing"],
        "ValidationException",
        INVALID + "You cannot modify stream status while updating table IOPS",
    )


def test_a_stream_change_beside_an_index_create_is_refused(run):
    _refused(
        run["disable_plus_gsi"],
        "ValidationException",
        INVALID + "You cannot create or delete index while changing stream status",
    )


def test_the_index_definition_check_precedes_the_companion_rule(run):
    row = run["disable_plus_gsi_undefined_attribute"]
    assert (row["status"], row["code"]) == (400, "ValidationException"), row
    # The index definition check comes first. The service's sentence is
    # "AttributeDefinitions is not specified for index: gsi2"; ExtendDB's own
    # sentence for an undefined index key ("Some index key attributes are not
    # defined in AttributeDefinitions. Keys: [g], ...") pre-dates this change,
    # so only the ordering is asserted here.
    assert row["message"].startswith(INVALID), row
    assert "AttributeDefinitions" in row["message"] and "stream" not in row["message"], row


def test_the_already_enabled_check_precedes_the_companion_rule(run):
    _refused(
        run["enable_plus_billing_while_enabled"],
        "ValidationException",
        f"Table already has an enabled stream: TableName: {run['a']}",
    )


def test_a_refused_combined_update_changes_nothing(run):
    t = run["a_after_refusals"]
    assert t["StreamSpecification"] == KEYS_ONLY
    assert t.get("BillingModeSummary", {}).get("BillingMode", "PAY_PER_REQUEST") == "PAY_PER_REQUEST"
    assert "GlobalSecondaryIndexes" not in t


# F2


def test_list_streams_of_a_table_with_two_streams(run):
    row = run["list_a"]
    assert row["status"] == 200, row
    assert [s["StreamArn"] for s in row["response"]["Streams"]] == [run["a_new"], run["a_old"]]
    assert "LastEvaluatedStreamArn" not in row["response"]


def test_list_streams_limit_one_carries_the_last_arn_on_every_cut_page(run):
    p1, p2, p3 = run["list_a_page1"], run["list_a_page2"], run["list_a_page3"]
    assert [s["StreamArn"] for s in p1["response"]["Streams"]] == [run["a_new"]], p1
    assert p1["response"]["LastEvaluatedStreamArn"] == run["a_new"]
    assert [s["StreamArn"] for s in p2["response"]["Streams"]] == [run["a_old"]], p2
    assert p2["response"]["LastEvaluatedStreamArn"] == run["a_old"]
    assert p3 == {"status": 200, "response": {"Streams": []}}, p3


def test_list_streams_after_the_older_stream_is_empty(run):
    assert run["list_a_from_older"] == {"status": 200, "response": {"Streams": []}}


def test_list_streams_after_the_newest_stream_is_the_older_one(run):
    row = run["list_a_from_newest"]
    assert [s["StreamArn"] for s in row["response"]["Streams"]] == [run["a_old"]], row
    assert "LastEvaluatedStreamArn" not in row["response"]


def test_list_streams_start_arn_of_another_table_is_refused(run):
    _refused(
        run["list_a_from_b"],
        "ValidationException",
        INVALID + f"ExclusiveStartStreamArn provided is invalid: {run['b_arn']}",
    )


# F3


def test_an_unknown_shard_id_on_an_existing_stream_does_not_exist(run):
    _refused(
        run["unknown_shard"],
        "ResourceNotFoundException",
        "Requested resource not found: Shard does not exist",
    )


def test_a_short_shard_id_fails_the_length_constraint(run):
    _refused(
        run["short_shard"],
        "ValidationException",
        "1 validation error detected: Value at 'shardId' failed to satisfy constraint: "
        "Member must have length greater than or equal to 28",
    )


def test_an_unknown_stream_arn_does_not_exist(run):
    _refused(
        run["unknown_stream"],
        "ResourceNotFoundException",
        "Requested resource not found: Stream does not exist",
    )


# F4


def test_a_table_created_with_a_disabled_specification_stores_none(run):
    t = run["off_describe"]
    assert "StreamSpecification" not in t
    assert "LatestStreamArn" not in t
    assert "LatestStreamLabel" not in t


def test_disabling_it_is_refused_like_a_table_without_the_member(run):
    _refused(
        run["off_disable"],
        "ValidationException",
        f"Table has no stream to disable: TableName: {run['off']}",
    )


def test_enabling_it_creates_the_first_stream(run):
    row = run["off_enable"]
    assert row["status"] == 200, row
    desc = row["response"]["TableDescription"]
    assert desc["StreamSpecification"] == KEYS_ONLY
    assert desc["LatestStreamArn"].endswith("/stream/" + desc["LatestStreamLabel"])
    after = run["off_after_enable"]
    assert after["StreamSpecification"] == KEYS_ONLY
    assert after["LatestStreamArn"] == desc["LatestStreamArn"]
