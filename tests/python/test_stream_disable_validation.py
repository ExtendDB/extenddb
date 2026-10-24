# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Stream lifecycle rows measured on Amazon DynamoDB on 2026-10-24, third round.

Request validation on CreateTable, UpdateTable and the streams API: a stream
enabled without a view type, a specification without StreamEnabled, a disable
beside a view type or beside DeletionProtection, the ShardId upper bound, the
SequenceNumber rules, DescribeStream and ListStreams member constraints, and
the DescribeStream sentence for an unknown stream of an existing table.
Companion to test_stream_disable.py and test_stream_disable_fold.py.
"""

from __future__ import annotations

import os

import boto3
import botocore.config
import pytest
from botocore.exceptions import ClientError

from helpers import unique_name, wait_for_active, wait_for_deleted

KEYS_ONLY = {"StreamEnabled": True, "StreamViewType": "KEYS_ONLY"}
INVALID = "One or more parameter values were invalid: "
VALIDATION = "1 validation error detected: Value "


def _client(service: str, endpoint_url):
    kwargs: dict = {
        "service_name": service,
        "region_name": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        # Client-side parameter validation off: every row here measures the
        # server's own checks.
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


def _refused(row: dict, code: str, sentence: str) -> None:
    assert (row.get("status"), row.get("code"), row.get("message")) == (400, code, sentence), row


@pytest.fixture(scope="module")
def run(endpoint_url):
    ddb = _client("dynamodb", endpoint_url)
    streams = _client("dynamodbstreams", endpoint_url)
    key = {
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "BillingMode": "PAY_PER_REQUEST",
    }
    on, off = unique_name("v482-on"), unique_name("v482-off")
    rows: dict = {"on": on, "off": off}
    created = []
    try:
        # CreateTable request validation.
        never = unique_name("v482-never")
        rows["create_enabled_no_view"] = _call(
            ddb.create_table, TableName=never, StreamSpecification={"StreamEnabled": True}, **key
        )
        rows["create_never_exists"] = _call(ddb.describe_table, TableName=never)
        rows["create_no_enabled"] = _call(
            ddb.create_table, TableName=never, StreamSpecification={"StreamViewType": "KEYS_ONLY"}, **key
        )
        ddb.create_table(TableName=on, StreamSpecification=KEYS_ONLY, **key)
        ddb.create_table(TableName=off, **key)
        created += [on, off]
        wait_for_active(ddb, on)
        wait_for_active(ddb, off)

        # UpdateTable request validation, on both tables.
        for label, name in (("on", on), ("off", off)):
            rows[f"update_no_enabled_{label}"] = _call(
                ddb.update_table, TableName=name, StreamSpecification={}
            )
            rows[f"disable_with_view_{label}"] = _call(
                ddb.update_table,
                TableName=name,
                StreamSpecification={"StreamEnabled": False, "StreamViewType": "KEYS_ONLY"},
            )
        rows["disable_plus_deletion_protection"] = _call(
            ddb.update_table,
            TableName=on,
            StreamSpecification={"StreamEnabled": False},
            DeletionProtectionEnabled=True,
        )
        rows["on_after"] = ddb.describe_table(TableName=on)["Table"]

        # Streams API member constraints.
        arn = rows["on_after"]["LatestStreamArn"]
        shard = streams.describe_stream(StreamArn=arn)["StreamDescription"]["Shards"][0]["ShardId"]
        rows["shard_66"] = _call(
            streams.get_shard_iterator,
            StreamArn=arn, ShardId="s" * 66, ShardIteratorType="TRIM_HORIZON",
        )
        rows["shard_65_unknown"] = _call(
            streams.get_shard_iterator,
            StreamArn=arn, ShardId="shardId-" + "0" * 57, ShardIteratorType="TRIM_HORIZON",
        )
        rows["at_no_sequence"] = _call(
            streams.get_shard_iterator,
            StreamArn=arn, ShardId=shard, ShardIteratorType="AT_SEQUENCE_NUMBER",
        )
        rows["at_sequence_0"] = _call(
            streams.get_shard_iterator,
            StreamArn=arn, ShardId=shard, ShardIteratorType="AT_SEQUENCE_NUMBER", SequenceNumber="0",
        )
        unknown = arn[:-3] + ("000" if not arn.endswith("000") else "001")
        rows["unknown_arn"] = unknown
        rows["describe_unknown"] = _call(streams.describe_stream, StreamArn=unknown)
        rows["describe_short_start"] = _call(
            streams.describe_stream, StreamArn=arn, ExclusiveStartShardId="short"
        )
        rows["list_limit_0"] = _call(streams.list_streams, TableName=on, Limit=0)
        rows["list_short_start"] = _call(
            streams.list_streams, TableName=on, ExclusiveStartStreamArn="arn:aws:dynamodb:x"
        )
        rows["list_never_existed"] = _call(streams.list_streams, TableName=unique_name("v482-none"))
        yield rows
    finally:
        for t in created:
            try:
                ddb.delete_table(TableName=t)
                wait_for_deleted(ddb, t)
            except ClientError:
                pass


# CreateTable


def test_create_table_with_a_stream_enabled_and_no_view_type_is_refused(run):
    _refused(
        run["create_enabled_no_view"],
        "ValidationException",
        INVALID + "Table is being created with a stream enabled, UpdateViewType is required",
    )
    assert run["create_never_exists"]["code"] == "ResourceNotFoundException", "no table created"


def test_create_table_without_stream_enabled_is_refused(run):
    _refused(
        run["create_no_enabled"],
        "ValidationException",
        VALIDATION + "null at 'streamSpecification.streamEnabled' failed to satisfy constraint: "
        "Member must not be null",
    )


# UpdateTable


def test_update_table_without_stream_enabled_is_refused(run):
    for label in ("on", "off"):
        _refused(
            run[f"update_no_enabled_{label}"],
            "ValidationException",
            VALIDATION + "null at 'streamSpecification.streamEnabled' failed to satisfy constraint: "
            "Member must not be null",
        )


def test_a_disable_beside_a_view_type_is_refused_before_the_no_stream_rule(run):
    for label in ("on", "off"):
        _refused(
            run[f"disable_with_view_{label}"],
            "ValidationException",
            INVALID + "If stream is being disabled, then UpdateViewType must not be specified",
        )


def test_a_disable_beside_deletion_protection_is_refused(run):
    _refused(
        run["disable_plus_deletion_protection"],
        "ValidationException",
        INVALID + "DeletionProtection modification must be the only operation in the request",
    )
    t = run["on_after"]
    assert t["StreamSpecification"] == KEYS_ONLY, "nothing applied"
    assert t.get("DeletionProtectionEnabled", False) is False


# Streams API


def test_a_shard_id_over_65_characters_fails_the_length_constraint(run):
    _refused(
        run["shard_66"],
        "ValidationException",
        VALIDATION + "at 'shardId' failed to satisfy constraint: Member must have length less than or equal to 65",
    )


def test_a_65_character_unknown_shard_id_does_not_exist(run):
    _refused(
        run["shard_65_unknown"],
        "ResourceNotFoundException",
        "Requested resource not found: Shard does not exist",
    )


def test_at_sequence_number_without_a_sequence_number_is_refused(run):
    _refused(run["at_no_sequence"], "ValidationException", INVALID + "SequenceNumber cannot be null")


def test_a_sequence_number_under_21_characters_fails_the_length_constraint(run):
    _refused(
        run["at_sequence_0"],
        "ValidationException",
        VALIDATION + "at 'sequenceNumber' failed to satisfy constraint: "
        "Member must have length greater than or equal to 21",
    )


def test_describe_stream_of_an_unknown_stream_of_an_existing_table(run):
    _refused(
        run["describe_unknown"],
        "ResourceNotFoundException",
        f"Requested resource not found: Stream: {run['unknown_arn']} not found",
    )


def test_describe_stream_with_a_short_exclusive_start_shard_id_is_refused(run):
    _refused(
        run["describe_short_start"],
        "ValidationException",
        VALIDATION + "at 'exclusiveStartShardId' failed to satisfy constraint: "
        "Member must have length greater than or equal to 28",
    )


def test_list_streams_limit_zero_is_refused(run):
    _refused(
        run["list_limit_0"],
        "ValidationException",
        VALIDATION + "at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1",
    )


def test_list_streams_with_a_short_start_arn_is_refused(run):
    _refused(
        run["list_short_start"],
        "ValidationException",
        VALIDATION + "at 'exclusiveStartStreamArn' failed to satisfy constraint: "
        "Member must have length greater than or equal to 37",
    )


def test_list_streams_of_a_table_that_never_existed_is_empty(run):
    assert run["list_never_existed"] == {"status": 200, "response": {"Streams": []}}
