# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""`ListStreams` with an `ExclusiveStartStreamArn` that is not a stream ARN.

Measured on Amazon DynamoDB in us-east-1 on 2026-11-21 with SDK parameter
validation off, each row twice: the `REFUSED` and `ACCEPTED` tables below.
A value of 37 to 1024 characters that is not a stream ARN is `Invalid
StreamArn` (1024 itself was not sent); under 37 characters it gets the
model's length sentence whatever its shape, and `Limit` 0 is answered before
the start ARN. U+0000 inside a stream ARN (table segment or label) is in
test_nul_arn_shapes.py.
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from helpers import assert_validation, list_streams, unique_name, wait_for_active, wait_for_deleted

NUL = "\u0000"
INVALID_STREAM_ARN = "Invalid StreamArn"
LENGTH_37 = (
    "1 validation error detected: Value at 'exclusiveStartStreamArn' failed to satisfy "
    "constraint: Member must have length greater than or equal to 37"
)
LIMIT_1 = (
    "1 validation error detected: Value at 'limit' failed to satisfy constraint: "
    "Member must have value greater than or equal to 1"
)


@pytest.fixture(scope="module")
def own(raw_client):
    """A table with a stream, and the ARNs built from it: `table`, `stream`,
    the ARN `prefix` up to `table/`, the stream `label`, the table `name` and
    its `region` and `account`."""
    name = unique_name("lsarn")
    raw_client.create_table(
        TableName=name,
        BillingMode="PAY_PER_REQUEST",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        StreamSpecification={"StreamEnabled": True, "StreamViewType": "KEYS_ONLY"},
    )
    wait_for_active(raw_client, name)
    table = raw_client.describe_table(TableName=name)["Table"]
    arn = table["TableArn"]
    _, _, _, region, account, _ = arn.split(":", 5)
    yield {
        "name": name,
        "table": arn,
        "stream": table["LatestStreamArn"],
        "label": table["LatestStreamLabel"],
        "prefix": arn[: arn.index("table/") + len("table/")],
        "region": region,
        "account": account,
    }
    try:
        raw_client.delete_table(TableName=name)
        wait_for_deleted(raw_client, name)
    except Exception:
        pass


# (row, start ARN as a format string over the `own` fixture, other members, sentence)
REFUSED = [
    ("table arn", "{table}", {}, INVALID_STREAM_ARN),
    ("table arn, missing table", "{prefix}lsarn-missing", {}, INVALID_STREAM_ARN),
    ("table arn with nul", "{prefix}lsarn" + NUL + "name", {}, INVALID_STREAM_ARN),
    ("table arn with nul, missing table", "{prefix}lsarn-missing" + NUL + "x", {}, INVALID_STREAM_ARN),
    ("table arn with TableName", "{table}", {"TableName": "{name}"}, INVALID_STREAM_ARN),
    ("index arn", "{table}/index/gsi1", {}, INVALID_STREAM_ARN),
    ("backup arn", "{table}/backup/01700000000000-abcdef12", {}, INVALID_STREAM_ARN),
    ("stream arn with a trailing slash", "{stream}/", {}, INVALID_STREAM_ARN),
    ("37 characters, not an arn", "x" * 37, {}, INVALID_STREAM_ARN),
    ("40 characters, not an arn", "x" * 40, {}, INVALID_STREAM_ARN),
    ("arn of four fields", "arn:aws:dynamodb:us-east-1:" + "x" * 13, {}, INVALID_STREAM_ARN),
    ("36 characters, not an arn", "x" * 36, {}, LENGTH_37),
    ("xyz", "xyz", {}, LENGTH_37),
    ("x nul y", "x" + NUL + "y", {}, LENGTH_37),
    ("table arn with Limit 0", "{table}", {"Limit": 0}, LIMIT_1),
]

ACCEPTED = [
    ("own stream arn", "{stream}"),
    ("label the table never had", "{table}/stream/2020-01-01T00:00:00.000"),
    ("table the account never had", "{prefix}lsarn-missing/stream/{label}"),
    ("another region", "arn:aws:dynamodb:us-west-2:{account}:table/{name}/stream/{label}"),
    ("another account", "arn:aws:dynamodb:{region}:123456789012:table/{name}/stream/{label}"),
    ("another partition", "arn:aws-cn:dynamodb:{region}:{account}:table/{name}/stream/{label}"),
    ("another service", "arn:aws:s3:{region}:{account}:table/{name}/stream/{label}"),
    ("empty region", "arn:aws:dynamodb::{account}:table/{name}/stream/{label}"),
    ("empty account", "arn:aws:dynamodb:{region}::table/{name}/stream/{label}"),
]


@pytest.mark.parametrize(("row", "start", "extra", "sentence"), REFUSED, ids=[r[0] for r in REFUSED])
def test_a_start_arn_that_is_not_a_stream_arn_is_refused(raw_streams_client, own, row, start, extra, sentence):
    params = {k: v.format(**own) if isinstance(v, str) else v for k, v in extra.items()}
    with pytest.raises(ClientError) as exc:
        list_streams(raw_streams_client, ExclusiveStartStreamArn=start.format(**own), **params)
    assert_validation(exc.value, sentence)


@pytest.mark.parametrize(("row", "start"), ACCEPTED, ids=[r[0] for r in ACCEPTED])
def test_a_stream_arn_of_any_scope_is_accepted(raw_streams_client, own, row, start):
    resp = list_streams(raw_streams_client, ExclusiveStartStreamArn=start.format(**own))
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200, resp
    assert "Streams" in resp, resp
