# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""CreateTable and UpdateTable refuse a ProjectionType or StreamViewType outside its set.

Measured against Amazon DynamoDB in us-east-1 on 13 and 14 Nov 2026, and
beside another such member or a member of the wrong JSON type on 17 Nov 2026.
Each request went out twice with the same answer.

- The service answers ``ValidationException`` with the enum text, the member
  path and the set in its own order.
- A member of the wrong JSON type answers ``SerializationException`` first.
  Its text there, such as "Unexpected field type", is not the text ExtendDB
  sends, so these rows check the exception only.
- UpdateTable refuses before it looks up the table. After a refused
  CreateTable no table exists. After a refused UpdateTable the table has no
  stream, and the same UpdateTable with ``NEW_IMAGE`` enables one.

The bodies go out as written, through ``raw_signed_request.signed_post``,
because the SDK checks a member's JSON type before it sends a request.
"""

from __future__ import annotations

import os

import boto3
import pytest

from conftest import wait_for_active, wait_for_deleted
from raw_signed_request import signed_post

REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
SERIALIZATION = "SerializationException"
PROJECTION_SET = "[ALL, INCLUDE, KEYS_ONLY]"
VIEW_SET = "[OLD_IMAGE, KEYS_ONLY, NEW_AND_OLD_IMAGES, NEW_IMAGE]"
ONE = "1 validation error detected: "


def _clause(value: str, path: str, constraint: str) -> str:
    return f"Value '{value}' at '{path}' failed to satisfy constraint: {constraint}"


def _refused(value: str, path: str, values: str) -> str:
    return ONE + _clause(value, path, f"Member must satisfy enum value set: {values}")


def _both(first: str, second: str) -> str:
    """The faults of two texts with one fault each, in that order."""
    return f"2 validation errors detected: {first.removeprefix(ONE)}; {second.removeprefix(ONE)}"


def _projection(value: str, member: str) -> str:
    return _refused(value, f"{member}.projection.projectionType", PROJECTION_SET)


def _view(value: str) -> str:
    return _refused(value, "streamSpecification.streamViewType", VIEW_SET)


def _defs(*names: str) -> list:
    return [{"AttributeName": n, "AttributeType": "S"} for n in names]


def _index(name: str, attr: str, projection: str) -> dict:
    return {
        "IndexName": name,
        "KeySchema": [{"AttributeName": attr, "KeyType": "HASH"}],
        "Projection": {"ProjectionType": projection},
    }


def _stream(enabled: bool, view: str) -> dict:
    return {"StreamEnabled": enabled, "StreamViewType": view}


HASH = [{"AttributeName": "pk", "KeyType": "HASH"}]
TABLE = {"AttributeDefinitions": _defs("pk", "g"), "KeySchema": HASH,
         "BillingMode": "PAY_PER_REQUEST"}
BAD_GSI = [_index("gsi", "g", "EVERY_ATTRIBUTE")]
BAD_CREATE = [{"Create": _index("gsi", "g", "ALL_ATTRIBUTES")}]
LSI = {
    "IndexName": "lsi",
    "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"},
                  {"AttributeName": "l", "KeyType": "RANGE"}],
    "Projection": {"ProjectionType": "INCLUDES"},
}
GSI_FAULT = _projection("EVERY_ATTRIBUTE", "globalSecondaryIndexes.1.member")
CREATE_FAULT = _projection("ALL_ATTRIBUTES", "globalSecondaryIndexUpdates.1.member.create")

# (id, operation, body without TableName, the answer's message or SERIALIZATION).
# UpdateTable rows name a table that does not exist.
ROWS = [
    ("ct-gsi", "CreateTable", {**TABLE, "GlobalSecondaryIndexes": BAD_GSI}, GSI_FAULT),
    ("ct-second-gsi", "CreateTable",
     {**TABLE, "AttributeDefinitions": _defs("pk", "g", "l"),
      "GlobalSecondaryIndexes": [_index("gsi", "g", "ALL"),
                                 _index("gsi2", "l", "keys_only")]},
     _projection("keys_only", "globalSecondaryIndexes.2.member")),
    ("ct-lsi", "CreateTable",
     {**TABLE, "AttributeDefinitions": _defs("pk", "sk", "l"),
      "KeySchema": HASH + [{"AttributeName": "sk", "KeyType": "RANGE"}],
      "LocalSecondaryIndexes": [LSI]},
     _projection("INCLUDES", "localSecondaryIndexes.1.member")),
    ("ut-gsi-create", "UpdateTable",
     {"AttributeDefinitions": _defs("g"), "GlobalSecondaryIndexUpdates": BAD_CREATE},
     CREATE_FAULT),
    ("ct-stream-disabled", "CreateTable",
     {**TABLE, "StreamSpecification": _stream(False, "NEW_IMAGES")}, _view("NEW_IMAGES")),
    ("ct-stream-enabled", "CreateTable",
     {**TABLE, "StreamSpecification": _stream(True, "NEW_IMAGES")}, _view("NEW_IMAGES")),
    # Two members outside their sets (measured 17 Nov 2026). The service lists
    # the GSIs, then the stream view type, then the LSIs, and on UpdateTable
    # the Create entries, then the stream view type.
    ("ct-two-gsis", "CreateTable",
     {**TABLE, "AttributeDefinitions": _defs("pk", "g", "l"),
      "GlobalSecondaryIndexes": BAD_GSI + [_index("gsi2", "l", "keys_only")]},
     _both(GSI_FAULT, _projection("keys_only", "globalSecondaryIndexes.2.member"))),
    ("ct-gsi-stream", "CreateTable",
     {**TABLE, "GlobalSecondaryIndexes": BAD_GSI,
      "StreamSpecification": _stream(True, "NEW_IMAGES")},
     _both(GSI_FAULT, _view("NEW_IMAGES"))),
    ("ct-gsi-lsi", "CreateTable",
     {**TABLE, "AttributeDefinitions": _defs("pk", "sk", "g", "l"),
      "KeySchema": HASH + [{"AttributeName": "sk", "KeyType": "RANGE"}],
      "GlobalSecondaryIndexes": BAD_GSI, "LocalSecondaryIndexes": [LSI]},
     _both(GSI_FAULT, _projection("INCLUDES", "localSecondaryIndexes.1.member"))),
    ("ut-gsi-create-stream", "UpdateTable",
     {"AttributeDefinitions": _defs("g"), "GlobalSecondaryIndexUpdates": BAD_CREATE,
      "StreamSpecification": _stream(True, "NEW_IMAGES")},
     _both(CREATE_FAULT, _view("NEW_IMAGES"))),
    ("ut-two-gsi-creates", "UpdateTable",
     {"AttributeDefinitions": _defs("g", "l"),
      "GlobalSecondaryIndexUpdates": BAD_CREATE + [{"Create": _index("gsi2", "l", "keys_only")}]},
     _both(CREATE_FAULT,
           _projection("keys_only", "globalSecondaryIndexUpdates.2.member.create"))),
] + [
    (f"ut-stream-{view or 'empty'}", "UpdateTable",
     {"StreamSpecification": _stream(True, view)}, _view(view))
    for view in ("NEW_AND_OLD_IMAGE", "keys_only", "", "TOTAL")
] + [
    # A member of the wrong JSON type answers first.
    ("ct-defs", "CreateTable",
     {**TABLE, "AttributeDefinitions": "x", "GlobalSecondaryIndexes": BAD_GSI},
     SERIALIZATION),
    ("ct-tags", "CreateTable",
     {**TABLE, "GlobalSecondaryIndexes": BAD_GSI, "Tags": "x"}, SERIALIZATION),
    ("ut-defs", "UpdateTable",
     {"AttributeDefinitions": "x", "GlobalSecondaryIndexUpdates": BAD_CREATE},
     SERIALIZATION),
    # ReplicaUpdates holds objects (measured 25 Nov 2026). The service's text
    # here is "Unexpected value type in payload".
    ("ut-replica-update-string", "UpdateTable", {"ReplicaUpdates": ["x"]}, SERIALIZATION),
    ("ut-replica-updates", "UpdateTable",
     {"AttributeDefinitions": _defs("g"), "GlobalSecondaryIndexUpdates": BAD_CREATE,
      "ReplicaUpdates": "x"},
     SERIALIZATION),
]


@pytest.fixture(scope="module")
def post(endpoint_url):
    """Send a body as written to the target."""
    creds = boto3.Session().get_credentials().get_frozen_credentials()
    endpoint = endpoint_url or f"https://dynamodb.{REGION}.amazonaws.com"

    def _post(operation: str, body: dict):
        return signed_post(endpoint, creds.access_key, creds.secret_key, REGION,
                           operation, body, session_token=creds.token)

    return _post


def _assert_answer(resp, expected: str) -> None:
    assert resp.status_code == 400, resp.text
    payload = resp.json()
    kind = payload.get("__type", "").rsplit("#", 1)[-1]
    if expected == SERIALIZATION:
        assert kind == SERIALIZATION, payload
        return
    assert kind == "ValidationException", payload
    assert payload.get("message", payload.get("Message")) == expected


def _streams(endpoint_url, name: str) -> list:
    kwargs: dict = {"region_name": REGION}
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url
        kwargs["verify"] = not endpoint_url.startswith("https://")
    return boto3.client("dynamodbstreams", **kwargs).list_streams(TableName=name)["Streams"]


@pytest.mark.parametrize(
    ("operation", "members", "expected"),
    [pytest.param(op, body, expected, id=row_id) for row_id, op, body, expected in ROWS],
)
def test_refused_request(dynamodb_client, unique_table_name, post, operation, members,
                         expected):
    resp = post(operation, {"TableName": unique_table_name, **members})
    if operation == "CreateTable" and resp.status_code == 200:
        dynamodb_client.delete_table(TableName=unique_table_name)
        wait_for_deleted(dynamodb_client, unique_table_name)
    _assert_answer(resp, expected)
    with pytest.raises(dynamodb_client.exceptions.ResourceNotFoundException):
        dynamodb_client.describe_table(TableName=unique_table_name)


def test_refused_update_leaves_existing_table_without_stream(
    create_and_cleanup_table, dynamodb_client, unique_table_name, post, endpoint_url
):
    create_and_cleanup_table()
    body = {"TableName": unique_table_name, "StreamSpecification": _stream(True, "NEW_IMAGES")}
    _assert_answer(post("UpdateTable", body), _view("NEW_IMAGES"))
    table = dynamodb_client.describe_table(TableName=unique_table_name)["Table"]
    assert "StreamSpecification" not in table, table
    assert "LatestStreamArn" not in table, table
    assert _streams(endpoint_url, unique_table_name) == []

    body["StreamSpecification"] = _stream(True, "NEW_IMAGE")
    resp = post("UpdateTable", body)
    assert resp.status_code == 200, resp.text
    wait_for_active(dynamodb_client, unique_table_name)
    arn = dynamodb_client.describe_table(TableName=unique_table_name)["Table"]["LatestStreamArn"]
    streams = _streams(endpoint_url, unique_table_name)
    assert [s["StreamArn"] for s in streams] == [arn], streams
