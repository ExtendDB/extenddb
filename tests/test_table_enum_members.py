# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""CreateTable and UpdateTable refuse members outside their sets.

Measured against Amazon DynamoDB in us-east-1 on 13 and 14 Nov 2026, and
beside a member of the wrong JSON type on 17 Nov 2026. The TableClass and
SSESpecification rows were measured on 13, 14 and 18 Nov 2026. Each request
went out twice with the same answer.

- The service answers ``ValidationException`` with the enum text, the member
  path and the set in its own order.
- A member of the wrong JSON type answers ``SerializationException`` first.
  Its text there is "Unexpected field type", which ExtendDB does not send,
  so these rows check the exception only.
- A TableClass outside its set gets the service's own sentence. A member of
  the wrong JSON type and a table name of a bad length answer ahead of it.
  It answers ahead of every member fault.
- CreateTable refuses an SSEType outside its set and a KMSMasterKeyId of
  more than 2048 characters with the other member faults. After them it
  refuses an SSESpecification whose members do not go together.
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
SSE_SET = "[AES256, KMS]"
SSE_TYPE = "sSESpecification.sSEType"
TABLE_CLASS = ("Invalid table-class parameter provided. Please try again with a valid "
               "table-class value: [STANDARD, STANDARD_INFREQUENT_ACCESS].")
ONE = "1 validation error detected: "


def _clause(value: str, path: str, constraint: str) -> str:
    return f"Value '{value}' at '{path}' failed to satisfy constraint: {constraint}"


def _refused(value: str, path: str, values: str) -> str:
    return ONE + _clause(value, path, f"Member must satisfy enum value set: {values}")


def _too_long(value: str, path: str, most: int) -> str:
    return ONE + _clause(value, path, f"Member must have length less than or equal to {most}")


def _both(first: str, second: str) -> str:
    """The faults of two texts with one fault each, in that order."""
    return f"2 validation errors detected: {first.removeprefix(ONE)}; {second.removeprefix(ONE)}"


def _invalid(reason: str) -> str:
    return f"One or more parameter values were invalid: {reason}"


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

# (id, operation, body without TableName, the answer's message or SERIALIZATION).
# UpdateTable rows name a table that does not exist.
ROWS = [
    ("ct-gsi", "CreateTable", {**TABLE, "GlobalSecondaryIndexes": BAD_GSI},
     _projection("EVERY_ATTRIBUTE", "globalSecondaryIndexes.1.member")),
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
     _projection("ALL_ATTRIBUTES", "globalSecondaryIndexUpdates.1.member.create")),
    ("ct-stream-disabled", "CreateTable",
     {**TABLE, "StreamSpecification": _stream(False, "NEW_IMAGES")}, _view("NEW_IMAGES")),
    ("ct-stream-enabled", "CreateTable",
     {**TABLE, "StreamSpecification": _stream(True, "NEW_IMAGES")}, _view("NEW_IMAGES")),
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
    ("ut-replica-updates", "UpdateTable",
     {"AttributeDefinitions": _defs("g"), "GlobalSecondaryIndexUpdates": BAD_CREATE,
      "ReplicaUpdates": "x"},
     SERIALIZATION),
]

# A TableClass or an SSESpecification the service refuses. PLAIN is the
# table keyed on pk alone. The ct-table-class-name row sends its own
# TableName.
PLAIN = {**TABLE, "AttributeDefinitions": _defs("pk")}
GSI_FAULT = _projection("EVERY_ATTRIBUTE", "globalSecondaryIndexes.1.member")
SSE_BOGUS = _refused("BOGUS", SSE_TYPE, SSE_SET)
KEY_2049 = "k" * 2049
KEY_FAULT = _too_long(KEY_2049, "sSESpecification.kMSMasterKeyId", 2048)
NAME_256 = "t" * 256
KMS_REQUIRED = _invalid("SSEType KMS is required if KMSMasterKeyId is specified")


def _sse(**spec) -> dict:
    return {**PLAIN, "SSESpecification": spec}


ROWS += [
    ("ct-sse-aes256", "CreateTable", _sse(Enabled=True, SSEType="AES256"),
     _invalid("SSEType AES256 is not supported")),
    ("ct-sse-bogus", "CreateTable", _sse(Enabled=True, SSEType="BOGUS"), SSE_BOGUS),
    ("ct-sse-aes", "CreateTable", _sse(Enabled=True, SSEType="AES"),
     _refused("AES", SSE_TYPE, SSE_SET)),
    ("ct-sse-type-disabled", "CreateTable", _sse(Enabled=False, SSEType="KMS"),
     _invalid("SSEType can not be specified if Enabled is false")),
    ("ct-sse-key-without-type", "CreateTable",
     _sse(Enabled=True, KMSMasterKeyId="alias/no-such-key"), KMS_REQUIRED),
    ("ct-sse-key-disabled", "CreateTable",
     _sse(Enabled=False, KMSMasterKeyId="alias/no-such-key"),
     _invalid("KMSMasterKeyId can not be specified if Enabled is false")),
    ("ct-sse-key-2049", "CreateTable",
     _sse(Enabled=True, SSEType="KMS", KMSMasterKeyId=KEY_2049), KEY_FAULT),
    # The service lists the key's length ahead of the SSEType.
    ("ct-sse-bogus-key-2049", "CreateTable",
     _sse(Enabled=True, SSEType="BOGUS", KMSMasterKeyId=KEY_2049), _both(KEY_FAULT, SSE_BOGUS)),
    # The service counts the key's length in characters.
    ("ct-sse-key-2048", "CreateTable", _sse(Enabled=True, KMSMasterKeyId="k" * 2048),
     KMS_REQUIRED),
    ("ct-sse-key-1025-emoji", "CreateTable",
     _sse(Enabled=True, KMSMasterKeyId="\U0001F600" * 1025), KMS_REQUIRED),
    ("ct-sse-bogus-gsi", "CreateTable",
     {**TABLE, "GlobalSecondaryIndexes": BAD_GSI,
      "SSESpecification": {"Enabled": True, "SSEType": "BOGUS"}},
     _both(GSI_FAULT, SSE_BOGUS)),
    ("ct-sse-bogus-stream", "CreateTable",
     {**_sse(Enabled=True, SSEType="BOGUS"), "StreamSpecification": _stream(True, "NEW_IMAGES")},
     _both(SSE_BOGUS, _view("NEW_IMAGES"))),
    ("ct-sse-aes256-gsi", "CreateTable",
     {**TABLE, "GlobalSecondaryIndexes": BAD_GSI,
      "SSESpecification": {"Enabled": True, "SSEType": "AES256"}},
     GSI_FAULT),
    ("ct-table-class", "CreateTable", {**PLAIN, "TableClass": "STANDARD_IA"}, TABLE_CLASS),
    ("ct-table-class-billing", "CreateTable",
     {**PLAIN, "BillingMode": "ON_DEMAND", "TableClass": "PAY_PER_REQUEST"}, TABLE_CLASS),
    ("ct-table-class-gsi", "CreateTable",
     {**TABLE, "GlobalSecondaryIndexes": BAD_GSI, "TableClass": "BOGUS"}, TABLE_CLASS),
    ("ct-table-class-name", "CreateTable",
     {**PLAIN, "TableName": NAME_256, "TableClass": "BOGUS"},
     _too_long(NAME_256, "tableName", 255)),
    ("ct-table-class-tags", "CreateTable", {**PLAIN, "TableClass": "BOGUS", "Tags": "x"},
     SERIALIZATION),
    ("ut-table-class", "UpdateTable", {"TableClass": "STANDARD_IA"}, TABLE_CLASS),
    ("ut-table-class-billing", "UpdateTable",
     {"BillingMode": "ALSO_BOGUS", "TableClass": "BOGUS"}, TABLE_CLASS),
    ("ut-table-class-defs", "UpdateTable",
     {"TableClass": "BOGUS", "AttributeDefinitions": "x"}, SERIALIZATION),
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
