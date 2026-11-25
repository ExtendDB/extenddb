# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""CreateTable and UpdateTable refuse members outside their sets.

Measured against Amazon DynamoDB in us-east-1 on 13 and 14 Nov 2026, and
beside another such member or a member of the wrong JSON type on 17 Nov 2026.
The TableClass and SSESpecification rows were measured on 13, 14 and 18 Nov
2026. Each request went out twice with the same answer.

- The service answers ``ValidationException`` with the enum text, the member
  path and the set in its own order.
- A member of the wrong JSON type answers ``SerializationException`` first.
  Its text there, such as "Unexpected field type", is not the text ExtendDB
  sends, so these rows check the exception only. The UpdateTable
  SSESpecification rows check the text too.
- A TableClass outside its set gets the service's own sentence. A member of
  the wrong JSON type and a table name of a bad length answer ahead of it.
  It answers ahead of every member fault.
- CreateTable refuses an SSEType outside its set and a KMSMasterKeyId of
  more than 2048 characters with the other member faults. After them it
  refuses an SSESpecification whose members do not go together.
- On a table created without an SSESpecification or with KMS, UpdateTable
  refuses the SSESpecification rows below with the sentences CreateTable
  uses. A specification without Enabled that names SSEType or
  KMSMasterKeyId is read as one with Enabled true. An Enabled written as a
  string is read as false when it is "false" and as true otherwise.
  Enabled false on a table created without an SSESpecification gets a
  sentence of its own, after the table lookup. The table keeps its
  SSEDescription.
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
NOT_FOUND = "ResourceNotFoundException"
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

# A TableClass or an SSESpecification the service refuses. PLAIN is the
# table keyed on pk alone. The ct-table-class-name row sends its own
# TableName.
PLAIN = {**TABLE, "AttributeDefinitions": _defs("pk")}
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

ROWS += [
    # An Enabled of "yes" reads as true, so the table lookup answers
    # (measured 2026-11-25).
    ("ut-sse-enabled-yes-kms", "UpdateTable",
     {"SSESpecification": {"Enabled": "yes", "SSEType": "KMS"}}, NOT_FOUND),
    # The SSE faults come ahead of the stream view type and the index faults,
    # also when the body sends the index first (measured 2026-11-25).
    ("ut-sse-bogus-stream-bogus", "UpdateTable",
     {"StreamSpecification": _stream(True, "BOGUS"),
      "SSESpecification": {"Enabled": True, "SSEType": "BOGUS"}},
     _both(SSE_BOGUS, _view("BOGUS"))),
    ("ut-gsi-bogus-sse-bogus", "UpdateTable",
     {"GlobalSecondaryIndexUpdates": [{"Create": _index("gsi1", "g", "BOGUS")}],
      "AttributeDefinitions": _defs("g"),
      "SSESpecification": {"Enabled": True, "SSEType": "BOGUS"}},
     _both(SSE_BOGUS, _projection("BOGUS", "globalSecondaryIndexUpdates.1.member.create"))),
]

# UpdateTable of an existing table keyed on pk alone, created without an
# SSESpecification or with KMS: (id, the table's SSESpecification, the
# request's, the answer's message). <account> is the table's account.
KMS = {"Enabled": True, "SSEType": "KMS"}
KEY_ARN = "arn:aws:kms:eu-west-1:<account>:key/11111111-2222-3333-4444-555555555555"
SSE_UPDATE_ROWS = [
    ("aes256", None, {"Enabled": True, "SSEType": "AES256"},
     _invalid("SSEType AES256 is not supported")),
    ("bogus", None, {"Enabled": True, "SSEType": "BOGUS"}, SSE_BOGUS),
    ("type-disabled", None, {"Enabled": False, "SSEType": "KMS"},
     _invalid("SSEType can not be specified if Enabled is false")),
    ("key-alias", None, {"Enabled": True, "KMSMasterKeyId": "alias/no-such-key"}, KMS_REQUIRED),
    ("key-id", None, {"Enabled": True, "KMSMasterKeyId": "11111111-2222-3333-4444-555555555555"},
     KMS_REQUIRED),
    ("key-arn", None, {"Enabled": True, "KMSMasterKeyId": KEY_ARN}, KMS_REQUIRED),
    ("bogus-key-2049", None, {"Enabled": True, "SSEType": "BOGUS", "KMSMasterKeyId": KEY_2049},
     _both(KEY_FAULT, SSE_BOGUS)),
    ("kms-aes256", KMS, {"Enabled": True, "SSEType": "AES256"},
     _invalid("SSEType AES256 is not supported")),
    ("kms-bogus", KMS, {"Enabled": True, "SSEType": "BOGUS"}, SSE_BOGUS),
    ("kms-aes256-without-enabled", KMS, {"SSEType": "AES256"},
     _invalid("SSEType AES256 is not supported")),
    ("kms-key-without-enabled", KMS, {"KMSMasterKeyId": "alias/no-such-key"}, KMS_REQUIRED),
    ("default-disabled", None, {"Enabled": False},
     _invalid("Table is already encrypted by default")),
    ("default-empty", None, {}, _invalid("Table is already encrypted by default")),
    # Measured on a table that does not exist on 2026-11-25. Each answer
    # comes before the table lookup.
    ("enabled-true-string", None, {"Enabled": "true", "SSEType": "AES256"},
     _invalid("SSEType AES256 is not supported")),
    ("enabled-yes-string", None, {"Enabled": "yes", "SSEType": "AES256"},
     _invalid("SSEType AES256 is not supported")),
    ("kms-enabled-false-string", KMS, {"Enabled": "false", "SSEType": "KMS"},
     _invalid("SSEType can not be specified if Enabled is false")),
    ("kms-specification-string", KMS, "x", (SERIALIZATION, "Unexpected field type")),
    ("sse-type-number", None, {"Enabled": True, "SSEType": 5},
     (SERIALIZATION, "NUMBER_VALUE cannot be converted to String")),
]

# KMS beside a stream change and a billing change, on a table created without
# an SSESpecification. A stream change beside ProvisionedThroughput answers its
# own sentence ahead of the SSESpecification one (measured 2026-11-18).
SSE_COMPANION_ROWS = [
    ("stream-billing", {"StreamSpecification": _stream(True, "NEW_IMAGE"),
                        "BillingMode": "PAY_PER_REQUEST"},
     _invalid("Server-Side Encryption modification must be the only operation in the request")),
    ("stream-throughput", {"StreamSpecification": _stream(True, "NEW_IMAGE"),
                           "BillingMode": "PROVISIONED",
                           "ProvisionedThroughput": {"ReadCapacityUnits": 5,
                                                     "WriteCapacityUnits": 5}},
     _invalid("You cannot modify stream status while updating table IOPS")),
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


def _assert_answer(resp, expected) -> None:
    """``expected`` is the text of a ValidationException, SERIALIZATION or
    NOT_FOUND for that exception with any text, or a pair of an exception and
    its text."""
    assert resp.status_code == 400, resp.text
    payload = resp.json()
    kind = payload.get("__type", "").rsplit("#", 1)[-1]
    if expected in (SERIALIZATION, NOT_FOUND):
        assert kind == expected, payload
        return
    if not isinstance(expected, tuple):
        expected = ("ValidationException", expected)
    assert kind == expected[0], payload
    assert payload.get("message", payload.get("Message")) == expected[1]


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


@pytest.mark.parametrize(
    ("created", "spec", "expected"),
    [pytest.param(created, spec, expected, id=row_id)
     for row_id, created, spec, expected in SSE_UPDATE_ROWS],
)
def test_refused_sse_update_keeps_the_table(create_and_cleanup_table, dynamodb_client,
                                            unique_table_name, post, created, spec, expected):
    table = create_and_cleanup_table(**({"SSESpecification": created} if created else {}))
    account = table["TableDescription"]["TableArn"].split(":")[4]
    if isinstance(spec, dict):
        spec = {k: v.replace("<account>", account) if isinstance(v, str) else v
                for k, v in spec.items()}
    before = dynamodb_client.describe_table(TableName=unique_table_name)["Table"]
    resp = post("UpdateTable", {"TableName": unique_table_name, "SSESpecification": spec})
    _assert_answer(resp, expected)
    after = dynamodb_client.describe_table(TableName=unique_table_name)["Table"]
    assert after.get("SSEDescription") == before.get("SSEDescription"), after


@pytest.mark.parametrize(
    ("members", "expected"),
    [pytest.param(members, expected, id=row_id)
     for row_id, members, expected in SSE_COMPANION_ROWS],
)
def test_refused_sse_beside_a_stream_change(create_and_cleanup_table, dynamodb_client,
                                            unique_table_name, post, members, expected):
    create_and_cleanup_table()
    before = dynamodb_client.describe_table(TableName=unique_table_name)["Table"]
    resp = post("UpdateTable", {"TableName": unique_table_name, "SSESpecification": KMS,
                                **members})
    _assert_answer(resp, expected)
    after = dynamodb_client.describe_table(TableName=unique_table_name)["Table"]
    for key in ("SSEDescription", "StreamSpecification", "BillingModeSummary"):
        assert after.get(key) == before.get(key), after


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
