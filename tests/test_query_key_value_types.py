# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Query key condition values checked against the key schema.

A ``KeyConditionExpression`` value is refused before any row is read when an
``N`` value is not a number (``The parameter cannot be converted to a numeric
value: <value>``) or when a key condition value's type is not the key
attribute's ``AttributeDefinitions`` type (``One or more parameter values were
invalid: Condition parameter type does not match schema type``). Every expected
answer below was measured against the service on 2026-10-24 (15:10 UTC and
21:00 UTC) on tables with S/S, S/N and B/B keys, a GSI and an LSI. The rows
also pin where the two checks fall against the other Query checks: the numeric
check after Limit and the expression parameter mix check and before the table
lookup; the type check after the key schema, filter, projection, unused value
and Select checks and before the ExclusiveStartKey check.

Dual-target: runs against the service and against extenddb with the same
assertions. The LeadingKeys class needs the extenddb management API and is
skipped against the service.

Run:
  EXTENDDB_TEST_ENDPOINT=https://localhost:18443 \\
  EXTENDDB_ADMIN_USER=admin EXTENDDB_ADMIN_PASSWORD=<password> \\
  pytest tests/test_query_key_value_types.py -v
"""

from __future__ import annotations

import os
import uuid
from typing import Any

import boto3
import pytest
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

from conftest import scoped_table, wait_for_active

NUMERIC = "The parameter cannot be converted to a numeric value: "
TYPE_MISMATCH = (
    "One or more parameter values were invalid: "
    "Condition parameter type does not match schema type"
)

S = {"AttributeType": "S"}


def _err(exc_info) -> tuple[str, str]:
    err = exc_info.value.response["Error"]
    return err["Code"], err["Message"]


@pytest.fixture(scope="module")
def ss_table(dynamodb_client):
    """pk S, sk S; LSI lsi1 on lsk S; GSI gsi1 on gpk S (hash only)."""
    with scoped_table(
        dynamodb_client,
        attribute_definitions=[
            {"AttributeName": "pk", **S},
            {"AttributeName": "sk", **S},
            {"AttributeName": "lsk", **S},
            {"AttributeName": "gpk", **S},
        ],
        key_schema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        LocalSecondaryIndexes=[{
            "IndexName": "lsi1",
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "lsk", "KeyType": "RANGE"},
            ],
            "Projection": {"ProjectionType": "ALL"},
        }],
        GlobalSecondaryIndexes=[{
            "IndexName": "gsi1",
            "KeySchema": [{"AttributeName": "gpk", "KeyType": "HASH"}],
            "Projection": {"ProjectionType": "ALL"},
        }],
    ) as name:
        for pk in ("alice", "bob"):
            dynamodb_client.put_item(TableName=name, Item={
                "pk": {"S": pk}, "sk": {"S": "1"}, "lsk": {"S": "l"}, "gpk": {"S": pk}})
        yield name


@pytest.fixture(scope="module")
def ns_table(dynamodb_client):
    """pk S, sk N; GSI gsi1 on gpk S / gsk S."""
    with scoped_table(
        dynamodb_client,
        attribute_definitions=[
            {"AttributeName": "pk", **S},
            {"AttributeName": "sk", "AttributeType": "N"},
            {"AttributeName": "gpk", **S},
            {"AttributeName": "gsk", **S},
        ],
        key_schema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        GlobalSecondaryIndexes=[{
            "IndexName": "gsi1",
            "KeySchema": [
                {"AttributeName": "gpk", "KeyType": "HASH"},
                {"AttributeName": "gsk", "KeyType": "RANGE"},
            ],
            "Projection": {"ProjectionType": "ALL"},
        }],
    ) as name:
        for pk in ("alice", "bob"):
            dynamodb_client.put_item(TableName=name, Item={
                "pk": {"S": pk}, "sk": {"N": "5"}, "gpk": {"S": pk}, "gsk": {"S": "g"}})
        yield name


@pytest.fixture(scope="module")
def bb_table(dynamodb_client):
    """bpk B, bsk B."""
    with scoped_table(
        dynamodb_client,
        attribute_definitions=[
            {"AttributeName": "bpk", "AttributeType": "B"},
            {"AttributeName": "bsk", "AttributeType": "B"},
        ],
        key_schema=[
            {"AttributeName": "bpk", "KeyType": "HASH"},
            {"AttributeName": "bsk", "KeyType": "RANGE"},
        ],
    ) as name:
        dynamodb_client.put_item(TableName=name, Item={"bpk": {"B": b"bob"}, "bsk": {"B": b"x"}})
        yield name


NB = {"N": "bob"}
N5 = {"N": "5"}
SX = {"S": "x"}
B_BOB = {"B": b"bob"}
MISSING = "no-such-table-kv-types"
LEGACY_PK = {"pk": {"ComparisonOperator": "EQ", "AttributeValueList": [SX]}}
L_NB = {"L": [{"S": "a"}, NB]}
M_NB = {"M": {"a": NB}}
NS_NB = {"NS": ["1", "bob"]}

LIMIT_MSG = (
    "1 validation error detected: Value at 'Limit' failed to satisfy constraint: "
    "Member must have value greater than or equal to 1"
)
MIX_MSG = (
    "Can not use both expression and non-expression parameters in the same request: "
    "Non-expression parameters: {KeyConditions} Expression parameters: {KeyConditionExpression}"
)
OVERFLOW_MSG = (
    "Number overflow. Attempting to store a number with magnitude larger than supported range"
)
DIGITS_MSG = "Attempting to store more than 38 significant digits in a Number"
MISSED_PK = "Query condition missed key schema element: pk"
MISSED_GPK = "Query condition missed key schema element: gpk"
RESERVED_KCE = (
    "Invalid KeyConditionExpression: Attribute name is a reserved keyword; reserved keyword: other"
)
NO_INDEX = "The table does not have the specified index: nope"
UNUSED_X = "Value provided in ExpressionAttributeValues unused in expressions: keys: {:x}"
GSI_CONSISTENT = "Consistent reads are not supported on global secondary indexes"
FILTER_BEGINS_WITH_N = (
    "Invalid FilterExpression: Incorrect operand type for operator or function; "
    "operator or function: begins_with, operand type: N"
)
PROJECTION_OVERLAP = (
    "Invalid ProjectionExpression: Two document paths overlap with each other; "
    "must remove or rewrite one of these paths; path one: [a], path two: [a, b]"
)
FILTER_RESERVED = (
    "Invalid FilterExpression: Attribute name is a reserved keyword; reserved keyword: name"
)
SELECT_PROJECTION = (
    "1 validation error detected: Cannot specify the ProjectionExpression when choosing to "
    "get ALL_ATTRIBUTES"
)
V = "ValidationException"
RNF = "ResourceNotFoundException"


def kce(expr, values, **extra):
    return {"KeyConditionExpression": expr, "ExpressionAttributeValues": values, **extra}


# (row, table, Query arguments, expected error code, expected message).
# Tables: ss (pk S, sk S, LSI lsi1 on lsk S, GSI gsi1 on gpk S), ns (pk S, sk N,
# GSI gsi1 on gpk S / gsk S), bb (bpk B, bsk B), missing (no such table).
ROWS = [
    # The numeric check against the other Query checks.
    ("a1 missing table", "missing", kce("pk = :u", {":u": NB}), V, NUMERIC + "bob"),
    ("a2 unknown IndexName", "ss", kce("pk = :u", {":u": NB}, IndexName="nope"), V, NUMERIC + "bob"),
    ("a3 undefined placeholder", "ss", kce("pk = :u AND sk = :zz", {":u": NB}), V, NUMERIC + "bob"),
    ("a4 unused value", "ss", kce("pk = :u", {":u": NB, ":x": SX}), V, NUMERIC + "bob"),
    ("a5 syntax error", "ss", kce("pk = :u AND", {":u": NB}), V, NUMERIC + "bob"),
    ("a6 Limit 0", "ss", kce("pk = :u", {":u": NB}, Limit=0), V, LIMIT_MSG),
    ("a7 N bob only in FilterExpression", "ss",
     kce("pk = :p", {":p": SX, ":n": NB}, FilterExpression="a = :n"), V, NUMERIC + "bob"),
    ("a8 N bob unused", "ss", kce("pk = :p", {":p": SX, ":n": NB}), V, NUMERIC + "bob"),
    ("a9 N bob in filter beside pk = N 5", "ss",
     kce("pk = :p", {":p": N5, ":n": NB}, FilterExpression="a = :n"), V, NUMERIC + "bob"),
    ("a10 ConsistentRead on GSI", "ss",
     kce("gpk = :u", {":u": NB}, IndexName="gsi1", ConsistentRead=True), V, NUMERIC + "bob"),
    ("a11 N empty", "ss", kce("pk = :u", {":u": {"N": ""}}), V, NUMERIC),
    ("a12 N 1e", "ss", kce("pk = :u", {":u": {"N": "1e"}}), V, NUMERIC + "1e"),
    ("a13 N 12abc", "ss", kce("pk = :u", {":u": {"N": "12abc"}}), V, NUMERIC + "12abc"),
    ("a14 sk only", "ss", kce("sk = :u", {":u": NB}), V, NUMERIC + "bob"),
    ("a15 KeyConditions and KeyConditionExpression", "ss",
     kce("pk = :u", {":u": NB}, KeyConditions=LEGACY_PK), V, MIX_MSG),
    ("a16 BETWEEN bounds reversed", "ss",
     kce("pk = :u AND sk BETWEEN :a AND :b", {":u": NB, ":a": {"S": "b"}, ":b": {"S": "a"}}),
     V, NUMERIC + "bob"),
    ("a17 nested path", "ss", kce("pk.x = :u", {":u": NB}), V, NUMERIC + "bob"),
    ("a18 pk >", "ss", kce("pk > :u", {":u": NB}), V, NUMERIC + "bob"),
    ("a19 ExclusiveStartKey bad", "ss",
     kce("pk = :u", {":u": NB}, ExclusiveStartKey={"zz": SX}), V, NUMERIC + "bob"),
    ("a20 pk = B AND sk = N bob", "ss",
     kce("pk = :u AND sk = :s", {":u": B_BOB, ":s": NB}), V, NUMERIC + "bob"),
    ("a21 pk = S AND sk = N bob", "ss",
     kce("pk = :u AND sk = :s", {":u": SX, ":s": NB}), V, NUMERIC + "bob"),
    ("a22 N 1e200", "ss", kce("pk = :u", {":u": {"N": "1e200"}}), V, OVERFLOW_MSG),
    ("a23 N 39 digits", "ss", kce("pk = :u", {":u": {"N": "1" * 39}}), V, DIGITS_MSG),
    ("a24 Select ALL_ATTRIBUTES with ProjectionExpression", "ss",
     kce("pk = :u", {":u": NB}, Select="ALL_ATTRIBUTES", ProjectionExpression="a"),
     V, NUMERIC + "bob"),
    ("a25 reserved word in filter", "ss",
     kce("pk = :u", {":u": NB, ":n": SX}, FilterExpression="name = :n"), V, NUMERIC + "bob"),
    ("a26 undefined name in filter", "ss",
     kce("pk = :u", {":u": NB, ":n": SX}, FilterExpression="#n = :n"), V, NUMERIC + "bob"),
    ("a27 two sort key conditions", "ss",
     kce("pk = :u AND sk = :s AND sk > :t", {":u": NB, ":s": N5, ":t": {"N": "6"}}),
     V, NUMERIC + "bob"),
    ("a28 sk = N bob with unused value", "ss",
     kce("pk = :u AND sk = :s", {":u": SX, ":s": NB, ":x": SX}), V, NUMERIC + "bob"),
    ("a29 ProjectionExpression overlap", "ss",
     kce("pk = :u", {":u": NB}, ProjectionExpression="a, a.b"), V, NUMERIC + "bob"),
    ("a30 N space 5", "ss", kce("pk = :u", {":u": {"N": " 5"}}), V, NUMERIC + " 5"),
    # The type check against the other key condition checks.
    ("b1 sk = N 5 only", "ss", kce("sk = :s", {":s": N5}), V, MISSED_PK),
    ("b2 reserved word in key condition", "ss",
     kce("pk = :u AND other = :s", {":u": SX, ":s": N5}), V, RESERVED_KCE),
    ("b4 BETWEEN N bounds on S sk", "ss",
     kce("pk = :u AND sk BETWEEN :a AND :b", {":u": SX, ":a": {"N": "1"}, ":b": {"N": "2"}}),
     V, TYPE_MISMATCH),
    ("b6 sk > N 5", "ss", kce("pk = :u AND sk > :s", {":u": SX, ":s": N5}), V, TYPE_MISMATCH),
    ("b7 sk < B", "ss", kce("pk = :u AND sk < :s", {":u": SX, ":s": B_BOB}), V, TYPE_MISMATCH),
    ("b9 unknown IndexName", "ss", kce("pk = :u", {":u": N5}, IndexName="nope"), V, NO_INDEX),
    ("b10 Limit 0", "ss", kce("pk = :u", {":u": N5}, Limit=0), V, LIMIT_MSG),
    ("b11 unused value", "ss", kce("pk = :u", {":u": N5, ":x": SX}), V, UNUSED_X),
    ("b13 missing table", "missing", kce("pk = :u", {":u": N5}), RNF, "Requested resource not found"),
    ("b14 GSI gpk = N 5", "ss", kce("gpk = :g", {":g": N5}, IndexName="gsi1"), V, TYPE_MISMATCH),
    ("b15 LSI lsk = N 5", "ss",
     kce("pk = :u AND lsk = :l", {":u": SX, ":l": N5}, IndexName="lsi1"), V, TYPE_MISMATCH),
    ("b16 GSI with base pk = N 5", "ss", kce("pk = :u", {":u": N5}, IndexName="gsi1"), V, MISSED_GPK),
    ("b17 GSI with base pk = S", "ss", kce("pk = :u", {":u": SX}, IndexName="gsi1"), V, MISSED_GPK),
    ("b18 N sk = S 5", "ns", kce("pk = :u AND sk = :s", {":u": SX, ":s": {"S": "5"}}), V, TYPE_MISMATCH),
    ("b19 N sk BETWEEN S", "ns",
     kce("pk = :u AND sk BETWEEN :a AND :b", {":u": SX, ":a": {"S": "1"}, ":b": {"S": "2"}}),
     V, TYPE_MISMATCH),
    ("b20 N sk begins_with S", "ns",
     kce("pk = :u AND begins_with(sk, :b)", {":u": SX, ":b": {"S": "1"}}), V, TYPE_MISMATCH),
    ("b22 pk = N 5 AND N sk = S 5", "ns",
     kce("pk = :u AND sk = :s", {":u": N5, ":s": {"S": "5"}}), V, TYPE_MISMATCH),
    ("b23 N sk = N bob", "ns", kce("pk = :u AND sk = :s", {":u": SX, ":s": NB}), V, NUMERIC + "bob"),
    ("b24 B pk = S", "bb", kce("bpk = :u", {":u": SX}), V, TYPE_MISMATCH),
    ("b25 B pk = N 5", "bb", kce("bpk = :u", {":u": N5}), V, TYPE_MISMATCH),
    ("b26 B pk = N bob", "bb", kce("bpk = :u", {":u": NB}), V, NUMERIC + "bob"),
    ("b27 B sk begins_with S", "bb",
     kce("bpk = :u AND begins_with(bsk, :b)", {":u": B_BOB, ":b": SX}), V, TYPE_MISMATCH),
    ("b28 ConsistentRead on GSI", "ss",
     kce("gpk = :g", {":g": N5}, IndexName="gsi1", ConsistentRead=True), V, GSI_CONSISTENT),
    ("b29 filter begins_with N", "ss",
     kce("pk = :u", {":u": N5, ":n": {"N": "1"}}, FilterExpression="begins_with(a, :n)"),
     V, FILTER_BEGINS_WITH_N),
    ("b30 ProjectionExpression overlap", "ss",
     kce("pk = :u", {":u": N5}, ProjectionExpression="a, a.b"), V, PROJECTION_OVERLAP),
    ("b31 ExclusiveStartKey bad", "ss",
     kce("pk = :u", {":u": N5}, ExclusiveStartKey={"zz": SX}), V, TYPE_MISMATCH),
    ("b32 pk = N 5 AND sk = N bob", "ss",
     kce("pk = :u AND sk = :s", {":u": N5, ":s": NB}), V, NUMERIC + "bob"),
    ("b33 pk = B AND sk = N 5", "ss",
     kce("pk = :u AND sk = :s", {":u": B_BOB, ":s": N5}), V, TYPE_MISMATCH),
    ("b35 pk = NULL", "ss", kce("pk = :u", {":u": {"NULL": True}}), V, TYPE_MISMATCH),
    ("b36 pk = SS", "ss", kce("pk = :u", {":u": {"SS": ["a"]}}), V, TYPE_MISMATCH),
    ("b37 pk = L", "ss", kce("pk = :u", {":u": {"L": [SX]}}), V, TYPE_MISMATCH),
    ("b38 pk = M", "ss", kce("pk = :u", {":u": {"M": {"a": SX}}}), V, TYPE_MISMATCH),
    ("b41 reserved word in filter", "ss",
     kce("pk = :u", {":u": N5, ":n": SX}, FilterExpression="name = :n"), V, FILTER_RESERVED),
    ("b42 Select ALL_ATTRIBUTES with ProjectionExpression", "ss",
     kce("pk = :u", {":u": N5}, Select="ALL_ATTRIBUTES", ProjectionExpression="a"),
     V, SELECT_PROJECTION),
    ("b43 non-key second clause", "ss",
     kce("pk = :u AND other = :s", {":u": N5, ":s": SX}), V, RESERVED_KCE),
    ("b45 pk = N 5 through #p", "ss",
     kce("#p = :u", {":u": N5}, ExpressionAttributeNames={"#p": "pk"}), V, TYPE_MISMATCH),
    ("b46 reversed operands", "ss",
     kce("pk = :u AND :s <= sk", {":u": SX, ":s": N5}), V, TYPE_MISMATCH),
    ("b47 GSI gsk = N 5", "ns",
     kce("gpk = :g AND gsk = :s", {":g": SX, ":s": N5}, IndexName="gsi1"), V, TYPE_MISMATCH),
    # Nested N values, values no expression reads, and the Limit and mix checks
    # against the table and index lookups (measured 2026-10-24 21:06 UTC).
    ("c1 L with N bob in filter", "ss",
     kce("pk = :p", {":p": SX, ":l": L_NB}, FilterExpression="a = :l"), V, NUMERIC + "bob"),
    ("c2 L with N bob unused", "ss", kce("pk = :p", {":p": SX, ":l": L_NB}), V, NUMERIC + "bob"),
    ("c3 M with N bob in filter", "ss",
     kce("pk = :p", {":p": SX, ":m": M_NB}, FilterExpression="a = :m"), V, NUMERIC + "bob"),
    ("c4 M with N bob unused", "ss", kce("pk = :p", {":p": SX, ":m": M_NB}), V, NUMERIC + "bob"),
    ("c5 NS with bob in filter", "ss",
     kce("pk = :p", {":p": SX, ":n": NS_NB}, FilterExpression="a = :n"), V, NUMERIC + "bob"),
    ("c6 NS with bob unused", "ss", kce("pk = :p", {":p": SX, ":n": NS_NB}), V, NUMERIC + "bob"),
    ("c7 L with N empty in filter", "ss",
     kce("pk = :p", {":p": SX, ":l": {"L": [{"N": ""}]}}, FilterExpression="a = :l"), V, NUMERIC),
    ("c8 M in L in M with N bob", "ss",
     kce("pk = :p", {":p": SX, ":m": {"M": {"a": {"L": [M_NB]}}}}), V, NUMERIC + "bob"),
    ("c9 N bob unused on a missing table", "missing",
     kce("pk = :p", {":p": SX, ":n": NB}), V, NUMERIC + "bob"),
    ("c12 N bob with no key condition", "ss",
     {"ExpressionAttributeValues": {":n": NB}}, V, NUMERIC + "bob"),
    ("c13 N bob beside legacy KeyConditions", "ss",
     {"KeyConditions": LEGACY_PK, "ExpressionAttributeValues": {":n": NB}}, V, NUMERIC + "bob"),
    ("d1 Limit 0 on a missing table", "missing", kce("pk = :p", {":p": SX}, Limit=0), V, LIMIT_MSG),
    ("d2 Limit 0 with unknown IndexName", "ss",
     kce("pk = :p", {":p": SX}, IndexName="nope", Limit=0), V, LIMIT_MSG),
    ("d3 Limit 0 with ConsistentRead on GSI", "ss",
     kce("gpk = :p", {":p": SX}, IndexName="gsi1", ConsistentRead=True, Limit=0), V, LIMIT_MSG),
    ("d4 KeyConditions and KeyConditionExpression on a missing table", "missing",
     kce("pk = :p", {":p": SX}, KeyConditions=LEGACY_PK), V, MIX_MSG),
    ("d5 Limit 0 with KeyConditions and KeyConditionExpression", "ss",
     kce("pk = :p", {":p": SX}, Limit=0, KeyConditions=LEGACY_PK), V, LIMIT_MSG),
    ("d6 Limit 0 with N bob and an undefined placeholder", "ss",
     kce("pk = :p AND sk = :zz", {":p": SX, ":n": NB}, Limit=0), V, LIMIT_MSG),
    # The other value types the earlier record covers.
    ("pk = BOOL", "ss", kce("pk = :u", {":u": {"BOOL": True}}), V, TYPE_MISMATCH),
    ("pk = N bob AND pk > S alice", "ss",
     kce("pk = :u AND pk > :v", {":u": NB, ":v": {"S": "alice"}}), V, NUMERIC + "bob"),
]


class TestKeyConditionValueTypes:
    """Each row: a Query and the service's answer."""

    @pytest.fixture
    def tables(self, ss_table, ns_table, bb_table):
        return {"ss": ss_table, "ns": ns_table, "bb": bb_table, "missing": MISSING}

    @pytest.mark.parametrize("row,table,kwargs,code,expected", ROWS, ids=[r[0] for r in ROWS])
    def test_rows(self, dynamodb_client_no_validation, tables, row, table, kwargs, code, expected):
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client_no_validation.query(TableName=tables[table], **kwargs)
        assert _err(exc_info) == (code, expected)

    def test_matching_types_read_the_named_partition(self, dynamodb_client, ss_table):
        resp = dynamodb_client.query(
            TableName=ss_table, KeyConditionExpression="pk = :u",
            ExpressionAttributeValues={":u": {"S": "bob"}})
        assert [it["pk"]["S"] for it in resp["Items"]] == ["bob"]

    def test_control_matching_types_on_every_table(self, dynamodb_client, ss_table, ns_table, bb_table):
        assert dynamodb_client.query(
            TableName=ss_table, KeyConditionExpression="pk = :u AND sk = :s",
            ExpressionAttributeValues={":u": {"S": "nope"}, ":s": SX})["Count"] == 0
        assert dynamodb_client.query(
            TableName=ns_table, KeyConditionExpression="pk = :u AND sk > :s",
            ExpressionAttributeValues={":u": {"S": "bob"}, ":s": {"N": "1"}})["Count"] == 1
        assert dynamodb_client.query(
            TableName=bb_table, KeyConditionExpression="bpk = :u",
            ExpressionAttributeValues={":u": B_BOB})["Count"] == 1


class TestLeadingKeysWithAnInvalidKeyValue:
    """A principal under each LeadingKeys fence form gets ValidationException and
    no items for a Query whose ExpressionAttributeValues carries an N value that
    is not a number, whether the key condition, the filter or nothing reads it.

    extenddb only: the fences need users created through the management API."""

    @pytest.fixture(autouse=True)
    def setup(self):
        endpoint = os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip()
        admin_user = os.environ.get("EXTENDDB_ADMIN_USER", "").strip()
        admin_pass = os.environ.get("EXTENDDB_ADMIN_PASSWORD", "").strip()
        if not endpoint:
            pytest.skip("LeadingKeys fences need the extenddb management API")
        if not admin_user or not admin_pass:
            pytest.fail("MISCONFIGURED: EXTENDDB_ADMIN_USER and EXTENDDB_ADMIN_PASSWORD are required")
        from management_helpers import ManagementClient

        self.endpoint = endpoint
        self.region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
        self.mgmt = ManagementClient(endpoint, admin_user, admin_pass)
        self.account_id = f"{uuid.uuid4().int % 10**12:012d}"
        resp = self.mgmt.create_account(self.account_id, f"kv-test-{self.account_id}")
        assert resp.status_code == 201, resp.text
        self._users: list[str] = []
        yield
        for u in self._users:
            try:
                self.mgmt.delete_user(self.account_id, u)
            except Exception:
                pass
        self.mgmt.delete_account(self.account_id)

    def _client(self, policy: dict | None) -> Any:
        name = f"kv-{uuid.uuid4().hex[:8]}"
        resp = self.mgmt.create_user(self.account_id, name, "TestPass123!")
        assert resp.status_code == 201, resp.text
        self._users.append(name)
        resp = self.mgmt.create_access_key(self.account_id, name)
        assert resp.status_code == 201, resp.text
        creds = resp.json()
        if policy is not None:
            resp = self.mgmt.put_user_policy(self.account_id, name, "p", policy)
            assert resp.status_code == 204, resp.text
        return boto3.client(
            "dynamodb", endpoint_url=self.endpoint, region_name=self.region,
            aws_access_key_id=creds["access_key_id"],
            aws_secret_access_key=creds["secret_access_key"],
            verify=False,
            config=BotoConfig(retries={"max_attempts": 0}, parameter_validation=False))

    def _fence(self, condition: dict, table: str) -> dict:
        arn = f"arn:aws:dynamodb:{self.region}:{self.account_id}:table/{table}"
        return {
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Action": "dynamodb:*",
                "Resource": [arn, arn + "/index/*"],
                "Condition": condition,
            }],
        }

    def test_every_fence_form_gets_validation_exception_and_no_items(self):
        owner = self._client({
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Action": "dynamodb:*", "Resource": "*"}],
        })
        table = f"kv-{uuid.uuid4().hex[:8]}"
        owner.create_table(
            TableName=table,
            AttributeDefinitions=[{"AttributeName": "pk", **S}],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST")
        wait_for_active(owner, table)
        for pk in ("alice", "bob"):
            owner.put_item(TableName=table, Item={"pk": {"S": pk}})

        fences = {
            "ForAllValues": {"ForAllValues:StringEquals": {"dynamodb:LeadingKeys": ["alice"]}},
            "ForAnyValue": {"ForAnyValue:StringEquals": {"dynamodb:LeadingKeys": ["alice"]}},
            "Null false": {"Null": {"dynamodb:LeadingKeys": "false"},
                           "ForAllValues:StringEquals": {"dynamodb:LeadingKeys": ["alice"]}},
            "StringEquals": {"StringEquals": {"dynamodb:LeadingKeys": "alice"}},
            "StringLike": {"StringLike": {"dynamodb:LeadingKeys": "ali*"}},
            "StringNotEquals": {"StringNotEquals": {"dynamodb:LeadingKeys": "bob"}},
        }
        # The reported shape, and an N value that only the filter reads or
        # nothing reads beside a key value the fence would bind.
        requests = {
            "pk = N bob": {"TableName": table, "KeyConditionExpression": "pk = :u",
                           "ExpressionAttributeValues": {":u": NB}},
            "pk = S alice, N bob in filter": {
                "TableName": table, "KeyConditionExpression": "pk = :u",
                "FilterExpression": "a = :n",
                "ExpressionAttributeValues": {":u": {"S": "alice"}, ":n": NB}},
            "pk = S bob, N bob unused": {
                "TableName": table, "KeyConditionExpression": "pk = :u",
                "ExpressionAttributeValues": {":u": {"S": "bob"}, ":n": NB}},
        }
        failures = []
        clients = {label: self._client(self._fence(condition, table))
                   for label, condition in fences.items()}
        clients["owner"] = owner
        for shape, request in requests.items():
            for label, client in clients.items():
                try:
                    resp = client.query(**request)
                    failures.append(f"{shape} / {label}: {resp['Count']} items returned")
                except ClientError as exc:
                    err = exc.response["Error"]
                    if (err["Code"], err["Message"]) != ("ValidationException", NUMERIC + "bob"):
                        failures.append(f"{shape} / {label}: {err['Code']} {err['Message']}")
        owner.delete_table(TableName=table)
        assert not failures, "\n".join(failures)
