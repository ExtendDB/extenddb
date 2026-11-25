# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Query key condition values checked against the key schema.

A ``KeyConditionExpression`` value is refused before any row is read when an
``N`` value is not a number (``The parameter cannot be converted to a numeric
value: <value>``) or when a key condition value's type is not the key
attribute's ``AttributeDefinitions`` type (``One or more parameter values were
invalid: Condition parameter type does not match schema type``). Every expected
answer below was measured against the service on 2026-10-24 (15:10, 21:00,
21:06 and 23:42 UTC), 2026-10-25 (11:31 UTC), 2026-11-06 (the begins_with
rows after b20) and 2026-11-11 (rows b48 to b50, f8, f9, g9 and g10, and the
digit runs) on tables with S/S, S/N and B/B keys, a GSI, an LSI and a GSI with
two HASH attributes. A Unicode decimal digit below U+10000 in an N value counts
as the digit it stands for.
The rows also pin where the two checks fall against the other Query checks:
the numeric check after Limit, the ReturnConsumedCapacity enum and the
expression parameter mix check, and before the Select enum and the table
lookup; the type check after the key schema, filter, projection, unused value
and Select checks and before the ExclusiveStartKey check.

Dual-target: runs against the service and against extenddb with the same
assertions. test_query_value_shapes.py holds the bodies of the wrong JSON
shape and the values the service cannot read as one type, and
test_query_leading_keys_values.py the LeadingKeys fences, which need the
extenddb management API.

Run:
  EXTENDDB_TEST_ENDPOINT=https://localhost:18443 \\
  EXTENDDB_ADMIN_USER=admin EXTENDDB_ADMIN_PASSWORD=<password> \\
  pytest tests/test_query_key_value_types.py -v
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table

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
        # The item the Unicode digit rows read: sk N 1 and a N 1 in ASCII.
        dynamodb_client.put_item(TableName=name, Item={
            "pk": {"S": "carol"}, "sk": {"N": "1"}, "a": {"N": "1"},
            "gpk": {"S": "carol"}, "gsk": {"S": "g"}})
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


@pytest.fixture(scope="module")
def mp_table(dynamodb_client):
    """pk S; GSI mpi with HASH h1 S and h2 N, RANGE r S."""
    with scoped_table(
        dynamodb_client,
        attribute_definitions=[
            {"AttributeName": "pk", **S},
            {"AttributeName": "h1", **S},
            {"AttributeName": "h2", "AttributeType": "N"},
            {"AttributeName": "r", **S},
        ],
        key_schema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        GlobalSecondaryIndexes=[{
            "IndexName": "mpi",
            "KeySchema": [
                {"AttributeName": "h1", "KeyType": "HASH"},
                {"AttributeName": "h2", "KeyType": "HASH"},
                {"AttributeName": "r", "KeyType": "RANGE"},
            ],
            "Projection": {"ProjectionType": "ALL"},
        }],
    ) as name:
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
UNUSED_F = "Value provided in ExpressionAttributeValues unused in expressions: keys: {:f}"
# Unicode decimal digits (general category Nd) and characters that are not.
FW_ONE = "\uff11"        # fullwidth digit one
AR_ONE = "\u0661"        # Arabic-Indic digit one
DEVA_ONE = "\u0967"      # Devanagari digit one
# Superscript two, vulgar half, fullwidth minus then 1, 1 fullwidth full stop 5.
NOT_DIGITS = ["\u00b2", "\u00bd", "\uff0d1", "1\uff0e5"]
CAROL = {"S": "carol"}
GSI_CONSISTENT = "Consistent reads are not supported on global secondary indexes"
FILTER_BEGINS_WITH_N = (
    "Invalid FilterExpression: Incorrect operand type for operator or function; "
    "operator or function: begins_with, operand type: N"
)
BEGINS_WITH_KCE = (
    "Invalid KeyConditionExpression: Incorrect operand type for operator or function; "
    "operator or function: begins_with, operand type: "
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
RCC_MSG = (
    "1 validation error detected: Value 'bad' at 'returnConsumedCapacity' failed to satisfy "
    "constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"
)
V = "ValidationException"
RNF = "ResourceNotFoundException"
LEGACY_PK_N5 = {"pk": {"ComparisonOperator": "EQ", "AttributeValueList": [N5]}}
MISSED_R = "Query condition missed key schema element: r"
# Digit one of every decimal digit (category Nd) run in Unicode 15.0: the
# service accepts the 36 runs below U+10000 outside ASCII and refuses the 31
# runs from U+104A0 up (measured 2026-11-11, each twice).
BMP_ONES = [chr(zero + 1) for zero in (
    0x0660, 0x06F0, 0x07C0, 0x0966, 0x09E6, 0x0A66, 0x0AE6, 0x0B66, 0x0BE6, 0x0C66, 0x0CE6,
    0x0D66, 0x0DE6, 0x0E50, 0x0ED0, 0x0F20, 0x1040, 0x1090, 0x17E0, 0x1810, 0x1946, 0x19D0,
    0x1A80, 0x1A90, 0x1B50, 0x1BB0, 0x1C40, 0x1C50, 0xA620, 0xA8D0, 0xA900, 0xA9D0, 0xA9F0,
    0xAA50, 0xABF0, 0xFF10)]
HIGH_ONES = [chr(zero + 1) for zero in (
    0x104A0, 0x10D30, 0x11066, 0x110F0, 0x11136, 0x111D0, 0x112F0, 0x11450, 0x114D0, 0x11650,
    0x116C0, 0x11730, 0x118E0, 0x11950, 0x11C50, 0x11D50, 0x11DA0, 0x11F50, 0x16A60, 0x16AC0,
    0x16B50, 0x1D7CE, 0x1D7D8, 0x1D7E2, 0x1D7EC, 0x1D7F6, 0x1E140, 0x1E2F0, 0x1E4F0, 0x1E950,
    0x1FBF0)]
# A refused value is echoed with each decimal digit below U+10000 folded.
FOLDED_ECHOES = [
    ("\uff11bob", "1bob"), ("\u0661\u0662x", "12x"), ("bob\uff11", "bob1"),
    ("\u0967\u0968a\u0969", "12a3"), ("\uff11\U0001d7cf", "1\U0001d7cf"),
]
MIXES = {
    "AttributesToGet": {"AttributesToGet": ["a"]},
    "QueryFilter": {"QueryFilter": {"a": {"ComparisonOperator": "EQ", "AttributeValueList": [SX]}}},
    "ConditionalOperator": {"ConditionalOperator": "AND"},
    "KeyConditions": {"KeyConditions": LEGACY_PK},
}


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
    # begins_with is refused by the type of its value, on every sort key type
    # and on an attribute outside the key schema, ahead of the unused value
    # check; an S or B value on a sort key of another type gets the type
    # mismatch (measured on Amazon DynamoDB in us-east-1 on 6 Nov 2026).
    ("q1 S sk begins_with N", "ss",
     kce("pk = :u AND begins_with(sk, :n)", {":u": SX, ":n": {"N": "1"}}), V, BEGINS_WITH_KCE + "N"),
    ("q2 N sk begins_with B", "ns",
     kce("pk = :u AND begins_with(sk, :b)", {":u": SX, ":b": B_BOB}), V, TYPE_MISMATCH),
    ("q3 B sk begins_with N", "bb",
     kce("bpk = :u AND begins_with(bsk, :n)", {":u": B_BOB, ":n": {"N": "1"}}),
     V, BEGINS_WITH_KCE + "N"),
    ("q4 N sk begins_with BOOL", "ns",
     kce("pk = :u AND begins_with(sk, :t)", {":u": SX, ":t": {"BOOL": True}}),
     V, BEGINS_WITH_KCE + "BOOL"),
    ("q5 S sk begins_with N with unused value", "ss",
     kce("pk = :u AND begins_with(sk, :n)", {":u": SX, ":n": {"N": "1"}, ":x": SX}),
     V, BEGINS_WITH_KCE + "N"),
    ("p2 N sk begins_with N", "ns",
     kce("pk = :u AND begins_with(sk, :n)", {":u": SX, ":n": {"N": "1"}}), V, BEGINS_WITH_KCE + "N"),
    ("p4 N sk begins_with N with unused value", "ns",
     kce("pk = :u AND begins_with(sk, :n)", {":u": SX, ":n": {"N": "1"}, ":x": SX}),
     V, BEGINS_WITH_KCE + "N"),
    ("m2 non-key begins_with N, S sk", "ss",
     kce("pk = :u AND begins_with(zz, :n)", {":u": SX, ":n": {"N": "1"}}), V, BEGINS_WITH_KCE + "N"),
    ("m3 non-key begins_with N, N sk", "ns",
     kce("pk = :u AND begins_with(zz, :n)", {":u": SX, ":n": {"N": "1"}}), V, BEGINS_WITH_KCE + "N"),
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
    # The two enum checks against the numeric check, Limit and the mix.
    ("f1 invalid Select with N bob", "ss", kce("pk = :u", {":u": NB}, Select="bad"), V, NUMERIC + "bob"),
    ("f2 invalid ReturnConsumedCapacity with N bob", "ss",
     kce("pk = :u", {":u": NB}, ReturnConsumedCapacity="bad"), V, RCC_MSG),
    ("f3 invalid Select with Limit 0", "ss",
     kce("pk = :u", {":u": {"S": "alice"}}, Select="bad", Limit=0), V, LIMIT_MSG),
    ("f4 invalid ReturnConsumedCapacity with Limit 0", "ss",
     kce("pk = :u", {":u": {"S": "alice"}}, ReturnConsumedCapacity="bad", Limit=0), V, LIMIT_MSG),
    ("f5 invalid Select with KeyConditions and KeyConditionExpression", "ss",
     kce("pk = :u", {":u": {"S": "alice"}}, Select="bad",
         KeyConditions={"pk": {"ComparisonOperator": "EQ", "AttributeValueList": [{"S": "alice"}]}}),
     V, MIX_MSG),
    ("f6 Limit -1 with N bob", "ss", kce("pk = :u", {":u": NB}, Limit=-1), V, LIMIT_MSG),
    ("f7 Limit 0 with a type mismatch and N bob", "ss",
     kce("pk = :u AND sk = :s", {":u": N5, ":s": NB}, Limit=0), V, LIMIT_MSG),
    # Unicode decimal digits are read as the digits they stand for; other
    # characters are refused with the value echoed as sent (measured
    # 2026-10-25 11:31 UTC on an S/N table).
    ("g1 39 Devanagari ones as sk", "ns",
     kce("pk = :u AND sk = :s", {":u": CAROL, ":s": {"N": DEVA_ONE * 39}}), V, DIGITS_MSG),
    ("g2 39 Devanagari ones in filter", "ns",
     kce("pk = :u AND sk = :s", {":u": CAROL, ":s": {"N": "1"}, ":f": {"N": DEVA_ONE * 39}},
         FilterExpression="a = :f"), V, DIGITS_MSG),
    ("g3 39 Devanagari ones unused", "ns",
     kce("pk = :u AND sk = :s", {":u": CAROL, ":s": {"N": "1"}, ":f": {"N": DEVA_ONE * 39}}),
     V, DIGITS_MSG),
    *[(f"g4 {raw!r} as sk", "ns",
       kce("pk = :u AND sk = :s", {":u": CAROL, ":s": {"N": raw}}), V, NUMERIC + raw)
      for raw in NOT_DIGITS],
    *[(f"g5 {raw!r} in filter", "ns",
       kce("pk = :u AND sk = :s", {":u": CAROL, ":s": {"N": "1"}, ":f": {"N": raw}},
           FilterExpression="a = :f"), V, NUMERIC + raw)
      for raw in NOT_DIGITS],
    *[(f"g6 {raw!r} unused", "ns",
       kce("pk = :u AND sk = :s", {":u": CAROL, ":s": {"N": "1"}, ":f": {"N": raw}}),
       V, NUMERIC + raw)
      for raw in NOT_DIGITS],
    *[(f"g7 {raw!r} unused passes the number check", "ns",
       kce("pk = :u AND sk = :s", {":u": CAROL, ":s": {"N": "1"}, ":f": {"N": raw}}),
       V, UNUSED_F)
      for raw in (FW_ONE, AR_ONE, DEVA_ONE, "1\uff12", "1e\uff12")],
    ("g8 NS with a fullwidth digit unused", "ns",
     kce("pk = :u AND sk = :s", {":u": CAROL, ":s": {"N": "1"}, ":f": {"NS": [FW_ONE]}}),
     V, UNUSED_F),
    # The other value types the earlier record covers.
    ("pk = BOOL", "ss", kce("pk = :u", {":u": {"BOOL": True}}), V, TYPE_MISMATCH),
    ("pk = N bob AND pk > S alice", "ss",
     kce("pk = :u AND pk > :v", {":u": NB, ":v": {"S": "alice"}}), V, NUMERIC + "bob"),
    # An invalid ReturnConsumedCapacity is reported ahead of each
    # non-expression parameter beside KeyConditionExpression, and the mix
    # ahead of the numbers (measured 2026-11-11).
    *[(f"f8 invalid ReturnConsumedCapacity with {name}", "ss",
       kce("pk = :u", {":u": NB}, ReturnConsumedCapacity="bad", **extra), V, RCC_MSG)
      for name, extra in MIXES.items()],
    *[(f"f9 {name} with N bob", "ss", kce("pk = :u", {":u": NB}, **extra), V,
       "Can not use both expression and non-expression parameters in the same request: "
       f"Non-expression parameters: {{{name}}} Expression parameters: {{KeyConditionExpression}}")
      for name, extra in MIXES.items() if name != "KeyConditions"],
    # A refused value is echoed with its decimal digits folded to ASCII, and a
    # digit from U+104A0 up is not a digit (measured 2026-11-11).
    *[(f"g9 {raw!r} echoed with ASCII digits", "ns",
       kce("pk = :u AND sk = :s", {":u": CAROL, ":s": {"N": "1"}, ":f": {"N": raw}}),
       V, NUMERIC + echo)
      for raw, echo in FOLDED_ECHOES],
    *[(f"g10 U+{ord(one):X} in filter", "ns",
       kce("pk = :u AND sk = :s", {":u": CAROL, ":s": {"N": "1"}, ":f": {"N": one}},
           FilterExpression="a = :f"), V, NUMERIC + one)
      for one in HIGH_ONES],
    # Legacy KeyConditions, and a key schema with two HASH attributes, where
    # the type is reported ahead of the clause on a non-key attribute
    # (measured 2026-11-11).
    ("b48 legacy KeyConditions pk EQ N 5", "ss", {"KeyConditions": LEGACY_PK_N5}, V, TYPE_MISMATCH),
    ("b49 two HASH attributes, h2 = S beside a non-key clause", "mp",
     kce("h1 = :a AND h2 = :b AND z = :c", {":a": SX, ":b": {"S": "1"}, ":c": SX},
         IndexName="mpi"), V, TYPE_MISMATCH),
    ("b50 two HASH attributes, h2 = N beside a non-key clause", "mp",
     kce("h1 = :a AND h2 = :b AND z = :c", {":a": SX, ":b": {"N": "1"}, ":c": SX},
         IndexName="mpi"), V, MISSED_R),
]


class TestKeyConditionValueTypes:
    """Each row: a Query and the service's answer."""

    @pytest.fixture
    def tables(self, ss_table, ns_table, bb_table, mp_table):
        return {"ss": ss_table, "ns": ns_table, "bb": bb_table, "mp": mp_table, "missing": MISSING}

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


class TestUnicodeDecimalDigitsPassTheCheck:
    """A Unicode decimal digit in an N value passes the request-time number
    check: the service reads it as the digit it stands for (measured 2026-10-25
    11:31 UTC). Reading such a value as the digit is not part of this change,
    so only the acceptance is asserted, in the positions where the request
    reaches no numeric key comparison."""

    DIGITS = [FW_ONE, AR_ONE, DEVA_ONE]
    DIGIT_IDS = ["fullwidth", "arabic-indic", "devanagari"]

    @pytest.mark.parametrize("digit", DIGITS, ids=DIGIT_IDS)
    def test_filter_value_is_accepted(self, dynamodb_client, ns_table, digit):
        resp = dynamodb_client.query(
            TableName=ns_table, KeyConditionExpression="pk = :u AND sk = :s",
            FilterExpression="a = :f",
            ExpressionAttributeValues={":u": CAROL, ":s": {"N": "1"}, ":f": {"N": digit}})
        assert resp["ScannedCount"] == 1

    @pytest.mark.parametrize("raw", ["1\uff12", "1e\uff12"], ids=["1-fullwidth-2", "1e-fullwidth-2"])
    def test_mixed_digits_in_the_filter_are_accepted(self, dynamodb_client, ns_table, raw):
        resp = dynamodb_client.query(
            TableName=ns_table, KeyConditionExpression="pk = :u AND sk = :s",
            FilterExpression="a = :f",
            ExpressionAttributeValues={":u": CAROL, ":s": {"N": "1"}, ":f": {"N": raw}})
        assert (resp["Count"], resp["ScannedCount"]) == (0, 1)

    def test_fullwidth_digit_in_a_number_set_passes_the_check(self, dynamodb_client, ns_table):
        resp = dynamodb_client.query(
            TableName=ns_table, KeyConditionExpression="pk = :u AND sk = :s",
            FilterExpression="contains(ns, :f)",
            ExpressionAttributeValues={":u": CAROL, ":s": {"N": "1"}, ":f": {"NS": [FW_ONE]}})
        assert (resp["Count"], resp["ScannedCount"]) == (0, 1)

    @pytest.mark.parametrize("one", BMP_ONES, ids=[f"U+{ord(one):04X}" for one in BMP_ONES])
    def test_digit_one_of_every_run_below_u_10000_is_accepted(self, dynamodb_client, ns_table, one):
        resp = dynamodb_client.query(
            TableName=ns_table, KeyConditionExpression="pk = :u AND sk = :s",
            FilterExpression="a = :f",
            ExpressionAttributeValues={":u": CAROL, ":s": {"N": "1"}, ":f": {"N": one}})
        assert resp["ScannedCount"] == 1
