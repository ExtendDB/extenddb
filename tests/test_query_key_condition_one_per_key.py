# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""A KeyConditionExpression carries one condition per key attribute.

Amazon DynamoDB refuses a key condition that names the same key attribute
twice, on the partition key and on the sort key alike, whatever the operators
are (issue #430):

    ValidationException: Invalid KeyConditionExpression: KeyConditionExpressions
    must only contain one condition per key

Every row below is a request and the answer the service gave it in us-east-1,
on the date in its class's docstring. The 2026-10-24 and 2026-10-31 rows ran on
a table with pk (S) HASH and sk (S) RANGE, and the 2026-11-11 rows from 09:43Z
to 12:44Z on such a table with a KEYS_ONLY global secondary index on g (S). The
2026-11-10 rows and J2 ran on a table keyed on pk (S) alone. Where the
service's text differs from extenddb's by wording alone (syntax errors), the
row compares the prefix.

Dual-target against Amazon DynamoDB and extenddb.
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table

ONE_PER_KEY = (
    "Invalid KeyConditionExpression: "
    "KeyConditionExpressions must only contain one condition per key"
)
NESTED = (
    "Invalid KeyConditionExpression: "
    "KeyConditionExpressions cannot have conditions on nested attributes"
)
REDUNDANT_PARENS = "Invalid KeyConditionExpression: The expression has redundant parentheses;"
BEGINS_WITH_N = (
    "Invalid KeyConditionExpression: Incorrect operand type for operator or "
    "function; operator or function: begins_with, operand type: N"
)
FILTER_BEGINS_WITH_N = (
    "Invalid FilterExpression: Incorrect operand type for operator or "
    "function; operator or function: begins_with, operand type: N"
)
FILTER_RESERVED_NAME = (
    "Invalid FilterExpression: Attribute name is a reserved keyword; reserved keyword: name"
)
LIMIT_ZERO = (
    "1 validation error detected: Value at 'Limit' failed to satisfy constraint: "
    "Member must have value greater than or equal to 1"
)
# A prefix match: the service words its syntax errors differently from extenddb.
KCE_PREFIX = "Invalid KeyConditionExpression: "
FILTER_PREFIX = "Invalid FilterExpression: "

A = {"S": "alice"}
B = {"S": "bob"}
C = {"S": "carol"}
N1 = {"N": "1"}
VALUES = {"a": A, "b": B, "c": C, "n": N1, "z": {"S": "zed"}, "v": A, "w": B, "extra": B}
GSI = "g-keys"
HASH = "hash-table"  # stands for the hash_table fixture in a row's TableName
MISSING_TABLE = "extenddb-test-484-no-such-table"
LEGACY_PK = {"pk": {"ComparisonOperator": "EQ", "AttributeValueList": [A]}}


def _undefined(reference: str, expression: str = "KeyConditionExpression") -> str:
    """The service's message for an undefined `#name` or `:value`."""
    if reference.startswith("#"):
        return (
            f"Invalid {expression}: An expression attribute name used in the "
            f"document path is not defined; attribute name: {reference}"
        )
    return (
        f"Invalid {expression}: An expression attribute value used in "
        f"expression is not defined; attribute value: {reference}"
    )


def _mixing(non_expression: str, *expression: str) -> str:
    return (
        "Can not use both expression and non-expression parameters in the same "
        f"request: Non-expression parameters: {{{non_expression}}} Expression "
        f"parameters: {{{', '.join(expression)}}}"
    )


def _q(kce: str | None = None, values=None, names: dict | None = None, **kwargs) -> dict:
    """A Query's parameters. `values` names the entries of VALUES to define,
    as "a b n", or is the ExpressionAttributeValues dict itself."""
    request = dict(kwargs)
    if kce is not None:
        request["KeyConditionExpression"] = kce
    if isinstance(values, str):
        values = {f":{k}": VALUES[k] for k in values.split()}
    if values:
        request["ExpressionAttributeValues"] = values
    if names:
        request["ExpressionAttributeNames"] = names
    return request


def _refused(client, table: str, request: dict, want: str) -> None:
    """Send the Query against `table` and compare the ValidationException's
    text with `want`, or its prefix when `want` is one. With KCE_PREFIX the
    text must also not contain "operator"."""
    request = {"TableName": table, **request}
    with pytest.raises(ClientError) as exc_info:
        client.query(**request)
    err = exc_info.value.response["Error"]
    assert err["Code"] == "ValidationException", err
    if want in (KCE_PREFIX, FILTER_PREFIX):
        assert err["Message"].startswith(want), err["Message"]
        if want == KCE_PREFIX:
            assert "operator" not in err["Message"], err["Message"]
    else:
        assert err["Message"] == want


@pytest.fixture(scope="class")
def range_table(dynamodb_client):
    """pk (S) HASH, sk (S) RANGE, holding alice/bob and alice/carol, with a
    KEYS_ONLY global secondary index on g (S)."""
    with scoped_table(
        dynamodb_client,
        attribute_definitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "g", "AttributeType": "S"},
        ],
        key_schema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": GSI,
                "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "KEYS_ONLY"},
            }
        ],
    ) as name:
        for sk in ("bob", "carol"):
            dynamodb_client.put_item(TableName=name, Item={"pk": {"S": "alice"}, "sk": {"S": sk}})
        yield name


@pytest.fixture(scope="class")
def hash_table(dynamodb_client):
    """pk (S) HASH and no sort key."""
    with scoped_table(dynamodb_client) as name:
        yield name


class TestOneConditionPerKey:
    """Every measured duplicate shape (2026-10-24, and 2026-11-11 for J1 and
    J2), the shapes that keep their own error, and the distinct conditions
    that are served."""

    ROWS = [
        ("pk = :a AND pk = :b", _q("pk = :a AND pk = :b", "a b"), ONE_PER_KEY),
        ("pk = :a AND pk = :a", _q("pk = :a AND pk = :a", "a"), ONE_PER_KEY),
        ("pk twice, sk", _q("pk = :a AND pk = :b AND sk = :c", "a b c"), ONE_PER_KEY),
        ("pk = :a AND pk > :b", _q("pk = :a AND pk > :b", "a b"), ONE_PER_KEY),
        ("pk > :b AND pk = :a", _q("pk > :b AND pk = :a", "a b"), ONE_PER_KEY),
        (":a = pk AND pk = :b", _q(":a = pk AND pk = :b", "a b"), ONE_PER_KEY),
        ("(pk = :a) AND (pk = :b)", _q("(pk = :a) AND (pk = :b)", "a b"), ONE_PER_KEY),
        ("sk = :b AND sk = :c", _q("pk = :a AND sk = :b AND sk = :c", "a b c"), ONE_PER_KEY),
        ("sk = :b AND sk = :b", _q("pk = :a AND sk = :b AND sk = :b", "a b"), ONE_PER_KEY),
        ("BETWEEN and =", _q("pk = :a AND sk BETWEEN :b AND :c AND sk = :b", "a b c"), ONE_PER_KEY),
        (
            "BETWEEN and begins_with",
            _q("pk = :a AND sk BETWEEN :b AND :c AND begins_with(sk, :b)", "a b c"),
            ONE_PER_KEY,
        ),
        (
            "begins_with and =",
            _q("pk = :a AND begins_with(sk, :b) AND sk = :c", "a b c"),
            ONE_PER_KEY,
        ),
        ("> and <", _q("pk = :a AND sk > :b AND sk < :c", "a b c"), ONE_PER_KEY),
        (
            "begins_with twice",
            _q("pk = :a AND begins_with(sk, :b) AND begins_with(sk, :c)", "a b c"),
            ONE_PER_KEY,
        ),
        (
            "pk twice, sk twice",
            _q("pk = :a AND pk = :b AND sk = :b AND sk = :c", "a b c"),
            ONE_PER_KEY,
        ),
        # No partition key condition: the duplicate comes ahead of the missing key.
        ("sk twice, no pk", _q("sk = :b AND sk = :c", "b c"), ONE_PER_KEY),
        ("#p = :a AND pk = :b", _q("#p = :a AND pk = :b", "a b", {"#p": "pk"}), ONE_PER_KEY),
        ("#p and #q", _q("#p = :a AND #q = :b", "a b", {"#p": "pk", "#q": "pk"}), ONE_PER_KEY),
        ("#s and sk", _q("pk = :a AND #s = :b AND sk = :c", "a b c", {"#s": "sk"}), ONE_PER_KEY),
        ("J1 non-key attribute twice", _q("pk = :a AND zz = :b AND zz = :c", "a b c"), ONE_PER_KEY),
        ("J2 hash-only table", _q("pk = :a AND pk = :b", "a b", TableName=HASH), ONE_PER_KEY),
        ("nested path first", _q("pk = :a AND pk.foo = :b", "a b"), NESTED),
        ("redundant parentheses first", _q("((pk = :a)) AND (pk = :b)", "a b"), REDUNDANT_PARENS),
        ("undefined #u first", _q("#u = :a AND #u = :b", "a b"), _undefined("#u")),
    ]

    @pytest.mark.parametrize("row", ROWS, ids=[r[0] for r in ROWS])
    def test_refused(self, dynamodb_client, range_table, hash_table, row):
        _, request, want = row
        if request.get("TableName") == HASH:
            request = {**request, "TableName": hash_table}
        _refused(dynamodb_client, range_table, request, want)

    @pytest.mark.parametrize(
        ("kce", "values", "count"),
        [
            ("pk = :a", "a", 2),
            ("pk = :a AND sk = :b", "a b", 1),
            ("sk = :b AND pk = :a", "a b", 1),
            ("pk = :a AND sk BETWEEN :b AND :c", "a b c", 2),
        ],
    )
    def test_distinct_key_conditions_still_served(
        self, dynamodb_client, range_table, kce, values, count
    ):
        got = dynamodb_client.query(TableName=range_table, **_q(kce, values))
        assert got["Count"] == count


class TestDuplicateCheckOrder:
    """The duplicate against undefined references, the key value type check
    and an unused value (2026-10-31)."""

    ROWS = [
        ("#u in clause 1", _q("#u = :a AND sk = :b AND sk = :c", "a b c"), _undefined("#u")),
        ("#u in clause 3", _q("pk = :a AND pk = :b AND #u = :c", "a b c"), _undefined("#u")),
        ("#u twice, >", _q("#u = :a AND #u > :b", "a b"), _undefined("#u")),
        ("no values", _q("pk = :a AND pk = :b"), _undefined(":a")),
        # A number on the S key: the duplicate comes ahead of the type check.
        ("#p and pk, :n", _q("#p = :a AND pk = :n", "a n", {"#p": "pk"}), ONE_PER_KEY),
        (
            "#s and sk, :n",
            _q("pk = :a AND #s = :b AND sk = :n", "a b n", {"#s": "sk"}),
            ONE_PER_KEY,
        ),
        ("unused :z", _q("pk = :a AND pk = :b", "a b z"), ONE_PER_KEY),
    ]

    @pytest.mark.parametrize("row", ROWS, ids=[r[0] for r in ROWS])
    def test_refused(self, dynamodb_client, range_table, row):
        _, request, want = row
        _refused(dynamodb_client, range_table, request, want)


class TestDuplicateAheadOfRequestChecks:
    """The duplicate against the FilterExpression (A), the table, index,
    ConsistentRead and Select checks (B) and parameter mixing (E8, E9),
    measured 2026-11-11T09:43Z and 11:23Z."""

    DUP = "pk = :a AND pk = :b"
    ROWS = [
        ("A1", _q(DUP, "a b", FilterExpression="#q = :a"), _undefined("#q", "FilterExpression")),
        ("A2", _q(DUP, "a b", FilterExpression="foo = :zz"), _undefined(":zz", "FilterExpression")),
        ("A3", _q(DUP, "a b n", FilterExpression="begins_with(foo, :n)"), FILTER_BEGINS_WITH_N),
        ("A4", _q(DUP, "a b", FilterExpression="name = :a"), FILTER_RESERVED_NAME),
        ("A5", _q(DUP, "a b", FilterExpression="zz = = :a"), FILTER_PREFIX),
        ("B1", _q(DUP, "a b", TableName=MISSING_TABLE), ONE_PER_KEY),
        ("B2", _q(DUP, "a b", IndexName="nope"), ONE_PER_KEY),
        ("B3", _q("g = :a AND g = :b", "a b", IndexName=GSI, ConsistentRead=True), ONE_PER_KEY),
        ("B4", _q(DUP, "a b", Select="BOGUS"), ONE_PER_KEY),
        ("B5", _q("g = :a AND g = :b", "a b", IndexName=GSI, Select="ALL_ATTRIBUTES"), ONE_PER_KEY),
        (
            "E8",
            _q(DUP, "a b", ConditionalOperator="AND"),
            _mixing("ConditionalOperator", "KeyConditionExpression"),
        ),
        (
            "E9",
            _q(DUP, "a b", KeyConditions=LEGACY_PK),
            _mixing("KeyConditions", "KeyConditionExpression"),
        ),
    ]

    @pytest.mark.parametrize("row", ROWS, ids=[r[0] for r in ROWS])
    def test_refused(self, dynamodb_client, range_table, row):
        _, request, want = row
        _refused(dynamodb_client, range_table, request, want)


class TestReferenceAndOperatorOrder:
    """The first undefined reference in the order the expression is written
    (C, 2026-11-11, and the 2026-11-10 rows on a table keyed on pk alone), and
    `<>`, `contains` and `begins_with` with and without a duplicate (D, E,
    2026-11-11)."""

    ROWS = [
        ("C1", _q("pk = :q AND #u = :c"), _undefined(":q")),
        ("C2", _q("sk = :b AND pk = :a"), _undefined(":b")),
        ("C3", _q("pk = :a AND sk = :missing", "a extra"), _undefined(":missing")),
        ("C4", _q("pk = :a AND pk = :b AND #u = :c"), _undefined(":a")),
        ("C5", _q("pk = :q AND pk = :b AND #u = :c", "b c"), _undefined(":q")),
        ("C6", _q("#u = :a AND #u = :b"), _undefined("#u")),
        ("C7", _q("pk = :b AND pk = :a"), _undefined(":b")),
        # 2026-11-10: each request also carries an unused name or value.
        ("10 Nov unused :w", _q("pk = :v", {":w": A}), _undefined(":v")),
        ("10 Nov #k, unused :w", _q("#k = :v", "v w"), _undefined("#k")),
        ("10 Nov #k, unused #j", _q("#k = :v", "v", {"#j": "pk"}), _undefined("#k")),
        ("10 Nov unused #j and :w", _q("pk = :v", {":w": A}, {"#j": "pk"}), _undefined(":v")),
        ("10 Nov #k, only :w", _q("#k = :v", {":w": A}), _undefined("#k")),
        ("D1", _q("pk = :a AND pk <> :b", "a b"), ONE_PER_KEY),
        ("D2", _q("pk = :a AND contains(pk, :b)", "a b"), ONE_PER_KEY),
        ("D3", _q("pk = :a AND begins_with(sk, :n) AND sk = :b", "a b n"), BEGINS_WITH_N),
        ("E11", _q("pk = :a AND sk <> :b AND sk = :c", "a b c"), ONE_PER_KEY),
        ("E12", _q("pk = :a AND contains(sk, :b) AND sk = :c", "a b c"), ONE_PER_KEY),
        ("E1", _q("pk = :a AND begins_with(sk, :n)", "a n"), BEGINS_WITH_N),
        ("E2", _q("pk = :a AND sk <> :b AND #u = :c", "a b c"), _undefined("#u")),
        ("E3", _q("pk = :a AND contains(sk, :b) AND #u = :c", "a b c"), _undefined("#u")),
        # The service answers a syntax error; the prefix match also checks that
        # the operator is not what is reported.
        ("E10", _q("pk = :a AND sk <> :b AND", "a b"), KCE_PREFIX),
    ]

    @pytest.mark.parametrize("row", ROWS, ids=[r[0] for r in ROWS])
    def test_refused(self, dynamodb_client, range_table, row):
        _, request, want = row
        _refused(dynamodb_client, range_table, request, want)


class TestQueryCheckOrder:
    """The order of a Query's checks without a duplicate (2026-11-11T12:42Z to
    12:44Z): parameters, then the FilterExpression, then the
    KeyConditionExpression, then Select, the table, the index and
    ConsistentRead. F3 is an undefined filter value on a partition with no
    items."""

    Q = "#q = :a"
    CO = {"ConditionalOperator": "AND"}
    UNDEFINED_Q = _undefined("#q", "FilterExpression")
    MIXING = _mixing("ConditionalOperator", "KeyConditionExpression")
    ROWS = [
        ("F1", _q("pk = :a AND sk = :missing", "a", FilterExpression=Q), UNDEFINED_Q),
        ("F1b", _q("pk = :a AND sk = :missing", "a", FilterExpression="zz = = :a"), FILTER_PREFIX),
        ("F1c", _q("pk = :a AND #s = :b", "a b", FilterExpression=Q), UNDEFINED_Q),
        (
            "F1d",
            _q("pk = :a AND #s = :b", "a b", FilterExpression="foo = :zz"),
            _undefined(":zz", "FilterExpression"),
        ),
        (
            "F1e",
            _q("pk = :a AND sk = :missing", "a", FilterExpression="foo = :a"),
            _undefined(":missing"),
        ),
        ("F1f", _q("pk = :a AND begins_with(sk, :n)", "a n", FilterExpression=Q), UNDEFINED_Q),
        ("F1g", _q("pk = :a AND", "a", FilterExpression=Q), UNDEFINED_Q),
        ("F2a", _q("pk = :a", "a", TableName=MISSING_TABLE, **CO), MIXING),
        ("F2b", _q("pk = :a", "a", IndexName="nope", **CO), MIXING),
        ("F2c", _q("pk = :a", "a", Select="BOGUS", **CO), MIXING),
        ("F2d", _q("g = :a", "a", IndexName=GSI, ConsistentRead=True, **CO), MIXING),
        ("H1", _q("pk = :a", "a", TableName=MISSING_TABLE, FilterExpression=Q), UNDEFINED_Q),
        ("H2", _q("pk = :a", "a", IndexName="nope", FilterExpression=Q), UNDEFINED_Q),
        ("H3", _q("pk = :a", "a", Select="BOGUS", FilterExpression=Q), UNDEFINED_Q),
        (
            "H4",
            _q("g = :a", "a", IndexName=GSI, ConsistentRead=True, FilterExpression=Q),
            UNDEFINED_Q,
        ),
        (
            "H5",
            _q("pk = :a", "a", FilterExpression=Q, **CO),
            _mixing("ConditionalOperator", "FilterExpression", "KeyConditionExpression"),
        ),
        (
            "F3",
            _q("pk = :a", {":a": {"S": "p484-empty"}}, FilterExpression="foo = :zz"),
            _undefined(":zz", "FilterExpression"),
        ),
    ]

    @pytest.mark.parametrize("row", ROWS, ids=[r[0] for r in ROWS])
    def test_refused(self, dynamodb_client, range_table, row):
        _, request, want = row
        _refused(dynamodb_client, range_table, request, want)


class TestLimitAndParameterChecksFirst:
    """Limit 0 ahead of every other fault (L), begins_with on the partition
    key (P1) and the parameter checks ahead of a missing table (M), measured
    2026-11-11T13:26Z. The SDK refuses Limit 0 before it sends the request, so
    the L rows use the client with its checks off."""

    ROWS = [
        ("L1", _q("pk = :a", "a", Limit=0), LIMIT_ZERO),
        ("L2", _q("pk = :a AND", "a", Limit=0), LIMIT_ZERO),
        ("L3", _q("pk = :a AND pk = :b", "a b", Limit=0), LIMIT_ZERO),
        ("L4", _q("pk = :a", "a", FilterExpression="#q = :a", Limit=0), LIMIT_ZERO),
        ("L5", _q("pk = :a", "a", ConditionalOperator="AND", Limit=0), LIMIT_ZERO),
        ("L6", _q("pk = :a", "a", TableName=MISSING_TABLE, Limit=0), LIMIT_ZERO),
        ("L7", _q("pk = :a", "a", Select="BOGUS", Limit=0), LIMIT_ZERO),
        ("P1", _q("begins_with(pk, :n)", "n"), BEGINS_WITH_N),
        (
            "M1",
            _q(None, "a", TableName=MISSING_TABLE, KeyConditions=LEGACY_PK),
            "ExpressionAttributeValues can only be specified when using expressions",
        ),
        (
            "M2",
            _q("", TableName=MISSING_TABLE),
            "Invalid KeyConditionExpression: The expression can not be empty;",
        ),
        (
            "M3",
            _q(None, TableName=MISSING_TABLE, KeyConditions=LEGACY_PK, ConditionalOperator="AND"),
            "ConditionalOperator cannot be used without Filter or Expected",
        ),
    ]

    @pytest.mark.parametrize("row", ROWS, ids=[r[0] for r in ROWS])
    def test_refused(self, dynamodb_client, dynamodb_client_no_validation, range_table, row):
        _, request, want = row
        client = dynamodb_client_no_validation if "Limit" in request else dynamodb_client
        _refused(client, range_table, request, want)
