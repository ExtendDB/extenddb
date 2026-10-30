# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Query whose key condition has no equality clause on the HASH attribute.

Every row below is one measured on the service in us-east-1: the issue table
rows on 2026-10-21, the rest on 2026-10-23 (recorded in the issue's fold
files, rows ``427-*`` and the two review row files). The table has HASH ``pk``
and RANGE ``sk``, an LSI ``lsi`` on RANGE ``ls``, an index ``single-idx`` with
HASH ``g`` and RANGE ``s``, an index ``multi-idx`` with HASH ``h`` and RANGE
``a``, ``b``, ``c``, ``d`` and an index ``two-hash`` with HASH ``h1``, ``h2``
and RANGE ``r``. A second table has only HASH ``pk`` and a third has an N
partition key. All attributes are strings unless stated.

The service answers with one of these, chosen by the key schema of the table
or index being queried and by the attributes the clauses name:

- a non-equality clause names the single HASH attribute: ``Query key condition
  not supported``
- no clause names the single HASH attribute: ``Query condition missed key
  schema element: <HASH>``
- the schema has several key attributes: ``HASH key attributes <names> must
  have equality conditions specified in the query``
- two conditions on one attribute: ``Invalid KeyConditionExpression:
  KeyConditionExpressions must only contain one condition per key``
- a ``#name`` or ``:value`` the request does not define, ahead of all of the
  above: ``Invalid KeyConditionExpression: An expression attribute ...``

An unused value, an invalid ``FilterExpression`` or ``ProjectionExpression``
and an invalid ``Select`` are reported ahead of the key schema wording. An
``ExclusiveStartKey`` and a key value size are reported after it.

Row 427-8c (``pk > :n`` with an N value on an S key, service: ``One or more
parameter values were invalid: Condition parameter type does not match schema
type``) and the ``begins_with(pk, :n)`` row on an N-keyed table (service:
``Invalid KeyConditionExpression: Incorrect operand type for operator or
function; operator or function: begins_with, operand type: N``) are not
asserted here. Both need the key value type check that arrives with the
multi-attribute range query change; a test for them belongs with that merge.
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from helpers import unique_name, wait_for_active, wait_for_deleted

NOT_SUPPORTED = "Query key condition not supported"
MISSED_PK = "Query condition missed key schema element: pk"
MISSED_G = "Query condition missed key schema element: g"
HASH_H = "HASH key attributes h must have equality conditions specified in the query"
HASH_H1_H2 = "HASH key attributes h1, h2 must have equality conditions specified in the query"
ONE_PER_KEY = (
    "Invalid KeyConditionExpression: KeyConditionExpressions must only contain one "
    "condition per key"
)
NO_VALUE = (
    "Invalid KeyConditionExpression: An expression attribute value used in expression is "
    "not defined; attribute value: :x"
)


def _attr(name: str, kind: str = "S") -> dict:
    return {"AttributeName": name, "AttributeType": kind}


def _key(name: str, key_type: str) -> dict:
    return {"AttributeName": name, "KeyType": key_type}


def _index(name: str, schema: list[dict]) -> dict:
    return {"IndexName": name, "KeySchema": schema, "Projection": {"ProjectionType": "ALL"}}


def _drop(client, name: str) -> None:
    try:
        client.delete_table(TableName=name)
    except client.exceptions.ResourceNotFoundException:
        return
    wait_for_deleted(client, name)


@pytest.fixture(scope="module")
def table(dynamodb_client):
    """HASH ``pk`` and RANGE ``sk``, the LSI and the three indexes above."""
    name = unique_name("qlne")
    names = ("pk", "sk", "ls", "g", "s", "h", "a", "b", "c", "d", "h1", "h2", "r")
    dynamodb_client.create_table(
        TableName=name,
        AttributeDefinitions=[_attr(n) for n in names],
        KeySchema=[_key("pk", "HASH"), _key("sk", "RANGE")],
        BillingMode="PAY_PER_REQUEST",
        LocalSecondaryIndexes=[_index("lsi", [_key("pk", "HASH"), _key("ls", "RANGE")])],
        GlobalSecondaryIndexes=[
            _index("single-idx", [_key("g", "HASH"), _key("s", "RANGE")]),
            _index(
                "multi-idx",
                [_key("h", "HASH")] + [_key(n, "RANGE") for n in ("a", "b", "c", "d")],
            ),
            _index(
                "two-hash",
                [_key("h1", "HASH"), _key("h2", "HASH"), _key("r", "RANGE")],
            ),
        ],
    )
    try:
        wait_for_active(dynamodb_client, name)
        yield name
    finally:
        _drop(dynamodb_client, name)


@pytest.fixture(scope="module")
def hash_only_table(dynamodb_client):
    """A table with only HASH ``pk`` (S)."""
    name = unique_name("qlne-hash")
    dynamodb_client.create_table(
        TableName=name,
        AttributeDefinitions=[_attr("pk")],
        KeySchema=[_key("pk", "HASH")],
        BillingMode="PAY_PER_REQUEST",
    )
    try:
        wait_for_active(dynamodb_client, name)
        yield name
    finally:
        _drop(dynamodb_client, name)


@pytest.fixture(scope="module")
def number_table(dynamodb_client):
    """A table with only HASH ``pk`` (N)."""
    name = unique_name("qlne-num")
    dynamodb_client.create_table(
        TableName=name,
        AttributeDefinitions=[_attr("pk", "N")],
        KeySchema=[_key("pk", "HASH")],
        BillingMode="PAY_PER_REQUEST",
    )
    try:
        wait_for_active(dynamodb_client, name)
        yield name
    finally:
        _drop(dynamodb_client, name)


def _refused_request(client, message: str, **request) -> None:
    """Run the Query and assert the refusal's code, status and exact text."""
    with pytest.raises(ClientError) as exc_info:
        client.query(**request)
    err = exc_info.value.response
    assert err["Error"]["Code"] == "ValidationException", err["Error"]["Message"]
    assert err["ResponseMetadata"]["HTTPStatusCode"] == 400
    assert err["Error"]["Message"] == message


def _refused_prefix(client, prefix: str, **request) -> None:
    """Like ``_refused_request`` for a refusal whose text ExtendDB words differently."""
    with pytest.raises(ClientError) as exc_info:
        client.query(**request)
    err = exc_info.value.response
    assert err["Error"]["Code"] == "ValidationException", err["Error"]["Message"]
    assert err["Error"]["Message"].startswith(prefix), err["Error"]["Message"]


def _refused_suffix(client, suffix: str, **request) -> None:
    """Like ``_refused_request`` for a refusal whose leading text ExtendDB words differently."""
    with pytest.raises(ClientError) as exc_info:
        client.query(**request)
    err = exc_info.value.response
    assert err["Error"]["Code"] == "ValidationException", err["Error"]["Message"]
    assert err["Error"]["Message"].endswith(suffix), err["Error"]["Message"]


def _values(expression: str) -> dict:
    """``:x`` and, when the expression uses it, ``:y`` as strings."""
    values = {":x": {"S": "a"}}
    if ":y" in expression:
        values[":y"] = {"S": "b"}
    return values


def _refused(
    client,
    table: str,
    message: str,
    expression: str,
    index: str | None = None,
    **extra,
) -> None:
    """``_refused_request`` with ``:x`` (and ``:y``) defined as strings."""
    request: dict = {
        "TableName": table,
        "KeyConditionExpression": expression,
        "ExpressionAttributeValues": _values(expression),
    }
    if index is not None:
        request["IndexName"] = index
    request.update(extra)
    _refused_request(client, message, **request)


class TestPartitionKeyClause:
    """Rows 1 to 3 of the issue: the clause names the single HASH attribute."""

    def test_table_partition_key_greater_than(self, dynamodb_client, table):
        _refused(dynamodb_client, table, NOT_SUPPORTED, "pk > :x")

    def test_table_partition_key_begins_with(self, dynamodb_client, table):
        _refused(dynamodb_client, table, NOT_SUPPORTED, "begins_with(pk, :x)")

    def test_index_partition_key_greater_than(self, dynamodb_client, table):
        _refused(dynamodb_client, table, NOT_SUPPORTED, "g > :x", index="single-idx")


class TestSortKeyClause:
    """Rows 4 and 5 of the issue: the clause names a sort key."""

    def test_table_sort_key_greater_than(self, dynamodb_client, table):
        _refused(dynamodb_client, table, MISSED_PK, "sk > :x")

    def test_index_sort_key_greater_than(self, dynamodb_client, table):
        _refused(dynamodb_client, table, MISSED_G, "s > :x", index="single-idx")


class TestMultiAttributeIndex:
    """Row 6 of the issue and the shapes measured around it."""

    def test_hash_attribute_greater_than(self, dynamodb_client, table):
        _refused(dynamodb_client, table, HASH_H, "h > :x", index="multi-idx")

    @pytest.mark.parametrize(
        "expression",
        ["a > :x", "d > :x", "z > :x", "begins_with(a, :x)", "a BETWEEN :x AND :y"],
    )
    def test_other_attributes_name_the_hash_attribute(self, dynamodb_client, table, expression):
        _refused(dynamodb_client, table, HASH_H, expression, index="multi-idx")

    def test_a_number_value_does_not_change_the_wording(self, dynamodb_client, table):
        # `h` is a string: the service answers the HASH wording, not a type error.
        _refused_request(
            dynamodb_client,
            HASH_H,
            TableName=table,
            IndexName="multi-idx",
            KeyConditionExpression="h > :n",
            ExpressionAttributeValues={":n": {"N": "1"}},
        )

    @pytest.mark.parametrize("expression", ["h1 > :x", "h2 > :x", "r > :x", "z > :x"])
    def test_two_hash_index_names_both_hash_attributes(self, dynamodb_client, table, expression):
        _refused(dynamodb_client, table, HASH_H1_H2, expression, index="two-hash")

    def test_two_hash_index_with_a_name_reference(self, dynamodb_client, table):
        _refused(
            dynamodb_client,
            table,
            HASH_H1_H2,
            "#a > :x",
            index="two-hash",
            ExpressionAttributeNames={"#a": "h1"},
        )


class TestMeasuredShapes:
    """Fold file rows 427-1 to 427-9: shapes the issue table did not cover."""

    def test_table_partition_key_between(self, dynamodb_client, table):
        _refused(dynamodb_client, table, NOT_SUPPORTED, "pk BETWEEN :x AND :y")

    def test_index_partition_key_between(self, dynamodb_client, table):
        _refused(dynamodb_client, table, NOT_SUPPORTED, "g BETWEEN :x AND :y", index="single-idx")

    def test_hash_only_table_begins_with(self, dynamodb_client, hash_only_table):
        _refused(dynamodb_client, hash_only_table, NOT_SUPPORTED, "begins_with(pk, :x)")

    def test_number_keyed_table_partition_key(self, dynamodb_client, number_table):
        # `pk > :n` with an N value on an N key (row 6b of service-427-review.jsonl).
        _refused_request(
            dynamodb_client,
            NOT_SUPPORTED,
            TableName=number_table,
            KeyConditionExpression="pk > :n",
            ExpressionAttributeValues={":n": {"N": "1"}},
        )

    def test_lsi_partition_key_greater_than(self, dynamodb_client, table):
        _refused(dynamodb_client, table, NOT_SUPPORTED, "pk > :x", index="lsi")

    def test_lsi_lone_range_clause(self, dynamodb_client, table):
        _refused(dynamodb_client, table, MISSED_PK, "ls > :x", index="lsi")

    def test_non_key_attribute(self, dynamodb_client, table):
        _refused(dynamodb_client, table, MISSED_PK, "z > :x")

    def test_legacy_key_conditions_greater_than_on_partition_key(self, dynamodb_client, table):
        _refused_request(
            dynamodb_client,
            NOT_SUPPORTED,
            TableName=table,
            KeyConditions={"pk": {"ComparisonOperator": "GT", "AttributeValueList": [{"S": "a"}]}},
        )

    def test_legacy_key_conditions_sort_key_only(self, dynamodb_client, table):
        _refused_request(
            dynamodb_client,
            MISSED_PK,
            TableName=table,
            KeyConditions={"sk": {"ComparisonOperator": "GT", "AttributeValueList": [{"S": "a"}]}},
        )


class TestNameReferences:
    """Rows 2a to 2d of service-427-review.jsonl: a defined ``#name`` is resolved."""

    @pytest.mark.parametrize(
        "expression,reference,attribute,message",
        [
            ("#p > :x", "#p", "pk", NOT_SUPPORTED),
            ("#s > :x", "#s", "sk", MISSED_PK),
            ("#z > :x", "#z", "zz", MISSED_PK),
            ("begins_with(#s, :x)", "#s", "sk", MISSED_PK),
        ],
    )
    def test_defined_name_is_resolved(
        self, dynamodb_client, table, expression, reference, attribute, message
    ):
        _refused(
            dynamodb_client,
            table,
            message,
            expression,
            ExpressionAttributeNames={reference: attribute},
        )


class TestUndefinedReferences:
    """Undefined references win over the key schema wording, with the prefix."""

    def test_undefined_name(self, dynamodb_client, table):
        _refused(
            dynamodb_client,
            table,
            "Invalid KeyConditionExpression: An expression attribute name used in the "
            "document path is not defined; attribute name: #nope",
            "#nope > :x",
        )

    def test_undefined_name_in_the_first_of_two_clauses(self, dynamodb_client, table):
        _refused(
            dynamodb_client,
            table,
            "Invalid KeyConditionExpression: An expression attribute name used in the "
            "document path is not defined; attribute name: #n",
            "#n > :x AND sk > :y",
        )

    @pytest.mark.parametrize("expression", ["pk > :x", "sk > :x", "pk = :x"])
    def test_undefined_value(self, dynamodb_client, table, expression):
        _refused_request(
            dynamodb_client, NO_VALUE, TableName=table, KeyConditionExpression=expression
        )

    def test_undefined_value_in_the_first_of_two_clauses(self, dynamodb_client, table):
        _refused_request(
            dynamodb_client,
            NO_VALUE,
            TableName=table,
            KeyConditionExpression="pk > :x AND sk > :y",
            ExpressionAttributeValues={":y": {"S": "b"}},
        )


class TestTwoClauses:
    """Rows of service-427-review-2.jsonl: one or two clauses, no HASH equality."""

    @pytest.mark.parametrize(
        "expression",
        [
            "pk > :x AND sk > :y",
            "sk > :x AND pk > :y",
            "pk > :x AND zz > :y",
            "zz > :x AND pk > :y",
            "pk >= :x AND sk = :y",
            "sk = :x AND pk > :y",
            "pk > :x AND sk = :y",
            "begins_with(pk, :x) AND sk = :y",
            "pk BETWEEN :x AND :y AND sk = :x",
            "zz = :x AND pk > :y",
        ],
    )
    def test_a_non_equality_names_the_partition_key(self, dynamodb_client, table, expression):
        _refused(dynamodb_client, table, NOT_SUPPORTED, expression)

    @pytest.mark.parametrize("expression", ["sk > :x AND zz > :y", "zz > :x AND sk > :y"])
    def test_no_clause_names_the_partition_key(self, dynamodb_client, table, expression):
        _refused(dynamodb_client, table, MISSED_PK, expression)

    @pytest.mark.parametrize(
        "expression",
        ["zz = :x AND sk > :y", "zz = :x AND yy = :y", "sk = :x AND zz = :y"],
    )
    def test_equalities_on_other_attributes_miss_the_partition_key(
        self, dynamodb_client, table, expression
    ):
        _refused(dynamodb_client, table, MISSED_PK, expression)

    @pytest.mark.parametrize("expression", ["g > :x AND s > :y", "g > :x AND s = :y"])
    def test_index_clause_names_the_index_partition_key(self, dynamodb_client, table, expression):
        _refused(dynamodb_client, table, NOT_SUPPORTED, expression, index="single-idx")

    @pytest.mark.parametrize(
        "expression", ["s > :x AND zz > :y", "pk > :x AND s > :y", "pk = :x AND s > :y"]
    )
    def test_index_clauses_that_miss_the_index_partition_key(
        self, dynamodb_client, table, expression
    ):
        _refused(dynamodb_client, table, MISSED_G, expression, index="single-idx")

    @pytest.mark.parametrize("expression", ["pk > :x AND pk < :y", "sk > :x AND sk < :y"])
    def test_two_conditions_on_one_key(self, dynamodb_client, table, expression):
        _refused(dynamodb_client, table, ONE_PER_KEY, expression)


class TestOrderAmongOtherRefusals:
    """Rows 5a, 5b, 8, 9, 10, 11 and P4 of service-427-review.jsonl.

    Reported ahead of the key schema wording: an unused value, an invalid
    ``FilterExpression`` or ``ProjectionExpression`` and an invalid ``Select``.
    Reported after it: an ``ExclusiveStartKey`` and a key value size.
    """

    def test_unused_value_is_reported_first(self, dynamodb_client, table):
        _refused_request(
            dynamodb_client,
            "Value provided in ExpressionAttributeValues unused in expressions: keys: {:y}",
            TableName=table,
            KeyConditionExpression="pk > :x",
            ExpressionAttributeValues={":x": {"S": "a"}, ":y": {"S": "b"}},
        )

    def test_invalid_filter_expression_is_reported_first(self, dynamodb_client, table):
        # The service's text is 'Invalid FilterExpression: Syntax error; token:
        # "<EOF>", near: "("'. ExtendDB words it differently, so only the label
        # is asserted: the filter error comes before the key condition wording.
        _refused_prefix(
            dynamodb_client,
            "Invalid FilterExpression: ",
            TableName=table,
            KeyConditionExpression="pk > :x",
            FilterExpression="nope(",
            ExpressionAttributeValues={":x": {"S": "a"}},
        )

    def test_invalid_projection_expression_is_reported_first(self, dynamodb_client, table):
        # The service's text is 'Invalid ProjectionExpression: Syntax error; token:
        # ",", near: ",,b"'.
        _refused_prefix(
            dynamodb_client,
            "Invalid ProjectionExpression: ",
            TableName=table,
            KeyConditionExpression="pk > :x",
            ProjectionExpression="a,,b",
            ExpressionAttributeValues={":x": {"S": "a"}},
        )

    def test_count_with_a_projection_is_reported_first(self, dynamodb_client, table):
        _refused_request(
            dynamodb_client,
            "1 validation error detected: Cannot specify the ProjectionExpression when "
            "choosing to get only the Count",
            TableName=table,
            KeyConditionExpression="pk > :x",
            Select="COUNT",
            ProjectionExpression="a",
            ExpressionAttributeValues={":x": {"S": "a"}},
        )

    def test_all_projected_attributes_without_an_index_is_reported_first(
        self, dynamodb_client, table
    ):
        # The service's text starts with "1 validation error detected: ";
        # ExtendDB's does not (the Select check is shared with Scan).
        _refused_suffix(
            dynamodb_client,
            "ALL_PROJECTED_ATTRIBUTES can be used only when Querying using an IndexName",
            TableName=table,
            KeyConditionExpression="pk > :x",
            Select="ALL_PROJECTED_ATTRIBUTES",
            ExpressionAttributeValues={":x": {"S": "a"}},
        )

    @pytest.mark.parametrize(
        "start_key",
        [
            {"pk": {"S": "other"}, "sk": {"S": "a"}},
            {"pk": {"S": "other"}},
        ],
        ids=["other-partition", "missing-sort-key"],
    )
    def test_exclusive_start_key_is_checked_after_the_wording(
        self, dynamodb_client, table, start_key
    ):
        _refused(dynamodb_client, table, NOT_SUPPORTED, "pk > :x", ExclusiveStartKey=start_key)

    def test_partition_key_size_is_checked_after_the_wording(self, dynamodb_client, table):
        _refused_request(
            dynamodb_client,
            NOT_SUPPORTED,
            TableName=table,
            KeyConditionExpression="pk > :x",
            ExpressionAttributeValues={":x": {"S": "x" * 2049}},
        )

    def test_sort_key_size_is_checked_after_the_wording(self, dynamodb_client, table):
        _refused_request(
            dynamodb_client,
            MISSED_PK,
            TableName=table,
            KeyConditionExpression="sk > :x",
            ExpressionAttributeValues={":x": {"S": "x" * 1025}},
        )
