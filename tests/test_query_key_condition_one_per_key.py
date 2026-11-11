# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""A KeyConditionExpression carries one condition per key attribute.

Amazon DynamoDB rejects a key condition that names the same key attribute twice,
on the partition key and on the sort key alike, whatever the operators are:

    ValidationException: Invalid KeyConditionExpression: KeyConditionExpressions
    must only contain one condition per key

Measured 2026-10-24T00:27:56Z and 00:30:39Z in us-east-1 on a table with
pk (S) HASH and sk (S) RANGE. Issue #430.

The order against undefined references, the key value type check and an
unused value was measured 2026-10-31T17:37Z in us-east-1 on a table with the
same key schema, two samples each: an undefined #name anywhere in the
expression and an undefined :value are reported ahead of the duplicate, and the
duplicate is reported ahead of the type check and the unused-value check.

The order against the FilterExpression and the table, index, ConsistentRead
and Select checks was measured 2026-11-11T09:43Z in us-east-1 on a table with
the same key schema and a KEYS_ONLY global secondary index on g (S), two
sends each: a FilterExpression fault is reported ahead of the duplicate, and
the duplicate ahead of a missing table, an unknown index, ConsistentRead on
the index, an invalid Select and Select ALL_ATTRIBUTES on the index. In the
same run the first undefined reference in the order the expression is written
was reported, a #name or a :value, with or without a duplicate; `<>` and
`contains` beside a duplicate answered the duplicate; and a number in
begins_with answered its operand type error ahead of it. On 2026-11-10 an
undefined reference was reported ahead of an unused name or value, on a table
keyed on pk (S) alone.

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
REDUNDANT_PARENS = (
    "Invalid KeyConditionExpression: The expression has redundant parentheses;"
)
UNDEFINED_NAME_EXACT = (
    "Invalid KeyConditionExpression: An expression attribute name used in the "
    "document path is not defined; attribute name: #u"
)
UNDEFINED_VALUE_A = (
    "Invalid KeyConditionExpression: An expression attribute value used in "
    "expression is not defined; attribute value: :a"
)

A = {"S": "alice"}
B = {"S": "bob"}
C = {"S": "carol"}
N1 = {"N": "1"}


KEYS_ONLY_INDEX = "g-keys"


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
                "IndexName": KEYS_ONLY_INDEX,
                "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "KEYS_ONLY"},
            }
        ],
    ) as name:
        for sk in ("bob", "carol"):
            dynamodb_client.put_item(
                TableName=name, Item={"pk": {"S": "alice"}, "sk": {"S": sk}}
            )
        yield name


def _error(func) -> tuple[str, str]:
    with pytest.raises(ClientError) as exc_info:
        func()
    err = exc_info.value.response["Error"]
    return err["Code"], err["Message"]


class TestOneConditionPerKey:
    """Every measured duplicate shape, and the shapes that keep their own error."""

    @pytest.mark.parametrize(
        ("expr", "values"),
        [
            # The shape in the issue: a second equality on the partition key is
            # served on extenddb before the fix, reading :b as a sort key value.
            ("pk = :a AND pk = :b", {":a": A, ":b": B}),
            ("pk = :a AND pk = :a", {":a": A}),
            ("pk = :a AND pk = :b AND sk = :c", {":a": A, ":b": B, ":c": C}),
            ("pk = :a AND pk > :b", {":a": A, ":b": B}),
            ("pk > :b AND pk = :a", {":a": A, ":b": B}),
            (":a = pk AND pk = :b", {":a": A, ":b": B}),
            ("(pk = :a) AND (pk = :b)", {":a": A, ":b": B}),
        ],
    )
    def test_duplicate_partition_key_condition(
        self, dynamodb_client, range_table, expr, values
    ):
        code, msg = _error(
            lambda: dynamodb_client.query(
                TableName=range_table,
                KeyConditionExpression=expr,
                ExpressionAttributeValues=values,
            )
        )
        assert code == "ValidationException"
        assert msg == ONE_PER_KEY

    @pytest.mark.parametrize(
        ("expr", "values"),
        [
            ("pk = :a AND sk = :b AND sk = :c", {":a": A, ":b": B, ":c": C}),
            ("pk = :a AND sk = :b AND sk = :b", {":a": A, ":b": B}),
            (
                "pk = :a AND sk BETWEEN :b AND :c AND sk = :b",
                {":a": A, ":b": B, ":c": C},
            ),
            (
                "pk = :a AND sk BETWEEN :b AND :c AND begins_with(sk, :b)",
                {":a": A, ":b": B, ":c": C},
            ),
            ("pk = :a AND begins_with(sk, :b) AND sk = :c", {":a": A, ":b": B, ":c": C}),
            ("pk = :a AND sk > :b AND sk < :c", {":a": A, ":b": B, ":c": C}),
            (
                "pk = :a AND begins_with(sk, :b) AND begins_with(sk, :c)",
                {":a": A, ":b": B, ":c": C},
            ),
            ("pk = :a AND pk = :b AND sk = :b AND sk = :c", {":a": A, ":b": B, ":c": C}),
            # No partition key condition at all: the duplicate is reported
            # ahead of the missing key schema element.
            ("sk = :b AND sk = :c", {":b": B, ":c": C}),
        ],
    )
    def test_duplicate_sort_key_condition(
        self, dynamodb_client, range_table, expr, values
    ):
        code, msg = _error(
            lambda: dynamodb_client.query(
                TableName=range_table,
                KeyConditionExpression=expr,
                ExpressionAttributeValues=values,
            )
        )
        assert code == "ValidationException"
        assert msg == ONE_PER_KEY

    @pytest.mark.parametrize(
        ("expr", "values", "names"),
        [
            ("#p = :a AND pk = :b", {":a": A, ":b": B}, {"#p": "pk"}),
            ("#p = :a AND #q = :b", {":a": A, ":b": B}, {"#p": "pk", "#q": "pk"}),
            (
                "pk = :a AND #s = :b AND sk = :c",
                {":a": A, ":b": B, ":c": C},
                {"#s": "sk"},
            ),
        ],
    )
    def test_duplicate_through_name_reference(
        self, dynamodb_client, range_table, expr, values, names
    ):
        """An #name alias resolving to the same attribute is a duplicate."""
        code, msg = _error(
            lambda: dynamodb_client.query(
                TableName=range_table,
                KeyConditionExpression=expr,
                ExpressionAttributeValues=values,
                ExpressionAttributeNames=names,
            )
        )
        assert code == "ValidationException"
        assert msg == ONE_PER_KEY

    def test_distinct_key_conditions_still_served(self, dynamodb_client, range_table):
        """One condition per key attribute, in either order, returns rows."""
        pk_only = dynamodb_client.query(
            TableName=range_table,
            KeyConditionExpression="pk = :a",
            ExpressionAttributeValues={":a": A},
        )
        assert pk_only["Count"] == 2

        pk_and_sk = dynamodb_client.query(
            TableName=range_table,
            KeyConditionExpression="pk = :a AND sk = :b",
            ExpressionAttributeValues={":a": A, ":b": B},
        )
        assert pk_and_sk["Count"] == 1

        sk_first = dynamodb_client.query(
            TableName=range_table,
            KeyConditionExpression="sk = :b AND pk = :a",
            ExpressionAttributeValues={":a": A, ":b": B},
        )
        assert sk_first["Count"] == 1

        sk_range = dynamodb_client.query(
            TableName=range_table,
            KeyConditionExpression="pk = :a AND sk BETWEEN :b AND :c",
            ExpressionAttributeValues={":a": A, ":b": B, ":c": C},
        )
        assert sk_range["Count"] == 2

    def test_nested_path_reported_ahead_of_duplicate(
        self, dynamodb_client, range_table
    ):
        code, msg = _error(
            lambda: dynamodb_client.query(
                TableName=range_table,
                KeyConditionExpression="pk = :a AND pk.foo = :b",
                ExpressionAttributeValues={":a": A, ":b": B},
            )
        )
        assert code == "ValidationException"
        assert msg == NESTED

    def test_redundant_parens_reported_ahead_of_duplicate(
        self, dynamodb_client, range_table
    ):
        code, msg = _error(
            lambda: dynamodb_client.query(
                TableName=range_table,
                KeyConditionExpression="((pk = :a)) AND (pk = :b)",
                ExpressionAttributeValues={":a": A, ":b": B},
            )
        )
        assert code == "ValidationException"
        assert msg == REDUNDANT_PARENS

    def test_undefined_name_reported_ahead_of_duplicate(
        self, dynamodb_client, range_table
    ):
        """Two conditions on an undefined #name: the name is reported."""
        code, msg = _error(
            lambda: dynamodb_client.query(
                TableName=range_table,
                KeyConditionExpression="#u = :a AND #u = :b",
                ExpressionAttributeValues={":a": A, ":b": B},
            )
        )
        assert code == "ValidationException"
        assert msg == UNDEFINED_NAME_EXACT


class TestDuplicateCheckOrder:
    """Where the duplicate is reported against the other checks (2026-10-31)."""

    @pytest.mark.parametrize(
        ("expr", "values"),
        [
            ("#u = :a AND sk = :b AND sk = :c", {":a": A, ":b": B, ":c": C}),
            ("pk = :a AND pk = :b AND #u = :c", {":a": A, ":b": B, ":c": C}),
            ("#u = :a AND #u > :b", {":a": A, ":b": B}),
        ],
    )
    def test_undefined_name_in_any_clause_reported_ahead_of_duplicate(
        self, dynamodb_client, range_table, expr, values
    ):
        """No ExpressionAttributeNames: the undefined #u is reported."""
        code, msg = _error(
            lambda: dynamodb_client.query(
                TableName=range_table,
                KeyConditionExpression=expr,
                ExpressionAttributeValues=values,
            )
        )
        assert code == "ValidationException"
        assert msg == UNDEFINED_NAME_EXACT

    def test_undefined_value_reported_ahead_of_duplicate(
        self, dynamodb_client, range_table
    ):
        """No ExpressionAttributeValues: the first undefined value is reported."""
        code, msg = _error(
            lambda: dynamodb_client.query(
                TableName=range_table,
                KeyConditionExpression="pk = :a AND pk = :b",
            )
        )
        assert code == "ValidationException"
        assert msg == UNDEFINED_VALUE_A

    @pytest.mark.parametrize(
        ("expr", "values", "names"),
        [
            # A number on the S key: the duplicate is reported ahead of the
            # key value type check, in either spelling.
            ("#p = :a AND pk = :n", {":a": A, ":n": N1}, {"#p": "pk"}),
            (
                "pk = :a AND #s = :b AND sk = :n",
                {":a": A, ":b": B, ":n": N1},
                {"#s": "sk"},
            ),
            # An unused value: the duplicate is reported ahead of the
            # unused-value check.
            ("pk = :a AND pk = :b", {":a": A, ":b": B, ":z": {"S": "zed"}}, None),
        ],
    )
    def test_duplicate_reported_ahead_of_value_checks(
        self, dynamodb_client, range_table, expr, values, names
    ):
        kwargs = {
            "TableName": range_table,
            "KeyConditionExpression": expr,
            "ExpressionAttributeValues": values,
        }
        if names is not None:
            kwargs["ExpressionAttributeNames"] = names
        code, msg = _error(lambda: dynamodb_client.query(**kwargs))
        assert code == "ValidationException"
        assert msg == ONE_PER_KEY


class TestDuplicateAheadOfRequestChecks:
    """Where the duplicate is reported against the FilterExpression and the
    table, index, ConsistentRead and Select checks (2026-11-11)."""

    @pytest.mark.parametrize(
        ("filter_expr", "values", "want"),
        [
            (
                "#q = :a",
                {":a": A, ":b": B},
                "Invalid FilterExpression: An expression attribute name used in "
                "the document path is not defined; attribute name: #q",
            ),
            (
                "foo = :zz",
                {":a": A, ":b": B},
                "Invalid FilterExpression: An expression attribute value used in "
                "expression is not defined; attribute value: :zz",
            ),
            (
                "begins_with(foo, :n)",
                {":a": A, ":b": B, ":n": N1},
                "Invalid FilterExpression: Incorrect operand type for operator or "
                "function; operator or function: begins_with, operand type: N",
            ),
            (
                "name = :a",
                {":a": A, ":b": B},
                "Invalid FilterExpression: Attribute name is a reserved keyword; "
                "reserved keyword: name",
            ),
        ],
        ids=["undefined-name", "undefined-value", "begins-with-number", "reserved-word"],
    )
    def test_filter_fault_reported_ahead_of_duplicate(
        self, dynamodb_client, range_table, filter_expr, values, want
    ):
        code, msg = _error(
            lambda: dynamodb_client.query(
                TableName=range_table,
                KeyConditionExpression="pk = :a AND pk = :b",
                FilterExpression=filter_expr,
                ExpressionAttributeValues=values,
            )
        )
        assert code == "ValidationException"
        assert msg == want

    def test_filter_syntax_error_reported_ahead_of_duplicate(
        self, dynamodb_client, range_table
    ):
        """The service answers `Invalid FilterExpression: Syntax error; token:
        "=", near: "= = :a"`; extenddb words its syntax errors differently, so
        only the expression the error names is compared."""
        code, msg = _error(
            lambda: dynamodb_client.query(
                TableName=range_table,
                KeyConditionExpression="pk = :a AND pk = :b",
                FilterExpression="zz = = :a",
                ExpressionAttributeValues={":a": A, ":b": B},
            )
        )
        assert code == "ValidationException"
        assert msg.startswith("Invalid FilterExpression: "), msg

    @pytest.mark.parametrize(
        ("kwargs", "on_index"),
        [
            ({"TableName": "extenddb-test-484-no-such-table"}, False),
            ({"IndexName": "nope"}, False),
            ({"IndexName": KEYS_ONLY_INDEX, "ConsistentRead": True}, True),
            ({"Select": "BOGUS"}, False),
            ({"IndexName": KEYS_ONLY_INDEX, "Select": "ALL_ATTRIBUTES"}, True),
        ],
        ids=[
            "missing-table",
            "unknown-index",
            "consistent-read-on-gsi",
            "invalid-select",
            "all-attributes-on-keys-only-gsi",
        ],
    )
    def test_duplicate_reported_ahead_of_table_index_and_select_checks(
        self, dynamodb_client, range_table, kwargs, on_index
    ):
        key = "g" if on_index else "pk"
        request = {
            "TableName": range_table,
            "KeyConditionExpression": f"{key} = :a AND {key} = :b",
            "ExpressionAttributeValues": {":a": A, ":b": B},
            **kwargs,
        }
        code, msg = _error(lambda: dynamodb_client.query(**request))
        assert code == "ValidationException"
        assert msg == ONE_PER_KEY

    @pytest.mark.parametrize(
        ("kwargs", "non_expression"),
        [
            ({"ConditionalOperator": "AND"}, "ConditionalOperator"),
            (
                {
                    "KeyConditions": {
                        "pk": {"ComparisonOperator": "EQ", "AttributeValueList": [A]}
                    }
                },
                "KeyConditions",
            ),
        ],
        ids=["conditional-operator", "key-conditions"],
    )
    def test_parameter_mixing_reported_ahead_of_duplicate(
        self, dynamodb_client, range_table, kwargs, non_expression
    ):
        """Measured 2026-11-11T11:23Z: a non-expression parameter beside the
        expression answers the mixing error, not the duplicate."""
        code, msg = _error(
            lambda: dynamodb_client.query(
                TableName=range_table,
                KeyConditionExpression="pk = :a AND pk = :b",
                ExpressionAttributeValues={":a": A, ":b": B},
                **kwargs,
            )
        )
        assert code == "ValidationException"
        assert msg == (
            "Can not use both expression and non-expression parameters in the same "
            f"request: Non-expression parameters: {{{non_expression}}} Expression "
            "parameters: {KeyConditionExpression}"
        )


def _undefined(reference: str) -> str:
    """The service's message for an undefined `#name` or `:value`."""
    if reference.startswith("#"):
        return (
            "Invalid KeyConditionExpression: An expression attribute name used in "
            f"the document path is not defined; attribute name: {reference}"
        )
    return (
        "Invalid KeyConditionExpression: An expression attribute value used in "
        f"expression is not defined; attribute value: {reference}"
    )


class TestReferenceAndOperatorOrder:
    """Which undefined reference is reported, and the operators beside a
    duplicate (2026-11-10 and 2026-11-11)."""

    @pytest.mark.parametrize(
        ("expr", "names", "values", "undefined"),
        [
            ("pk = :q AND #u = :c", None, None, ":q"),
            ("sk = :b AND pk = :a", None, None, ":b"),
            ("pk = :a AND sk = :missing", None, {":a": A, ":extra": B}, ":missing"),
            ("pk = :a AND pk = :b AND #u = :c", None, None, ":a"),
            ("pk = :q AND pk = :b AND #u = :c", None, {":b": B, ":c": C}, ":q"),
            ("#u = :a AND #u = :b", None, None, "#u"),
            ("pk = :b AND pk = :a", None, None, ":b"),
            # Measured 2026-11-10 on a table keyed on pk (S) alone; each
            # request also carries an unused name or value.
            ("pk = :v", None, {":w": A}, ":v"),
            ("#k = :v", None, {":v": A, ":w": B}, "#k"),
            ("#k = :v", {"#j": "pk"}, {":v": A}, "#k"),
            ("pk = :v", {"#j": "pk"}, {":w": A}, ":v"),
            ("#k = :v", None, {":w": A}, "#k"),
        ],
    )
    def test_first_undefined_reference_in_text_order_is_reported(
        self, dynamodb_client, range_table, expr, names, values, undefined
    ):
        kwargs = {"TableName": range_table, "KeyConditionExpression": expr}
        if names is not None:
            kwargs["ExpressionAttributeNames"] = names
        if values is not None:
            kwargs["ExpressionAttributeValues"] = values
        code, msg = _error(lambda: dynamodb_client.query(**kwargs))
        assert code == "ValidationException"
        assert msg == _undefined(undefined)

    @pytest.mark.parametrize(
        ("expr", "values", "want"),
        [
            ("pk = :a AND pk <> :b", {":a": A, ":b": B}, ONE_PER_KEY),
            ("pk = :a AND contains(pk, :b)", {":a": A, ":b": B}, ONE_PER_KEY),
            (
                "pk = :a AND begins_with(sk, :n) AND sk = :b",
                {":a": A, ":b": B, ":n": N1},
                "Invalid KeyConditionExpression: Incorrect operand type for operator "
                "or function; operator or function: begins_with, operand type: N",
            ),
        ],
    )
    def test_operators_beside_a_duplicate(
        self, dynamodb_client, range_table, expr, values, want
    ):
        code, msg = _error(
            lambda: dynamodb_client.query(
                TableName=range_table,
                KeyConditionExpression=expr,
                ExpressionAttributeValues=values,
            )
        )
        assert code == "ValidationException"
        assert msg == want
