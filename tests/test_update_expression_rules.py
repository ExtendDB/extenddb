# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""UpdateExpression request-level rules, and the single-item write prefix.

Amazon DynamoDB rejects these before it reads the table:

  - a repeated section (`SET ... SET ...`, likewise REMOVE, ADD, DELETE),
    reported after the whole expression has parsed, so a syntax error
    anywhere wins over it, and after that section's own actions have been
    checked (test_expression_semantics.py pins that order);
  - `if_not_exists` and `list_append` with other than two operands;
  - `if_not_exists` whose first operand is not a document path;
  - `list_append` as an operand of another function
    (`list_append(if_not_exists(l, :e), :v)` is allowed; the reverse is not);
  - two actions on overlapping paths, across sections, after `#name`
    resolution, reported as `path one: [...], path two: [...]`.

Order on the service when a request carries two of these: syntax error,
then one walk over the actions in expression order (undefined `#name` or
`:value`, function rules, and a repeated section at the end of its section),
then overlap, then unused name or value.

PutItem, UpdateItem, and DeleteItem wrap every expression-stage error in
`1 validation error detected: `; TransactWriteItems and the read APIs do not.
Every expected message was captured from Amazon DynamoDB on 2026-10-14.
Dual-target.
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table

P = "1 validation error detected: "
U = "Invalid UpdateExpression: "
SECTION_ONCE = U + 'The "{}" section can only be used once in an update expression;'
OPERANDS = U + "Incorrect number of operands for operator or function; operator or function: {}, number of operands: {}"
NEEDS_PATH = U + "Operator or function requires a document path; operator or function: if_not_exists"
NOT_THIS_WAY = U + "The function is not allowed to be used this way in an expression; function: list_append"
OVERLAP = U + "Two document paths overlap with each other; must remove or rewrite one of these paths; path one: {}, path two: {}"


@pytest.fixture(scope="module")
def table(dynamodb_client):
    with scoped_table(dynamodb_client) as name:
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "p"}, "fieldx": {"N": "1"}, "mapx": {"M": {"num": {"N": "1"}}},
                  "lst": {"L": [{"M": {"ok": {"N": "1"}}}, {"M": {"x": {"N": "1"}}}]}},
        )
        yield name


KEY = {"pk": {"S": "p"}}
V = {":v": {"N": "1"}}


def _expect(func, message: str):
    with pytest.raises(ClientError) as exc_info:
        func()
    err = exc_info.value.response["Error"]
    assert err["Code"] == "ValidationException", err
    assert err["Message"] == message, err


def _update(client, table, expression, names=None, values=None):
    kwargs = {"TableName": table, "Key": KEY, "UpdateExpression": expression}
    if names:
        kwargs["ExpressionAttributeNames"] = names
    if values:
        kwargs["ExpressionAttributeValues"] = values
    return lambda: client.update_item(**kwargs)


def _transact_update(client, table, expression, names=None, values=None):
    item = {"TableName": table, "Key": KEY, "UpdateExpression": expression}
    if names:
        item["ExpressionAttributeNames"] = names
    if values:
        item["ExpressionAttributeValues"] = values
    return lambda: client.transact_write_items(TransactItems=[{"Update": item}])


class TestRepeatedSections:
    @pytest.mark.parametrize(
        ("expression", "section"),
        [
            ("SET fieldx = :v SET otherx = :v", "SET"),
            ("REMOVE a REMOVE b", "REMOVE"),
            ("ADD a :v ADD b :v", "ADD"),
            ("DELETE a :s DELETE b :s", "DELETE"),
        ],
    )
    def test_section_used_twice(self, dynamodb_client, table, expression, section):
        values = {":s": {"SS": ["x"]}} if "DELETE" in expression else V
        _expect(_update(dynamodb_client, table, expression, values=values), P + SECTION_ONCE.format(section))

    def test_section_check_follows_the_grammar_pass(self, dynamodb_client, table):
        # The service answers 'Syntax error; token: "<EOF>", near: "+"'; ExtendDB's
        # parser wording differs today (tracked under #108 message text). The
        # guarantee pinned here is the order: the syntax error wins over the
        # repeated-section error.
        with pytest.raises(ClientError) as exc_info:
            _update(dynamodb_client, table, "SET a = :v SET b = :v +", values=V)()
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException", err
        assert err["Message"].startswith(P + U), err
        assert "section can only be used once" not in err["Message"], err

    def test_repeated_section_beats_overlap(self, dynamodb_client, table):
        _expect(_update(dynamodb_client, table, "SET a = :v SET a = :v", values=V), P + SECTION_ONCE.format("SET"))

    def test_transaction_reports_it_without_the_prefix(self, dynamodb_client, table):
        _expect(
            _transact_update(dynamodb_client, table, "SET fieldx = :v SET otherx = :v", values=V),
            SECTION_ONCE.format("SET"),
        )


class TestFunctionOperands:
    def test_if_not_exists_one_operand(self, dynamodb_client, table):
        _expect(_update(dynamodb_client, table, "SET a = if_not_exists(a)"), P + OPERANDS.format("if_not_exists", 1))

    def test_if_not_exists_three_operands(self, dynamodb_client, table):
        _expect(
            _update(dynamodb_client, table, "SET a = if_not_exists(a, :v, :v)", values=V),
            P + OPERANDS.format("if_not_exists", 3),
        )

    def test_list_append_one_operand(self, dynamodb_client, table):
        _expect(
            _update(dynamodb_client, table, "SET l = list_append(:l)", values={":l": {"L": []}}),
            P + OPERANDS.format("list_append", 1),
        )

    def test_if_not_exists_first_operand_must_be_a_path(self, dynamodb_client, table):
        _expect(_update(dynamodb_client, table, "SET a = if_not_exists(:v, :v)", values=V), P + NEEDS_PATH)

    def test_nested_if_not_exists_is_not_a_path(self, dynamodb_client, table):
        _expect(
            _update(
                dynamodb_client, table, "SET #r = if_not_exists(if_not_exists(n, :zero), :one)",
                names={"#r": "result"}, values={":zero": {"N": "0"}, ":one": {"N": "1"}},
            ),
            P + NEEDS_PATH,
        )

    def test_list_append_inside_list_append(self, dynamodb_client, table):
        _expect(
            _update(
                dynamodb_client, table, "SET #r = list_append(list_append(l, :tail), :tail2)",
                names={"#r": "result"}, values={":tail": {"L": [{"S": "b"}]}, ":tail2": {"L": [{"S": "c"}]}},
            ),
            P + NOT_THIS_WAY,
        )

    def test_list_append_inside_if_not_exists(self, dynamodb_client, table):
        _expect(
            _update(
                dynamodb_client, table, "SET l = if_not_exists(l, list_append(:e, :v))",
                values={":e": {"L": []}, ":v": {"L": [{"S": "x"}]}},
            ),
            P + NOT_THIS_WAY,
        )

    def test_if_not_exists_inside_list_append_is_allowed(self, dynamodb_client, table):
        dynamodb_client.update_item(
            TableName=table, Key=KEY,
            UpdateExpression="SET l2 = list_append(if_not_exists(l2, :e), :v)",
            ExpressionAttributeValues={":e": {"L": []}, ":v": {"L": [{"S": "x"}]}},
        )
        item = dynamodb_client.get_item(TableName=table, Key=KEY)["Item"]
        assert item["l2"] == {"L": [{"S": "x"}]}


class TestOverlappingPaths:
    @pytest.mark.parametrize(
        ("expression", "names", "values", "one", "two"),
        [
            ("SET fieldx = :v, fieldx = :w", None, {":v": {"N": "1"}, ":w": {"N": "2"}}, "[fieldx]", "[fieldx]"),
            ("SET mapx = :m, mapx.num = :v", None, {":m": {"M": {}}, ":v": {"N": "2"}}, "[mapx]", "[mapx, num]"),
            ("SET #a = :v, fieldx = :v", {"#a": "fieldx"}, V, "[fieldx]", "[fieldx]"),
            ("SET a = :v REMOVE a", None, V, "[a]", "[a]"),
            ("SET a.b = :v REMOVE a", None, V, "[a, b]", "[a]"),
            ("REMOVE a, a.b", None, None, "[a]", "[a, b]"),
            ("SET a = :v ADD a :v", None, V, "[a]", "[a]"),
            ("SET #l[1] = :v, #l[1].x = :v", {"#l": "lst"}, V, "[lst, [1]]", "[lst, [1], x]"),
        ],
    )
    def test_overlap(self, dynamodb_client, table, expression, names, values, one, two):
        _expect(_update(dynamodb_client, table, expression, names=names, values=values), P + OVERLAP.format(one, two))

    def test_siblings_do_not_overlap(self, dynamodb_client, table):
        dynamodb_client.update_item(
            TableName=table, Key=KEY,
            UpdateExpression="SET mapx.a = :v, mapx.b = :v",
            ExpressionAttributeValues=V,
        )

    def test_undefined_value_beats_overlap(self, dynamodb_client, table):
        _expect(
            _update(dynamodb_client, table, "SET a = :v, a = :missing", values=V),
            P + U + "An expression attribute value used in expression is not defined; attribute value: :missing",
        )

    def test_undefined_name_beats_overlap(self, dynamodb_client, table):
        _expect(
            _update(dynamodb_client, table, "SET a = :v, a = :v, #m = :v", values=V),
            P + U + "An expression attribute name used in the document path is not defined; attribute name: #m",
        )

    def test_overlap_beats_unused_name(self, dynamodb_client, table):
        _expect(
            _update(dynamodb_client, table, "SET a = :v, a = :v", names={"#u": "x"}, values=V),
            P + OVERLAP.format("[a]", "[a]"),
        )

    def test_transaction_overlap_without_the_prefix(self, dynamodb_client, table):
        _expect(
            _transact_update(dynamodb_client, table, "SET fieldx = :s, fieldx = :o",
                             values={":s": {"S": "new"}, ":o": {"S": "other"}}),
            OVERLAP.format("[fieldx]", "[fieldx]"),
        )
        _expect(
            _transact_update(dynamodb_client, table, "SET lst[0] = :v, lst[0].ok = :v", values=V),
            OVERLAP.format("[lst, [0]]", "[lst, [0], ok]"),
        )


class TestSingleItemWritePrefix:
    """PutItem, UpdateItem, DeleteItem prefix expression-stage errors; the read APIs do not."""

    RESERVED_C = "Invalid ConditionExpression: Attribute name is a reserved keyword; reserved keyword: count"
    UNUSED_N = "Value provided in ExpressionAttributeNames unused in expressions: keys: {#u}"
    UNUSED_V = "Value provided in ExpressionAttributeValues unused in expressions: keys: {:u}"
    NAMES_NO_EXPR = "ExpressionAttributeNames can only be specified when using expressions"
    VALUES_NO_EXPR = "ExpressionAttributeValues can only be specified when using expressions"
    UNDEF_V = "Invalid ConditionExpression: An expression attribute value used in expression is not defined; attribute value: :missing"
    UNDEF_N = "Invalid ConditionExpression: An expression attribute name used in the document path is not defined; attribute name: #m"
    SIZE_C = "Invalid ConditionExpression: Expression size has exceeded the maximum allowed size;"
    SIZE_U = "Invalid UpdateExpression: Expression size has exceeded the maximum allowed size;"
    SIZE_F = "Invalid FilterExpression: Expression size has exceeded the maximum allowed size;"

    def test_put_reserved_word(self, dynamodb_client, table):
        _expect(lambda: dynamodb_client.put_item(TableName=table, Item=KEY, ConditionExpression="count = :v",
                                                 ExpressionAttributeValues=V), P + self.RESERVED_C)

    def test_delete_reserved_word(self, dynamodb_client, table):
        _expect(lambda: dynamodb_client.delete_item(TableName=table, Key=KEY, ConditionExpression="count = :v",
                                                    ExpressionAttributeValues=V), P + self.RESERVED_C)

    def test_unused_names_and_values(self, dynamodb_client, table):
        _expect(lambda: dynamodb_client.update_item(TableName=table, Key=KEY, UpdateExpression="SET a = :v",
                                                    ExpressionAttributeNames={"#u": "x"}, ExpressionAttributeValues=V), P + self.UNUSED_N)
        _expect(lambda: dynamodb_client.put_item(TableName=table, Item=KEY, ConditionExpression="attribute_exists(pk)",
                                                 ExpressionAttributeNames={"#u": "x"}), P + self.UNUSED_N)
        _expect(lambda: dynamodb_client.delete_item(TableName=table, Key=KEY, ConditionExpression="attribute_exists(pk)",
                                                    ExpressionAttributeValues={":u": {"N": "1"}}), P + self.UNUSED_V)

    def test_names_or_values_without_expression(self, dynamodb_client, table):
        _expect(lambda: dynamodb_client.put_item(TableName=table, Item=KEY, ExpressionAttributeNames={"#u": "x"}),
                P + self.NAMES_NO_EXPR)
        _expect(lambda: dynamodb_client.delete_item(TableName=table, Key=KEY, ExpressionAttributeValues=V),
                P + self.VALUES_NO_EXPR)
        _expect(lambda: dynamodb_client.update_item(TableName=table, Key=KEY, ExpressionAttributeValues=V),
                P + self.VALUES_NO_EXPR)

    def test_undefined_references(self, dynamodb_client, table):
        _expect(lambda: dynamodb_client.put_item(TableName=table, Item=KEY, ConditionExpression="a = :missing"),
                P + self.UNDEF_V)
        _expect(lambda: dynamodb_client.delete_item(TableName=table, Key=KEY, ConditionExpression="#m = :v",
                                                    ExpressionAttributeValues=V), P + self.UNDEF_N)

    def test_over_size_condition_has_no_length_suffix(self, dynamodb_client, table):
        big = "attribute_exists(" + "a" * 4097 + ")"
        _expect(lambda: dynamodb_client.put_item(TableName=table, Item=KEY, ConditionExpression=big), P + self.SIZE_C)
        _expect(lambda: dynamodb_client.update_item(TableName=table, Key=KEY, UpdateExpression="SET a = :v",
                                                    ConditionExpression=big, ExpressionAttributeValues=V), P + self.SIZE_C)
        _expect(lambda: dynamodb_client.update_item(TableName=table, Key=KEY, UpdateExpression="SET " + "a" * 4097 + " = :v",
                                                    ExpressionAttributeValues=V), P + self.SIZE_U)

    def test_transaction_condition_keeps_the_length_suffix(self, dynamodb_client, table):
        big = "attribute_exists(" + "a" * 4097 + ")"
        _expect(
            lambda: dynamodb_client.transact_write_items(TransactItems=[
                {"ConditionCheck": {"TableName": table, "Key": KEY, "ConditionExpression": big}}]),
            self.SIZE_C + f" expression size: {len(big)}",
        )

    def test_query_filter_has_no_suffix_scan_filter_has_one(self, dynamodb_client, table):
        big = "attribute_exists(" + "a" * 4097 + ")"
        _expect(lambda: dynamodb_client.query(TableName=table, KeyConditionExpression="pk = :v", FilterExpression=big,
                                              ExpressionAttributeValues={":v": {"S": "p"}}), self.SIZE_F)
        _expect(lambda: dynamodb_client.scan(TableName=table, FilterExpression=big), self.SIZE_F + f" expression size: {len(big)}")

    def test_read_apis_carry_no_prefix(self, dynamodb_client, table):
        _expect(lambda: dynamodb_client.get_item(TableName=table, Key=KEY, ProjectionExpression="#a",
                                                 ExpressionAttributeNames={"#a": "a", "#u": "x"}), self.UNUSED_N)
        _expect(lambda: dynamodb_client.query(TableName=table, KeyConditionExpression="pk = :v",
                                              ExpressionAttributeValues={":v": {"S": "p"}, ":u": {"N": "1"}}), self.UNUSED_V)
