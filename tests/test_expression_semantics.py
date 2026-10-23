# SPDX-License-Identifier: Apache-2.0

"""Expression semantics Amazon DynamoDB checks before it reads the table.

After the grammar pass, the service walks the parsed expression once, in
expression order, checking each node after its operands, and only then
checks overlapping paths and unused names and values. Every expectation
below was captured from the service on 2026-10-14 (issue #108).

UpdateExpression:
  - `+` and `-` take numbers; `list_append` takes lists; `ADD` takes a number
    or a set; `DELETE` takes a set. A placeholder operand of the wrong type is
    refused with its type: `S`, `N`, `L` for the operators and functions,
    `STRING`, `NUMBER`, `Binary`, `BOOL`, `NULL`, `LIST`, `MAP` for ADD and
    DELETE.
  - Function names are matched as written: `IF_NOT_EXISTS(x, :v)` and
    `foo(x)` are `Invalid function name`; `size(x)` is `not allowed in an
    update expression`; `list_append` is refused as an operand of a function
    or of `+`.
  - A repeated section is reported after that section's own actions, so
    `SET a = :v SET b = :q` reports `:q` and `SET a = :v SET b = :v ADD n :s`
    reports the repeated `SET`.
  - Every unused name or value is listed in one set.
  - UpdateItem checks the ConditionExpression before the UpdateExpression;
    inside TransactWriteItems the UpdateExpression comes first.

ConditionExpression and FilterExpression:
  - the six condition functions are matched as written; `if_not_exists` and
    `list_append` are `not allowed in a condition expression`; a boolean
    function in operand position and `size` in condition position are `not
    allowed to be used this way`; operand counts are fixed; `attribute_exists`
    and `attribute_not_exists` take a document path; `begins_with` takes a
    string or binary and `attribute_type` a string naming a type.

Query and Scan:
  - the FilterExpression, then the KeyConditionExpression, then the
    ProjectionExpression are checked for undefined references and function
    names, then unused names and values, all before the table is looked up.

Key-shaped APIs (GetItem, DeleteItem, UpdateItem, the batch key paths, and a
BatchWriteItem Put) refuse a key with a missing, mistyped, or extra attribute
with `The provided key element does not match the schema`; PutItem names the
attribute. BatchWriteItem returns `ItemCollectionMetrics: {}` when SIZE is
requested and no written table has a local secondary index.

Dual-target: runs against the service and against ExtendDB on every backend.
"""

from __future__ import annotations

import re
import uuid

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table

P = "1 validation error detected: "
U = "Invalid UpdateExpression: "
C = "Invalid ConditionExpression: "
F = "Invalid FilterExpression: "
K = "Invalid KeyConditionExpression: "
PR = "Invalid ProjectionExpression: "

UNDEF_VALUE = "An expression attribute value used in expression is not defined; attribute value: {}"
UNDEF_NAME = "An expression attribute name used in the document path is not defined; attribute name: {}"
OPERAND_TYPE = "Incorrect operand type for operator or function; operator or function: {}, operand type: {}"
ADD_DELETE_TYPE = "Incorrect operand type for operator or function; operator: {}, operand type: {}, typeSet: ALLOWED_FOR_ADD_OPERAND"
INVALID_FN = "Invalid function name; function: {}"
NOT_THIS_WAY = "The function is not allowed to be used this way in an expression; function: {}"
NOT_IN_UPDATE = "The function is not allowed in an update expression; function: {}"
NOT_IN_CONDITION = "The function is not allowed in a condition expression; function: {}"
OPERAND_COUNT = "Incorrect number of operands for operator or function; operator or function: {}, number of operands: {}"
NEEDS_PATH = "Operator or function requires a document path; operator or function: {}"
SECTION_ONCE = 'The "{}" section can only be used once in an update expression;'
OVERLAP_A_AB = "Two document paths overlap with each other; must remove or rewrite one of these paths; path one: [a], path two: [a, b]"
EVAL_TYPE = "An operand in the update expression has an incorrect data type"
KEY_SCHEMA = "The provided key element does not match the schema"
EMPTY_ARGS_SYNTAX = 'Syntax error; token: ")", near: "()"'

N = {"N": "1"}
S = {"S": "x"}
L = {"L": []}
SS = {"SS": ["a"]}
KEY = {"pk": {"S": "p"}}
WRITE_KEY = {"pk": {"S": "w"}}


@pytest.fixture(scope="module")
def table(dynamodb_client):
    with scoped_table(dynamodb_client) as name:
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "p"}, "n": N, "s": SS, "l": L, "m": {"M": {"x": N}}},
        )
        yield name


@pytest.fixture(scope="module")
def range_table(dynamodb_client):
    with scoped_table(
        dynamodb_client,
        attribute_definitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
        ],
        key_schema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
    ) as name:
        yield name


@pytest.fixture(scope="module")
def lsi_table(dynamodb_client):
    with scoped_table(
        dynamodb_client,
        attribute_definitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "alt", "AttributeType": "S"},
        ],
        key_schema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        LocalSecondaryIndexes=[
            {
                "IndexName": "byalt",
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "alt", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
    ) as name:
        yield name


@pytest.fixture(scope="module")
def missing_table() -> str:
    return f"extenddb-missing-{uuid.uuid4().hex[:12]}"


def _expect(func, message: str, code: str = "ValidationException"):
    with pytest.raises(ClientError) as exc_info:
        func()
    err = exc_info.value.response["Error"]
    assert err["Code"] == code, err
    assert err["Message"] == message, err


def _expect_prefix(func, prefix: str, code: str = "ValidationException"):
    """For syntax errors, whose inner wording ExtendDB still phrases its own way."""
    with pytest.raises(ClientError) as exc_info:
        func()
    err = exc_info.value.response["Error"]
    assert err["Code"] == code, err
    assert err["Message"].startswith(prefix), err


def _expect_unused(func, kind: str, keys: set[str]):
    """The service lists every unused key in one set, in an order it does not document."""
    with pytest.raises(ClientError) as exc_info:
        func()
    err = exc_info.value.response["Error"]
    assert err["Code"] == "ValidationException", err
    match = re.fullmatch(
        rf"(?:{re.escape(P)})?Value provided in ExpressionAttribute{kind} unused in expressions: keys: \{{(.*)\}}",
        err["Message"],
    )
    assert match, err
    assert set(match.group(1).split(", ")) == keys, err


def _update(client, table, expression, values=None, names=None, condition=None, key=WRITE_KEY):
    kwargs = {"TableName": table, "Key": key, "UpdateExpression": expression}
    if values is not None:
        kwargs["ExpressionAttributeValues"] = values
    if names is not None:
        kwargs["ExpressionAttributeNames"] = names
    if condition is not None:
        kwargs["ConditionExpression"] = condition
    return lambda: client.update_item(**kwargs)


def _transact_update(client, table, expression, values=None, condition=None):
    item = {"TableName": table, "Key": WRITE_KEY, "UpdateExpression": expression}
    if values is not None:
        item["ExpressionAttributeValues"] = values
    if condition is not None:
        item["ConditionExpression"] = condition
    return lambda: client.transact_write_items(TransactItems=[{"Update": item}])


def _put_cond(client, table, condition, values=None, names=None):
    kwargs = {"TableName": table, "Item": WRITE_KEY, "ConditionExpression": condition}
    if values is not None:
        kwargs["ExpressionAttributeValues"] = values
    if names is not None:
        kwargs["ExpressionAttributeNames"] = names
    return lambda: client.put_item(**kwargs)


class TestUpdateOperandTypes:
    @pytest.mark.parametrize(
        ("operator", "value", "type_name"),
        [
            ("ADD", S, "STRING"),
            ("ADD", {"B": b"x"}, "Binary"),
            ("ADD", {"BOOL": True}, "BOOL"),
            ("ADD", {"NULL": True}, "NULL"),
            ("ADD", L, "LIST"),
            ("ADD", {"M": {}}, "MAP"),
            ("DELETE", S, "STRING"),
            ("DELETE", N, "NUMBER"),
            ("DELETE", {"B": b"x"}, "Binary"),
            ("DELETE", {"BOOL": True}, "BOOL"),
            ("DELETE", {"NULL": True}, "NULL"),
            ("DELETE", L, "LIST"),
            ("DELETE", {"M": {}}, "MAP"),
        ],
    )
    def test_add_and_delete_refuse_the_type_at_request_time(self, dynamodb_client, table, operator, value, type_name):
        _expect(_update(dynamodb_client, table, f"{operator} attr :v", {":v": value}),
                P + U + ADD_DELETE_TYPE.format(operator, type_name))

    def test_add_takes_numbers_and_sets_delete_takes_sets(self, dynamodb_client, table):
        dynamodb_client.update_item(TableName=table, Key=WRITE_KEY, UpdateExpression="ADD n :n, s :ss",
                                    ExpressionAttributeValues={":n": N, ":ss": SS})
        dynamodb_client.update_item(TableName=table, Key=WRITE_KEY, UpdateExpression="DELETE s :ss",
                                    ExpressionAttributeValues={":ss": SS})

    def test_set_mismatch_against_the_stored_attribute_is_an_evaluation_error(self, dynamodb_client, table):
        # Request-time rules accept a set; the stored attribute decides the rest,
        # and that message carries no prefix.
        _expect(_update(dynamodb_client, table, "ADD n :ss", {":ss": SS}, key=KEY), EVAL_TYPE)
        _expect(_update(dynamodb_client, table, "ADD s :ns", {":ns": {"NS": ["1"]}}, key=KEY), EVAL_TYPE)

    @pytest.mark.parametrize(
        ("expression", "values", "symbol", "type_code"),
        [
            ("SET a = :s + :n", {":s": S, ":n": N}, "+", "S"),
            ("SET a = :n + :s", {":s": S, ":n": N}, "+", "S"),
            ("SET a = :s + :s2", {":s": S, ":s2": {"S": "y"}}, "+", "S"),
            ("SET a = n + :s", {":s": S}, "+", "S"),
            ("SET a = :l - :n", {":l": L, ":n": N}, "-", "L"),
            ("SET a = :b + :n", {":b": {"B": b"x"}, ":n": N}, "+", "B"),
            ("SET a = :z + :n", {":z": {"NULL": True}, ":n": N}, "+", "NULL"),
            ("SET a = :m + :n", {":m": {"M": {}}, ":n": N}, "+", "M"),
            ("SET a = :t + :n", {":t": {"BOOL": True}, ":n": N}, "+", "BOOL"),
            ("SET a = :ss + :n", {":ss": SS, ":n": N}, "+", "SS"),
        ],
    )
    def test_arithmetic_operands_must_be_numbers(self, dynamodb_client, table, expression, values, symbol, type_code):
        _expect(_update(dynamodb_client, table, expression, values), P + U + OPERAND_TYPE.format(symbol, type_code))

    def test_arithmetic_on_a_function_result_is_checked_at_evaluation(self, dynamodb_client, table):
        _expect(_update(dynamodb_client, table, "SET a = if_not_exists(a, :s) + :n", {":s": S, ":n": N}, key=KEY),
                EVAL_TYPE)

    @pytest.mark.parametrize(
        ("expression", "values", "type_code"),
        [
            ("SET l = list_append(:v, :l)", {":v": S, ":l": L}, "S"),
            ("SET l = list_append(:v, :l)", {":v": N, ":l": L}, "N"),
            ("SET l = list_append(:v, :l)", {":v": {"M": {}}, ":l": L}, "M"),
            ("SET l = list_append(:v, :l)", {":v": SS, ":l": L}, "SS"),
            ("SET l = list_append(:v, :l)", {":v": {"BOOL": True}, ":l": L}, "BOOL"),
            ("SET l = list_append(:v, :l)", {":v": {"B": b"x"}, ":l": L}, "B"),
            ("SET l = list_append(:v, :l)", {":v": {"NULL": True}, ":l": L}, "NULL"),
            ("SET l = list_append(:v, :l)", {":v": {"NS": ["1"]}, ":l": L}, "NS"),
            ("SET l = list_append(:v, :l)", {":v": {"BS": [b"a"]}, ":l": L}, "BS"),
            ("SET l = list_append(:l, :v)", {":v": S, ":l": L}, "S"),
            ("SET l = list_append(l, :v)", {":v": S}, "S"),
            ("SET l = list_append(:s, :n)", {":s": S, ":n": N}, "S"),
        ],
    )
    def test_list_append_operands_must_be_lists(self, dynamodb_client, table, expression, values, type_code):
        _expect(_update(dynamodb_client, table, expression, values), P + U + OPERAND_TYPE.format("list_append", type_code))

    def test_list_append_of_a_function_result_is_accepted(self, dynamodb_client, table):
        dynamodb_client.update_item(TableName=table, Key=WRITE_KEY,
                                    UpdateExpression="SET l = list_append(if_not_exists(l, :e), :l)",
                                    ExpressionAttributeValues={":e": L, ":l": L})


class TestUpdateFunctionNames:
    @pytest.mark.parametrize(
        ("expression", "values", "name"),
        [
            ("SET x = IF_NOT_EXISTS(x, :v)", {":v": N}, "IF_NOT_EXISTS"),
            ("SET l = List_Append(:l, :l)", {":l": L}, "List_Append"),
            ("SET x = foo(x, :v)", {":v": N}, "foo"),
            ("SET x = ATTRIBUTE_EXISTS(x)", None, "ATTRIBUTE_EXISTS"),
            ("SET x = Size(x)", None, "Size"),
            ("SET a = if_not_exists(foo(x), :v)", {":v": N}, "foo"),
            ("SET a = if_not_exists(a, foo(x))", None, "foo"),
            ("SET l = list_append(:l, foo(x))", {":l": L}, "foo"),
        ],
    )
    def test_unknown_or_miscased_names_are_refused(self, dynamodb_client, table, expression, values, name):
        _expect(_update(dynamodb_client, table, expression, values), P + U + INVALID_FN.format(name))

    @pytest.mark.parametrize("name", ["attribute_exists", "size"])
    def test_condition_functions_are_refused_by_name(self, dynamodb_client, table, name):
        _expect(_update(dynamodb_client, table, f"SET x = {name}(x), y = :q"), P + U + NOT_IN_UPDATE.format(name))

    def test_a_call_with_no_operands_is_a_syntax_error(self, dynamodb_client, table):
        _expect(_update(dynamodb_client, table, "SET x = foo()"), P + U + EMPTY_ARGS_SYNTAX)

    @pytest.mark.parametrize(
        "expression",
        ["SET l = list_append(:l, :l) + :n", "SET l = :n + list_append(:l, :l)"],
    )
    def test_list_append_is_refused_as_an_arithmetic_operand(self, dynamodb_client, table, expression):
        _expect(_update(dynamodb_client, table, expression, {":l": L, ":n": N}),
                P + U + NOT_THIS_WAY.format("list_append"))

    def test_nested_list_append_is_reported_where_it_stands(self, dynamodb_client, table):
        # The nested call is refused before its sibling operand is resolved,
        # but an undefined reference inside it is found first.
        _expect(_update(dynamodb_client, table, "SET l = if_not_exists(list_append(:a, :b), :q)", {":a": L, ":b": L}),
                P + U + NOT_THIS_WAY.format("list_append"))
        _expect(_update(dynamodb_client, table, "SET l = if_not_exists(list_append(:q, :b), :a)", {":a": L, ":b": L}),
                P + U + UNDEF_VALUE.format(":q"))


class TestUpdateWalkOrder:
    def test_references_in_expression_order(self, dynamodb_client, table):
        _expect(_update(dynamodb_client, table, "SET a = :q, #n = :v", {":v": N}), P + U + UNDEF_VALUE.format(":q"))
        _expect(_update(dynamodb_client, table, "SET #n = :q"), P + U + UNDEF_NAME.format("#n"))

    @pytest.mark.parametrize(
        ("expression", "values", "message"),
        [
            ("SET a = :q SET b = :v", {":v": N}, UNDEF_VALUE.format(":q")),
            ("SET a = :v SET b = :q", {":v": N}, UNDEF_VALUE.format(":q")),
            ("SET #n = :v SET b = :v", {":v": N}, UNDEF_NAME.format("#n")),
            ("SET a = :v REMOVE #n SET b = :v", {":v": N}, UNDEF_NAME.format("#n")),
            ("SET a = :v SET b = :v ADD n :s", {":v": N, ":s": S}, SECTION_ONCE.format("SET")),
            ("SET a = :v SET b = :v ADD n :q", {":v": N}, SECTION_ONCE.format("SET")),
            ("SET a = :v ADD n :s SET b = :v", {":v": N, ":s": S}, ADD_DELETE_TYPE.format("ADD", "STRING")),
            ("ADD n :s ADD m :n", {":s": S, ":n": N}, ADD_DELETE_TYPE.format("ADD", "STRING")),
            ("DELETE s :ss DELETE t :s", {":s": S, ":ss": SS}, ADD_DELETE_TYPE.format("DELETE", "STRING")),
            ("SET a = :n SET b = :s + :n", {":s": S, ":n": N}, OPERAND_TYPE.format("+", "S")),
            ("SET a = :v SET x = foo(x)", {":v": N}, INVALID_FN.format("foo")),
            ("SET l = list_append(list_append(:a, :a), :a) SET b = :v", {":v": N, ":a": L}, NOT_THIS_WAY.format("list_append")),
        ],
    )
    def test_repeated_section_is_reported_after_its_own_actions(self, dynamodb_client, table, expression, values, message):
        _expect(_update(dynamodb_client, table, expression, values), P + U + message)

    @pytest.mark.parametrize(
        ("expression", "values", "message"),
        [
            ("SET a = :q ADD n :s", {":s": S}, UNDEF_VALUE.format(":q")),
            ("ADD n :s SET a = if_not_exists(a)", {":s": S}, ADD_DELETE_TYPE.format("ADD", "STRING")),
            ("SET a = if_not_exists(a) ADD n :s", {":s": S}, OPERAND_COUNT.format("if_not_exists", 1)),
            ("SET a = if_not_exists(a, :q, :q2)", None, UNDEF_VALUE.format(":q")),
            ("SET a = if_not_exists(a), b = :q", None, OPERAND_COUNT.format("if_not_exists", 1)),
            ("SET a = if_not_exists(:a)", {":a": N}, OPERAND_COUNT.format("if_not_exists", 1)),
            ("SET l = list_append(:s)", {":s": S}, OPERAND_COUNT.format("list_append", 1)),
            ("SET x = foo(x), y = :q", None, INVALID_FN.format("foo")),
            ("SET y = :q, x = foo(x)", None, UNDEF_VALUE.format(":q")),
            ("SET x = foo(#n)", None, UNDEF_NAME.format("#n")),
            ("SET x = foo(:q)", None, UNDEF_VALUE.format(":q")),
            ("ADD n :q", None, UNDEF_VALUE.format(":q")),
            ("ADD n :s DELETE s :s", {":s": S}, ADD_DELETE_TYPE.format("ADD", "STRING")),
            ("SET a = :n, a.b = :n ADD m :s", {":s": S, ":n": N}, ADD_DELETE_TYPE.format("ADD", "STRING")),
            ("SET a = :s + :n ADD n :s", {":s": S, ":n": N}, OPERAND_TYPE.format("+", "S")),
        ],
    )
    def test_each_node_is_checked_after_its_operands(self, dynamodb_client, table, expression, values, message):
        _expect(_update(dynamodb_client, table, expression, values), P + U + message)

    def test_operand_type_and_function_name_beat_unused(self, dynamodb_client, table):
        _expect(_update(dynamodb_client, table, "ADD n :s", {":s": S, ":u": N}), P + U + ADD_DELETE_TYPE.format("ADD", "STRING"))
        _expect(_update(dynamodb_client, table, "SET x = foo(x)", {":u": N}), P + U + INVALID_FN.format("foo"))
        _expect(_update(dynamodb_client, table, "ADD n :s", {":s": S}, {"#u": "u"}), P + U + ADD_DELETE_TYPE.format("ADD", "STRING"))

    def test_a_syntax_error_anywhere_still_wins(self, dynamodb_client, table):
        for expression, values in [
            ("SET a = if_not_exists(a), b = ", None),
            ("SET l = list_append(list_append(:a, :a), :a), b = ", {":a": L}),
            ("SET a = :s + :n, b = ", {":s": S, ":n": N}),
            ("SET x = foo(x), b = ", None),
            ("ADD n :s SET a = ", {":s": S}),
        ]:
            _expect_prefix(_update(dynamodb_client, table, expression, values), P + U)

    def test_transaction_reports_the_operand_type_bare_and_top_level(self, dynamodb_client, table):
        _expect(_transact_update(dynamodb_client, table, "ADD n :s", {":s": S}), U + ADD_DELETE_TYPE.format("ADD", "STRING"))
        _expect(_transact_update(dynamodb_client, table, "SET x = foo(x)"), U + INVALID_FN.format("foo"))

    def test_update_item_checks_the_condition_first(self, dynamodb_client, table):
        _expect(_update(dynamodb_client, table, "SET #a = :v", {":v": N}, condition="attribute_exists(#b)"),
                P + C + UNDEF_NAME.format("#b"))
        _expect(_update(dynamodb_client, table, "SET a = ", condition="attribute_exists(#b)"),
                P + C + UNDEF_NAME.format("#b"))
        _expect(_update(dynamodb_client, table, "SET a = :v", {":v": N}, condition="b = :q"),
                P + C + UNDEF_VALUE.format(":q"))
        _expect(_update(dynamodb_client, table, "SET a = :v, a.b = :v", {":v": N}, condition="attribute_exists(#b)"),
                P + C + UNDEF_NAME.format("#b"))
        _expect_prefix(_update(dynamodb_client, table, "SET #a = :v", {":v": N}, condition="attribute_exists("), P + C)

    def test_transaction_checks_the_update_first(self, dynamodb_client, table):
        _expect(_transact_update(dynamodb_client, table, "SET #a = :v", {":v": N}, condition="attribute_exists(#b)"),
                U + UNDEF_NAME.format("#a"))
        _expect(_transact_update(dynamodb_client, table, "SET #a = :v", {":v": N}, condition="attribute_exists("),
                U + UNDEF_NAME.format("#a"))
        _expect(_transact_update(dynamodb_client, table, "SET a = :v, a.b = :v", {":v": N}, condition="attribute_exists(#b)"),
                U + OVERLAP_A_AB)
        _expect(_transact_update(dynamodb_client, table, "ADD n :s", {":s": S}, condition="attribute_exists(#b)"),
                U + ADD_DELETE_TYPE.format("ADD", "STRING"))
        _expect_prefix(_transact_update(dynamodb_client, table, "SET a = ", condition="attribute_exists(#b)"), U)


class TestUnusedKeysAreListedTogether:
    def test_values(self, dynamodb_client, table):
        _expect_unused(_update(dynamodb_client, table, "SET a = :u", {":u": N, ":x": N, ":y": N, ":z": N}),
                       "Values", {":x", ":y", ":z"})

    def test_names(self, dynamodb_client, table):
        _expect_unused(_update(dynamodb_client, table, "SET a = :u", {":u": N}, {"#x": "x", "#y": "y", "#z": "z"}),
                       "Names", {"#x", "#y", "#z"})

    def test_query_names(self, dynamodb_client, table):
        _expect_unused(
            lambda: dynamodb_client.query(TableName=table, KeyConditionExpression="pk = :p",
                                          ExpressionAttributeNames={"#b": "b", "#a": "a"},
                                          ExpressionAttributeValues={":p": S}),
            "Names", {"#a", "#b"},
        )


class TestConditionFunctions:
    @pytest.mark.parametrize(
        ("condition", "values", "message"),
        [
            ("ATTRIBUTE_EXISTS(pk)", None, INVALID_FN.format("ATTRIBUTE_EXISTS")),
            ("SIZE(a) > :n", {":n": N}, INVALID_FN.format("SIZE")),
            ("Begins_With(pk, :v)", {":v": S}, INVALID_FN.format("Begins_With")),
            ("foo(pk) AND bar(pk)", None, INVALID_FN.format("foo")),
            ("foo(pk) > :n", {":n": N}, INVALID_FN.format("foo")),
            ("pk = foo(pk)", None, INVALID_FN.format("foo")),
            ("foo(pk, :n)", {":n": N}, INVALID_FN.format("foo")),
            ("if_not_exists(a, :v) = :v", {":v": N}, NOT_IN_CONDITION.format("if_not_exists")),
            ("list_append(a, :v) = :v", {":v": N}, NOT_IN_CONDITION.format("list_append")),
            ("attribute_exists(pk) = :v", {":v": N}, NOT_THIS_WAY.format("attribute_exists")),
            ("attribute_exists(pk) > :n", {":n": N}, NOT_THIS_WAY.format("attribute_exists")),
            ("begins_with(pk, :v) = :v", {":v": S}, NOT_THIS_WAY.format("begins_with")),
            ("contains(pk, :v) = :v", {":v": S}, NOT_THIS_WAY.format("contains")),
            ("attribute_type(pk, :v) = :v", {":v": S}, NOT_THIS_WAY.format("attribute_type")),
            ("pk IN (:v, attribute_exists(pk))", {":v": S}, NOT_THIS_WAY.format("attribute_exists")),
            ("size(attribute_exists(pk)) > :n", {":n": N}, NOT_THIS_WAY.format("attribute_exists")),
            ("size(pk)", None, NOT_THIS_WAY.format("size")),
            ("NOT size(pk)", None, NOT_THIS_WAY.format("size")),
            ("attribute_exists(size(pk))", None, NEEDS_PATH.format("attribute_exists")),
            ("attribute_exists(:v)", {":v": S}, NEEDS_PATH.format("attribute_exists")),
            ("attribute_not_exists(:v)", {":v": S}, NEEDS_PATH.format("attribute_not_exists")),
            ("attribute_exists(pk, pk)", None, OPERAND_COUNT.format("attribute_exists", 2)),
            ("attribute_exists(:v, :w)", {":v": S, ":w": S}, OPERAND_COUNT.format("attribute_exists", 2)),
            ("begins_with(pk)", None, OPERAND_COUNT.format("begins_with", 1)),
            ("contains(pk)", None, OPERAND_COUNT.format("contains", 1)),
            ("attribute_type(pk)", None, OPERAND_COUNT.format("attribute_type", 1)),
            ("size(pk, pk) > :n", {":n": N}, OPERAND_COUNT.format("size", 2)),
            ("foo()", None, EMPTY_ARGS_SYNTAX),
            ("attribute_exists()", None, EMPTY_ARGS_SYNTAX),
            ("foo(#n)", None, UNDEF_NAME.format("#n")),
            ("foo(:q)", None, UNDEF_VALUE.format(":q")),
            ("attribute_exists(pk, :q)", None, UNDEF_VALUE.format(":q")),
            (":q = #n", None, UNDEF_VALUE.format(":q")),
            ("#n = :q", None, UNDEF_NAME.format("#n")),
            ("a = :q AND #n = :v", {":v": N}, UNDEF_VALUE.format(":q")),
            ("begins_with(pk, :n)", {":n": N}, OPERAND_TYPE.format("begins_with", "N")),
            ("attribute_type(pk, :n)", {":n": N}, OPERAND_TYPE.format("attribute_type", "N")),
            ("attribute_type(pk, :t)", {":t": {"S": "XX"}},
             "Invalid attribute type name found; type: XX, valid types: {S,SS,N,NS,B,BS,BOOL,NULL,L,M}"),
            ("begins_with(pk, :n) AND #x = :v", {":n": N, ":v": S}, OPERAND_TYPE.format("begins_with", "N")),
            ("#x = :v AND begins_with(pk, :n)", {":n": N, ":v": S}, UNDEF_NAME.format("#x")),
            ("pk > :l AND #x = :v", {":l": L, ":v": S}, OPERAND_TYPE.format(">", "L")),
            ("size(pk) BETWEEN :l AND :n", {":l": L, ":n": N}, OPERAND_TYPE.format("BETWEEN", "L")),
        ],
    )
    def test_put_item_refuses_with_the_prefix(self, dynamodb_client, table, condition, values, message):
        _expect(_put_cond(dynamodb_client, table, condition, values), P + C + message)

    def test_function_name_beats_unused(self, dynamodb_client, table):
        _expect(_put_cond(dynamodb_client, table, "foo(pk)", {":u": N}), P + C + INVALID_FN.format("foo"))

    @pytest.mark.parametrize(
        ("condition", "values"),
        [
            ("foo(pk) AND", None),
            ("ATTRIBUTE_EXISTS(pk) AND", None),
            ("begins_with(pk, :n) AND", {":n": N}),
            ("attribute_exists(#x) AND", None),
        ],
    )
    def test_a_syntax_error_beats_the_semantic_errors(self, dynamodb_client, table, condition, values):
        # The service answers 'Syntax error; token: "<EOF>", near: "AND"' for each of
        # these (measured 2026-10-23): the grammar pass runs over the whole text
        # before the function name, the operand type or the undefined name is
        # looked at. ExtendDB's syntax wording differs today (tracked under #108
        # message text); the guarantee pinned here is the order.
        with pytest.raises(ClientError) as exc_info:
            _put_cond(dynamodb_client, table, condition, values)()
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException", err
        assert err["Message"].startswith(P + C), err
        for semantic in ("Invalid function name", "Incorrect operand type", "is not defined"):
            assert semantic not in err["Message"], err

    @pytest.mark.parametrize(
        ("condition", "values"),
        [
            ("size(pk) = size(pk)", None),
            ("begins_with(:v, pk)", {":v": S}),
            ("contains(:v, :w)", {":v": S, ":w": S}),
            ("attribute_type(:v, :t)", {":v": S, ":t": {"S": "S"}}),
            ("attribute_type(pk, :t)", {":t": {"S": "S"}}),
            ("size(pk) > :n AND attribute_exists(pk)", {":n": N}),
        ],
    )
    def test_accepted_shapes(self, dynamodb_client, table, condition, values):
        # The request passes validation; whether the condition then holds on
        # the item is a separate matter.
        try:
            _put_cond(dynamodb_client, table, condition, values)()
        except ClientError as exc:
            assert exc.response["Error"]["Code"] == "ConditionalCheckFailedException", exc.response["Error"]

    def test_transaction_and_read_apis_carry_their_own_prefixes(self, dynamodb_client, table):
        _expect(
            lambda: dynamodb_client.transact_write_items(TransactItems=[
                {"ConditionCheck": {"TableName": table, "Key": KEY, "ConditionExpression": "ATTRIBUTE_EXISTS(x)"}}]),
            C + INVALID_FN.format("ATTRIBUTE_EXISTS"),
        )
        _expect(lambda: dynamodb_client.query(TableName=table, KeyConditionExpression="pk = :p",
                                              FilterExpression="ATTRIBUTE_EXISTS(x)", ExpressionAttributeValues={":p": S}),
                F + INVALID_FN.format("ATTRIBUTE_EXISTS"))
        _expect(lambda: dynamodb_client.scan(TableName=table, FilterExpression="foo(pk)"), F + INVALID_FN.format("foo"))
        _expect(lambda: dynamodb_client.query(TableName=table, KeyConditionExpression="pk = :p AND Begins_With(sk, :v)",
                                              ExpressionAttributeValues={":p": S, ":v": S}),
                K + INVALID_FN.format("Begins_With"))


class TestReadExpressionReferences:
    """Query and Scan check their expressions before the table, on an empty table too."""

    @pytest.fixture(scope="class")
    def empty(self, dynamodb_client):
        with scoped_table(dynamodb_client) as name:
            yield name

    def test_scan_and_query_filters(self, dynamodb_client, empty):
        _expect(lambda: dynamodb_client.scan(TableName=empty, FilterExpression="x = :q"), F + UNDEF_VALUE.format(":q"))
        _expect(lambda: dynamodb_client.scan(TableName=empty, FilterExpression="#n = :q", ExpressionAttributeValues={":q": S}),
                F + UNDEF_NAME.format("#n"))
        _expect(lambda: dynamodb_client.query(TableName=empty, KeyConditionExpression="pk = :p", FilterExpression="x = :q",
                                              ExpressionAttributeValues={":p": S}), F + UNDEF_VALUE.format(":q"))

    def test_key_condition_references_carry_the_prefix(self, dynamodb_client, empty):
        _expect(lambda: dynamodb_client.query(TableName=empty, KeyConditionExpression="pk = :p"), K + UNDEF_VALUE.format(":p"))
        _expect(lambda: dynamodb_client.query(TableName=empty, KeyConditionExpression="#n = :p", ExpressionAttributeValues={":p": S}),
                K + UNDEF_NAME.format("#n"))
        _expect(lambda: dynamodb_client.query(TableName=empty, KeyConditionExpression="pk = :p AND #n = :q",
                                              ExpressionAttributeValues={":p": S}), K + UNDEF_NAME.format("#n"))

    def test_filter_then_key_condition_then_projection(self, dynamodb_client, empty):
        q = dynamodb_client.query
        _expect(lambda: q(TableName=empty, KeyConditionExpression="pk = :p", FilterExpression="x = :q"),
                F + UNDEF_VALUE.format(":q"))
        _expect(lambda: q(TableName=empty, KeyConditionExpression="#n = :p", FilterExpression="#m = :p",
                          ExpressionAttributeValues={":p": S}), F + UNDEF_NAME.format("#m"))
        _expect(lambda: q(TableName=empty, KeyConditionExpression="pk = :p", ProjectionExpression="#p"),
                K + UNDEF_VALUE.format(":p"))
        _expect(lambda: q(TableName=empty, KeyConditionExpression="pk = :p", FilterExpression="x = :q", ProjectionExpression="#p",
                          ExpressionAttributeValues={":p": S}), F + UNDEF_VALUE.format(":q"))
        _expect(lambda: q(TableName=empty, KeyConditionExpression="zz = :p", ProjectionExpression="#p",
                          ExpressionAttributeValues={":p": S}), PR + UNDEF_NAME.format("#p"))
        _expect(lambda: q(TableName=empty, KeyConditionExpression="zz = :p", FilterExpression="x = :q",
                          ExpressionAttributeValues={":p": S}), F + UNDEF_VALUE.format(":q"))
        _expect_prefix(lambda: q(TableName=empty, KeyConditionExpression="pk = :p", FilterExpression="x = "), F)
        _expect(lambda: q(TableName=empty, KeyConditionExpression="pk = ", FilterExpression="x = :q"), F + UNDEF_VALUE.format(":q"))
        _expect(lambda: q(TableName=empty, KeyConditionExpression="pk = :p", ProjectionExpression="a,,b"), K + UNDEF_VALUE.format(":p"))
        _expect(lambda: dynamodb_client.scan(TableName=empty, FilterExpression="x = :q", ProjectionExpression="#p"),
                F + UNDEF_VALUE.format(":q"))
        _expect_prefix(lambda: dynamodb_client.scan(TableName=empty, FilterExpression="x = ", ProjectionExpression="#p"), F)

    def test_unused_before_the_key_schema_match(self, dynamodb_client, empty):
        _expect_unused(lambda: dynamodb_client.query(TableName=empty, KeyConditionExpression="zz = :p",
                                                      ExpressionAttributeValues={":p": S, ":u": S}), "Values", {":u"})

    def test_everything_before_the_table_lookup(self, dynamodb_client, missing_table):
        _expect(lambda: dynamodb_client.query(TableName=missing_table, KeyConditionExpression="pk = :p", FilterExpression="x = :q",
                                              ExpressionAttributeValues={":p": S}), F + UNDEF_VALUE.format(":q"))
        _expect(lambda: dynamodb_client.query(TableName=missing_table, KeyConditionExpression="pk = :p", ProjectionExpression="#p",
                                              ExpressionAttributeValues={":p": S}), PR + UNDEF_NAME.format("#p"))
        _expect(lambda: dynamodb_client.query(TableName=missing_table, KeyConditionExpression="#n = :p",
                                              ExpressionAttributeValues={":p": S}), K + UNDEF_NAME.format("#n"))
        _expect(lambda: dynamodb_client.scan(TableName=missing_table, FilterExpression="x = :q"), F + UNDEF_VALUE.format(":q"))
        _expect(lambda: dynamodb_client.get_item(TableName=missing_table, Key=KEY, ProjectionExpression="#p"),
                PR + UNDEF_NAME.format("#p"))
        _expect_unused(lambda: dynamodb_client.scan(TableName=missing_table, FilterExpression="x = :v",
                                                     ExpressionAttributeValues={":v": N, ":u": N}), "Values", {":u"})
        _expect_unused(lambda: dynamodb_client.query(TableName=missing_table, KeyConditionExpression="pk = :p",
                                                      ExpressionAttributeValues={":p": S, ":u": N}), "Values", {":u"})

    def test_parameter_checks_come_before_the_expressions(self, dynamodb_client, dynamodb_client_no_validation, empty):
        _expect(lambda: dynamodb_client_no_validation.scan(TableName=empty, FilterExpression="#n = :q",
                                                           ExpressionAttributeValues={":q": S}, Limit=0),
                P + "Value '0' at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1")
        _expect(lambda: dynamodb_client.scan(TableName=empty, FilterExpression="x = :q", Segment=0),
                "The TotalSegments parameter is required but was not present in the request when Segment parameter is present")
        _expect(lambda: dynamodb_client.scan(TableName=empty, ProjectionExpression="#p", ExpressionAttributeValues={":u": N}),
                "ExpressionAttributeValues can only be specified when using expressions: FilterExpression is null")


class TestKeyShapeMessages:
    BAD_TYPE = {"pk": {"N": "1"}}
    MISSING = {"zz": {"S": "x"}}
    EXTRA = {"pk": {"S": "x"}, "zz": {"S": "x"}}

    @pytest.mark.parametrize("key", [BAD_TYPE, MISSING, EXTRA], ids=["type", "missing", "extra"])
    def test_key_shaped_apis(self, dynamodb_client, table, key):
        _expect(lambda: dynamodb_client.get_item(TableName=table, Key=key), KEY_SCHEMA)
        _expect(lambda: dynamodb_client.delete_item(TableName=table, Key=key), KEY_SCHEMA)
        _expect(lambda: dynamodb_client.update_item(TableName=table, Key=key, UpdateExpression="SET a = :v",
                                                    ExpressionAttributeValues={":v": N}), KEY_SCHEMA)
        _expect(lambda: dynamodb_client.batch_get_item(RequestItems={table: {"Keys": [key]}}), KEY_SCHEMA)
        _expect(lambda: dynamodb_client.batch_write_item(RequestItems={table: [{"DeleteRequest": {"Key": key}}]}), KEY_SCHEMA)

    def test_missing_range_key(self, dynamodb_client, range_table):
        hash_only = {"pk": {"S": "x"}}
        _expect(lambda: dynamodb_client.get_item(TableName=range_table, Key=hash_only), KEY_SCHEMA)
        _expect(lambda: dynamodb_client.update_item(TableName=range_table, Key=hash_only, UpdateExpression="SET a = :v",
                                                    ExpressionAttributeValues={":v": N}), KEY_SCHEMA)
        _expect(lambda: dynamodb_client.get_item(TableName=range_table, Key={"pk": {"S": "x"}, "sk": {"N": "1"}}), KEY_SCHEMA)
        _expect(lambda: dynamodb_client.batch_write_item(RequestItems={range_table: [{"PutRequest": {"Item": hash_only}}]}), KEY_SCHEMA)
        _expect(lambda: dynamodb_client.batch_write_item(RequestItems={range_table: [{"DeleteRequest": {"Key": hash_only}}]}), KEY_SCHEMA)
        _expect(lambda: dynamodb_client.put_item(TableName=range_table, Item=hash_only),
                "One or more parameter values were invalid: Missing the key sk in the item")
        _expect(lambda: dynamodb_client.put_item(TableName=range_table, Item={"pk": {"S": "x"}, "sk": {"N": "1"}}),
                "One or more parameter values were invalid: Type mismatch for key sk expected: S actual: N")

    def test_batch_write_put_uses_the_key_message_put_item_names_the_attribute(self, dynamodb_client, table):
        bwi = dynamodb_client.batch_write_item
        _expect(lambda: bwi(RequestItems={table: [{"PutRequest": {"Item": self.MISSING}}]}), KEY_SCHEMA)
        _expect(lambda: bwi(RequestItems={table: [{"PutRequest": {"Item": self.BAD_TYPE}}]}), KEY_SCHEMA)
        _expect(lambda: dynamodb_client.put_item(TableName=table, Item=self.MISSING),
                "One or more parameter values were invalid: Missing the key pk in the item")
        _expect(lambda: dynamodb_client.put_item(TableName=table, Item=self.BAD_TYPE),
                "One or more parameter values were invalid: Type mismatch for key pk expected: S actual: N")

    def test_empty_and_oversize_key_values_keep_their_messages(self, dynamodb_client, table):
        _expect(lambda: dynamodb_client.get_item(TableName=table, Key={"pk": {"S": ""}}),
                "One or more parameter values are not valid. The AttributeValue for a key attribute cannot contain an empty string value. Key: pk")
        _expect(lambda: dynamodb_client.delete_item(TableName=table, Key={"pk": {"S": "x" * 3000}}),
                "One or more parameter values were invalid: Size of hashkey has exceeded the maximum size limit of2048 bytes")


class TestBatchWriteItemCollectionMetrics:
    def test_empty_map_when_requested_without_an_lsi(self, dynamodb_client, table):
        resp = dynamodb_client.batch_write_item(RequestItems={table: [{"PutRequest": {"Item": {"pk": {"S": "icm"}}}}]},
                                                ReturnItemCollectionMetrics="SIZE")
        assert resp["ItemCollectionMetrics"] == {}
        assert resp["UnprocessedItems"] == {}

    def test_absent_when_not_requested(self, dynamodb_client, table):
        resp = dynamodb_client.batch_write_item(RequestItems={table: [{"PutRequest": {"Item": {"pk": {"S": "icm2"}}}}]})
        assert "ItemCollectionMetrics" not in resp

    def test_only_lsi_tables_appear(self, dynamodb_client, table, lsi_table):
        resp = dynamodb_client.batch_write_item(
            RequestItems={table: [{"PutRequest": {"Item": {"pk": {"S": "b"}}}}],
                          lsi_table: [{"PutRequest": {"Item": {"pk": {"S": "b"}, "sk": {"S": "1"}}}}]},
            ReturnItemCollectionMetrics="SIZE",
        )
        assert set(resp["ItemCollectionMetrics"]) == {lsi_table}
        [metric] = resp["ItemCollectionMetrics"][lsi_table]
        assert metric["ItemCollectionKey"] == {"pk": {"S": "b"}}
        assert metric["SizeEstimateRangeGB"] == [0.0, 1.0]
        resp = dynamodb_client.batch_write_item(
            RequestItems={lsi_table: [{"DeleteRequest": {"Key": {"pk": {"S": "zz"}, "sk": {"S": "1"}}}}]},
            ReturnItemCollectionMetrics="SIZE",
        )
        assert set(resp["ItemCollectionMetrics"]) == {lsi_table}

    def test_single_item_apis_omit_the_member_without_an_lsi(self, dynamodb_client, table, lsi_table):
        assert "ItemCollectionMetrics" not in dynamodb_client.put_item(
            TableName=table, Item={"pk": {"S": "icm3"}}, ReturnItemCollectionMetrics="SIZE")
        assert "ItemCollectionMetrics" not in dynamodb_client.update_item(
            TableName=table, Key={"pk": {"S": "icm3"}}, UpdateExpression="SET a = :v",
            ExpressionAttributeValues={":v": N}, ReturnItemCollectionMetrics="SIZE")
        assert "ItemCollectionMetrics" not in dynamodb_client.delete_item(
            TableName=table, Key={"pk": {"S": "icm3"}}, ReturnItemCollectionMetrics="SIZE")
        resp = dynamodb_client.put_item(TableName=lsi_table, Item={"pk": {"S": "b"}, "sk": {"S": "2"}},
                                        ReturnItemCollectionMetrics="SIZE")
        assert resp["ItemCollectionMetrics"]["ItemCollectionKey"] == {"pk": {"S": "b"}}
