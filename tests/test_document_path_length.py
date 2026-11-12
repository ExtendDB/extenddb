# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Length limit of a document path in an expression.

Amazon DynamoDB refuses a document path of more than 32 elements (each
attribute name and each list index counts as one) in every expression kind,
with ValidationException ``Invalid <Kind>: The document path has too many
nesting levels; nesting levels: <n>``, naming the first such path in the
expression. A syntax error anywhere in the expression is reported instead.
The service checks a path's ``#name`` references before its length, path by
path in the order the paths appear, and finishes the checks of the
FilterExpression before the ProjectionExpression and of the
ConditionExpression before the UpdateExpression. Measured on 2026-10-26 at
02:26, 02:29, 03:38, 03:39, 04:21, 04:40, 04:41, 04:43, 05:15 and 05:20 UTC
(us-east-1), each case twice.

PutItem, UpdateItem and DeleteItem prefix every expression error with
``1 validation error detected: ``; the other operations do not. That prefix
is a separate change, so for those three operations the tests accept the
message with or without it.

Dual-target against Amazon DynamoDB and extenddb.
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table

N = {"#m": "m", "#k": "k"}
S = {":s": {"S": "s"}}
PREFIX = "1 validation error detected: "


def too_long(kind: str, n: int) -> str:
    return f"Invalid {kind}: The document path has too many nesting levels; nesting levels: {n}"


def p(n: int) -> str:
    """A path of n elements: #m then n - 1 times .#k."""
    return "#m" + ".#k" * (n - 1)


def nmap(wrappers: int) -> dict:
    value = {"S": "leaf"}
    for _ in range(wrappers):
        value = {"M": {"k": value}}
    return value


def _error(call) -> tuple[str, str]:
    with pytest.raises(ClientError) as exc_info:
        call()
    err = exc_info.value.response["Error"]
    return err["Code"], err["Message"]


def _single_item_error(call) -> tuple[str, str]:
    code, message = _error(call)
    return code, message.removeprefix(PREFIX)


@pytest.fixture(scope="module")
def table(dynamodb_client):
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


@pytest.fixture
def key(dynamodb_client, table, request):
    k = {"pk": {"S": "plen"}, "sk": {"S": request.node.name}}
    dynamodb_client.put_item(TableName=table, Item={**k, "x": {"S": "x"}, "m": nmap(31)})
    return k


def _stored(client, table, key):
    return client.get_item(TableName=table, Key=key, ConsistentRead=True)["Item"]


def _update(client, table, key, expr, names=N, values=S, **kw):
    args = {"TableName": table, "Key": key, "UpdateExpression": expr, **kw}
    if names:
        args["ExpressionAttributeNames"] = names
    if values:
        args["ExpressionAttributeValues"] = values
    return client.update_item(**args)


# (case, update expression, names, values, elements reported)
UPDATE_REFUSED = [
    ("set_33", f"SET {p(33)} = :s", N, S, 33),
    ("set_34", f"SET {p(34)} = :s", N, S, 34),
    ("set_40", f"SET {p(40)} = :s", N, S, 40),
    ("set_index_33", "SET #m" + "[0]" * 32 + " = :s", {"#m": "m"}, S, 33),
    ("set_mixed_33", "SET #m" + ".#k[0]" * 16 + " = :s", N, S, 33),
    ("first_of_33_and_40", f"SET {p(33)} = :s, {p(40)} = :s", N, S, 33),
    ("first_of_40_and_33", f"SET {p(40)} = :s, {p(33)} = :s", N, S, 40),
    ("value_side", f"SET #x = {p(33)}", {**N, "#x": "x"}, None, 33),
    ("if_not_exists", f"SET #x = if_not_exists({p(33)}, :s)", {**N, "#x": "x"}, S, 33),
    ("list_append", f"SET #x = list_append({p(33)}, :l)", {**N, "#x": "x"}, {":l": {"L": []}}, 33),
    ("remove", f"REMOVE {p(33)}", N, None, 33),
    ("delete", f"DELETE {p(33)} :ss", N, {":ss": {"SS": ["a"]}}, 33),
    ("add", f"ADD {p(33)} :one", N, {":one": {"N": "1"}}, 33),
    ("undefined_name_after", f"SET {p(33)} = :s, #zz = :s", N, S, 33),
    ("undefined_value", f"SET {p(33)} = :nope", N, S, 33),
    ("unused_value", f"SET {p(33)} = :s", N, {**S, ":unused": {"S": "u"}}, 33),
    ("unused_name", f"SET {p(33)} = :s", {**N, "#unused": "u"}, S, 33),
    ("overlapping_paths", f"SET {p(33)} = :s, {p(33)} = :s", N, S, 33),
]


@pytest.mark.parametrize(("case", "expr", "names", "values", "n"), UPDATE_REFUSED,
                         ids=[c[0] for c in UPDATE_REFUSED])
def test_update_expression_path_past_the_limit(dynamodb_client, table, key, case, expr, names, values, n):
    assert _single_item_error(lambda: _update(dynamodb_client, table, key, expr, names, values)) == (
        "ValidationException", too_long("UpdateExpression", n))
    assert _stored(dynamodb_client, table, key)["x"] == {"S": "x"}


def test_update_expression_path_at_the_limit(dynamodb_client, table, key):
    _update(dynamodb_client, table, key, f"SET {p(32)} = :s")
    item = _stored(dynamodb_client, table, key)
    leaf = item["m"]
    for _ in range(31):
        leaf = leaf["M"]["k"]
    assert leaf == {"S": "s"}


def test_a_syntax_error_after_a_long_path_comes_first(dynamodb_client, table, key):
    # The service answers `Syntax error; token: "<EOF>", near: ","`; ExtendDB
    # words its syntax errors differently, so only the kind of error is compared.
    code, message = _single_item_error(lambda: _update(dynamodb_client, table, key, f"SET {p(33)} = :s,"))
    assert code == "ValidationException"
    assert message.startswith("Invalid UpdateExpression: "), message
    assert "nesting levels" not in message, message


def test_condition_expression_on_update_item(dynamodb_client, table, key):
    assert _single_item_error(lambda: _update(
        dynamodb_client, table, key, "SET #x = :s", names={**N, "#x": "x"},
        values={**S, ":z": {"N": "0"}}, ConditionExpression=f"size({p(33)}) > :z",
    )) == ("ValidationException", too_long("ConditionExpression", 33))


def test_condition_expression_on_put_item(dynamodb_client, table, key):
    assert _single_item_error(lambda: dynamodb_client.put_item(
        TableName=table, Item={**key, "x": {"S": "y"}},
        ConditionExpression=f"attribute_not_exists({p(33)})", ExpressionAttributeNames=N,
    )) == ("ValidationException", too_long("ConditionExpression", 33))
    assert _stored(dynamodb_client, table, key)["x"] == {"S": "x"}


def test_condition_expression_on_delete_item(dynamodb_client, table, key):
    assert _single_item_error(lambda: dynamodb_client.delete_item(
        TableName=table, Key=key, ConditionExpression=f"attribute_exists({p(33)})", ExpressionAttributeNames=N,
    )) == ("ValidationException", too_long("ConditionExpression", 33))
    assert "Item" in dynamodb_client.get_item(TableName=table, Key=key, ConsistentRead=True)


def test_condition_expression_at_the_limit(dynamodb_client, table, key):
    # size() of the 32-element path is the 4 characters of the leaf string.
    _update(dynamodb_client, table, key, "SET #x = :s", names={**N, "#x": "x"},
            values={**S, ":four": {"N": "4"}},
            ConditionExpression=f"attribute_exists(#x) AND size({p(32)}) = :four")
    assert _stored(dynamodb_client, table, key)["x"] == {"S": "s"}


UNDEFINED_NAME = "An expression attribute name used in the document path is not defined; attribute name: #zz"
UNDEFINED_VALUE = "An expression attribute value used in expression is not defined; attribute value: :nope"
X = {**N, "#x": "x"}


# (case, update expression, names, values, condition, the message without its prefix)
SINGLE_ITEM_ORDER = [
    ("name_in_earlier_path", f"SET #zz = :s, {p(33)} = :s", N, S, None, f"Invalid UpdateExpression: {UNDEFINED_NAME}"),
    ("name_first_in_long_path", "SET #zz" + ".#k" * 32 + " = :s", N, S, None,
     f"Invalid UpdateExpression: {UNDEFINED_NAME}"),
    ("name_last_in_long_path", f"SET {p(32)}.#zz = :s", N, S, None, f"Invalid UpdateExpression: {UNDEFINED_NAME}"),
    ("condition_name_before_update", f"SET {p(33)} = :s", N, S, "attribute_exists(#zz)",
     f"Invalid ConditionExpression: {UNDEFINED_NAME}"),
    ("condition_value_before_update", f"SET {p(33)} = :s", X, S, "#x = :nope",
     f"Invalid ConditionExpression: {UNDEFINED_VALUE}"),
    ("condition_between_before_update", f"SET {p(33)} = :s", X, {**S, ":hi": {"N": "5"}, ":lo": {"N": "1"}},
     "#x BETWEEN :hi AND :lo",
     "Invalid ConditionExpression: The BETWEEN operator requires upper bound to be greater than or equal to "
     "lower bound; lower bound operand: AttributeValue: {N:5}, upper bound operand: AttributeValue: {N:1}"),
    ("condition_operand_before_update", f"SET {p(33)} = :s", X, {**S, ":n": {"N": "1"}}, "begins_with(#x, :n)",
     "Invalid ConditionExpression: Incorrect operand type for operator or function; operator or function: "
     "begins_with, operand type: N"),
]


@pytest.mark.parametrize(("case", "expr", "names", "values", "condition", "message"), SINGLE_ITEM_ORDER,
                         ids=[c[0] for c in SINGLE_ITEM_ORDER])
def test_update_item_reports_an_earlier_fault_first(dynamodb_client, table, key, case, expr, names, values,
                                                    condition, message):
    kw = {"ConditionExpression": condition} if condition else {}
    assert _single_item_error(lambda: _update(dynamodb_client, table, key, expr, names, values, **kw)) == (
        "ValidationException", message)


NESTING = "Nesting Levels have exceeded supported limits: Attributes in the item have nested levels beyond supported limit"
BETWEEN = ("The BETWEEN operator requires upper bound to be greater than or equal to lower bound; "
           "lower bound operand: AttributeValue: {N:5}, upper bound operand: AttributeValue: {N:1}")
BETWEEN_TYPES = ("The BETWEEN operator requires same data type for lower and upper bounds; "
                 "lower bound operand: AttributeValue: {N:1}, upper bound operand: AttributeValue: {S:a}")


def deep_value() -> dict:
    return nmap(32)


def test_a_value_nested_too_deep_is_reported_first(dynamodb_client, table, key):
    assert _single_item_error(lambda: _update(dynamodb_client, table, key, f"SET {p(33)} = :d",
                                              values={":d": deep_value()})) == ("ValidationException", NESTING)
    assert _single_item_error(lambda: dynamodb_client.put_item(
        TableName=table, Item={**key, "x": {"S": "y"}},
        ConditionExpression=f"attribute_exists({p(33)}) OR #m <> :d", ExpressionAttributeNames=N,
        ExpressionAttributeValues={":d": deep_value()},
    )) == ("ValidationException", NESTING)
    assert _single_item_error(lambda: _update(dynamodb_client, table, key, "SET #m = :d,", names={"#m": "m"},
                                              values={":d": deep_value()})) == ("ValidationException", NESTING)


MIXING = "Can not use both expression and non-expression parameters in the same request: "
BAD_NUMBER = "The parameter cannot be converted to a numeric value: abc"


# (case, extra UpdateItem arguments, non-expression and expression parameters named)
MIXED = [
    ("attribute_updates", {"UpdateExpression": "SET #m = :d",
                           "AttributeUpdates": {"y": {"Action": "PUT", "Value": {"S": "v"}}}},
     "{AttributeUpdates}", "{UpdateExpression}"),
    ("expected", {"UpdateExpression": "SET #m = :d", "Expected": {"x": {"Exists": False}}},
     "{Expected}", "{UpdateExpression}"),
    ("conditional_operator", {"UpdateExpression": "SET #m = :d", "ConditionalOperator": "AND"},
     "{ConditionalOperator}", "{UpdateExpression}"),
    ("attribute_updates_and_condition", {"ConditionExpression": "#m <> :d",
                                         "AttributeUpdates": {"y": {"Action": "PUT", "Value": {"S": "v"}}}},
     "{AttributeUpdates}", "{ConditionExpression}"),
]


@pytest.mark.parametrize(("case", "args", "legacy", "expression"), MIXED, ids=[c[0] for c in MIXED])
def test_mixed_parameters_come_before_a_value_nested_too_deep(dynamodb_client, table, key, case, args, legacy,
                                                               expression):
    assert _single_item_error(lambda: dynamodb_client.update_item(
        TableName=table, Key=key, ExpressionAttributeNames={"#m": "m"},
        ExpressionAttributeValues={":d": deep_value()}, **args,
    )) == ("ValidationException",
           f"{MIXING}Non-expression parameters: {legacy} Expression parameters: {expression}")


@pytest.mark.parametrize("bad", [{"N": "abc"}, {"M": {"z": {"N": "abc"}}}], ids=["top_level", "nested"])
def test_an_invalid_number_in_the_item_comes_before_a_value_nested_too_deep(dynamodb_client, table, key, bad):
    assert _single_item_error(lambda: dynamodb_client.put_item(
        TableName=table, Item={**key, "y": bad}, ConditionExpression="#m <> :d",
        ExpressionAttributeNames={"#m": "m"}, ExpressionAttributeValues={":d": deep_value()},
    )) == ("ValidationException", BAD_NUMBER)


REVERSED = {":hi": {"N": "5"}, ":lo": {"N": "1"}}

# (case, Item attributes, condition, names, values, message): the Item's
# numbers and nesting come before the condition, with or without
# ExpressionAttributeValues (measured 2026-11-12, each request twice).
ITEM_BEFORE_CONDITION = [
    ("number_before_the_length", {"n": {"N": "abc"}}, f"attribute_exists({p(33)})", N, None, BAD_NUMBER),
    ("nesting_before_the_length", {"d": nmap(33)}, f"attribute_exists({p(33)})", N, None, NESTING),
    ("nesting_before_the_between_bounds", {"d": nmap(33)}, "a BETWEEN :hi AND :lo", None, REVERSED, NESTING),
]


def _put_body(table, key, attrs, condition, names, values):
    body = {"TableName": table, "Item": {**key, **attrs}, "ConditionExpression": condition}
    if names:
        body["ExpressionAttributeNames"] = names
    if values:
        body["ExpressionAttributeValues"] = values
    return body


@pytest.mark.parametrize(("case", "attrs", "condition", "names", "values", "message"), ITEM_BEFORE_CONDITION,
                         ids=[c[0] for c in ITEM_BEFORE_CONDITION])
def test_put_item_checks_the_item_before_the_condition(dynamodb_client, table, key, case, attrs, condition, names,
                                                       values, message):
    body = _put_body(table, key, attrs, condition, names, values)
    assert _single_item_error(lambda: dynamodb_client.put_item(**body)) == ("ValidationException", message)


@pytest.mark.parametrize(("case", "attrs", "condition", "names", "values", "message"), ITEM_BEFORE_CONDITION[1:],
                         ids=[c[0] for c in ITEM_BEFORE_CONDITION[1:]])
def test_transact_write_items_put_checks_the_item_nesting_before_the_condition(
        dynamodb_client, table, key, case, attrs, condition, names, values, message):
    body = _put_body(table, key, attrs, condition, names, values)
    assert _error(lambda: dynamodb_client.transact_write_items(TransactItems=[{"Put": body}])) == (
        "ValidationException", message)


@pytest.mark.parametrize(("lower", "upper", "message"), [
    ({"N": "5"}, {"N": "1"}, BETWEEN),
    ({"N": "1"}, {"S": "a"}, BETWEEN_TYPES),
], ids=["lower_above_upper", "different_types"])
def test_between_bounds_are_checked_whatever_the_item(dynamodb_client, table, key, lower, upper, message):
    bounds = {":a": lower, ":b": upper, ":s": {"S": "s"}}
    missing = {"pk": {"S": "plen"}, "sk": {"S": "between-no-item"}}
    for k, cond in ((missing, "#x BETWEEN :a AND :b"), (key, "#x BETWEEN :a AND :b"),
                    (key, "attribute_exists(#n) AND #x BETWEEN :a AND :b")):
        names = {"#x": "x", "#n": "nope"} if "#n" in cond else {"#x": "x"}
        assert _single_item_error(lambda: _update(dynamodb_client, table, k, "SET #x = :s", names=names,
                                                  values=bounds, ConditionExpression=cond)) == (
            "ValidationException", f"Invalid ConditionExpression: {message}"), cond
    assert "Item" not in dynamodb_client.get_item(TableName=table, Key=missing, ConsistentRead=True)
    for partition in ("plen-empty", "plen"):
        assert _error(lambda: dynamodb_client.query(
            TableName=table, KeyConditionExpression="pk = :p", FilterExpression="#x BETWEEN :a AND :b",
            ExpressionAttributeNames={"#x": "x"},
            ExpressionAttributeValues={":p": {"S": partition}, ":a": lower, ":b": upper},
        )) == ("ValidationException", f"Invalid FilterExpression: {message}"), partition


def test_legacy_expected_between_bounds_out_of_order_are_refused(dynamodb_client, table, key):
    # The service words this `One or more parameter values were invalid: The
    # BETWEEN condition was provided a range where the lower bound is greater
    # than the upper bound` (measured 2026-11-12); ExtendDB answers with the
    # ConditionExpression sentence, so only the class and the operator are
    # compared.
    expected = {"n": {"ComparisonOperator": "BETWEEN", "AttributeValueList": [{"N": "5"}, {"N": "1"}]}}
    missing = {"pk": {"S": "plen"}, "sk": {"S": "between-expected-no-item"}}
    for k in (missing, key):
        code, message = _single_item_error(lambda: dynamodb_client.put_item(
            TableName=table, Item={**k, "n": {"N": "3"}}, Expected=expected))
        assert (code, "BETWEEN" in message) == ("ValidationException", True), message


def test_projection_names_come_before_the_length(dynamodb_client, table, key):
    for projection in (f"#zz, {p(33)}", "#zz" + ".#k" * 32, f"{p(32)}.#zz"):
        assert _error(lambda: dynamodb_client.get_item(
            TableName=table, Key=key, ProjectionExpression=projection, ExpressionAttributeNames=N,
        )) == ("ValidationException", f"Invalid ProjectionExpression: {UNDEFINED_NAME}"), projection
    assert _error(lambda: dynamodb_client.transact_get_items(TransactItems=[
        {"Get": {"TableName": table, "Key": key, "ProjectionExpression": "#zz"}},
        {"Get": {"TableName": table, "Key": key, "ProjectionExpression": p(33), "ExpressionAttributeNames": N}},
    ])) == ("ValidationException", f"Invalid ProjectionExpression: {UNDEFINED_NAME}")


OVERLAP_K = ("Invalid ProjectionExpression: Two document paths overlap with each other; must remove or rewrite "
             "one of these paths; path one: [k], path two: [k]")

# (case, projection, message): the service checks the paths one after another,
# each path's length before its overlap with an earlier path (measured
# 2026-11-12, each request twice).
PROJECTION_ORDER = [
    ("overlap_before_a_later_length", f"#k, #k, {p(33)}", OVERLAP_K),
    ("length_before_a_later_overlap", f"{p(33)}, #k, #k", too_long("ProjectionExpression", 33)),
    ("length_between_overlapping_paths", f"#k, {p(33)}, #k", too_long("ProjectionExpression", 33)),
    ("length_before_its_own_overlap", f"#m, {p(33)}", too_long("ProjectionExpression", 33)),
    ("overlap_after_a_path_at_the_limit", f"{p(32)}, #k, #k", OVERLAP_K),
]


@pytest.mark.parametrize(("case", "projection", "message"), PROJECTION_ORDER, ids=[c[0] for c in PROJECTION_ORDER])
def test_projection_paths_are_checked_one_after_another(dynamodb_client, table, key, case, projection, message):
    assert _error(lambda: dynamodb_client.get_item(
        TableName=table, Key=key, ProjectionExpression=projection, ExpressionAttributeNames=N,
    )) == ("ValidationException", message)


def test_projection_overlap_before_a_later_length_on_every_read(dynamodb_client, table, key):
    projection = f"#k, #k, {p(33)}"
    reads = {
        "scan": lambda: dynamodb_client.scan(
            TableName=table, Limit=1, ProjectionExpression=projection, ExpressionAttributeNames=N),
        "query": lambda: _query(dynamodb_client, table, ProjectionExpression=projection),
        "batch_get_item": lambda: dynamodb_client.batch_get_item(RequestItems={table: {
            "Keys": [key], "ProjectionExpression": projection, "ExpressionAttributeNames": N}}),
        "transact_get_items": lambda: dynamodb_client.transact_get_items(TransactItems=[{"Get": {
            "TableName": table, "Key": key, "ProjectionExpression": projection, "ExpressionAttributeNames": N}}]),
    }
    for operation, read in reads.items():
        assert _error(read) == ("ValidationException", OVERLAP_K), operation


def test_transact_write_items_update(dynamodb_client, table, key):
    assert _error(lambda: dynamodb_client.transact_write_items(TransactItems=[{"Update": {
        "TableName": table, "Key": key, "UpdateExpression": f"SET {p(33)} = :s",
        "ExpressionAttributeNames": N, "ExpressionAttributeValues": S,
    }}])) == ("ValidationException", too_long("UpdateExpression", 33))


# (case, update expression, names, values): one UpdateExpression defect each,
# beside a condition with an undefined name. The service answers the
# condition first, in a transaction as in UpdateItem (measured 2026-11-12,
# each request twice).
TRANSACTION_UPDATE_DEFECTS = [
    ("length", f"SET {p(33)} = :s", N, S),
    ("syntax", "SET a = = :s", None, S),
    ("undefined_name", "SET #nope = :s", None, S),
    ("undefined_value", "SET a = :nope", None, None),
    ("overlap", "SET a = :s, a.b = :s", None, S),
    ("reserved_word", "SET name = :s", None, S),
]


@pytest.mark.parametrize(("case", "expr", "names", "values"), TRANSACTION_UPDATE_DEFECTS,
                         ids=[c[0] for c in TRANSACTION_UPDATE_DEFECTS])
def test_transact_write_items_update_checks_the_condition_first(dynamodb_client, table, key, case, expr, names,
                                                                values):
    update = {"TableName": table, "Key": key, "UpdateExpression": expr, "ConditionExpression": "attribute_exists(#zz)"}
    if names:
        update["ExpressionAttributeNames"] = names
    if values:
        update["ExpressionAttributeValues"] = values
    assert _error(lambda: dynamodb_client.transact_write_items(TransactItems=[{"Update": update}])) == (
        "ValidationException", f"Invalid ConditionExpression: {UNDEFINED_NAME}")


def test_transact_write_items_update_checks_the_between_bounds_first(dynamodb_client, table, key):
    assert _error(lambda: dynamodb_client.transact_write_items(TransactItems=[{"Update": {
        "TableName": table, "Key": key, "UpdateExpression": f"SET {p(33)} = :s",
        "ConditionExpression": "#x BETWEEN :hi AND :lo", "ExpressionAttributeNames": X,
        "ExpressionAttributeValues": {**S, ":hi": {"N": "5"}, ":lo": {"N": "1"}},
    }}])) == ("ValidationException", f"Invalid ConditionExpression: {BETWEEN}")


def test_transact_write_items_condition_check(dynamodb_client, table, key):
    assert _error(lambda: dynamodb_client.transact_write_items(TransactItems=[{"ConditionCheck": {
        "TableName": table, "Key": key, "ConditionExpression": f"attribute_exists({p(33)})",
        "ExpressionAttributeNames": N,
    }}])) == ("ValidationException", too_long("ConditionExpression", 33))


def test_transact_write_items_put_condition(dynamodb_client, table, key):
    assert _error(lambda: dynamodb_client.transact_write_items(TransactItems=[{"Put": {
        "TableName": table, "Item": {**key, "x": {"S": "y"}},
        "ConditionExpression": f"attribute_not_exists({p(33)})", "ExpressionAttributeNames": N,
    }}])) == ("ValidationException", too_long("ConditionExpression", 33))
    assert _stored(dynamodb_client, table, key)["x"] == {"S": "x"}


def test_get_item_projection(dynamodb_client, table, key):
    assert _error(lambda: dynamodb_client.get_item(
        TableName=table, Key=key, ProjectionExpression=p(33), ExpressionAttributeNames=N,
    )) == ("ValidationException", too_long("ProjectionExpression", 33))
    assert _error(lambda: dynamodb_client.get_item(
        TableName=table, Key=key, ProjectionExpression="#m" + "[0]" * 32, ExpressionAttributeNames={"#m": "m"},
    )) == ("ValidationException", too_long("ProjectionExpression", 33))


def test_get_item_projection_at_the_limit(dynamodb_client, table, key):
    item = dynamodb_client.get_item(TableName=table, Key=key, ProjectionExpression=p(32),
                                    ExpressionAttributeNames=N)["Item"]
    assert set(item) == {"m"}


def test_batch_get_item_projection(dynamodb_client, table, key):
    assert _error(lambda: dynamodb_client.batch_get_item(RequestItems={table: {
        "Keys": [key], "ProjectionExpression": p(33), "ExpressionAttributeNames": N,
    }})) == ("ValidationException", too_long("ProjectionExpression", 33))


def test_transact_get_items_projection(dynamodb_client, table, key):
    assert _error(lambda: dynamodb_client.transact_get_items(TransactItems=[{"Get": {
        "TableName": table, "Key": key, "ProjectionExpression": p(33), "ExpressionAttributeNames": N,
    }}])) == ("ValidationException", too_long("ProjectionExpression", 33))


def _query(client, table, **kw):
    values = {":p": {"S": "plen"}, **kw.pop("values", {})}
    return client.query(TableName=table, KeyConditionExpression=kw.pop("key", "pk = :p"),
                        ExpressionAttributeNames=N, ExpressionAttributeValues=values, **kw)


def test_query_filter(dynamodb_client, table, key):
    assert _error(lambda: _query(dynamodb_client, table, FilterExpression=f"attribute_exists({p(33)})")) == (
        "ValidationException", too_long("FilterExpression", 33))
    assert _query(dynamodb_client, table, FilterExpression=f"attribute_exists({p(32)})")["Count"] >= 1


def test_query_projection(dynamodb_client, table, key):
    assert _error(lambda: _query(dynamodb_client, table, ProjectionExpression=p(33))) == (
        "ValidationException", too_long("ProjectionExpression", 33))


def test_query_key_condition(dynamodb_client, table, key):
    assert _error(lambda: _query(dynamodb_client, table, key=f"pk = :p AND {p(33)} = :s", values=S)) == (
        "ValidationException", too_long("KeyConditionExpression", 33))
    assert _error(lambda: _query(dynamodb_client, table, key=f"pk = :p AND {p(32)} = :s", values=S)) == (
        "ValidationException",
        "Invalid KeyConditionExpression: KeyConditionExpressions cannot have conditions on nested attributes")


KEY_NAME = f"Invalid KeyConditionExpression: {UNDEFINED_NAME}"
KEY_VALUE = f"Invalid KeyConditionExpression: {UNDEFINED_VALUE}"
KEY_LENGTH = too_long("KeyConditionExpression", 33)

# (case, key condition, names, values defined, message): the names, the
# values and each path's length in one pass in text order, then the
# nested-attribute refusal (measured 2026-11-12, each request twice).
KEY_CONDITION_ORDER = [
    ("name_inside_the_long_path", "pk = :p AND #zz" + ".#k" * 32 + " = :s", {"#k": "k"}, (":p", ":s"), KEY_NAME),
    ("value_in_an_earlier_clause", f"pk = :nope AND {p(33)} = :s", N, (":s",), KEY_VALUE),
    ("length_before_a_later_value", f"{p(33)} = :s AND pk = :nope", N, (":s",), KEY_LENGTH),
    ("name_in_an_earlier_clause", f"#zz = :p AND {p(33)} = :s", N, (":p", ":s"), KEY_NAME),
    ("length_before_a_later_name", f"{p(33)} = :s AND #zz = :p", N, (":p", ":s"), KEY_LENGTH),
    ("length_before_its_value", f"pk = :p AND {p(33)} = :nope", N, (":p",), KEY_LENGTH),
    ("name_at_the_end_of_the_long_path", f"pk = :p AND {p(32)}.#zz = :s", N, (":p", ":s"), KEY_NAME),
    ("name_before_the_nested_refusal", f"#zz = :p AND {p(32)} = :s", N, (":p", ":s"), KEY_NAME),
    ("value_before_the_nested_refusal", f"{p(32)} = :s AND pk = :nope", N, (":s",), KEY_VALUE),
]


@pytest.mark.parametrize(("case", "condition", "names", "values", "message"), KEY_CONDITION_ORDER,
                         ids=[c[0] for c in KEY_CONDITION_ORDER])
def test_key_condition_checks_names_values_and_lengths_in_text_order(dynamodb_client, table, case, condition,
                                                                     names, values, message):
    assert _error(lambda: dynamodb_client.query(
        TableName=table, KeyConditionExpression=condition, ExpressionAttributeNames=names,
        ExpressionAttributeValues={v: {"S": "plen"} for v in values},
    )) == ("ValidationException", message)


def test_query_filter_is_checked_before_the_projection(dynamodb_client, table, key):
    assert _error(lambda: _query(dynamodb_client, table,
                                 FilterExpression=f"attribute_exists(#zz) OR attribute_exists({p(33)})")) == (
        "ValidationException", f"Invalid FilterExpression: {UNDEFINED_NAME}")
    assert _error(lambda: _query(dynamodb_client, table, ProjectionExpression=p(33),
                                 FilterExpression="attribute_exists(#zz)")) == (
        "ValidationException", f"Invalid FilterExpression: {UNDEFINED_NAME}")


def test_scan_filter_is_checked_before_the_projection(dynamodb_client, table, key):
    for flt, names, message in (("attribute_exists(#zz)", N, UNDEFINED_NAME), ("#x = :nope", X, UNDEFINED_VALUE)):
        assert _error(lambda: dynamodb_client.scan(
            TableName=table, Limit=1, ProjectionExpression=p(33), FilterExpression=flt,
            ExpressionAttributeNames=names,
        )) == ("ValidationException", f"Invalid FilterExpression: {message}"), flt


def test_scan_filter_and_projection(dynamodb_client, table, key):
    assert _error(lambda: dynamodb_client.scan(
        TableName=table, Limit=1, FilterExpression=f"{p(33)} = :s",
        ExpressionAttributeNames=N, ExpressionAttributeValues=S,
    )) == ("ValidationException", too_long("FilterExpression", 33))
    assert _error(lambda: dynamodb_client.scan(
        TableName=table, Limit=1, ProjectionExpression=p(33), ExpressionAttributeNames=N,
    )) == ("ValidationException", too_long("ProjectionExpression", 33))
