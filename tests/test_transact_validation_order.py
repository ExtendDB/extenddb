# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Validation order inside TransactGetItems and TransactWriteItems.

When a transaction carries two defects, Amazon DynamoDB reports the earlier
of these phases whichever item carries it:

  1. per item, in order: expression grammar and the semantic walk (a
     `#name` or `:value` that is not defined counts here, not as a
     cancellation reason), unused names and values, then the item's 400 KB
     size, all as a top-level ValidationException;
  2. a table that does not exist, as ResourceNotFoundException;
  3. the 4 MB request total, as a top-level ValidationException;
  4. a key that does not fit the table's key schema, as
     TransactionCanceledException with a ValidationError reason on the first
     offending item and None on every other item, even when several keys
     fail: conditions are not evaluated and runtime update errors are not
     reported when any item fails here;
  5. two operations on one item, as a top-level ValidationException.

Every expected response below was captured from Amazon DynamoDB (issue #108).
Dual-target: runs against the service and against ExtendDB on every backend.
"""

from __future__ import annotations

import uuid

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table

DUPLICATE = "Transaction request cannot include multiple operations on one item"
RESERVED_PROJECTION = (
    "Invalid ProjectionExpression: Attribute name is a reserved keyword; "
    "reserved keyword: COMMENT"
)
UNUSED_NAME = "Value provided in ExpressionAttributeNames unused in expressions: keys: {#unused}"
MISSING_PROJECTION_NAME = (
    "Invalid ProjectionExpression: An expression attribute name used in the "
    "document path is not defined; attribute name: #missing"
)
MISSING_CONDITION_NAME = (
    "Invalid ConditionExpression: An expression attribute name used in the "
    "document path is not defined; attribute name: #missing"
)
MISSING_CONDITION_VALUE = (
    "Invalid ConditionExpression: An expression attribute value used in "
    "expression is not defined; attribute value: :missing"
)
# The service's full text is 'Invalid UpdateExpression: Syntax error; token:
# "<EOF>", near: "+"'. ExtendDB's parser wording differs today (tracked under
# #108 message text); this file asserts the class and prefix, which is what
# the ordering guarantees.
UPDATE_SYNTAX_PREFIX = "Invalid UpdateExpression:"
# A key that does not fit the schema: a Delete, Update, ConditionCheck, or Get
# key gets the first text; a Put item keeps the single-item wording.
KEY_SCHEMA_REASONS = {
    "The provided key element does not match the schema",
    "One or more parameter values were invalid: Type mismatch for key pk expected: S actual: N",
}


@pytest.fixture(scope="module")
def table(dynamodb_client):
    """Composite-key table (pk S, sk S) with one item."""
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
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "p"}, "sk": {"S": "s"}, "note": {"S": "old"}},
        )
        yield name


@pytest.fixture(scope="module")
def missing_table() -> str:
    return f"extenddb-missing-{uuid.uuid4().hex[:12]}"


GOOD_KEY = {"pk": {"S": "p"}, "sk": {"S": "s"}}
OTHER_KEY = {"pk": {"S": "other"}, "sk": {"S": "s"}}
BAD_TYPE_KEY = {"pk": {"N": "1"}, "sk": {"S": "s"}}


def _get(table_name: str, key: dict, **extra) -> dict:
    return {"Get": {"TableName": table_name, "Key": key, **extra}}


def _expect(func, code: str, message: str | None = None):
    with pytest.raises(ClientError) as exc_info:
        func()
    err = exc_info.value.response["Error"]
    assert err["Code"] == code, err
    if message is not None:
        assert err["Message"] == message, err
    return exc_info.value.response


def _expect_prefix(func, prefix: str):
    """For syntax errors, whose inner wording ExtendDB still phrases its own way."""
    with pytest.raises(ClientError) as exc_info:
        func()
    err = exc_info.value.response["Error"]
    assert err["Code"] == "ValidationException", err
    assert err["Message"].startswith(prefix), err


def _expect_cancelled(func, reasons: list[str | None]):
    """Assert a cancellation whose per-item codes are exactly `reasons`.

    An item that did not fail carries the literal code "None" and no message.
    """
    response = _expect(func, "TransactionCanceledException")
    codes = [r.get("Code") for r in response["CancellationReasons"]]
    assert codes == ["None" if r is None else r for r in reasons], response["CancellationReasons"]
    for reason in response["CancellationReasons"]:
        if reason["Code"] == "ValidationError":
            assert reason["Message"] in KEY_SCHEMA_REASONS, reason
    expected_text = ", ".join("None" if r is None else r for r in reasons)
    assert response["Error"]["Message"] == (
        "Transaction cancelled, please refer cancellation reasons for specific "
        f"reasons [{expected_text}]"
    )


class TestTransactGetItemsOrder:
    def test_projection_grammar_beats_duplicate_keys(self, dynamodb_client, table):
        _expect(
            lambda: dynamodb_client.transact_get_items(
                TransactItems=[
                    _get(table, GOOD_KEY, ProjectionExpression="COMMENT"),
                    _get(table, GOOD_KEY),
                    _get(table, GOOD_KEY),
                ]
            ),
            "ValidationException",
            RESERVED_PROJECTION,
        )

    def test_later_projection_grammar_beats_earlier_duplicate_keys(self, dynamodb_client, table):
        _expect(
            lambda: dynamodb_client.transact_get_items(
                TransactItems=[
                    _get(table, GOOD_KEY),
                    _get(table, GOOD_KEY),
                    _get(table, OTHER_KEY, ProjectionExpression="COMMENT"),
                ]
            ),
            "ValidationException",
            RESERVED_PROJECTION,
        )

    def test_unused_projection_name_beats_duplicate_keys(self, dynamodb_client, table):
        _expect(
            lambda: dynamodb_client.transact_get_items(
                TransactItems=[
                    _get(
                        table,
                        GOOD_KEY,
                        ProjectionExpression="#ok",
                        ExpressionAttributeNames={"#ok": "ok", "#unused": "COMMENT"},
                    ),
                    _get(table, GOOD_KEY),
                ]
            ),
            "ValidationException",
            UNUSED_NAME,
        )

    def test_undefined_projection_name_beats_duplicate_keys(self, dynamodb_client, table):
        _expect(
            lambda: dynamodb_client.transact_get_items(
                TransactItems=[
                    _get(table, GOOD_KEY, ProjectionExpression="#missing"),
                    _get(table, GOOD_KEY),
                ]
            ),
            "ValidationException",
            MISSING_PROJECTION_NAME,
        )

    def test_projection_grammar_beats_later_missing_table(self, dynamodb_client, table, missing_table):
        _expect(
            lambda: dynamodb_client.transact_get_items(
                TransactItems=[
                    _get(table, GOOD_KEY, ProjectionExpression="COMMENT"),
                    _get(missing_table, GOOD_KEY),
                ]
            ),
            "ValidationException",
            RESERVED_PROJECTION,
        )

    def test_missing_table_beats_duplicate_keys_either_order(self, dynamodb_client, table, missing_table):
        for items in (
            [_get(missing_table, GOOD_KEY), _get(table, GOOD_KEY), _get(table, GOOD_KEY)],
            [_get(table, GOOD_KEY), _get(table, GOOD_KEY), _get(missing_table, GOOD_KEY)],
        ):
            _expect(
                lambda items=items: dynamodb_client.transact_get_items(TransactItems=items),
                "ResourceNotFoundException",
                "Requested resource not found",
            )

    def test_key_schema_beats_duplicate_keys_either_order(self, dynamodb_client, table):
        _expect_cancelled(
            lambda: dynamodb_client.transact_get_items(
                TransactItems=[
                    _get(table, GOOD_KEY),
                    _get(table, GOOD_KEY),
                    _get(table, BAD_TYPE_KEY),
                ]
            ),
            [None, None, "ValidationError"],
        )
        _expect_cancelled(
            lambda: dynamodb_client.transact_get_items(
                TransactItems=[
                    _get(table, BAD_TYPE_KEY),
                    _get(table, GOOD_KEY),
                    _get(table, GOOD_KEY),
                ]
            ),
            ["ValidationError", None, None],
        )

    def test_duplicate_keys_alone_still_refused(self, dynamodb_client, table):
        _expect(
            lambda: dynamodb_client.transact_get_items(
                TransactItems=[_get(table, GOOD_KEY), _get(table, GOOD_KEY)]
            ),
            "ValidationException",
            DUPLICATE,
        )


class TestTransactWriteItemsOrder:
    def test_undefined_condition_value_is_top_level(self, dynamodb_client, table):
        _expect(
            lambda: dynamodb_client.transact_write_items(
                TransactItems=[
                    {
                        "ConditionCheck": {
                            "TableName": table,
                            "Key": GOOD_KEY,
                            "ConditionExpression": "#c = :v",
                            "ExpressionAttributeNames": {"#c": "COMMENT"},
                        }
                    }
                ]
            ),
            "ValidationException",
            "Invalid ConditionExpression: An expression attribute value used in "
            "expression is not defined; attribute value: :v",
        )

    def test_undefined_value_beats_later_false_condition(self, dynamodb_client, table):
        _expect(
            lambda: dynamodb_client.transact_write_items(
                TransactItems=[
                    {
                        "ConditionCheck": {
                            "TableName": table,
                            "Key": GOOD_KEY,
                            "ConditionExpression": "note = :missing",
                        }
                    },
                    {
                        "ConditionCheck": {
                            "TableName": table,
                            "Key": {"pk": {"S": "q"}, "sk": {"S": "s"}},
                            "ConditionExpression": "note = :v",
                            "ExpressionAttributeValues": {":v": {"S": "different"}},
                        }
                    },
                ]
            ),
            "ValidationException",
            MISSING_CONDITION_VALUE,
        )

    def test_undefined_value_beats_key_schema_mismatch(self, dynamodb_client, table):
        _expect(
            lambda: dynamodb_client.transact_write_items(
                TransactItems=[
                    {
                        "ConditionCheck": {
                            "TableName": table,
                            "Key": GOOD_KEY,
                            "ConditionExpression": "note = :missing",
                        }
                    },
                    {"Delete": {"TableName": table, "Key": BAD_TYPE_KEY}},
                ]
            ),
            "ValidationException",
            MISSING_CONDITION_VALUE,
        )

    def test_undefined_name_beats_put_key_schema_mismatch(self, dynamodb_client, table):
        _expect(
            lambda: dynamodb_client.transact_write_items(
                TransactItems=[
                    {
                        "Delete": {
                            "TableName": table,
                            "Key": GOOD_KEY,
                            "ConditionExpression": "#missing = :v",
                            "ExpressionAttributeValues": {":v": {"S": "old"}},
                        }
                    },
                    {
                        "Put": {
                            "TableName": table,
                            "Item": {"pk": {"N": "1"}, "sk": {"S": "s"}, "note": {"S": "x"}},
                        }
                    },
                ]
            ),
            "ValidationException",
            MISSING_CONDITION_NAME,
        )

    def test_unused_name_beats_earlier_missing_table(self, dynamodb_client, table, missing_table):
        _expect(
            lambda: dynamodb_client.transact_write_items(
                TransactItems=[
                    {
                        "ConditionCheck": {
                            "TableName": missing_table,
                            "Key": GOOD_KEY,
                            "ConditionExpression": "attribute_exists(pk)",
                        }
                    },
                    {
                        "Put": {
                            "TableName": table,
                            "Item": {"pk": {"S": "p"}, "sk": {"S": "s2"}, "note": {"S": "new"}},
                            "ConditionExpression": "attribute_not_exists(pk)",
                            "ExpressionAttributeNames": {"#unused": "COMMENT"},
                        }
                    },
                ]
            ),
            "ValidationException",
            UNUSED_NAME,
        )

    def test_update_grammar_beats_missing_table_either_order(self, dynamodb_client, table, missing_table):
        bad_update = {
            "Update": {
                "TableName": table,
                "Key": GOOD_KEY,
                "UpdateExpression": "SET note = :v +",
                "ExpressionAttributeValues": {":v": {"N": "1"}},
            }
        }
        missing = {
            "ConditionCheck": {
                "TableName": missing_table,
                "Key": GOOD_KEY,
                "ConditionExpression": "attribute_exists(pk)",
            }
        }
        for items in ([bad_update, missing], [missing, bad_update]):
            response = _expect(
                lambda items=items: dynamodb_client.transact_write_items(TransactItems=items),
                "ValidationException",
            )
            assert response["Error"]["Message"].startswith(UPDATE_SYNTAX_PREFIX), response["Error"]

    def test_key_schema_mismatch_suppresses_condition_evaluation(self, dynamodb_client, table):
        false_condition = {
            "ConditionCheck": {
                "TableName": table,
                "Key": GOOD_KEY,
                "ConditionExpression": "note = :v",
                "ExpressionAttributeValues": {":v": {"S": "different"}},
            }
        }
        bad_delete = {"Delete": {"TableName": table, "Key": BAD_TYPE_KEY}}
        _expect_cancelled(
            lambda: dynamodb_client.transact_write_items(TransactItems=[false_condition, bad_delete]),
            [None, "ValidationError"],
        )
        _expect_cancelled(
            lambda: dynamodb_client.transact_write_items(TransactItems=[bad_delete, false_condition]),
            ["ValidationError", None],
        )

    def test_key_schema_mismatch_suppresses_runtime_update_error(self, dynamodb_client, table):
        _expect_cancelled(
            lambda: dynamodb_client.transact_write_items(
                TransactItems=[
                    {
                        "Update": {
                            "TableName": table,
                            "Key": GOOD_KEY,
                            "UpdateExpression": "SET n = n + note",
                        }
                    },
                    {"Delete": {"TableName": table, "Key": BAD_TYPE_KEY}},
                ]
            ),
            [None, "ValidationError"],
        )

    def test_key_schema_mismatch_beats_duplicate_items(self, dynamodb_client, table):
        _expect_cancelled(
            lambda: dynamodb_client.transact_write_items(
                TransactItems=[
                    {
                        "Put": {
                            "TableName": table,
                            "Item": {"pk": {"S": "p"}, "sk": {"S": "s"}, "note": {"S": "new"}},
                        }
                    },
                    {
                        "ConditionCheck": {
                            "TableName": table,
                            "Key": GOOD_KEY,
                            "ConditionExpression": "attribute_exists(pk)",
                        }
                    },
                    {"Delete": {"TableName": table, "Key": BAD_TYPE_KEY}},
                ]
            ),
            [None, None, "ValidationError"],
        )

    def test_duplicate_items_alone_still_refused(self, dynamodb_client, table):
        _expect(
            lambda: dynamodb_client.transact_write_items(
                TransactItems=[
                    {"Put": {"TableName": table, "Item": {"pk": {"S": "p"}, "sk": {"S": "s"}}}},
                    {
                        "ConditionCheck": {
                            "TableName": table,
                            "Key": GOOD_KEY,
                            "ConditionExpression": "attribute_exists(pk)",
                        }
                    },
                ]
            ),
            "ValidationException",
            DUPLICATE,
        )

    def test_key_mismatch_reason_text_follows_the_operation(self, dynamodb_client, table):
        """A Put keeps the single-item wording; a Delete gets the key-element wording."""
        response = _expect(
            lambda: dynamodb_client.transact_write_items(
                TransactItems=[
                    {"Put": {"TableName": table, "Item": {"pk": {"N": "1"}, "sk": {"S": "s"}}}},
                ]
            ),
            "TransactionCanceledException",
        )
        assert response["CancellationReasons"][0]["Message"] == (
            "One or more parameter values were invalid: Type mismatch for key pk expected: S actual: N"
        )
        response = _expect(
            lambda: dynamodb_client.transact_write_items(
                TransactItems=[{"Delete": {"TableName": table, "Key": BAD_TYPE_KEY}}]
            ),
            "TransactionCanceledException",
        )
        assert response["CancellationReasons"][0]["Message"] == (
            "The provided key element does not match the schema"
        )

    def test_false_condition_alone_still_cancels(self, dynamodb_client, table):
        _expect_cancelled(
            lambda: dynamodb_client.transact_write_items(
                TransactItems=[
                    {
                        "ConditionCheck": {
                            "TableName": table,
                            "Key": GOOD_KEY,
                            "ConditionExpression": "note = :v",
                            "ExpressionAttributeValues": {":v": {"S": "different"}},
                        }
                    }
                ]
            ),
            ["ConditionalCheckFailed"],
        )


class TestTransactWriteItemsResponseShape:
    """Measured against Amazon DynamoDB on 2026-10-14."""

    def test_item_collection_metrics_is_an_empty_map_when_requested_without_lsi(self, dynamodb_client, table):
        resp = dynamodb_client.transact_write_items(
            ReturnConsumedCapacity="TOTAL",
            ReturnItemCollectionMetrics="SIZE",
            TransactItems=[{"Put": {"TableName": table, "Item": {"pk": {"S": "icm"}, "sk": {"S": "s"}}}}],
        )
        assert resp["ItemCollectionMetrics"] == {}
        assert [c["TableName"] for c in resp["ConsumedCapacity"]] == [table]

    def test_item_collection_metrics_absent_when_not_requested(self, dynamodb_client, table):
        resp = dynamodb_client.transact_write_items(
            TransactItems=[{"Put": {"TableName": table, "Item": {"pk": {"S": "icm2"}, "sk": {"S": "s"}}}}],
        )
        assert "ItemCollectionMetrics" not in resp
        assert "ConsumedCapacity" not in resp


BIG_ITEM = {"pk": {"S": "big"}, "sk": {"S": "s"}, "blob": {"S": "x" * 410_000}}
ITEM_SIZE = "Item size has exceeded the maximum allowed size"
UNDEF_UPDATE_VALUE = (
    "Invalid UpdateExpression: An expression attribute value used in "
    "expression is not defined; attribute value: :q"
)


def _put(table_name: str, item: dict, **extra) -> dict:
    return {"Put": {"TableName": table_name, "Item": item, **extra}}


def _update(table_name: str, key: dict, expression: str = "SET a = :v", values: dict | None = None) -> dict:
    return {"Update": {"TableName": table_name, "Key": key, "UpdateExpression": expression,
                       "ExpressionAttributeValues": values if values is not None else {":v": {"N": "1"}}}}


class TestKeySchemaFirstOffenderOnly:
    """Measured 2026-10-14: only the first item whose key fails carries ValidationError."""

    def test_write_items_first_and_third(self, dynamodb_client, table):
        _expect_cancelled(
            lambda: dynamodb_client.transact_write_items(TransactItems=[
                _update(table, BAD_TYPE_KEY), _update(table, GOOD_KEY), _update(table, {"pk": {"N": "3"}, "sk": {"S": "s"}})]),
            ["ValidationError", None, None],
        )

    def test_write_items_all_three(self, dynamodb_client, table):
        _expect_cancelled(
            lambda: dynamodb_client.transact_write_items(TransactItems=[
                _update(table, BAD_TYPE_KEY), _update(table, {"pk": {"N": "2"}, "sk": {"S": "s"}}),
                _update(table, {"pk": {"N": "3"}, "sk": {"S": "s"}})]),
            ["ValidationError", None, None],
        )

    def test_write_items_middle_only(self, dynamodb_client, table):
        _expect_cancelled(
            lambda: dynamodb_client.transact_write_items(TransactItems=[
                _update(table, GOOD_KEY), _update(table, BAD_TYPE_KEY), _update(table, OTHER_KEY)]),
            [None, "ValidationError", None],
        )

    def test_false_condition_between_two_offenders_is_not_evaluated(self, dynamodb_client, table):
        _expect_cancelled(
            lambda: dynamodb_client.transact_write_items(TransactItems=[
                _update(table, BAD_TYPE_KEY),
                {"ConditionCheck": {"TableName": table, "Key": GOOD_KEY, "ConditionExpression": "note = :z",
                                    "ExpressionAttributeValues": {":z": {"S": "never"}}}},
                _update(table, {"pk": {"N": "3"}, "sk": {"S": "s"}})]),
            ["ValidationError", None, None],
        )

    def test_get_items(self, dynamodb_client, table):
        _expect_cancelled(
            lambda: dynamodb_client.transact_get_items(TransactItems=[
                _get(table, BAD_TYPE_KEY), _get(table, GOOD_KEY), _get(table, {"pk": {"N": "3"}, "sk": {"S": "s"}})]),
            ["ValidationError", None, None],
        )
        _expect_cancelled(
            lambda: dynamodb_client.transact_get_items(TransactItems=[
                _get(table, BAD_TYPE_KEY), _get(table, {"pk": {"N": "2"}, "sk": {"S": "s"}}),
                _get(table, {"pk": {"N": "3"}, "sk": {"S": "s"}})]),
            ["ValidationError", None, None],
        )


class TestItemSizePhase:
    """The 400 KB item size follows the item's own expressions and precedes the
    next item's, the table lookup, the key schema, and duplicates (measured
    2026-10-14)."""

    def test_size_follows_the_same_items_expressions(self, dynamodb_client, table):
        _expect_prefix(
            lambda: dynamodb_client.transact_write_items(TransactItems=[
                _put(table, BIG_ITEM, ConditionExpression="attribute_exists(")]),
            "Invalid ConditionExpression:",
        )
        _expect(
            lambda: dynamodb_client.transact_write_items(TransactItems=[
                _put(table, BIG_ITEM, ConditionExpression="a = :q")]),
            "ValidationException", MISSING_CONDITION_VALUE.replace(":missing", ":q"),
        )
        _expect(
            lambda: dynamodb_client.transact_write_items(TransactItems=[
                _put(table, BIG_ITEM, ConditionExpression="a = :v",
                     ExpressionAttributeValues={":v": {"N": "1"}, ":u": {"N": "1"}})]),
            "ValidationException", "Value provided in ExpressionAttributeValues unused in expressions: keys: {:u}",
        )

    def test_size_precedes_the_next_items_expressions(self, dynamodb_client, table):
        _expect(lambda: dynamodb_client.transact_write_items(TransactItems=[
            _put(table, BIG_ITEM), _update(table, GOOD_KEY, "SET a = :q")]), "ValidationException", ITEM_SIZE)
        _expect(lambda: dynamodb_client.transact_write_items(TransactItems=[
            _update(table, GOOD_KEY, "SET a = :q"), _put(table, BIG_ITEM)]), "ValidationException", UNDEF_UPDATE_VALUE)
        _expect_prefix(lambda: dynamodb_client.transact_write_items(TransactItems=[
            _update(table, GOOD_KEY, "SET a = "), _put(table, BIG_ITEM)]), UPDATE_SYNTAX_PREFIX)
        _expect(lambda: dynamodb_client.transact_write_items(TransactItems=[
            _put(table, BIG_ITEM), _update(table, GOOD_KEY, "SET a = ")]), "ValidationException", ITEM_SIZE)
        _expect(lambda: dynamodb_client.transact_write_items(TransactItems=[
            _put(table, BIG_ITEM),
            _put(table, GOOD_KEY, ConditionExpression="a = :v",
                 ExpressionAttributeValues={":v": {"N": "1"}, ":u": {"N": "1"}})]),
            "ValidationException", ITEM_SIZE)

    def test_size_precedes_table_lookup_key_schema_and_duplicates(self, dynamodb_client, table, missing_table):
        _expect(lambda: dynamodb_client.transact_write_items(TransactItems=[
            _put(missing_table, GOOD_KEY), _put(table, BIG_ITEM)]), "ValidationException", ITEM_SIZE)
        _expect(lambda: dynamodb_client.transact_write_items(TransactItems=[
            _update(table, BAD_TYPE_KEY), _put(table, BIG_ITEM)]), "ValidationException", ITEM_SIZE)
        _expect(lambda: dynamodb_client.transact_write_items(TransactItems=[
            _put(table, GOOD_KEY), _put(table, GOOD_KEY), _put(table, BIG_ITEM)]), "ValidationException", ITEM_SIZE)
        _expect(lambda: dynamodb_client.transact_write_items(TransactItems=[
            _put(table, {"pk": {"N": "1"}, "sk": {"S": "s"}, "blob": {"S": "x" * 410_000}})]),
            "ValidationException", ITEM_SIZE)
        _expect(lambda: dynamodb_client.transact_write_items(TransactItems=[
            _put(table, {"zz": {"S": "1"}, "blob": {"S": "x" * 410_000}})]), "ValidationException", ITEM_SIZE)

    def test_request_total_follows_table_lookup_and_precedes_key_schema_and_duplicates(
        self, dynamodb_client, table, missing_table
    ):
        # The service's message carries its own serialized payload size
        # ("Transaction payload size cannot exceed 4MB. Payload Size: N");
        # ExtendDB states the limit without a size. Both name the 4 MB limit.
        mids = [_put(table, {"pk": {"S": f"m{i}"}, "sk": {"S": "s"}, "blob": {"S": "y" * 390_000}}) for i in range(11)]

        def expect_total(items):
            with pytest.raises(ClientError) as exc_info:
                dynamodb_client.transact_write_items(TransactItems=items)
            err = exc_info.value.response["Error"]
            assert err["Code"] == "ValidationException", err
            assert "4MB" in err["Message"] or "4 MB" in err["Message"], err

        expect_total(mids)
        expect_total(mids + [_update(table, BAD_TYPE_KEY)])
        expect_total(mids + [_put(table, GOOD_KEY), _put(table, GOOD_KEY)])
        expect_total(mids + [{"ConditionCheck": {"TableName": table, "Key": GOOD_KEY, "ConditionExpression": "note = :z",
                                                 "ExpressionAttributeValues": {":z": {"S": "never"}}}}])
        _expect(lambda: dynamodb_client.transact_write_items(TransactItems=mids + [_put(missing_table, GOOD_KEY)]),
                "ResourceNotFoundException")
        _expect_prefix(lambda: dynamodb_client.transact_write_items(TransactItems=mids + [_update(table, GOOD_KEY, "SET a = ")]),
                       UPDATE_SYNTAX_PREFIX)
        _expect_prefix(lambda: dynamodb_client.transact_write_items(TransactItems=[_update(table, GOOD_KEY, "SET a = ")] + mids),
                       UPDATE_SYNTAX_PREFIX)
