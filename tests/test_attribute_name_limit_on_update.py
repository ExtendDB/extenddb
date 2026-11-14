# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Attribute-name size limit on PutItem, UpdateItem, DeleteItem, TransactWriteItems and BatchWriteItem.

An attribute name is at most 65,535 bytes of UTF-8, at every depth: a top-level
name and a map key inside any value. The refusal is a ValidationException whose
wording depends on the request form. The rules asserted here:

- PutItem refuses a 65,536-byte name, top-level or nested, with
  "1 validation error detected: Attribute name is too large, must be less than
  65536 bytes", ahead of the table lookup. 65,535 bytes is accepted.
- UpdateItem refuses a 65,536-byte name supplied through
  ExpressionAttributeNames with the same message plus " for key #n", naming the
  placeholder, and a 65,536-byte map key inside an ExpressionAttributeValues
  value with the message alone. Both are input validation: they are reported
  ahead of the table lookup, a failing ConditionExpression and a
  secondary-index key type mismatch, and nothing is written. With a long name
  in both maps, the ExpressionAttributeNames placeholder is reported.
- PutItem and DeleteItem refuse a 65,536-byte name that arrives only through
  the ConditionExpression's ExpressionAttributeNames with the same
  " for key #n" message, and nothing is written or deleted.
- A transaction member refuses an over-limit name in its expression maps with
  a top-level ValidationException and no CancellationReasons, on Put, Update,
  Delete and ConditionCheck members alike. The message names the map and the
  placeholder: "ExpressionAttributeNames contains invalid value: Attribute
  name is too large, must be less than 65536 bytes for key #n", and
  "ExpressionAttributeValues contains invalid value: ... for key :v" for a map
  key inside a value. With a long name in both maps of an Update or Put
  member, the ExpressionAttributeValues placeholder is reported; a Put member
  with a long item name and a long ExpressionAttributeNames value reports the
  ExpressionAttributeNames placeholder (a standalone PutItem reports the item).
- A transacted Put and a BatchWriteItem PutRequest refuse an over-limit item
  name with "Attribute name is too large, must be less than 65536 bytes",
  without the "1 validation error detected:" prefix.
- In a transaction and a batch, the name check covers every member before any
  table lookup: a missing table, or a missing key in a batch item, does not
  take precedence over the name refusal, and no member is written.
- The limit counts UTF-8 bytes, not characters.

Run with EXTENDDB_TEST_ENDPOINT unset to assert the same against the service.
"""

from __future__ import annotations

import uuid

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table

LIMIT = 65535
OVER = "a" * (LIMIT + 1)
AT = "b" * LIMIT
BARE = "Attribute name is too large, must be less than 65536 bytes"
TOO_LARGE = f"1 validation error detected: {BARE}"
TXN_NAMES = f"ExpressionAttributeNames contains invalid value: {BARE} for key #n"
TXN_VALUES = f"ExpressionAttributeValues contains invalid value: {BARE} for key :v"
TXN_CONDITION_VALUES = f"ExpressionAttributeValues contains invalid value: {BARE} for key :c"


def _for_key(placeholder: str) -> str:
    return f"{TOO_LARGE} for key {placeholder}"


def _missing_table() -> str:
    return f"extenddb-test-missing-{uuid.uuid4().hex[:12]}"


@pytest.fixture(scope="module")
def table(dynamodb_client):
    """A hash-key table with a string-keyed GSI, for the index-key ordering cases."""
    with scoped_table(
        dynamodb_client,
        attribute_definitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "gk", "AttributeType": "S"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "gk-index",
                "KeySchema": [{"AttributeName": "gk", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
    ) as name:
        yield name


def _key(k: str) -> dict:
    return {"pk": {"S": k}}


def _seed(client, table: str, k: str) -> dict:
    key = _key(k)
    client.put_item(TableName=table, Item={**key, "v": {"N": "1"}})
    return key


def _item(client, table: str, key: dict) -> dict | None:
    return client.get_item(TableName=table, Key=key, ConsistentRead=True).get("Item")


def _assert_too_large(exc_info, message: str) -> None:
    err = exc_info.value.response["Error"]
    assert err["Code"] == "ValidationException"
    assert err["Message"] == message
    assert "CancellationReasons" not in exc_info.value.response


NESTED_VALUES = [
    pytest.param({"M": {OVER: {"S": "x"}}}, id="map-key"),
    pytest.param({"L": [{"S": "first"}, {"M": {OVER: {"S": "x"}}}]}, id="map-key-inside-list"),
    pytest.param({"M": {"ok": {"M": {OVER: {"NULL": True}}}}}, id="map-key-two-levels-down"),
]


class TestPutItem:
    """The PutItem refusal carries the service's message."""

    def test_top_level_name_over_limit(self, dynamodb_client, table):
        key = _key("put-top")
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.put_item(TableName=table, Item={**key, OVER: {"S": "x"}})
        _assert_too_large(exc_info, TOO_LARGE)
        assert _item(dynamodb_client, table, key) is None

    def test_nested_map_key_over_limit(self, dynamodb_client, table):
        key = _key("put-nested")
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.put_item(TableName=table, Item={**key, "doc": {"M": {OVER: {"S": "x"}}}})
        _assert_too_large(exc_info, TOO_LARGE)
        assert _item(dynamodb_client, table, key) is None

    def test_reported_ahead_of_a_missing_table(self, dynamodb_client):
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.put_item(TableName=_missing_table(), Item={**_key("x"), OVER: {"S": "x"}})
        _assert_too_large(exc_info, TOO_LARGE)

    def test_placeholder_name_only_in_the_condition(self, dynamodb_client, table):
        # Probe row P07.
        key = _key("put-cond-name")
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.put_item(
                TableName=table,
                Item={**key, "v": {"N": "1"}},
                ConditionExpression="attribute_not_exists(#n)",
                ExpressionAttributeNames={"#n": OVER},
            )
        _assert_too_large(exc_info, _for_key("#n"))
        assert _item(dynamodb_client, table, key) is None

    def test_names_at_limit_are_accepted(self, dynamodb_client, table):
        key = _key("put-at")
        dynamodb_client.put_item(
            TableName=table, Item={**key, AT: {"S": "x"}, "doc": {"M": {AT: {"S": "y"}}}}
        )
        stored = _item(dynamodb_client, table, key)
        assert stored[AT] == {"S": "x"}
        assert stored["doc"] == {"M": {AT: {"S": "y"}}}


class TestUpdateItem:
    """UpdateItem refuses an over-limit name from either input map."""

    def test_placeholder_name_over_limit(self, dynamodb_client, table):
        key = _seed(dynamodb_client, table, "upd-name")
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.update_item(
                TableName=table,
                Key=key,
                UpdateExpression="SET #n = :v",
                ExpressionAttributeNames={"#n": OVER},
                ExpressionAttributeValues={":v": {"S": "x"}},
            )
        _assert_too_large(exc_info, _for_key("#n"))
        assert _item(dynamodb_client, table, key) == {**key, "v": {"N": "1"}}

    def test_names_map_reported_before_the_values_map(self, dynamodb_client, table):
        # Probe row U13: the transacted Update reports the other map first.
        key = _seed(dynamodb_client, table, "upd-both-maps")
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.update_item(
                TableName=table,
                Key=key,
                UpdateExpression="SET #n = :v",
                ExpressionAttributeNames={"#n": OVER},
                ExpressionAttributeValues={":v": {"M": {"c" * (LIMIT + 1): {"S": "x"}}}},
            )
        _assert_too_large(exc_info, _for_key("#n"))
        assert _item(dynamodb_client, table, key) == {**key, "v": {"N": "1"}}

    def test_reported_ahead_of_a_missing_table(self, dynamodb_client):
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.update_item(
                TableName=_missing_table(),
                Key=_key("x"),
                UpdateExpression="SET #n = :v",
                ExpressionAttributeNames={"#n": OVER},
                ExpressionAttributeValues={":v": {"S": "x"}},
            )
        _assert_too_large(exc_info, _for_key("#n"))

    def test_placeholder_name_over_limit_does_not_create_the_item(self, dynamodb_client, table):
        key = _key("upd-name-absent")
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.update_item(
                TableName=table,
                Key=key,
                UpdateExpression="SET #n = :v",
                ExpressionAttributeNames={"#n": OVER},
                ExpressionAttributeValues={":v": {"S": "x"}},
            )
        _assert_too_large(exc_info, _for_key("#n"))
        assert _item(dynamodb_client, table, key) is None

    def test_placeholder_name_over_limit_in_a_nested_path(self, dynamodb_client, table):
        key = _key("upd-name-nested-path")
        dynamodb_client.put_item(TableName=table, Item={**key, "doc": {"M": {}}})
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.update_item(
                TableName=table,
                Key=key,
                UpdateExpression="SET doc.#n = :v",
                ExpressionAttributeNames={"#n": OVER},
                ExpressionAttributeValues={":v": {"S": "x"}},
            )
        _assert_too_large(exc_info, _for_key("#n"))
        assert _item(dynamodb_client, table, key)["doc"] == {"M": {}}

    @pytest.mark.parametrize("value", NESTED_VALUES)
    def test_map_key_over_limit_inside_a_value(self, dynamodb_client, table, value):
        key = _seed(dynamodb_client, table, "upd-value")
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.update_item(
                TableName=table,
                Key=key,
                UpdateExpression="SET doc = :v",
                ExpressionAttributeValues={":v": value},
            )
        _assert_too_large(exc_info, TOO_LARGE)
        assert "doc" not in _item(dynamodb_client, table, key)

    def test_names_at_limit_are_accepted(self, dynamodb_client, table):
        key = _seed(dynamodb_client, table, "upd-at")
        dynamodb_client.update_item(
            TableName=table,
            Key=key,
            UpdateExpression="SET #n = :v, doc = :d",
            ExpressionAttributeNames={"#n": AT},
            ExpressionAttributeValues={":v": {"S": "x"}, ":d": {"M": {AT: {"S": "y"}}}},
        )
        stored = _item(dynamodb_client, table, key)
        assert stored[AT] == {"S": "x"}
        assert stored["doc"] == {"M": {AT: {"S": "y"}}}

    @pytest.mark.parametrize(
        "names, values, update, message",
        [
            pytest.param({"#n": OVER}, {":v": {"S": "x"}}, "SET #n = :v", _for_key("#n"), id="placeholder-name"),
            pytest.param(None, {":v": {"M": {OVER: {"S": "x"}}}}, "SET doc = :v", TOO_LARGE, id="map-key-in-value"),
        ],
    )
    def test_reported_ahead_of_a_failing_condition(self, dynamodb_client, table, names, values, update, message):
        key = _seed(dynamodb_client, table, "upd-cond")
        request = {
            "TableName": table,
            "Key": key,
            "UpdateExpression": update,
            "ConditionExpression": "attribute_exists(nope)",
            "ExpressionAttributeValues": values,
        }
        if names:
            request["ExpressionAttributeNames"] = names
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.update_item(**request)
        _assert_too_large(exc_info, message)

    @pytest.mark.parametrize(
        "names, values, update, message",
        [
            pytest.param(
                {"#n": OVER},
                {":v": {"S": "x"}, ":g": {"N": "1"}},
                "SET #n = :v, gk = :g",
                _for_key("#n"),
                id="placeholder-name",
            ),
            pytest.param(
                None,
                {":v": {"M": {OVER: {"S": "x"}}}, ":g": {"N": "1"}},
                "SET doc = :v, gk = :g",
                TOO_LARGE,
                id="map-key-in-value",
            ),
        ],
    )
    def test_reported_ahead_of_an_index_key_type_mismatch(self, dynamodb_client, table, names, values, update, message):
        key = _seed(dynamodb_client, table, "upd-gsi")
        request = {
            "TableName": table,
            "Key": key,
            "UpdateExpression": update,
            "ExpressionAttributeValues": values,
        }
        if names:
            request["ExpressionAttributeNames"] = names
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.update_item(**request)
        _assert_too_large(exc_info, message)
        assert _item(dynamodb_client, table, key) == {**key, "v": {"N": "1"}}

    def test_limit_counts_utf8_bytes(self, dynamodb_client, table):
        key = _seed(dynamodb_client, table, "upd-bytes")
        # 32,768 two-byte characters: 65,536 bytes.
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.update_item(
                TableName=table,
                Key=key,
                UpdateExpression="SET #n = :v",
                ExpressionAttributeNames={"#n": "\u00e9" * 32768},
                ExpressionAttributeValues={":v": {"S": "x"}},
            )
        _assert_too_large(exc_info, _for_key("#n"))
        # 32,767 two-byte characters and one ASCII character: 65,535 bytes.
        at_limit = "\u00e9" * 32767 + "a"
        dynamodb_client.update_item(
            TableName=table,
            Key=key,
            UpdateExpression="SET #n = :v",
            ExpressionAttributeNames={"#n": at_limit},
            ExpressionAttributeValues={":v": {"S": "x"}},
        )
        assert _item(dynamodb_client, table, key)[at_limit] == {"S": "x"}

    @pytest.mark.parametrize(
        "updates",
        [
            pytest.param({OVER: {"Action": "PUT", "Value": {"S": "x"}}}, id="attribute-name"),
            pytest.param({"doc": {"Action": "PUT", "Value": {"M": {OVER: {"S": "x"}}}}}, id="map-key-in-value"),
        ],
    )
    def test_legacy_attribute_updates(self, dynamodb_client, table, updates):
        key = _seed(dynamodb_client, table, "upd-legacy")
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.update_item(TableName=table, Key=key, AttributeUpdates=updates)
        _assert_too_large(exc_info, TOO_LARGE)
        assert _item(dynamodb_client, table, key) == {**key, "v": {"N": "1"}}


class TestDeleteItem:
    """DeleteItem refuses an over-limit name in its condition's ExpressionAttributeNames."""

    def test_placeholder_name_only_in_the_condition(self, dynamodb_client, table):
        # Probe row D01.
        key = _seed(dynamodb_client, table, "del-cond-name")
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.delete_item(
                TableName=table,
                Key=key,
                ConditionExpression="attribute_not_exists(#n)",
                ExpressionAttributeNames={"#n": OVER},
            )
        _assert_too_large(exc_info, _for_key("#n"))
        assert _item(dynamodb_client, table, key) == {**key, "v": {"N": "1"}}


def _txn_update(table: str, key: dict, update: str, names: dict | None, values: dict, **extra) -> dict:
    member = {"TableName": table, "Key": key, "UpdateExpression": update, "ExpressionAttributeValues": values}
    if names:
        member["ExpressionAttributeNames"] = names
    member.update(extra)
    return {"Update": member}


class TestTransactUpdate:
    """A transacted Update with an over-limit name fails the request before any member runs."""

    def test_placeholder_name_over_limit(self, dynamodb_client, table):
        key = _seed(dynamodb_client, table, "txn-name")
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.transact_write_items(
                TransactItems=[_txn_update(table, key, "SET #n = :v", {"#n": OVER}, {":v": {"S": "x"}})]
            )
        _assert_too_large(exc_info, TXN_NAMES)
        assert _item(dynamodb_client, table, key) == {**key, "v": {"N": "1"}}

    @pytest.mark.parametrize("value", NESTED_VALUES)
    def test_map_key_over_limit_inside_a_value(self, dynamodb_client, table, value):
        key = _seed(dynamodb_client, table, "txn-value")
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.transact_write_items(
                TransactItems=[_txn_update(table, key, "SET doc = :v", None, {":v": value})]
            )
        _assert_too_large(exc_info, TXN_VALUES)
        assert "doc" not in _item(dynamodb_client, table, key)

    def test_values_map_reported_before_the_names_map(self, dynamodb_client, table):
        # Probe row T18: a standalone UpdateItem reports the other map first.
        key = _seed(dynamodb_client, table, "txn-both-maps")
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.transact_write_items(
                TransactItems=[
                    _txn_update(
                        table,
                        key,
                        "SET #n = :v",
                        {"#n": OVER},
                        {":v": {"M": {"c" * (LIMIT + 1): {"S": "x"}}}},
                    )
                ]
            )
        _assert_too_large(exc_info, TXN_VALUES)
        assert _item(dynamodb_client, table, key) == {**key, "v": {"N": "1"}}

    def test_reported_ahead_of_another_members_missing_table(self, dynamodb_client, table):
        # Probe row T14: every member's names are checked before any table lookup.
        key = _key("txn-missing-other")
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.transact_write_items(
                TransactItems=[
                    {"Put": {"TableName": _missing_table(), "Item": {**_key("x"), "v": {"N": "1"}}}},
                    _txn_update(table, key, "SET #n = :v", {"#n": OVER}, {":v": {"S": "x"}}),
                ]
            )
        _assert_too_large(exc_info, TXN_NAMES)
        assert _item(dynamodb_client, table, key) is None

    @pytest.mark.parametrize("update_first", [False, True], ids=["put-then-update", "update-then-put"])
    def test_no_member_is_written(self, dynamodb_client, table, update_first):
        key = _seed(dynamodb_client, table, "txn-two")
        other = _key(f"txn-two-other-{update_first}")
        put = {"Put": {"TableName": table, "Item": {**other, "v": {"N": "1"}}}}
        update = _txn_update(table, key, "SET #n = :v", {"#n": OVER}, {":v": {"S": "x"}})
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.transact_write_items(TransactItems=[update, put] if update_first else [put, update])
        _assert_too_large(exc_info, TXN_NAMES)
        assert _item(dynamodb_client, table, other) is None

    def test_reported_ahead_of_a_failing_condition(self, dynamodb_client, table):
        key = _seed(dynamodb_client, table, "txn-cond")
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.transact_write_items(
                TransactItems=[
                    _txn_update(
                        table,
                        key,
                        "SET #n = :v",
                        {"#n": OVER},
                        {":v": {"S": "x"}},
                        ConditionExpression="attribute_exists(nope)",
                    )
                ]
            )
        _assert_too_large(exc_info, TXN_NAMES)

    def test_reported_ahead_of_another_members_failing_condition(self, dynamodb_client, table):
        checked = _seed(dynamodb_client, table, "txn-check")
        key = _key("txn-check-update")
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.transact_write_items(
                TransactItems=[
                    {"ConditionCheck": {"TableName": table, "Key": checked, "ConditionExpression": "attribute_exists(nope)"}},
                    _txn_update(table, key, "SET #n = :v", {"#n": OVER}, {":v": {"S": "x"}}),
                ]
            )
        _assert_too_large(exc_info, TXN_NAMES)
        assert _item(dynamodb_client, table, key) is None

    def test_reported_ahead_of_an_index_key_type_mismatch(self, dynamodb_client, table):
        key = _seed(dynamodb_client, table, "txn-gsi")
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.transact_write_items(
                TransactItems=[
                    _txn_update(
                        table,
                        key,
                        "SET #n = :v, gk = :g",
                        {"#n": OVER},
                        {":v": {"S": "x"}, ":g": {"N": "1"}},
                    )
                ]
            )
        _assert_too_large(exc_info, TXN_NAMES)
        assert _item(dynamodb_client, table, key) == {**key, "v": {"N": "1"}}

    def test_reported_ahead_of_another_members_index_key_type_mismatch(self, dynamodb_client, table):
        mismatched = _seed(dynamodb_client, table, "txn-gsi-other")
        key = _key("txn-gsi-other-update")
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.transact_write_items(
                TransactItems=[
                    _txn_update(table, mismatched, "SET gk = :g", None, {":g": {"N": "1"}}),
                    _txn_update(table, key, "SET #n = :v", {"#n": OVER}, {":v": {"S": "x"}}),
                ]
            )
        _assert_too_large(exc_info, TXN_NAMES)
        assert _item(dynamodb_client, table, mismatched) == {**mismatched, "v": {"N": "1"}}
        assert _item(dynamodb_client, table, key) is None

    def test_reported_ahead_of_a_missing_table(self, dynamodb_client):
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.transact_write_items(
                TransactItems=[
                    _txn_update(_missing_table(), _key("x"), "SET #n = :v", {"#n": OVER}, {":v": {"S": "x"}})
                ]
            )
        _assert_too_large(exc_info, TXN_NAMES)

    def test_name_at_limit_is_accepted(self, dynamodb_client, table):
        key = _seed(dynamodb_client, table, "txn-at")
        dynamodb_client.transact_write_items(
            TransactItems=[_txn_update(table, key, "SET #n = :v", {"#n": AT}, {":v": {"S": "x"}})]
        )
        assert _item(dynamodb_client, table, key)[AT] == {"S": "x"}


ITEM_NAMES = [
    pytest.param({OVER: {"S": "x"}}, id="top-level-name"),
    pytest.param({"doc": {"M": {OVER: {"S": "x"}}}}, id="nested-map-key"),
]


class TestTransactPut:
    """A transacted Put refuses an over-limit name with the sentence alone."""

    @pytest.mark.parametrize("attributes", ITEM_NAMES)
    def test_name_over_limit(self, dynamodb_client, table, attributes):
        key = _key("txn-put")
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.transact_write_items(
                TransactItems=[{"Put": {"TableName": table, "Item": {**key, **attributes}}}]
            )
        _assert_too_large(exc_info, BARE)
        assert _item(dynamodb_client, table, key) is None

    def test_reported_ahead_of_a_missing_table(self, dynamodb_client):
        # Probe row T13.
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.transact_write_items(
                TransactItems=[{"Put": {"TableName": _missing_table(), "Item": {**_key("x"), OVER: {"S": "x"}}}}]
            )
        _assert_too_large(exc_info, BARE)

    def test_placeholder_name_only_in_the_condition(self, dynamodb_client, table):
        # Probe row T19.
        key = _key("txn-put-cond-name")
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.transact_write_items(
                TransactItems=[
                    {
                        "Put": {
                            "TableName": table,
                            "Item": {**key, "v": {"N": "1"}},
                            "ConditionExpression": "attribute_not_exists(#n)",
                            "ExpressionAttributeNames": {"#n": OVER},
                        }
                    }
                ]
            )
        _assert_too_large(exc_info, TXN_NAMES)
        assert _item(dynamodb_client, table, key) is None

    def test_map_key_over_limit_inside_a_condition_value(self, dynamodb_client, table):
        # Probe row T22.
        key = _key("txn-put-cond-value")
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.transact_write_items(
                TransactItems=[
                    {
                        "Put": {
                            "TableName": table,
                            "Item": {**key, "v": {"N": "1"}},
                            "ConditionExpression": "attribute_not_exists(zz) OR zz = :c",
                            "ExpressionAttributeValues": {":c": {"M": {OVER: {"S": "x"}}}},
                        }
                    }
                ]
            )
        _assert_too_large(exc_info, TXN_CONDITION_VALUES)
        assert _item(dynamodb_client, table, key) is None

    def test_names_map_reported_before_the_item(self, dynamodb_client, table):
        # Probe row T25: a standalone PutItem reports the item first.
        key = _key("txn-put-item-and-name")
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.transact_write_items(
                TransactItems=[
                    {
                        "Put": {
                            "TableName": table,
                            "Item": {**key, "c" * (LIMIT + 1): {"S": "x"}},
                            "ConditionExpression": "attribute_not_exists(#n)",
                            "ExpressionAttributeNames": {"#n": OVER},
                        }
                    }
                ]
            )
        _assert_too_large(exc_info, TXN_NAMES)
        assert _item(dynamodb_client, table, key) is None

    def test_values_map_reported_before_the_names_map(self, dynamodb_client, table):
        # Probe row T26.
        key = _key("txn-put-both-maps")
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.transact_write_items(
                TransactItems=[
                    {
                        "Put": {
                            "TableName": table,
                            "Item": {**key, "v": {"N": "1"}},
                            "ConditionExpression": "attribute_not_exists(#n) OR zz = :c",
                            "ExpressionAttributeNames": {"#n": OVER},
                            "ExpressionAttributeValues": {":c": {"M": {OVER: {"S": "x"}}}},
                        }
                    }
                ]
            )
        _assert_too_large(exc_info, TXN_CONDITION_VALUES)
        assert _item(dynamodb_client, table, key) is None


class TestTransactDeleteAndConditionCheck:
    """Delete and ConditionCheck members refuse an over-limit name in their condition's names map."""

    def test_delete_placeholder_name_only_in_the_condition(self, dynamodb_client, table):
        # Probe row T20.
        key = _seed(dynamodb_client, table, "txn-del-cond-name")
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.transact_write_items(
                TransactItems=[
                    {
                        "Delete": {
                            "TableName": table,
                            "Key": key,
                            "ConditionExpression": "attribute_not_exists(#n)",
                            "ExpressionAttributeNames": {"#n": OVER},
                        }
                    }
                ]
            )
        _assert_too_large(exc_info, TXN_NAMES)
        assert _item(dynamodb_client, table, key) == {**key, "v": {"N": "1"}}

    def test_condition_check_placeholder_name(self, dynamodb_client, table):
        # Probe row T21.
        key = _seed(dynamodb_client, table, "txn-check-cond-name")
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.transact_write_items(
                TransactItems=[
                    {
                        "ConditionCheck": {
                            "TableName": table,
                            "Key": key,
                            "ConditionExpression": "attribute_not_exists(#n)",
                            "ExpressionAttributeNames": {"#n": OVER},
                        }
                    }
                ]
            )
        _assert_too_large(exc_info, TXN_NAMES)


class TestBatchWriteItem:
    """A BatchWriteItem PutRequest refuses an over-limit name with the sentence alone."""

    @pytest.mark.parametrize("attributes", ITEM_NAMES)
    def test_name_over_limit(self, dynamodb_client, table, attributes):
        key = _key("batch-put")
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.batch_write_item(RequestItems={table: [{"PutRequest": {"Item": {**key, **attributes}}}]})
        _assert_too_large(exc_info, BARE)
        assert _item(dynamodb_client, table, key) is None

    def test_reported_ahead_of_a_missing_table(self, dynamodb_client):
        # Probe row B03.
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.batch_write_item(
                RequestItems={_missing_table(): [{"PutRequest": {"Item": {**_key("x"), OVER: {"S": "x"}}}}]}
            )
        _assert_too_large(exc_info, BARE)

    def test_reported_ahead_of_a_missing_key(self, dynamodb_client, table):
        # Probe row B04: the item has no pk attribute.
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.batch_write_item(
                RequestItems={table: [{"PutRequest": {"Item": {"nope": {"S": "x"}, OVER: {"S": "x"}}}}]}
            )
        _assert_too_large(exc_info, BARE)
