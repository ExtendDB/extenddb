"""Secondary-index key validation on UpdateItem and TransactWriteItems.

Every expected code and message here was measured against live DynamoDB. The
rules:

- An UpdateItem whose statement leaves a secondary-index key empty (empty
  string or empty binary) or wrong-typed is refused with ValidationException.
  The empty case carries the update-expression wording; the type case carries
  the Type mismatch wording naming the index. The type case is reported ahead
  of a ConditionExpression; the empty case after the condition passes.
- In TransactWriteItems a wrong-typed index key on a Put item or on the
  post-update item of an Update cancels the transaction with a positional
  ValidationError reason. An index key left empty aborts the whole transaction
  with a top-level ValidationException and no CancellationReasons. On a Put
  member both are reported ahead of the member's condition; on an Update
  member the type case is reported ahead of the condition and the empty case
  after it.
- Across members: a wrong-typed index key on any member wins (the transaction
  is cancelled, None in the other positions); otherwise an empty index key on
  any member wins over failed conditions and unapplicable statements on the
  others.
- if_not_exists(k, :empty) is judged on what the statement writes: accepted
  when k is present, refused when k is absent.
- Nothing is written in any refused case.

Run with EXTENDDB_TEST_ENDPOINT unset to assert the same against the service.
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table

EMPTY_UPDATE_WORDING = (
    "One or more parameter values are not valid. The update expression attempted to "
    "update a secondary index key to a value that is not supported. The AttributeValue "
    "for a key attribute cannot contain an empty {kind} value."
)
EMPTY_ITEM_WORDING = (
    "One or more parameter values are not valid. A value specified for a secondary index "
    "key is not supported. The AttributeValue for a key attribute cannot contain an empty "
    "{kind} value. IndexName: {index}, IndexKey: {attr}"
)
TYPE_WORDING = (
    "One or more parameter values were invalid: Type mismatch for Index Key {attr} "
    "Expected: {expected} Actual: {actual} IndexName: {index}"
)
CANCELLED = "Transaction cancelled, please refer cancellation reasons for specific reasons"


@pytest.fixture(scope="module")
def indexed_table(dynamodb_client):
    """A composite-key table with a binary-keyed GSI, a string-keyed GSI and an LSI."""
    with scoped_table(
        dynamodb_client,
        attribute_definitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "bidx", "AttributeType": "B"},
            {"AttributeName": "sidx", "AttributeType": "S"},
            {"AttributeName": "lk", "AttributeType": "S"},
        ],
        key_schema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "bidx-index",
                "KeySchema": [{"AttributeName": "bidx", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
            },
            {
                "IndexName": "sidx-index",
                "KeySchema": [{"AttributeName": "sidx", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
            },
        ],
        LocalSecondaryIndexes=[
            {
                "IndexName": "lk-index",
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "lk", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
    ) as name:
        yield name


def _key(sk: str) -> dict:
    return {"pk": {"S": "ik"}, "sk": {"S": sk}}


def _err(exc_info) -> dict:
    return exc_info.value.response["Error"]


def _seed(client, table: str, sk: str) -> dict:
    key = _key(sk)
    client.put_item(TableName=table, Item={**key, "v": {"N": "1"}})
    return key


def _item(client, table: str, key: dict) -> dict | None:
    return client.get_item(TableName=table, Key=key, ConsistentRead=True).get("Item")


def _update(client, table: str, key: dict, attr: str, value: dict):
    return client.update_item(
        TableName=table,
        Key=key,
        UpdateExpression=f"SET {attr} = :v",
        ExpressionAttributeValues={":v": value},
    )


def _txn_update(client, table: str, key: dict, attr: str, value: dict, *, lead_put: dict | None = None):
    members = []
    if lead_put is not None:
        members.append({"Put": {"TableName": table, "Item": lead_put}})
    members.append(
        {
            "Update": {
                "TableName": table,
                "Key": key,
                "UpdateExpression": f"SET {attr} = :v",
                "ExpressionAttributeValues": {":v": value},
            }
        }
    )
    return client.transact_write_items(TransactItems=members)


def _txn_put(client, table: str, item: dict, *, lead_put: dict | None = None):
    members = []
    if lead_put is not None:
        members.append({"Put": {"TableName": table, "Item": lead_put}})
    members.append({"Put": {"TableName": table, "Item": item}})
    return client.transact_write_items(TransactItems=members)


class TestUpdateItemEmptyIndexKey:
    """UpdateItem leaving a secondary-index key empty is refused up front."""

    @pytest.mark.parametrize(
        "attr, value, kind",
        [
            ("bidx", {"B": b""}, "binary"),
            ("sidx", {"S": ""}, "string"),
            ("lk", {"S": ""}, "string"),
        ],
        ids=["gsi-binary", "gsi-string", "lsi-string"],
    )
    def test_existing_item(self, dynamodb_client, indexed_table, attr, value, kind):
        key = _seed(dynamodb_client, indexed_table, f"e-{attr}")
        with pytest.raises(ClientError) as exc_info:
            _update(dynamodb_client, indexed_table, key, attr, value)
        assert _err(exc_info)["Code"] == "ValidationException"
        assert _err(exc_info)["Message"] == EMPTY_UPDATE_WORDING.format(kind=kind)
        assert attr not in _item(dynamodb_client, indexed_table, key)

    def test_new_item_is_not_created(self, dynamodb_client, indexed_table):
        key = _key("e-new")
        with pytest.raises(ClientError) as exc_info:
            _update(dynamodb_client, indexed_table, key, "bidx", {"B": b""})
        assert _err(exc_info)["Code"] == "ValidationException"
        assert _err(exc_info)["Message"] == EMPTY_UPDATE_WORDING.format(kind="binary")
        assert _item(dynamodb_client, indexed_table, key) is None


class TestUpdateItemWrongTypedIndexKey:
    """UpdateItem setting a secondary-index key to another type is refused, naming the index."""

    @pytest.mark.parametrize(
        "attr, value, expected, actual, index",
        [
            ("bidx", {"N": "1"}, "B", "N", "bidx-index"),
            ("sidx", {"N": "1"}, "S", "N", "sidx-index"),
            ("sidx", {"L": []}, "S", "L", "sidx-index"),
            ("lk", {"N": "1"}, "S", "N", "lk-index"),
        ],
        ids=["gsi-binary-gets-number", "gsi-string-gets-number", "gsi-string-gets-list", "lsi-gets-number"],
    )
    def test_existing_item(self, dynamodb_client, indexed_table, attr, value, expected, actual, index):
        key = _seed(dynamodb_client, indexed_table, f"t-{attr}-{actual}")
        with pytest.raises(ClientError) as exc_info:
            _update(dynamodb_client, indexed_table, key, attr, value)
        assert _err(exc_info)["Code"] == "ValidationException"
        assert _err(exc_info)["Message"] == TYPE_WORDING.format(
            attr=attr, expected=expected, actual=actual, index=index
        )
        assert attr not in _item(dynamodb_client, indexed_table, key)

    def test_new_item_is_not_created(self, dynamodb_client, indexed_table):
        key = _key("t-new")
        with pytest.raises(ClientError) as exc_info:
            _update(dynamodb_client, indexed_table, key, "bidx", {"N": "1"})
        assert _err(exc_info)["Code"] == "ValidationException"
        assert _item(dynamodb_client, indexed_table, key) is None

    def test_a_valid_value_still_writes(self, dynamodb_client, indexed_table):
        key = _seed(dynamodb_client, indexed_table, "t-ok")
        _update(dynamodb_client, indexed_table, key, "sidx", {"S": "x"})
        assert _item(dynamodb_client, indexed_table, key)["sidx"] == {"S": "x"}


class TestTransactUpdateEmptyIndexKey:
    """A transacted Update leaving an index key empty aborts the whole transaction up front."""

    @pytest.mark.parametrize(
        "attr, value, kind",
        [
            ("bidx", {"B": b""}, "binary"),
            ("sidx", {"S": ""}, "string"),
            ("lk", {"S": ""}, "string"),
        ],
        ids=["gsi-binary", "gsi-string", "lsi-string"],
    )
    def test_single_member(self, dynamodb_client, indexed_table, attr, value, kind):
        key = _seed(dynamodb_client, indexed_table, f"xe-{attr}")
        with pytest.raises(ClientError) as exc_info:
            _txn_update(dynamodb_client, indexed_table, key, attr, value)
        assert _err(exc_info)["Code"] == "ValidationException"
        assert _err(exc_info)["Message"] == EMPTY_UPDATE_WORDING.format(kind=kind)
        assert attr not in _item(dynamodb_client, indexed_table, key)

    def test_a_passing_member_does_not_make_it_a_cancellation(self, dynamodb_client, indexed_table):
        key = _seed(dynamodb_client, indexed_table, "xe-two")
        lead = {"pk": {"S": "lead"}, "sk": {"S": "xe-two"}}
        with pytest.raises(ClientError) as exc_info:
            _txn_update(dynamodb_client, indexed_table, key, "bidx", {"B": b""}, lead_put=lead)
        assert _err(exc_info)["Code"] == "ValidationException"
        assert _err(exc_info)["Message"] == EMPTY_UPDATE_WORDING.format(kind="binary")
        assert "CancellationReasons" not in exc_info.value.response
        assert _item(dynamodb_client, indexed_table, lead) is None


class TestTransactUpdateWrongTypedIndexKey:
    """A transacted Update setting an index key to another type is a positional cancellation."""

    @pytest.mark.parametrize(
        "attr, value, expected, actual, index",
        [
            ("bidx", {"N": "1"}, "B", "N", "bidx-index"),
            ("sidx", {"L": []}, "S", "L", "sidx-index"),
            ("lk", {"N": "1"}, "S", "N", "lk-index"),
        ],
        ids=["gsi-binary-gets-number", "gsi-string-gets-list", "lsi-gets-number"],
    )
    def test_single_member(self, dynamodb_client, indexed_table, attr, value, expected, actual, index):
        key = _seed(dynamodb_client, indexed_table, f"xt-{attr}-{actual}")
        with pytest.raises(ClientError) as exc_info:
            _txn_update(dynamodb_client, indexed_table, key, attr, value)
        resp = exc_info.value.response
        assert resp["Error"]["Code"] == "TransactionCanceledException"
        assert resp["Error"]["Message"] == f"{CANCELLED} [ValidationError]"
        reasons = resp["CancellationReasons"]
        assert [r["Code"] for r in reasons] == ["ValidationError"]
        assert reasons[0]["Message"] == TYPE_WORDING.format(
            attr=attr, expected=expected, actual=actual, index=index
        )
        assert attr not in _item(dynamodb_client, indexed_table, key)

    def test_reason_is_positional_after_a_passing_member(self, dynamodb_client, indexed_table):
        key = _seed(dynamodb_client, indexed_table, "xt-two")
        lead = {"pk": {"S": "lead"}, "sk": {"S": "xt-two"}}
        with pytest.raises(ClientError) as exc_info:
            _txn_update(dynamodb_client, indexed_table, key, "bidx", {"N": "1"}, lead_put=lead)
        resp = exc_info.value.response
        assert resp["Error"]["Code"] == "TransactionCanceledException"
        assert resp["Error"]["Message"] == f"{CANCELLED} [None, ValidationError]"
        assert [r["Code"] for r in resp["CancellationReasons"]] == ["None", "ValidationError"]
        assert _item(dynamodb_client, indexed_table, lead) is None


class TestTransactPutIndexKey:
    """A transacted Put splits the same way: empty aborts up front, wrong type cancels positionally."""

    @pytest.mark.parametrize(
        "attr, value, kind, index",
        [
            ("bidx", {"B": b""}, "binary", "bidx-index"),
            ("sidx", {"S": ""}, "string", "sidx-index"),
            ("lk", {"S": ""}, "string", "lk-index"),
        ],
        ids=["gsi-binary", "gsi-string", "lsi-string"],
    )
    def test_empty_key_aborts_the_transaction(self, dynamodb_client, indexed_table, attr, value, kind, index):
        item = {**_key(f"pe-{attr}"), attr: value}
        with pytest.raises(ClientError) as exc_info:
            _txn_put(dynamodb_client, indexed_table, item)
        assert _err(exc_info)["Code"] == "ValidationException"
        assert _err(exc_info)["Message"] == EMPTY_ITEM_WORDING.format(kind=kind, index=index, attr=attr)
        assert _item(dynamodb_client, indexed_table, _key(f"pe-{attr}")) is None

    @pytest.mark.parametrize(
        "attr, value, expected, actual, index",
        [
            ("bidx", {"N": "1"}, "B", "N", "bidx-index"),
            ("lk", {"N": "1"}, "S", "N", "lk-index"),
        ],
        ids=["gsi-binary-gets-number", "lsi-gets-number"],
    )
    def test_wrong_type_cancels(self, dynamodb_client, indexed_table, attr, value, expected, actual, index):
        item = {**_key(f"pt-{attr}"), attr: value}
        with pytest.raises(ClientError) as exc_info:
            _txn_put(dynamodb_client, indexed_table, item)
        resp = exc_info.value.response
        assert resp["Error"]["Code"] == "TransactionCanceledException"
        reasons = resp["CancellationReasons"]
        assert [r["Code"] for r in reasons] == ["ValidationError"]
        assert reasons[0]["Message"] == TYPE_WORDING.format(
            attr=attr, expected=expected, actual=actual, index=index
        )
        assert _item(dynamodb_client, indexed_table, _key(f"pt-{attr}")) is None

    def test_wrong_type_reason_is_positional(self, dynamodb_client, indexed_table):
        lead = {"pk": {"S": "lead"}, "sk": {"S": "pt-two"}}
        item = {**_key("pt-two"), "bidx": {"N": "1"}}
        with pytest.raises(ClientError) as exc_info:
            _txn_put(dynamodb_client, indexed_table, item, lead_put=lead)
        resp = exc_info.value.response
        assert resp["Error"]["Code"] == "TransactionCanceledException"
        assert resp["Error"]["Message"] == f"{CANCELLED} [None, ValidationError]"
        assert [r["Code"] for r in resp["CancellationReasons"]] == ["None", "ValidationError"]
        assert _item(dynamodb_client, indexed_table, lead) is None


class TestOrderAgainstTheCondition:
    """Where the index-key checks sit relative to a ConditionExpression, as measured.

    A wrong-typed index key is reported ahead of the condition. A failed
    condition is reported ahead of an empty index key and ahead of a statement
    that cannot be applied (arithmetic on a missing attribute).
    """

    def test_update_item_type_mismatch_beats_a_failing_condition(self, dynamodb_client, indexed_table):
        key = _seed(dynamodb_client, indexed_table, "o-type")
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.update_item(
                TableName=indexed_table,
                Key=key,
                UpdateExpression="SET bidx = :v",
                ConditionExpression="attribute_not_exists(pk)",
                ExpressionAttributeValues={":v": {"N": "1"}},
            )
        assert _err(exc_info)["Code"] == "ValidationException"
        assert _err(exc_info)["Message"] == TYPE_WORDING.format(
            attr="bidx", expected="B", actual="N", index="bidx-index"
        )

    def test_update_item_failing_condition_beats_an_empty_key(self, dynamodb_client, indexed_table):
        key = _seed(dynamodb_client, indexed_table, "o-empty")
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.update_item(
                TableName=indexed_table,
                Key=key,
                UpdateExpression="SET sidx = :v",
                ConditionExpression="attribute_not_exists(pk)",
                ExpressionAttributeValues={":v": {"S": ""}},
            )
        assert _err(exc_info)["Code"] == "ConditionalCheckFailedException"

    def test_update_item_failing_condition_beats_a_statement_that_cannot_apply(self, dynamodb_client, indexed_table):
        key = _key("o-apply-missing")
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.update_item(
                TableName=indexed_table,
                Key=key,
                UpdateExpression="SET v = v + :one",
                ConditionExpression="attribute_exists(pk)",
                ExpressionAttributeValues={":one": {"N": "1"}},
            )
        assert _err(exc_info)["Code"] == "ConditionalCheckFailedException"

    def test_transact_update_type_mismatch_beats_a_failing_condition(self, dynamodb_client, indexed_table):
        key = _seed(dynamodb_client, indexed_table, "xo-type")
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.transact_write_items(
                TransactItems=[
                    {
                        "Update": {
                            "TableName": indexed_table,
                            "Key": key,
                            "UpdateExpression": "SET bidx = :v",
                            "ConditionExpression": "attribute_not_exists(pk)",
                            "ExpressionAttributeValues": {":v": {"N": "1"}},
                        }
                    }
                ]
            )
        resp = exc_info.value.response
        assert resp["Error"]["Code"] == "TransactionCanceledException"
        assert [r["Code"] for r in resp["CancellationReasons"]] == ["ValidationError"]
        assert resp["CancellationReasons"][0]["Message"] == TYPE_WORDING.format(
            attr="bidx", expected="B", actual="N", index="bidx-index"
        )

    def test_transact_update_failing_condition_beats_an_empty_key(self, dynamodb_client, indexed_table):
        key = _seed(dynamodb_client, indexed_table, "xo-empty")
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.transact_write_items(
                TransactItems=[
                    {
                        "Update": {
                            "TableName": indexed_table,
                            "Key": key,
                            "UpdateExpression": "SET sidx = :v",
                            "ConditionExpression": "attribute_not_exists(pk)",
                            "ExpressionAttributeValues": {":v": {"S": ""}},
                        }
                    }
                ]
            )
        resp = exc_info.value.response
        assert resp["Error"]["Code"] == "TransactionCanceledException"
        assert [r["Code"] for r in resp["CancellationReasons"]] == ["ConditionalCheckFailed"]

    def test_transact_update_failing_condition_beats_a_statement_that_cannot_apply(self, dynamodb_client, indexed_table):
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.transact_write_items(
                TransactItems=[
                    {
                        "Update": {
                            "TableName": indexed_table,
                            "Key": _key("xo-apply-missing"),
                            "UpdateExpression": "SET v = v + :one",
                            "ConditionExpression": "attribute_exists(pk)",
                            "ExpressionAttributeValues": {":one": {"N": "1"}},
                        }
                    }
                ]
            )
        resp = exc_info.value.response
        assert resp["Error"]["Code"] == "TransactionCanceledException"
        assert [r["Code"] for r in resp["CancellationReasons"]] == ["ConditionalCheckFailed"]

    def test_transact_update_type_mismatch_wins_over_an_empty_key_in_the_same_statement(self, dynamodb_client, indexed_table):
        key = _seed(dynamodb_client, indexed_table, "xo-both")
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.transact_write_items(
                TransactItems=[
                    {
                        "Update": {
                            "TableName": indexed_table,
                            "Key": key,
                            "UpdateExpression": "SET sidx = :e, bidx = :bad",
                            "ExpressionAttributeValues": {":e": {"S": ""}, ":bad": {"N": "1"}},
                        }
                    }
                ]
            )
        resp = exc_info.value.response
        assert resp["Error"]["Code"] == "TransactionCanceledException"
        assert [r["Code"] for r in resp["CancellationReasons"]] == ["ValidationError"]
        assert resp["CancellationReasons"][0]["Message"] == TYPE_WORDING.format(
            attr="bidx", expected="B", actual="N", index="bidx-index"
        )


def _upd(table: str, pk: str, expr: str, vals: dict, cond: str | None = None) -> dict:
    member = {
        "TableName": table,
        "Key": _key(pk),
        "UpdateExpression": expr,
        "ExpressionAttributeValues": vals,
    }
    if cond:
        member["ConditionExpression"] = cond
    return {"Update": member}


class TestTransactPutOrderAgainstTheCondition:
    """On a Put member both index-key checks come ahead of the condition, as measured."""

    def test_wrong_type_beats_a_failing_condition(self, dynamodb_client, indexed_table):
        _seed(dynamodb_client, indexed_table, "po-type")
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.transact_write_items(
                TransactItems=[
                    {
                        "Put": {
                            "TableName": indexed_table,
                            "Item": {**_key("po-type"), "bidx": {"N": "1"}},
                            "ConditionExpression": "attribute_not_exists(pk)",
                        }
                    }
                ]
            )
        resp = exc_info.value.response
        assert resp["Error"]["Code"] == "TransactionCanceledException"
        assert [r["Code"] for r in resp["CancellationReasons"]] == ["ValidationError"]

    def test_empty_key_beats_a_failing_condition(self, dynamodb_client, indexed_table):
        _seed(dynamodb_client, indexed_table, "po-empty")
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.transact_write_items(
                TransactItems=[
                    {
                        "Put": {
                            "TableName": indexed_table,
                            "Item": {**_key("po-empty"), "sidx": {"S": ""}},
                            "ConditionExpression": "attribute_not_exists(pk)",
                        }
                    }
                ]
            )
        assert _err(exc_info)["Code"] == "ValidationException"
        assert _err(exc_info)["Message"] == EMPTY_ITEM_WORDING.format(
            kind="string", index="sidx-index", attr="sidx"
        )
        assert "CancellationReasons" not in exc_info.value.response

    def test_a_plain_put_still_reports_its_failed_condition(self, dynamodb_client, indexed_table):
        _seed(dynamodb_client, indexed_table, "po-plain")
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.transact_write_items(
                TransactItems=[
                    {
                        "Put": {
                            "TableName": indexed_table,
                            "Item": {**_key("po-plain"), "v": {"S": "2"}},
                            "ConditionExpression": "attribute_not_exists(pk)",
                        }
                    }
                ]
            )
        resp = exc_info.value.response
        assert resp["Error"]["Code"] == "TransactionCanceledException"
        assert [r["Code"] for r in resp["CancellationReasons"]] == ["ConditionalCheckFailed"]


class TestCrossMemberPrecedence:
    """Which fault a transaction reports when different members carry different faults."""

    @pytest.mark.parametrize("type_first", [True, False], ids=["type-then-empty", "empty-then-type"])
    def test_a_wrong_type_on_one_member_wins_over_an_empty_key_on_another(
        self, dynamodb_client, indexed_table, type_first
    ):
        a = _seed(dynamodb_client, indexed_table, "cm-a")
        b = _seed(dynamodb_client, indexed_table, "cm-b")
        typed = _upd(indexed_table, "cm-a", "SET bidx = :v", {":v": {"N": "1"}})
        empty = _upd(indexed_table, "cm-b", "SET sidx = :v", {":v": {"S": ""}})
        members = [typed, empty] if type_first else [empty, typed]
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.transact_write_items(TransactItems=members)
        resp = exc_info.value.response
        assert resp["Error"]["Code"] == "TransactionCanceledException"
        codes = [r["Code"] for r in resp["CancellationReasons"]]
        assert codes == (["ValidationError", "None"] if type_first else ["None", "ValidationError"])
        typed_reason = resp["CancellationReasons"][0 if type_first else 1]
        assert typed_reason["Message"] == TYPE_WORDING.format(
            attr="bidx", expected="B", actual="N", index="bidx-index"
        )
        assert "bidx" not in _item(dynamodb_client, indexed_table, a)
        assert "sidx" not in _item(dynamodb_client, indexed_table, b)

    @pytest.mark.parametrize("empty_first", [True, False], ids=["empty-then-cond", "cond-then-empty"])
    def test_an_empty_key_on_one_member_wins_over_a_failed_condition_on_another(
        self, dynamodb_client, indexed_table, empty_first
    ):
        _seed(dynamodb_client, indexed_table, "ce-a")
        _seed(dynamodb_client, indexed_table, "ce-b")
        empty = _upd(indexed_table, "ce-a", "SET sidx = :v", {":v": {"S": ""}})
        failing = _upd(indexed_table, "ce-b", "SET v = :v", {":v": {"N": "2"}}, "attribute_not_exists(pk)")
        members = [empty, failing] if empty_first else [failing, empty]
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.transact_write_items(TransactItems=members)
        assert _err(exc_info)["Code"] == "ValidationException"
        assert _err(exc_info)["Message"] == EMPTY_UPDATE_WORDING.format(kind="string")
        assert "CancellationReasons" not in exc_info.value.response

    def test_an_empty_key_wins_over_a_statement_that_cannot_apply_elsewhere(self, dynamodb_client, indexed_table):
        _seed(dynamodb_client, indexed_table, "ca-b")
        cannot_apply = _upd(indexed_table, "ca-missing", "SET v = v + :one", {":one": {"N": "1"}})
        empty = _upd(indexed_table, "ca-b", "SET sidx = :v", {":v": {"S": ""}})
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.transact_write_items(TransactItems=[cannot_apply, empty])
        assert _err(exc_info)["Code"] == "ValidationException"
        assert _err(exc_info)["Message"] == EMPTY_UPDATE_WORDING.format(kind="string")
        assert _item(dynamodb_client, indexed_table, _key("ca-missing")) is None

    def test_a_wrong_type_masks_a_failed_condition_on_another_member(self, dynamodb_client, indexed_table):
        _seed(dynamodb_client, indexed_table, "ct-a")
        _seed(dynamodb_client, indexed_table, "ct-b")
        failing = _upd(indexed_table, "ct-a", "SET v = :v", {":v": {"N": "2"}}, "attribute_not_exists(pk)")
        typed = _upd(indexed_table, "ct-b", "SET bidx = :v", {":v": {"N": "1"}})
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.transact_write_items(TransactItems=[failing, typed])
        resp = exc_info.value.response
        assert resp["Error"]["Code"] == "TransactionCanceledException"
        assert [r["Code"] for r in resp["CancellationReasons"]] == ["None", "ValidationError"]

    def test_a_wrong_type_masks_a_statement_that_cannot_apply_on_another_member(self, dynamodb_client, indexed_table):
        _seed(dynamodb_client, indexed_table, "cx-b")
        cannot_apply = _upd(indexed_table, "cx-missing", "SET v = v + :one", {":one": {"N": "1"}})
        typed = _upd(indexed_table, "cx-b", "SET bidx = :v", {":v": {"N": "1"}})
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.transact_write_items(TransactItems=[cannot_apply, typed])
        resp = exc_info.value.response
        assert resp["Error"]["Code"] == "TransactionCanceledException"
        assert [r["Code"] for r in resp["CancellationReasons"]] == ["None", "ValidationError"]
        assert _item(dynamodb_client, indexed_table, _key("cx-missing")) is None

    @pytest.mark.parametrize("bad_key_first", [True, False], ids=["table-key-then-cond", "cond-then-table-key"])
    def test_a_wrong_typed_table_key_on_a_put_masks_a_failed_condition_elsewhere(
        self, dynamodb_client, indexed_table, bad_key_first
    ):
        _seed(dynamodb_client, indexed_table, "tk-a")
        bad_put = {"Put": {"TableName": indexed_table, "Item": {"pk": {"N": "1"}, "sk": {"S": "s"}}}}
        failing = _upd(indexed_table, "tk-a", "SET v = :v", {":v": {"N": "2"}}, "attribute_not_exists(pk)")
        members = [bad_put, failing] if bad_key_first else [failing, bad_put]
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.transact_write_items(TransactItems=members)
        resp = exc_info.value.response
        assert resp["Error"]["Code"] == "TransactionCanceledException"
        codes = [r["Code"] for r in resp["CancellationReasons"]]
        assert codes == (["ValidationError", "None"] if bad_key_first else ["None", "ValidationError"])

    def test_a_wrong_typed_table_key_on_a_put_masks_an_empty_index_key_elsewhere(self, dynamodb_client, indexed_table):
        _seed(dynamodb_client, indexed_table, "tk-b")
        bad_put = {"Put": {"TableName": indexed_table, "Item": {"pk": {"N": "1"}, "sk": {"S": "s"}}}}
        empty = _upd(indexed_table, "tk-b", "SET sidx = :v", {":v": {"S": ""}})
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.transact_write_items(TransactItems=[empty, bad_put])
        resp = exc_info.value.response
        assert resp["Error"]["Code"] == "TransactionCanceledException"
        assert [r["Code"] for r in resp["CancellationReasons"]] == ["None", "ValidationError"]
        assert "sidx" not in _item(dynamodb_client, indexed_table, _key("tk-b"))

    def test_two_wrong_types_report_only_the_first(self, dynamodb_client, indexed_table):
        _seed(dynamodb_client, indexed_table, "cd-a")
        _seed(dynamodb_client, indexed_table, "cd-b")
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.transact_write_items(
                TransactItems=[
                    _upd(indexed_table, "cd-a", "SET bidx = :v", {":v": {"N": "1"}}),
                    _upd(indexed_table, "cd-b", "SET sidx = :v", {":v": {"N": "1"}}),
                ]
            )
        resp = exc_info.value.response
        assert resp["Error"]["Code"] == "TransactionCanceledException"
        assert [r["Code"] for r in resp["CancellationReasons"]] == ["ValidationError", "None"]
        assert resp["CancellationReasons"][0]["Message"] == TYPE_WORDING.format(
            attr="bidx", expected="B", actual="N", index="bidx-index"
        )

    def test_an_empty_key_whose_own_condition_fails_is_a_plain_cancellation(self, dynamodb_client, indexed_table):
        _seed(dynamodb_client, indexed_table, "cc-a")
        _seed(dynamodb_client, indexed_table, "cc-b")
        empty_failing = _upd(indexed_table, "cc-a", "SET sidx = :v", {":v": {"S": ""}}, "attribute_not_exists(pk)")
        plain = _upd(indexed_table, "cc-b", "SET v = :v", {":v": {"N": "2"}})
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.transact_write_items(TransactItems=[empty_failing, plain])
        resp = exc_info.value.response
        assert resp["Error"]["Code"] == "TransactionCanceledException"
        assert [r["Code"] for r in resp["CancellationReasons"]] == ["ConditionalCheckFailed", "None"]
        assert _item(dynamodb_client, indexed_table, _key("cc-b"))["v"] == {"N": "1"}


class TestIfNotExistsIsJudgedOnWhatIsWritten:
    """if_not_exists(k, :empty) writes nothing bad when k is present and is refused when k is absent."""

    def test_present_key_is_kept_and_the_statement_is_accepted(self, dynamodb_client, indexed_table):
        key = _key("ine-present")
        dynamodb_client.put_item(TableName=indexed_table, Item={**key, "sidx": {"S": "x"}})
        dynamodb_client.update_item(
            TableName=indexed_table,
            Key=key,
            UpdateExpression="SET sidx = if_not_exists(sidx, :e)",
            ExpressionAttributeValues={":e": {"S": ""}},
        )
        assert _item(dynamodb_client, indexed_table, key)["sidx"] == {"S": "x"}

    def test_absent_key_makes_the_statement_a_refused_empty_key(self, dynamodb_client, indexed_table):
        key = _seed(dynamodb_client, indexed_table, "ine-absent")
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.update_item(
                TableName=indexed_table,
                Key=key,
                UpdateExpression="SET sidx = if_not_exists(sidx, :e)",
                ExpressionAttributeValues={":e": {"S": ""}},
            )
        assert _err(exc_info)["Code"] == "ValidationException"
        assert _err(exc_info)["Message"] == EMPTY_UPDATE_WORDING.format(kind="string")
        assert "sidx" not in _item(dynamodb_client, indexed_table, key)

    def test_transacted_present_key_is_accepted(self, dynamodb_client, indexed_table):
        key = _key("ine-txn-present")
        dynamodb_client.put_item(TableName=indexed_table, Item={**key, "sidx": {"S": "x"}})
        dynamodb_client.transact_write_items(
            TransactItems=[_upd(indexed_table, "ine-txn-present", "SET sidx = if_not_exists(sidx, :e)", {":e": {"S": ""}})]
        )
        assert _item(dynamodb_client, indexed_table, key)["sidx"] == {"S": "x"}


COMPUTED_WORDING = "The update expression attempted to update the secondary index key to unsupported type"


class TestLiteralVersusComputedIndexKeyValues:
    """Two type checks, as measured. A literal operand on an index key is checked on the
    statement before the condition and before any action is applied, with the per-index
    wording. A value computed from the item (an attribute reference, the fallback of
    if_not_exists) is checked after the condition passes and the statement is applied,
    with a wording that names neither attribute nor index."""

    def test_attribute_reference_of_the_wrong_type_uses_the_computed_wording(self, dynamodb_client, indexed_table):
        key = _key("lc-ref")
        dynamodb_client.put_item(TableName=indexed_table, Item={**key, "sidx": {"S": "x"}, "n": {"N": "5"}})
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.update_item(TableName=indexed_table, Key=key, UpdateExpression="SET sidx = n")
        assert _err(exc_info)["Code"] == "ValidationException"
        assert _err(exc_info)["Message"] == COMPUTED_WORDING
        assert _item(dynamodb_client, indexed_table, key)["sidx"] == {"S": "x"}

    def test_attribute_reference_is_checked_after_the_condition(self, dynamodb_client, indexed_table):
        key = _key("lc-ref-cond")
        dynamodb_client.put_item(TableName=indexed_table, Item={**key, "sidx": {"S": "x"}, "n": {"N": "5"}})
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.update_item(
                TableName=indexed_table, Key=key, UpdateExpression="SET sidx = n",
                ConditionExpression="attribute_not_exists(pk)",
            )
        assert _err(exc_info)["Code"] == "ConditionalCheckFailedException"

    def test_if_not_exists_fallback_of_the_wrong_type_uses_the_computed_wording(self, dynamodb_client, indexed_table):
        key = _seed(dynamodb_client, indexed_table, "lc-ine")
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.update_item(
                TableName=indexed_table, Key=key,
                UpdateExpression="SET sidx = if_not_exists(nope, :num)",
                ExpressionAttributeValues={":num": {"N": "9"}},
            )
        assert _err(exc_info)["Code"] == "ValidationException"
        assert _err(exc_info)["Message"] == COMPUTED_WORDING

    def test_if_not_exists_fallback_is_checked_after_the_condition(self, dynamodb_client, indexed_table):
        key = _seed(dynamodb_client, indexed_table, "lc-ine-cond")
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.update_item(
                TableName=indexed_table, Key=key,
                UpdateExpression="SET sidx = if_not_exists(nope, :num)",
                ConditionExpression="attribute_not_exists(pk)",
                ExpressionAttributeValues={":num": {"N": "9"}},
            )
        assert _err(exc_info)["Code"] == "ConditionalCheckFailedException"

    def test_list_append_of_literals_is_a_literal(self, dynamodb_client, indexed_table):
        key = _seed(dynamodb_client, indexed_table, "lc-la")
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.update_item(
                TableName=indexed_table, Key=key,
                UpdateExpression="SET sidx = list_append(:l, :l)",
                ConditionExpression="attribute_not_exists(pk)",
                ExpressionAttributeValues={":l": {"L": []}},
            )
        assert _err(exc_info)["Code"] == "ValidationException"
        assert _err(exc_info)["Message"] == TYPE_WORDING.format(attr="sidx", expected="S", actual="L", index="sidx-index")

    @pytest.mark.parametrize(
        "expr, vals, actual",
        [
            ("ADD sidx :one", {":one": {"N": "1"}}, "N"),
            ("ADD sidx :ss", {":ss": {"SS": ["a"]}}, "SS"),
            ("DELETE sidx :ss", {":ss": {"SS": ["a"]}}, "SS"),
        ],
        ids=["add-number-to-string-key", "add-set-to-string-key", "delete-set-from-string-key"],
    )
    def test_add_and_delete_on_an_index_key_are_literal_type_mismatches(self, dynamodb_client, indexed_table, expr, vals, actual):
        key = _seed(dynamodb_client, indexed_table, f"lc-{expr.split()[0].lower()}-{actual}")
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.update_item(
                TableName=indexed_table, Key=key, UpdateExpression=expr,
                ConditionExpression="attribute_not_exists(pk)", ExpressionAttributeValues=vals,
            )
        assert _err(exc_info)["Code"] == "ValidationException"
        assert _err(exc_info)["Message"] == TYPE_WORDING.format(attr="sidx", expected="S", actual=actual, index="sidx-index")

    def test_delete_on_an_absent_item_is_still_a_type_mismatch(self, dynamodb_client, indexed_table):
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.update_item(
                TableName=indexed_table, Key=_key("lc-del-absent"), UpdateExpression="DELETE sidx :ss",
                ExpressionAttributeValues={":ss": {"SS": ["a"]}},
            )
        assert _err(exc_info)["Code"] == "ValidationException"
        assert _err(exc_info)["Message"] == TYPE_WORDING.format(attr="sidx", expected="S", actual="SS", index="sidx-index")
        assert _item(dynamodb_client, indexed_table, _key("lc-del-absent")) is None

    def test_a_literal_fault_beats_an_action_that_cannot_apply(self, dynamodb_client, indexed_table):
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.update_item(
                TableName=indexed_table, Key=_key("lc-apply"),
                UpdateExpression="SET v = v + :one, bidx = :num",
                ConditionExpression="attribute_exists(pk)",
                ExpressionAttributeValues={":one": {"N": "1"}, ":num": {"N": "9"}},
            )
        assert _err(exc_info)["Code"] == "ValidationException"
        assert _err(exc_info)["Message"] == TYPE_WORDING.format(attr="bidx", expected="B", actual="N", index="bidx-index")

    def test_a_literal_fault_beats_an_invalid_document_path(self, dynamodb_client, indexed_table):
        key = _seed(dynamodb_client, indexed_table, "lc-path")
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.update_item(
                TableName=indexed_table, Key=key,
                UpdateExpression="SET plain.x = :num, bidx = :num",
                ExpressionAttributeValues={":num": {"N": "9"}},
            )
        assert _err(exc_info)["Code"] == "ValidationException"
        assert _err(exc_info)["Message"] == TYPE_WORDING.format(attr="bidx", expected="B", actual="N", index="bidx-index")

    def test_an_empty_literal_does_not_beat_an_action_that_cannot_apply(self, dynamodb_client, indexed_table):
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.update_item(
                TableName=indexed_table, Key=_key("lc-empty-apply"),
                UpdateExpression="SET v = v + :one, sidx = :e",
                ExpressionAttributeValues={":one": {"N": "1"}, ":e": {"S": ""}},
            )
        assert _err(exc_info)["Code"] == "ValidationException"
        assert _err(exc_info)["Message"] == "The provided expression refers to an attribute that does not exist in the item"

    def test_transacted_computed_value_is_a_cancellation_with_the_computed_wording(self, dynamodb_client, indexed_table):
        key = _key("lc-txn-ref")
        dynamodb_client.put_item(TableName=indexed_table, Item={**key, "sidx": {"S": "x"}, "n": {"N": "5"}})
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.transact_write_items(
                TransactItems=[{"Update": {"TableName": indexed_table, "Key": key, "UpdateExpression": "SET sidx = n"}}]
            )
        resp = exc_info.value.response
        assert resp["Error"]["Code"] == "TransactionCanceledException"
        assert [r["Code"] for r in resp["CancellationReasons"]] == ["ValidationError"]
        assert resp["CancellationReasons"][0]["Message"] == COMPUTED_WORDING

    def test_transacted_computed_value_is_checked_after_the_condition(self, dynamodb_client, indexed_table):
        key = _key("lc-txn-ref-cond")
        dynamodb_client.put_item(TableName=indexed_table, Item={**key, "sidx": {"S": "x"}, "n": {"N": "5"}})
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.transact_write_items(
                TransactItems=[{"Update": {"TableName": indexed_table, "Key": key, "UpdateExpression": "SET sidx = n", "ConditionExpression": "attribute_not_exists(pk)"}}]
            )
        resp = exc_info.value.response
        assert [r["Code"] for r in resp["CancellationReasons"]] == ["ConditionalCheckFailed"]

    def test_transacted_literal_fault_beats_an_action_that_cannot_apply(self, dynamodb_client, indexed_table):
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.transact_write_items(
                TransactItems=[_upd(indexed_table, "lc-txn-apply", "SET v = v + :one, bidx = :num", {":one": {"N": "1"}, ":num": {"N": "9"}}, "attribute_exists(pk)")]
            )
        resp = exc_info.value.response
        assert resp["Error"]["Code"] == "TransactionCanceledException"
        assert [r["Code"] for r in resp["CancellationReasons"]] == ["ValidationError"]
        assert resp["CancellationReasons"][0]["Message"] == TYPE_WORDING.format(attr="bidx", expected="B", actual="N", index="bidx-index")
