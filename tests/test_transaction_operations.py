# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Tests for TransactGetItems and TransactWriteItems operations.

REQ-TEST-001: All implemented operations have passing tests.
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from conftest import wait_for_active, scoped_table
@pytest.fixture(scope="module")
def hash_table(dynamodb_client):
    """Create a hash-only table for the module, delete on teardown."""
    with scoped_table(dynamodb_client) as name:
        yield name
# ---------------------------------------------------------------------------
# TransactWriteItems — happy paths
# ---------------------------------------------------------------------------
def test_transact_write_put_single(dynamodb_client, hash_table):
    """TransactWriteItems with a single Put."""
    dynamodb_client.transact_write_items(
        TransactItems=[
            {"Put": {"TableName": hash_table, "Item": {"pk": {"S": "tw-1"}, "data": {"S": "hello"}}}}
        ]
    )
    resp = dynamodb_client.get_item(TableName=hash_table, Key={"pk": {"S": "tw-1"}})
    assert resp["Item"]["data"]["S"] == "hello"
def test_transact_write_put_multiple(dynamodb_client, hash_table):
    """TransactWriteItems with multiple Puts across same table."""
    dynamodb_client.transact_write_items(
        TransactItems=[
            {"Put": {"TableName": hash_table, "Item": {"pk": {"S": f"tw-{i}"}, "val": {"N": str(i)}}}}
            for i in range(5)
        ]
    )
    for i in range(5):
        resp = dynamodb_client.get_item(TableName=hash_table, Key={"pk": {"S": f"tw-{i}"}})
        assert resp["Item"]["val"]["N"] == str(i)
def test_transact_write_delete(dynamodb_client, hash_table):
    """TransactWriteItems with Delete."""
    dynamodb_client.put_item(TableName=hash_table, Item={"pk": {"S": "del-1"}, "x": {"S": "y"}})
    dynamodb_client.transact_write_items(
        TransactItems=[
            {"Delete": {"TableName": hash_table, "Key": {"pk": {"S": "del-1"}}}}
        ]
    )
    resp = dynamodb_client.get_item(TableName=hash_table, Key={"pk": {"S": "del-1"}})
    assert "Item" not in resp
def test_transact_write_update(dynamodb_client, hash_table):
    """TransactWriteItems with Update."""
    dynamodb_client.put_item(TableName=hash_table, Item={"pk": {"S": "upd-1"}, "counter": {"N": "0"}})
    dynamodb_client.transact_write_items(
        TransactItems=[
            {
                "Update": {
                    "TableName": hash_table,
                    "Key": {"pk": {"S": "upd-1"}},
                    "UpdateExpression": "SET #c = #c + :inc",
                    "ExpressionAttributeNames": {"#c": "counter"},
                    "ExpressionAttributeValues": {":inc": {"N": "5"}},
                }
            }
        ]
    )
    resp = dynamodb_client.get_item(TableName=hash_table, Key={"pk": {"S": "upd-1"}})
    assert resp["Item"]["counter"]["N"] == "5"
def test_transact_write_condition_check_pass(dynamodb_client, hash_table):
    """TransactWriteItems with ConditionCheck that passes."""
    dynamodb_client.put_item(TableName=hash_table, Item={"pk": {"S": "cc-1"}, "status": {"S": "active"}})
    dynamodb_client.transact_write_items(
        TransactItems=[
            {
                "ConditionCheck": {
                    "TableName": hash_table,
                    "Key": {"pk": {"S": "cc-1"}},
                    "ConditionExpression": "#s = :v",
                    "ExpressionAttributeNames": {"#s": "status"},
                    "ExpressionAttributeValues": {":v": {"S": "active"}},
                }
            },
            {"Put": {"TableName": hash_table, "Item": {"pk": {"S": "cc-2"}, "data": {"S": "ok"}}}},
        ]
    )
    resp = dynamodb_client.get_item(TableName=hash_table, Key={"pk": {"S": "cc-2"}})
    assert resp["Item"]["data"]["S"] == "ok"
def test_transact_write_mixed_ops(dynamodb_client, hash_table):
    """TransactWriteItems with Put + Delete + Update in one transaction."""
    dynamodb_client.put_item(TableName=hash_table, Item={"pk": {"S": "mix-del"}, "x": {"S": "y"}})
    dynamodb_client.put_item(TableName=hash_table, Item={"pk": {"S": "mix-upd"}, "n": {"N": "1"}})
    dynamodb_client.transact_write_items(
        TransactItems=[
            {"Put": {"TableName": hash_table, "Item": {"pk": {"S": "mix-put"}, "v": {"S": "new"}}}},
            {"Delete": {"TableName": hash_table, "Key": {"pk": {"S": "mix-del"}}}},
            {
                "Update": {
                    "TableName": hash_table,
                    "Key": {"pk": {"S": "mix-upd"}},
                    "UpdateExpression": "SET n = n + :one",
                    "ExpressionAttributeValues": {":one": {"N": "1"}},
                }
            },
        ]
    )
    assert dynamodb_client.get_item(TableName=hash_table, Key={"pk": {"S": "mix-put"}})["Item"]["v"]["S"] == "new"
    assert "Item" not in dynamodb_client.get_item(TableName=hash_table, Key={"pk": {"S": "mix-del"}})
    assert dynamodb_client.get_item(TableName=hash_table, Key={"pk": {"S": "mix-upd"}})["Item"]["n"]["N"] == "2"
# ---------------------------------------------------------------------------
# TransactWriteItems — atomicity (condition failure rolls back all)
# ---------------------------------------------------------------------------
def test_transact_write_condition_fail_rolls_back(dynamodb_client, hash_table):
    """When a ConditionCheck fails, the entire transaction is rolled back."""
    dynamodb_client.put_item(TableName=hash_table, Item={"pk": {"S": "guard"}, "status": {"S": "inactive"}})
    with pytest.raises(ClientError) as exc_info:
        dynamodb_client.transact_write_items(
            TransactItems=[
                {"Put": {"TableName": hash_table, "Item": {"pk": {"S": "should-not-exist"}, "v": {"S": "x"}}}},
                {
                    "ConditionCheck": {
                        "TableName": hash_table,
                        "Key": {"pk": {"S": "guard"}},
                        "ConditionExpression": "#s = :v",
                        "ExpressionAttributeNames": {"#s": "status"},
                        "ExpressionAttributeValues": {":v": {"S": "active"}},
                    }
                },
            ]
        )
    assert exc_info.value.response["Error"]["Code"] == "TransactionCanceledException"
    # The Put should NOT have been applied
    resp = dynamodb_client.get_item(TableName=hash_table, Key={"pk": {"S": "should-not-exist"}})
    assert "Item" not in resp
def test_transact_write_cancellation_reasons(dynamodb_client, hash_table):
    """TransactionCanceledException includes per-item CancellationReasons."""
    dynamodb_client.put_item(TableName=hash_table, Item={"pk": {"S": "cr-1"}, "status": {"S": "bad"}})
    with pytest.raises(ClientError) as exc_info:
        dynamodb_client.transact_write_items(
            TransactItems=[
                {"Put": {"TableName": hash_table, "Item": {"pk": {"S": "cr-new"}, "v": {"S": "x"}}}},
                {
                    "ConditionCheck": {
                        "TableName": hash_table,
                        "Key": {"pk": {"S": "cr-1"}},
                        "ConditionExpression": "#s = :v",
                        "ExpressionAttributeNames": {"#s": "status"},
                        "ExpressionAttributeValues": {":v": {"S": "good"}},
                    }
                },
            ]
        )
    err = exc_info.value.response["Error"]
    assert err["Code"] == "TransactionCanceledException"
    reasons = exc_info.value.response.get("CancellationReasons", [])
    assert len(reasons) == 2
    assert reasons[0]["Code"] == "None"
    assert reasons[1]["Code"] == "ConditionalCheckFailed"
# ---------------------------------------------------------------------------
# TransactWriteItems — validation errors
# ---------------------------------------------------------------------------
def test_transact_write_empty_items(dynamodb_client):
    """TransactWriteItems with empty TransactItems → client-side validation."""
    with pytest.raises(Exception):
        dynamodb_client.transact_write_items(TransactItems=[])
def test_transact_write_too_many_items(dynamodb_client, hash_table):
    """TransactWriteItems with > 100 items → ValidationException."""
    items = [
        {"Put": {"TableName": hash_table, "Item": {"pk": {"S": f"tw-{i}"}}}}
        for i in range(101)
    ]
    with pytest.raises(ClientError) as exc_info:
        dynamodb_client.transact_write_items(TransactItems=items)
    assert exc_info.value.response["Error"]["Code"] == "ValidationException"
def test_transact_write_duplicate_targets(dynamodb_client, hash_table):
    """TransactWriteItems with two ops targeting same item → ValidationException."""
    with pytest.raises(ClientError) as exc_info:
        dynamodb_client.transact_write_items(
            TransactItems=[
                {"Put": {"TableName": hash_table, "Item": {"pk": {"S": "dup"}, "v": {"S": "a"}}}},
                {"Put": {"TableName": hash_table, "Item": {"pk": {"S": "dup"}, "v": {"S": "b"}}}},
            ]
        )
    assert exc_info.value.response["Error"]["Code"] == "ValidationException"
def test_transact_write_put_wrong_key_type(dynamodb_client, hash_table):
    """TransactWriteItems Put with wrong key type → TransactionCanceledException."""
    with pytest.raises(ClientError) as exc_info:
        dynamodb_client.transact_write_items(
            TransactItems=[
                {"Put": {"TableName": hash_table, "Item": {"pk": {"N": "123"}}}}
            ]
        )
    err = exc_info.value.response["Error"]
    assert err["Code"] == "TransactionCanceledException"
    reasons = exc_info.value.response.get("CancellationReasons", [])
    assert len(reasons) == 1
    assert reasons[0]["Code"] == "ValidationError"
def test_transact_write_delete_wrong_key_type(dynamodb_client, hash_table):
    """TransactWriteItems Delete with wrong key type → TransactionCanceledException."""
    with pytest.raises(ClientError) as exc_info:
        dynamodb_client.transact_write_items(
            TransactItems=[
                {"Delete": {"TableName": hash_table, "Key": {"pk": {"N": "123"}}}}
            ]
        )
    err = exc_info.value.response["Error"]
    assert err["Code"] == "TransactionCanceledException"
    reasons = exc_info.value.response.get("CancellationReasons", [])
    assert len(reasons) == 1
    assert reasons[0]["Code"] == "ValidationError"
def test_transact_write_update_wrong_key_type(dynamodb_client, hash_table):
    """TransactWriteItems Update with wrong key type → TransactionCanceledException."""
    with pytest.raises(ClientError) as exc_info:
        dynamodb_client.transact_write_items(
            TransactItems=[
                {
                    "Update": {
                        "TableName": hash_table,
                        "Key": {"pk": {"N": "123"}},
                        "UpdateExpression": "SET v = :v",
                        "ExpressionAttributeValues": {":v": {"S": "x"}},
                    }
                }
            ]
        )
    err = exc_info.value.response["Error"]
    assert err["Code"] == "TransactionCanceledException"
    reasons = exc_info.value.response.get("CancellationReasons", [])
    assert len(reasons) == 1
    assert reasons[0]["Code"] == "ValidationError"
# ---------------------------------------------------------------------------
# TransactGetItems — happy paths
# ---------------------------------------------------------------------------
def test_transact_get_single(dynamodb_client, hash_table):
    """TransactGetItems with a single Get."""
    dynamodb_client.put_item(TableName=hash_table, Item={"pk": {"S": "tg-1"}, "data": {"S": "val"}})
    resp = dynamodb_client.transact_get_items(
        TransactItems=[
            {"Get": {"TableName": hash_table, "Key": {"pk": {"S": "tg-1"}}}}
        ]
    )
    assert len(resp["Responses"]) == 1
    assert resp["Responses"][0]["Item"]["data"]["S"] == "val"
def test_transact_get_multiple(dynamodb_client, hash_table):
    """TransactGetItems with multiple Gets."""
    for i in range(3):
        dynamodb_client.put_item(TableName=hash_table, Item={"pk": {"S": f"tg-{i}"}, "n": {"N": str(i)}})
    resp = dynamodb_client.transact_get_items(
        TransactItems=[
            {"Get": {"TableName": hash_table, "Key": {"pk": {"S": f"tg-{i}"}}}}
            for i in range(3)
        ]
    )
    assert len(resp["Responses"]) == 3
    for i in range(3):
        assert resp["Responses"][i]["Item"]["n"]["N"] == str(i)
def test_transact_get_missing_item(dynamodb_client, hash_table):
    """TransactGetItems returns empty object (no Item key) for missing items."""
    dynamodb_client.put_item(TableName=hash_table, Item={"pk": {"S": "exists"}, "v": {"S": "yes"}})
    resp = dynamodb_client.transact_get_items(
        TransactItems=[
            {"Get": {"TableName": hash_table, "Key": {"pk": {"S": "exists"}}}},
            {"Get": {"TableName": hash_table, "Key": {"pk": {"S": "missing"}}}},
        ]
    )
    assert len(resp["Responses"]) == 2
    assert resp["Responses"][0]["Item"]["v"]["S"] == "yes"
    # Missing item returns empty object — no Item key present.
    assert "Item" not in resp["Responses"][1]
# ---------------------------------------------------------------------------
# TransactGetItems — validation errors
# ---------------------------------------------------------------------------
def test_transact_get_empty_items(dynamodb_client):
    """TransactGetItems with empty TransactItems → client-side validation."""
    with pytest.raises(Exception):
        dynamodb_client.transact_get_items(TransactItems=[])
def test_transact_get_wrong_key_type(dynamodb_client, hash_table):
    """TransactGetItems with wrong key type → TransactionCanceledException."""
    with pytest.raises(ClientError) as exc_info:
        dynamodb_client.transact_get_items(
            TransactItems=[
                {"Get": {"TableName": hash_table, "Key": {"pk": {"N": "123"}}}}
            ]
        )
    err = exc_info.value.response["Error"]
    assert err["Code"] == "TransactionCanceledException"
    reasons = exc_info.value.response.get("CancellationReasons", [])
    assert len(reasons) == 1
    assert reasons[0]["Code"] == "ValidationError"
# ---------------------------------------------------------------------------
# TransactWriteItems — conditional Put
# ---------------------------------------------------------------------------
def test_transact_write_conditional_put_pass(dynamodb_client, hash_table):
    """TransactWriteItems Put with passing condition."""
    dynamodb_client.transact_write_items(
        TransactItems=[
            {
                "Put": {
                    "TableName": hash_table,
                    "Item": {"pk": {"S": "cp-1"}, "v": {"S": "new"}},
                    "ConditionExpression": "attribute_not_exists(pk)",
                }
            }
        ]
    )
    resp = dynamodb_client.get_item(TableName=hash_table, Key={"pk": {"S": "cp-1"}})
    assert resp["Item"]["v"]["S"] == "new"
def test_transact_write_conditional_put_fail(dynamodb_client, hash_table):
    """TransactWriteItems Put with failing condition → TransactionCanceledException."""
    dynamodb_client.put_item(TableName=hash_table, Item={"pk": {"S": "cp-2"}, "v": {"S": "old"}})
    with pytest.raises(ClientError) as exc_info:
        dynamodb_client.transact_write_items(
            TransactItems=[
                {
                    "Put": {
                        "TableName": hash_table,
                        "Item": {"pk": {"S": "cp-2"}, "v": {"S": "new"}},
                        "ConditionExpression": "attribute_not_exists(pk)",
                    }
                }
            ]
        )
    assert exc_info.value.response["Error"]["Code"] == "TransactionCanceledException"
    # Original item unchanged
    resp = dynamodb_client.get_item(TableName=hash_table, Key={"pk": {"S": "cp-2"}})
    assert resp["Item"]["v"]["S"] == "old"


# ---------------------------------------------------------------------------
# TransactWriteItems — size limit and condition edge cases
# ---------------------------------------------------------------------------


def test_transact_write_total_size_exceeds_4mb(dynamodb_client, hash_table):
    """TransactWriteItems with total item size > 4MB is rejected."""
    # Each item is ~400KB, 11 items = ~4.4MB > 4MB limit.
    items = []
    for i in range(11):
        items.append({
            "Put": {
                "TableName": hash_table,
                "Item": {
                    "pk": {"S": f"big-{i}"},
                    "data": {"S": "x" * (390 * 1024)},
                },
            }
        })
    with pytest.raises(ClientError) as exc_info:
        dynamodb_client.transact_write_items(TransactItems=items)
    assert exc_info.value.response["Error"]["Code"] == "ValidationException"


def test_transact_write_condition_on_nonexistent_item(dynamodb_client, hash_table):
    """ConditionCheck with attribute_not_exists on missing item passes."""
    dynamodb_client.transact_write_items(
        TransactItems=[
            {
                "ConditionCheck": {
                    "TableName": hash_table,
                    "Key": {"pk": {"S": "tw-ghost-check"}},
                    "ConditionExpression": "attribute_not_exists(pk)",
                }
            },
            {
                "Put": {
                    "TableName": hash_table,
                    "Item": {"pk": {"S": "tw-after-ghost"}, "v": {"S": "ok"}},
                }
            },
        ]
    )
    resp = dynamodb_client.get_item(TableName=hash_table, Key={"pk": {"S": "tw-after-ghost"}})
    assert resp["Item"]["v"]["S"] == "ok"


# ---------------------------------------------------------------------------
# TransactGetItems — empty projection omits Item (0acc23c)
# ---------------------------------------------------------------------------


def test_transact_get_empty_projection_omits_item(dynamodb_client, hash_table):
    """TransactGetItems with projection resolving to no attributes omits Item."""
    dynamodb_client.put_item(
        TableName=hash_table,
        Item={"pk": {"S": "proj-empty"}, "real_attr": {"S": "value"}},
    )
    resp = dynamodb_client.transact_get_items(
        TransactItems=[
            {
                "Get": {
                    "TableName": hash_table,
                    "Key": {"pk": {"S": "proj-empty"}},
                    "ProjectionExpression": "nonexistent_attr",
                }
            }
        ]
    )
    # Item exists but projection yields nothing — no Item key in response
    assert "Item" not in resp["Responses"][0]


def test_transact_get_valid_projection_includes_item(dynamodb_client, hash_table):
    """TransactGetItems with projection resolving to attributes includes Item."""
    dynamodb_client.put_item(
        TableName=hash_table,
        Item={"pk": {"S": "proj-valid"}, "a": {"S": "val"}},
    )
    resp = dynamodb_client.transact_get_items(
        TransactItems=[
            {
                "Get": {
                    "TableName": hash_table,
                    "Key": {"pk": {"S": "proj-valid"}},
                    "ProjectionExpression": "a",
                }
            }
        ]
    )
    assert resp["Responses"][0]["Item"]["a"]["S"] == "val"


def test_transact_get_missing_item_with_projection_omits_item(dynamodb_client, hash_table):
    """TransactGetItems on missing item with projection still omits Item."""
    resp = dynamodb_client.transact_get_items(
        TransactItems=[
            {
                "Get": {
                    "TableName": hash_table,
                    "Key": {"pk": {"S": "truly-missing"}},
                    "ProjectionExpression": "anything",
                }
            }
        ]
    )
    assert "Item" not in resp["Responses"][0]


# ---------------------------------------------------------------------------
# TransactGetItems — consumed capacity uses 2x RCU (ff1eed1)
# ---------------------------------------------------------------------------


def test_transact_get_single_item_costs_2_rcu(dynamodb_client, hash_table):
    """A single small existing item costs 2 RCU in TransactGetItems."""
    dynamodb_client.put_item(
        TableName=hash_table, Item={"pk": {"S": "rcu-single"}, "v": {"S": "x"}},
    )
    resp = dynamodb_client.transact_get_items(
        ReturnConsumedCapacity="TOTAL",
        TransactItems=[
            {"Get": {"TableName": hash_table, "Key": {"pk": {"S": "rcu-single"}}}}
        ],
    )
    cap = resp["ConsumedCapacity"][0]
    assert cap["CapacityUnits"] == 2.0
    assert cap["ReadCapacityUnits"] == 2.0


def test_transact_get_missing_item_costs_2_rcu(dynamodb_client, hash_table):
    """A missing item still costs 2 RCU in TransactGetItems."""
    resp = dynamodb_client.transact_get_items(
        ReturnConsumedCapacity="TOTAL",
        TransactItems=[
            {"Get": {"TableName": hash_table, "Key": {"pk": {"S": "rcu-ghost"}}}}
        ],
    )
    cap = resp["ConsumedCapacity"][0]
    assert cap["CapacityUnits"] == 2.0
    assert cap["ReadCapacityUnits"] == 2.0


def test_transact_get_two_items_costs_4_rcu(dynamodb_client, hash_table):
    """Two items (one existing, one missing) cost 4 RCU total."""
    dynamodb_client.put_item(
        TableName=hash_table, Item={"pk": {"S": "rcu-pair"}},
    )
    resp = dynamodb_client.transact_get_items(
        ReturnConsumedCapacity="TOTAL",
        TransactItems=[
            {"Get": {"TableName": hash_table, "Key": {"pk": {"S": "rcu-pair"}}}},
            {"Get": {"TableName": hash_table, "Key": {"pk": {"S": "rcu-pair-miss"}}}},
        ],
    )
    cap = resp["ConsumedCapacity"][0]
    assert cap["CapacityUnits"] == 4.0
    assert cap["ReadCapacityUnits"] == 4.0


# ---------------------------------------------------------------------------
# TransactGetItems — capacity breakdown includes RCU/WCU fields (34cec40)
# ---------------------------------------------------------------------------


def test_transact_get_indexes_capacity_has_rcu_field(dynamodb_client, hash_table):
    """INDEXES-level capacity includes ReadCapacityUnits in Table breakdown."""
    dynamodb_client.put_item(
        TableName=hash_table, Item={"pk": {"S": "cap-idx"}},
    )
    resp = dynamodb_client.transact_get_items(
        ReturnConsumedCapacity="INDEXES",
        TransactItems=[
            {"Get": {"TableName": hash_table, "Key": {"pk": {"S": "cap-idx"}}}}
        ],
    )
    cap = resp["ConsumedCapacity"][0]
    assert cap["ReadCapacityUnits"] == 2.0
    # Table breakdown should also have ReadCapacityUnits
    table_cap = cap.get("Table", {})
    assert table_cap.get("ReadCapacityUnits") == 2.0
    # WriteCapacityUnits should not be present for reads
    assert table_cap.get("WriteCapacityUnits") is None
