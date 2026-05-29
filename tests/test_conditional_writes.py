# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Conditional write correctness tests.

Verifies that ConditionExpression on PutItem and UpdateItem behaves identically
to real DynamoDB: exactly one writer wins under concurrency, losers receive
ConditionalCheckFailedException, and ReturnValuesOnConditionCheckFailure
returns the correct item (or no item) depending on the request parameters.

These tests run against both real DynamoDB and extenddb with identical assertions.
"""

from __future__ import annotations

import os
import uuid
from concurrent.futures import ThreadPoolExecutor

import boto3
import pytest
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

from conftest import wait_for_active, wait_for_deleted


def _make_client():
    """Create a fresh boto3 client for the current thread."""
    endpoint = os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip()
    kwargs: dict = {
        "service_name": "dynamodb",
        "region_name": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        "config": BotoConfig(retries={"max_attempts": 1, "mode": "standard"}),
    }
    if endpoint:
        kwargs["endpoint_url"] = endpoint
        if endpoint.startswith("https://"):
            kwargs["verify"] = False
    return boto3.client(**kwargs)


@pytest.fixture()
def condition_table(dynamodb_client):
    """Table with pk (HASH) + sk (RANGE) for condition tests."""
    name = f"extenddb-cond-{uuid.uuid4().hex[:8]}"
    dynamodb_client.create_table(
        TableName=name,
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    wait_for_active(dynamodb_client, name)
    yield name
    try:
        dynamodb_client.delete_table(TableName=name)
        wait_for_deleted(dynamodb_client, name)
    except Exception:
        pass


class TestConditionalPutItem:
    """PutItem with ConditionExpression — correctness and response fidelity."""

    # --- Linearizability under concurrency ---

    def test_exactly_one_winner(self, dynamodb_client, condition_table):
        """50 concurrent PutItem with attribute_not_exists — exactly 1 wins."""
        key = {"pk": {"S": "race"}, "sk": {"S": "1"}}

        def attempt(i):
            c = _make_client()
            try:
                c.put_item(
                    TableName=condition_table,
                    Item={**key, "winner": {"N": str(i)}},
                    ConditionExpression="attribute_not_exists(pk)",
                )
                return ("ok", i)
            except ClientError as e:
                if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                    return ("conflict", i)
                return ("error", str(e))

        with ThreadPoolExecutor(max_workers=50) as pool:
            results = list(pool.map(attempt, range(50)))

        successes = [r for r in results if r[0] == "ok"]
        conflicts = [r for r in results if r[0] == "conflict"]
        errors = [r for r in results if r[0] == "error"]

        assert len(errors) == 0, f"Unexpected errors: {errors}"
        assert len(successes) == 1, f"Expected 1 winner, got {len(successes)}: {successes}"
        assert len(conflicts) == 49

    def test_race_loser_gets_item_with_all_old(self, dynamodb_client, condition_table):
        """Losers get the winner's item when ReturnValuesOnConditionCheckFailure=ALL_OLD."""
        key = {"pk": {"S": "race-ccf"}, "sk": {"S": "1"}}

        def attempt(i):
            c = _make_client()
            try:
                c.put_item(
                    TableName=condition_table,
                    Item={**key, "winner": {"N": str(i)}},
                    ConditionExpression="attribute_not_exists(pk)",
                    ReturnValuesOnConditionCheckFailure="ALL_OLD",
                )
                return ("won", i, None)
            except ClientError as e:
                if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                    return ("lost", i, e.response.get("Item"))
                return ("error", i, str(e))

        with ThreadPoolExecutor(max_workers=50) as pool:
            results = list(pool.map(attempt, range(50)))

        winners = [r for r in results if r[0] == "won"]
        losers = [r for r in results if r[0] == "lost"]

        assert len(winners) == 1
        winner_id = str(winners[0][1])
        for _, _, item in losers:
            assert item is not None, "Loser should get item with ALL_OLD"
            assert item["winner"]["N"] == winner_id

    def test_race_loser_no_item_without_all_old(self, dynamodb_client, condition_table):
        """Losers get no item when ReturnValuesOnConditionCheckFailure is not set."""
        key = {"pk": {"S": "race-none"}, "sk": {"S": "1"}}

        def attempt(i):
            c = _make_client()
            try:
                c.put_item(
                    TableName=condition_table,
                    Item={**key, "winner": {"N": str(i)}},
                    ConditionExpression="attribute_not_exists(pk)",
                )
                return ("won", i, None)
            except ClientError as e:
                if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                    return ("lost", i, e.response.get("Item"))
                return ("error", i, str(e))

        with ThreadPoolExecutor(max_workers=50) as pool:
            results = list(pool.map(attempt, range(50)))

        winners = [r for r in results if r[0] == "won"]
        losers = [r for r in results if r[0] == "lost"]

        assert len(winners) == 1
        for _, _, item in losers:
            assert item is None, "Loser should NOT get item without ALL_OLD"

    # --- Response shape on condition failure (non-concurrent) ---

    def test_condition_fail_response_shape(self, dynamodb_client, condition_table):
        """ConditionalCheckFailedException has correct error code and message."""
        dynamodb_client.put_item(
            TableName=condition_table,
            Item={"pk": {"S": "exists"}, "sk": {"S": "1"}, "data": {"S": "original"}},
        )
        with pytest.raises(ClientError) as exc:
            dynamodb_client.put_item(
                TableName=condition_table,
                Item={"pk": {"S": "exists"}, "sk": {"S": "1"}, "data": {"S": "new"}},
                ConditionExpression="attribute_not_exists(pk)",
            )
        err = exc.value.response
        assert err["Error"]["Code"] == "ConditionalCheckFailedException"
        assert err["Error"]["Message"] == "The conditional request failed"
        assert "Item" not in err

    def test_condition_fail_all_old_returns_existing_item(self, dynamodb_client, condition_table):
        """ALL_OLD returns the item that blocked the write."""
        dynamodb_client.put_item(
            TableName=condition_table,
            Item={"pk": {"S": "blocker"}, "sk": {"S": "1"}, "data": {"S": "original"}},
        )
        with pytest.raises(ClientError) as exc:
            dynamodb_client.put_item(
                TableName=condition_table,
                Item={"pk": {"S": "blocker"}, "sk": {"S": "1"}, "data": {"S": "new"}},
                ConditionExpression="attribute_not_exists(pk)",
                ReturnValuesOnConditionCheckFailure="ALL_OLD",
            )
        err = exc.value.response
        assert err["Item"]["pk"]["S"] == "blocker"
        assert err["Item"]["data"]["S"] == "original"

    def test_condition_fail_none_returns_no_item(self, dynamodb_client, condition_table):
        """NONE explicitly suppresses item return."""
        dynamodb_client.put_item(
            TableName=condition_table,
            Item={"pk": {"S": "blocker2"}, "sk": {"S": "1"}, "data": {"S": "x"}},
        )
        with pytest.raises(ClientError) as exc:
            dynamodb_client.put_item(
                TableName=condition_table,
                Item={"pk": {"S": "blocker2"}, "sk": {"S": "1"}, "data": {"S": "y"}},
                ConditionExpression="attribute_not_exists(pk)",
                ReturnValuesOnConditionCheckFailure="NONE",
            )
        assert "Item" not in exc.value.response

    def test_condition_fail_no_consumed_capacity_in_error(self, dynamodb_client, condition_table):
        """ConsumedCapacity is NOT returned on condition failure."""
        dynamodb_client.put_item(
            TableName=condition_table,
            Item={"pk": {"S": "cap"}, "sk": {"S": "1"}},
        )
        with pytest.raises(ClientError) as exc:
            dynamodb_client.put_item(
                TableName=condition_table,
                Item={"pk": {"S": "cap"}, "sk": {"S": "1"}, "data": {"S": "x"}},
                ConditionExpression="attribute_not_exists(pk)",
                ReturnConsumedCapacity="TOTAL",
            )
        assert "ConsumedCapacity" not in exc.value.response

    # --- attribute_exists on non-existent item ---

    def test_attribute_exists_fails_on_missing_item(self, dynamodb_client, condition_table):
        """attribute_exists(pk) fails when item doesn't exist."""
        with pytest.raises(ClientError) as exc:
            dynamodb_client.put_item(
                TableName=condition_table,
                Item={"pk": {"S": "ghost"}, "sk": {"S": "1"}},
                ConditionExpression="attribute_exists(pk)",
            )
        assert exc.value.response["Error"]["Code"] == "ConditionalCheckFailedException"
        assert "Item" not in exc.value.response

    def test_attribute_exists_fails_on_missing_with_all_old(self, dynamodb_client, condition_table):
        """attribute_exists on missing item with ALL_OLD — no item to return."""
        with pytest.raises(ClientError) as exc:
            dynamodb_client.put_item(
                TableName=condition_table,
                Item={"pk": {"S": "ghost2"}, "sk": {"S": "1"}},
                ConditionExpression="attribute_exists(pk)",
                ReturnValuesOnConditionCheckFailure="ALL_OLD",
            )
        assert exc.value.response["Error"]["Code"] == "ConditionalCheckFailedException"
        assert "Item" not in exc.value.response

    # --- Successful conditional write ---

    def test_conditional_put_succeeds_on_new_item(self, dynamodb_client, condition_table):
        """attribute_not_exists succeeds when item doesn't exist."""
        resp = dynamodb_client.put_item(
            TableName=condition_table,
            Item={"pk": {"S": "new"}, "sk": {"S": "1"}, "data": {"S": "created"}},
            ConditionExpression="attribute_not_exists(pk)",
            ReturnConsumedCapacity="TOTAL",
        )
        assert "ConsumedCapacity" in resp
        # Verify item was written
        get = dynamodb_client.get_item(
            TableName=condition_table,
            Key={"pk": {"S": "new"}, "sk": {"S": "1"}},
        )
        assert get["Item"]["data"]["S"] == "created"


# ---------------------------------------------------------------------------
# BETWEEN bounds validation (27a6d99)
# ---------------------------------------------------------------------------


class TestBetweenBoundsValidation:
    """BETWEEN operator validates bounds ordering and type consistency."""

    @pytest.fixture()
    def between_table(self, dynamodb_client):
        """Table for BETWEEN tests."""
        name = f"extenddb-between-{uuid.uuid4().hex[:8]}"
        dynamodb_client.create_table(
            TableName=name,
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        wait_for_active(dynamodb_client, name)
        yield name
        try:
            dynamodb_client.delete_table(TableName=name)
            wait_for_deleted(dynamodb_client, name)
        except Exception:
            pass

    def test_between_rejects_reversed_bounds(self, dynamodb_client, between_table):
        """BETWEEN with lower > upper is rejected with ValidationException."""
        dynamodb_client.put_item(
            TableName=between_table,
            Item={"pk": {"S": "k1"}, "v": {"N": "50"}},
        )
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.delete_item(
                TableName=between_table,
                Key={"pk": {"S": "k1"}},
                ConditionExpression="v BETWEEN :hi AND :lo",
                ExpressionAttributeValues={":hi": {"N": "100"}, ":lo": {"N": "1"}},
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert "BETWEEN" in err["Message"]

    def test_between_rejects_mismatched_types(self, dynamodb_client, between_table):
        """BETWEEN with different types for lower and upper bounds is rejected."""
        dynamodb_client.put_item(
            TableName=between_table,
            Item={"pk": {"S": "k2"}, "v": {"N": "50"}},
        )
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.delete_item(
                TableName=between_table,
                Key={"pk": {"S": "k2"}},
                ConditionExpression="v BETWEEN :lo AND :hi",
                ExpressionAttributeValues={":lo": {"N": "1"}, ":hi": {"S": "100"}},
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert "BETWEEN" in err["Message"]
        assert "same data type" in err["Message"]

    def test_between_accepts_valid_bounds(self, dynamodb_client, between_table):
        """BETWEEN with valid ordered same-type bounds succeeds."""
        dynamodb_client.put_item(
            TableName=between_table,
            Item={"pk": {"S": "k3"}, "v": {"N": "50"}},
        )
        # v BETWEEN 1 AND 100 — should pass, item deleted
        dynamodb_client.delete_item(
            TableName=between_table,
            Key={"pk": {"S": "k3"}},
            ConditionExpression="v BETWEEN :lo AND :hi",
            ExpressionAttributeValues={":lo": {"N": "1"}, ":hi": {"N": "100"}},
        )
        resp = dynamodb_client.get_item(TableName=between_table, Key={"pk": {"S": "k3"}})
        assert "Item" not in resp

    def test_between_accepts_equal_bounds(self, dynamodb_client, between_table):
        """BETWEEN with lower == upper is valid (matches exact value)."""
        dynamodb_client.put_item(
            TableName=between_table,
            Item={"pk": {"S": "k4"}, "v": {"N": "50"}},
        )
        # v BETWEEN 50 AND 50 — should pass
        dynamodb_client.delete_item(
            TableName=between_table,
            Key={"pk": {"S": "k4"}},
            ConditionExpression="v BETWEEN :lo AND :hi",
            ExpressionAttributeValues={":lo": {"N": "50"}, ":hi": {"N": "50"}},
        )
        resp = dynamodb_client.get_item(TableName=between_table, Key={"pk": {"S": "k4"}})
        assert "Item" not in resp

    def test_between_string_bounds_reversed_rejected(self, dynamodb_client, between_table):
        """BETWEEN with string bounds in wrong order is rejected."""
        dynamodb_client.put_item(
            TableName=between_table,
            Item={"pk": {"S": "k5"}, "v": {"S": "m"}},
        )
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.delete_item(
                TableName=between_table,
                Key={"pk": {"S": "k5"}},
                ConditionExpression="v BETWEEN :lo AND :hi",
                ExpressionAttributeValues={":lo": {"S": "z"}, ":hi": {"S": "a"}},
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert "BETWEEN" in err["Message"]

    def test_between_in_update_condition(self, dynamodb_client, between_table):
        """BETWEEN validation also applies in UpdateItem ConditionExpression."""
        dynamodb_client.put_item(
            TableName=between_table,
            Item={"pk": {"S": "k6"}, "v": {"N": "10"}},
        )
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.update_item(
                TableName=between_table,
                Key={"pk": {"S": "k6"}},
                UpdateExpression="SET v = :new",
                ConditionExpression="v BETWEEN :hi AND :lo",
                ExpressionAttributeValues={
                    ":new": {"N": "20"},
                    ":hi": {"N": "100"},
                    ":lo": {"N": "1"},
                },
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert "BETWEEN" in err["Message"]
