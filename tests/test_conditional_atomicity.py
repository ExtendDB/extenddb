# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Regression tests for conditional-write atomicity fixes (PR #338 B1/B3/B4).

B1 — attribute_not_exists condition on a non-key attribute must be atomic:
     exactly one of N concurrent PutItem calls with the same key and
     condition_expression="attribute_not_exists(guard)" must succeed.

B3 — conditional DeleteItem must not race with a concurrent UpdateItem:
     no serial ordering permits both succeeding while leaving the item absent.

B4 — unconditional PutItem must not be silently overwritten by a concurrent
     UpdateItem that started before the put landed.

REQ-TEST-B1, REQ-TEST-B3, REQ-TEST-B4
"""

from __future__ import annotations

import threading
import uuid

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table, wait_for_active


def _make_client(endpoint_url):
    import os
    import boto3
    kwargs = {
        "service_name": "dynamodb",
        "region_name": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
    }
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url
        if endpoint_url.startswith("https://"):
            kwargs["verify"] = False
    return boto3.client(**kwargs)


# ---------------------------------------------------------------------------
# B1 — attribute_not_exists on a non-key attribute is atomic
# ---------------------------------------------------------------------------

class TestB1AttributeNotExistsAtomicity:
    """Exactly one concurrent PutItem with attribute_not_exists(guard) must win."""

    @pytest.fixture(scope="class")
    def table(self, dynamodb_client):
        with scoped_table(dynamodb_client) as name:
            yield name

    def test_exactly_one_winner(self, table, endpoint_url):
        pk = f"b1-{uuid.uuid4().hex[:12]}"
        n_threads = 20
        successes = []
        failures = []
        barrier = threading.Barrier(n_threads)

        def _attempt(i):
            client = _make_client(endpoint_url)
            barrier.wait()
            try:
                client.put_item(
                    TableName=table,
                    Item={
                        "pk": {"S": pk},
                        "guard": {"S": f"owner-{i}"},
                    },
                    ConditionExpression="attribute_not_exists(#g)",
                    ExpressionAttributeNames={"#g": "guard"},
                )
                successes.append(i)
            except ClientError as e:
                if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                    failures.append(i)
                else:
                    raise

        threads = [threading.Thread(target=_attempt, args=(i,)) for i in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(successes) == 1, (
            f"Expected exactly 1 winner, got {len(successes)}: {successes}"
        )
        assert len(failures) == n_threads - 1


# ---------------------------------------------------------------------------
# B3 — conditional DeleteItem races with UpdateItem
# ---------------------------------------------------------------------------

class TestB3DeleteUpdateRace:
    """After a conditional delete and a concurrent update, item must not vanish
    while the update also reports success — that would violate serializability."""

    @pytest.fixture(scope="class")
    def table(self, dynamodb_client):
        with scoped_table(dynamodb_client) as name:
            yield name

    def test_no_lost_update_on_delete_race(self, table, endpoint_url):
        n_rounds = 30
        for _ in range(n_rounds):
            pk = f"b3-{uuid.uuid4().hex[:12]}"
            seed_client = _make_client(endpoint_url)
            seed_client.put_item(
                TableName=table,
                Item={"pk": {"S": pk}, "v": {"N": "1"}},
            )

            delete_ok = []
            update_ok = []
            barrier = threading.Barrier(2)

            def _delete():
                client = _make_client(endpoint_url)
                barrier.wait()
                try:
                    client.delete_item(
                        TableName=table,
                        Key={"pk": {"S": pk}},
                        ConditionExpression="#v = :one",
                        ExpressionAttributeNames={"#v": "v"},
                        ExpressionAttributeValues={":one": {"N": "1"}},
                    )
                    delete_ok.append(True)
                except ClientError as e:
                    if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
                        raise

            def _update():
                client = _make_client(endpoint_url)
                barrier.wait()
                try:
                    client.update_item(
                        TableName=table,
                        Key={"pk": {"S": pk}},
                        UpdateExpression="SET #v = :two",
                        ConditionExpression="#v = :one",
                        ExpressionAttributeNames={"#v": "v"},
                        ExpressionAttributeValues={
                            ":one": {"N": "1"},
                            ":two": {"N": "2"},
                        },
                    )
                    update_ok.append(True)
                except ClientError as e:
                    if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
                        raise

            td = threading.Thread(target=_delete)
            tu = threading.Thread(target=_update)
            td.start()
            tu.start()
            td.join()
            tu.join()

            # Both cannot succeed: that would require the item to be deleted
            # (v=1 matched delete) AND updated (v=1 matched update) — impossible
            # in any serial order.
            assert not (delete_ok and update_ok), (
                f"pk={pk}: both delete and update succeeded — atomicity violation"
            )

            # Verify final state is consistent with whichever operation won.
            resp = seed_client.get_item(
                TableName=table, Key={"pk": {"S": pk}}
            )
            item = resp.get("Item")
            if delete_ok:
                assert item is None, "Delete succeeded but item still present"
            elif update_ok:
                assert item is not None and item["v"]["N"] == "2", (
                    "Update succeeded but item has wrong value"
                )
            # If neither succeeded (both lost the race to each other's version
            # fence), the item remains at v=1 — that is a valid serial outcome.


# ---------------------------------------------------------------------------
# B4 — unconditional PutItem is not silently overwritten by a racing UpdateItem
# ---------------------------------------------------------------------------

class TestB4PutUpdateRace:
    """An unconditional PutItem must not be silently lost to a concurrent
    UpdateItem that read the old version before the put landed."""

    @pytest.fixture(scope="class")
    def table(self, dynamodb_client):
        with scoped_table(dynamodb_client) as name:
            yield name

    def test_put_not_lost_to_concurrent_update(self, table, endpoint_url):
        n_rounds = 30
        for _ in range(n_rounds):
            pk = f"b4-{uuid.uuid4().hex[:12]}"
            seed_client = _make_client(endpoint_url)
            seed_client.put_item(
                TableName=table,
                Item={"pk": {"S": pk}, "src": {"S": "orig"}},
            )

            barrier = threading.Barrier(2)
            put_done = threading.Event()
            update_done = threading.Event()

            def _put():
                client = _make_client(endpoint_url)
                barrier.wait()
                client.put_item(
                    TableName=table,
                    Item={"pk": {"S": pk}, "src": {"S": "put"}},
                )
                put_done.set()

            def _update():
                client = _make_client(endpoint_url)
                barrier.wait()
                try:
                    client.update_item(
                        TableName=table,
                        Key={"pk": {"S": pk}},
                        UpdateExpression="SET u = :one",
                        ExpressionAttributeValues={":one": {"N": "1"}},
                    )
                except ClientError:
                    pass
                update_done.set()

            tp = threading.Thread(target=_put)
            tu = threading.Thread(target=_update)
            tp.start()
            tu.start()
            tp.join()
            tu.join()

            resp = seed_client.get_item(
                TableName=table, Key={"pk": {"S": pk}}
            )
            item = resp.get("Item")
            assert item is not None, "Item disappeared"

            # The put replaced the whole item. If the update also succeeded it
            # must have been serialized AFTER the put (so src="put" and u=1),
            # or BEFORE the put (so src="put" and u absent). Either way src
            # must be "put" — the put must not be silently overwritten.
            assert item["src"]["S"] == "put", (
                f"PutItem result was overwritten: item={item}"
            )


# ---------------------------------------------------------------------------
# B5 — idempotency token is released on transaction cancellation
# ---------------------------------------------------------------------------

class TestB5IdempotencyTokenReleasedOnCancel:
    """A TransactWriteItems that fails a condition must release its token so
    a subsequent retry with the same token (after fixing the condition) succeeds."""

    @pytest.fixture(scope="class")
    def table(self, dynamodb_client):
        with scoped_table(dynamodb_client) as name:
            yield name

    def test_token_released_after_cancellation(self, table, endpoint_url):
        client = _make_client(endpoint_url)
        pk_target = f"b5-target-{uuid.uuid4().hex[:12]}"
        pk_blocker = f"b5-blocker-{uuid.uuid4().hex[:12]}"

        # Seed the target so attribute_not_exists(pk) fails on first attempt.
        client.put_item(
            TableName=table,
            Item={"pk": {"S": pk_target}, "v": {"N": "0"}},
        )

        token = f"t-{uuid.uuid4().hex[:33]}"

        def _transact():
            return client.transact_write_items(
                TransactItems=[
                    {
                        "Put": {
                            "TableName": table,
                            "Item": {"pk": {"S": pk_blocker}, "v": {"N": "1"}},
                            "ConditionExpression": "attribute_not_exists(pk)",
                        }
                    },
                    {
                        "Put": {
                            "TableName": table,
                            "Item": {"pk": {"S": pk_target}, "v": {"N": "1"}},
                            "ConditionExpression": "attribute_not_exists(pk)",
                        }
                    },
                ],
                ClientRequestToken=token,
            )

        # First attempt: pk_target already exists → condition fails → TransactionCanceledException.
        with pytest.raises(ClientError) as exc_info:
            _transact()
        assert exc_info.value.response["Error"]["Code"] == "TransactionCanceledException"

        # Remove the blocker so both conditions can pass on retry.
        client.delete_item(TableName=table, Key={"pk": {"S": pk_target}})

        # Retry with the same token — must succeed and write the items (not replay as no-op).
        _transact()

        resp = client.get_item(TableName=table, Key={"pk": {"S": pk_blocker}})
        assert "Item" in resp, (
            "Retry with same token after cancellation wrote nothing — token was poisoned (B5)"
        )
