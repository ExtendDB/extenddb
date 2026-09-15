# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""RFC-0003 §4 concurrency conformance — strict, no client-side retries.

Every scenario asserts that the backend surfaces the correct DDB error
class (or no error at all) even under sustained contention. If the
backend produces `InternalServerError` under any of these workloads,
the test fails — DDB is not permitted to surface internal concurrency-
control mechanisms as client errors, and neither is a conformant
backend.

The RFC-0003 stress-test scenarios covered here:

- §4.1  Two concurrent unconditional `PutItem` on the same key must
        both succeed (last-writer-wins).
- §4.1  Concurrent conditional `PutItem` with `attribute_not_exists`:
        exactly one succeeds, the rest fail with
        `ConditionalCheckFailedException` — nothing internal.
- §4.4  Concurrent `UpdateItem ADD counter :one` on the same item must
        all succeed; the final counter value must equal the total
        increments applied (no lost updates, no internal errors).
- §4.1  Concurrent unconditional `DeleteItem` on the same key must all
        return without error (last-writer-wins semantics for delete).

These tests deliberately use a boto3 client with `retries={"max_attempts": 0}`
so any InternalServerError surfaces immediately instead of being masked
by the SDK's retry policy.
"""

from __future__ import annotations

import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest
from botocore.exceptions import ClientError

from helpers import unique_name, wait_for_active, wait_for_deleted


NUM_THREADS = 50
INCREMENTS_PER_THREAD = 20  # 50 * 20 = 1_000 increments


@pytest.fixture()
def counter_table(dynamodb_client):
    """A plain HASH-keyed table for the RFC-0003 §4.x scenarios."""
    name = unique_name("rfc4x")
    dynamodb_client.create_table(
        TableName=name,
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        BillingMode="PAY_PER_REQUEST",
    )
    wait_for_active(dynamodb_client, name)
    yield name
    dynamodb_client.delete_table(TableName=name)
    wait_for_deleted(dynamodb_client, name)


def _classify(exc: ClientError) -> str:
    """Return the ClientError's DynamoDB error code."""
    return exc.response.get("Error", {}).get("Code", "")


class TestRfc0003UnconditionalPutOnHotKey:
    """RFC-0003 §4.1 — concurrent unconditional PutItem on the same key.

    Both must succeed; DDB never surfaces contention as a client error
    for unconditional writes. This is the scenario the pre-Phase-6
    backend violated by returning `InternalServerError` after 50 retry
    attempts of a snapshot-txn WriteConflict loop.
    """

    def test_all_writers_succeed(self, dynamodb_client, counter_table):
        key = f"hot-{uuid.uuid4().hex[:8]}"
        errors: list[str] = []

        def _write(thread_id: int) -> None:
            try:
                dynamodb_client.put_item(
                    TableName=counter_table,
                    Item={
                        "pk": {"S": key},
                        "writer": {"N": str(thread_id)},
                    },
                )
            except ClientError as e:
                errors.append(_classify(e))

        with ThreadPoolExecutor(max_workers=NUM_THREADS) as pool:
            futs = [pool.submit(_write, tid) for tid in range(NUM_THREADS)]
            for f in as_completed(futs):
                f.result()

        # DDB contract: all writes succeed. Any error is a conformance failure.
        assert errors == [], f"unexpected errors: {errors}"

        # And the item exists with *some* writer's value — last-writer-wins,
        # so we don't assert which one, just that the item is there.
        resp = dynamodb_client.get_item(
            TableName=counter_table,
            Key={"pk": {"S": key}},
            ConsistentRead=True,
        )
        assert "Item" in resp


class TestRfc0003ConditionalPutOnHotKey:
    """RFC-0003 §4.1 — concurrent conditional PutItem with attribute_not_exists.

    Exactly one writer wins (item created). Everyone else must fail with
    `ConditionalCheckFailedException`, not `InternalServerError`.
    """

    def test_one_winner_rest_ccf(self, dynamodb_client, counter_table):
        key = f"race-{uuid.uuid4().hex[:8]}"
        outcomes: list[tuple[str, str]] = []  # (result, error_code)

        def _conditional_put(thread_id: int) -> None:
            try:
                dynamodb_client.put_item(
                    TableName=counter_table,
                    Item={
                        "pk": {"S": key},
                        "winner": {"N": str(thread_id)},
                    },
                    ConditionExpression="attribute_not_exists(pk)",
                )
                outcomes.append(("ok", ""))
            except ClientError as e:
                outcomes.append(("err", _classify(e)))

        with ThreadPoolExecutor(max_workers=NUM_THREADS) as pool:
            futs = [pool.submit(_conditional_put, tid) for tid in range(NUM_THREADS)]
            for f in as_completed(futs):
                f.result()

        winners = [o for o in outcomes if o[0] == "ok"]
        losers = [o for o in outcomes if o[0] == "err"]

        assert len(winners) == 1, f"expected exactly one winner, got {len(winners)}"
        assert len(losers) == NUM_THREADS - 1

        # All losers must have ConditionalCheckFailedException — nothing
        # else. Any InternalServerError is a conformance failure.
        for _, code in losers:
            assert code == "ConditionalCheckFailedException", (
                f"loser returned {code!r} instead of ConditionalCheckFailedException"
            )


class TestRfc0003AtomicCounterAdd:
    """RFC-0003 §4.4 — concurrent `UpdateItem ADD counter :one`.

    Every increment must apply cumulatively; the final counter equals
    NUM_THREADS * INCREMENTS_PER_THREAD. Every UpdateItem call must
    succeed — no InternalServerError, no retries at the client.

    The mongo backend uses an aggregation-pipeline update
    (`$toString` of `$add` of `$toDecimal`) so 50+ concurrent ADD
    calls converge at MongoDB's doc-lock level without OCC retries.
    """

    def test_all_increments_apply(self, dynamodb_client, counter_table):
        key = f"counter-{uuid.uuid4().hex[:8]}"
        dynamodb_client.put_item(
            TableName=counter_table,
            Item={"pk": {"S": key}, "counter": {"N": "0"}},
        )
        errors: list[str] = []

        def _increment(thread_id: int) -> int:
            done = 0
            for _ in range(INCREMENTS_PER_THREAD):
                try:
                    dynamodb_client.update_item(
                        TableName=counter_table,
                        Key={"pk": {"S": key}},
                        UpdateExpression="ADD #c :one",
                        ExpressionAttributeNames={"#c": "counter"},
                        ExpressionAttributeValues={":one": {"N": "1"}},
                    )
                    done += 1
                except ClientError as e:
                    errors.append(_classify(e))
            return done

        with ThreadPoolExecutor(max_workers=NUM_THREADS) as pool:
            futs = [pool.submit(_increment, tid) for tid in range(NUM_THREADS)]
            total_done = sum(f.result() for f in as_completed(futs))

        assert errors == [], f"unexpected errors: {errors}"
        assert total_done == NUM_THREADS * INCREMENTS_PER_THREAD

        # The counter must equal every increment applied. No lost updates.
        resp = dynamodb_client.get_item(
            TableName=counter_table,
            Key={"pk": {"S": key}},
            ConsistentRead=True,
        )
        final = int(resp["Item"]["counter"]["N"])
        assert final == NUM_THREADS * INCREMENTS_PER_THREAD


class TestRfc0003UnconditionalDeleteOnHotKey:
    """RFC-0003 §4.1 — concurrent unconditional DeleteItem on the same key.

    All succeed. If the item exists, one delete removes it and the rest
    are no-ops; if it doesn't, all are no-ops. Never an error.
    """

    def test_all_deletes_succeed(self, dynamodb_client, counter_table):
        key = f"delkey-{uuid.uuid4().hex[:8]}"
        dynamodb_client.put_item(
            TableName=counter_table,
            Item={"pk": {"S": key}, "val": {"S": "seed"}},
        )
        errors: list[str] = []

        def _delete(_thread_id: int) -> None:
            try:
                dynamodb_client.delete_item(
                    TableName=counter_table,
                    Key={"pk": {"S": key}},
                )
            except ClientError as e:
                errors.append(_classify(e))

        with ThreadPoolExecutor(max_workers=NUM_THREADS) as pool:
            futs = [pool.submit(_delete, tid) for tid in range(NUM_THREADS)]
            for f in as_completed(futs):
                f.result()

        assert errors == [], f"unexpected errors: {errors}"

        # Item must be gone.
        resp = dynamodb_client.get_item(
            TableName=counter_table,
            Key={"pk": {"S": key}},
            ConsistentRead=True,
        )
        assert "Item" not in resp
