# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Phase 22 concurrency stress tests — 50 threads against extenddb.

Exercises parallel inserts, atomic counters (ADD), concurrent list_append,
concurrent set union (ADD on SS), concurrent nested path writes, parallel
random reads, and parallel deletes. Each thread gets its own boto3 client.

**Execution order dependency:** TestParallelReads and TestParallelDeletes depend
on the 50,000 items created by TestParallelInserts. pytest executes test classes
in file order by default. Running TestParallelReads or TestParallelDeletes in
isolation will produce misleading results (reads return nothing, deletes delete
nothing and falsely pass). Always run the full module.

REQ-TEST-001
"""

from __future__ import annotations

import os
import random
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
import pytest
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

from conftest import wait_for_active, wait_for_deleted

NUM_THREADS = 50
ITEMS_PER_THREAD = 1000
INCREMENTS_PER_THREAD = 100

# Maximum retries for operations that hit pool exhaustion under single-row
# contention.  50 threads all contending on one row can saturate a 20-connection
# pool, causing transient InternalServerError from pool-acquire timeouts.
_MAX_RETRIES = 20
_RETRY_BASE_SLEEP = 0.05
_RETRYABLE_CODES = {"InternalServerError", "TransactionConflictException"}

def _retry_on_internal_error(fn, max_retries: int = _MAX_RETRIES):
    """Call *fn*; retry on transient errors with exponential backoff + jitter.

    Retries InternalServerError (connection-pool exhaustion) and
    TransactionConflictException (OCC contention under high concurrency,
    more likely on Cassandra where Paxos latency is higher than on
    PostgreSQL/SQLite).
    """
    for attempt in range(max_retries + 1):
        try:
            return fn()
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code in _RETRYABLE_CODES and attempt < max_retries:
                sleep = _RETRY_BASE_SLEEP * (2 ** min(attempt, 6))
                time.sleep(sleep + random.random() * sleep)
                continue
            raise
def _make_client():
    """Create a fresh boto3 DynamoDB client for the current thread.

    Retries are disabled at the SDK level — the test harness handles retries
    via ``_retry_on_internal_error`` to avoid compounding pool pressure.
    """
    endpoint = os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip()
    kwargs: dict = {
        "service_name": "dynamodb",
        "region_name": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        "config": BotoConfig(retries={"max_attempts": 1, "mode": "standard"}),
    }
    if endpoint:
        kwargs["endpoint_url"] = endpoint
        # D4: Self-signed certs from ``extenddb init`` — disable SSL verification.
        if endpoint.startswith("https://"):
            kwargs["verify"] = False
    return boto3.client(**kwargs)
def _scan_all(client, table_name: str) -> list[dict]:
    """Scan all items from a table, handling pagination."""
    items: list[dict] = []
    kwargs: dict = {"TableName": table_name}
    while True:
        resp = client.scan(**kwargs)
        items.extend(resp.get("Items", []))
        if "LastEvaluatedKey" not in resp:
            break
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    return items
@pytest.fixture(scope="module")
def table_name():
    """Unique table name for the concurrency test module."""
    return f"extenddb-conc-{uuid.uuid4().hex[:12]}"
@pytest.fixture(scope="module")
def setup_table(table_name):
    """Create the test table once for the module, delete on teardown."""
    client = _make_client()
    client.create_table(
        TableName=table_name,
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
        ],
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    wait_for_active(client, table_name)
    yield table_name
    try:
        client.delete_table(TableName=table_name)
    except Exception:
        pass
    else:
        wait_for_deleted(client, table_name)
class TestParallelInserts:
    """50 threads × 1,000 items = 50,000 items inserted in parallel."""

    def test_parallel_inserts(self, setup_table):
        table = setup_table
        total = NUM_THREADS * ITEMS_PER_THREAD

        def _insert_batch(thread_id: int) -> int:
            client = _make_client()
            count = 0
            for i in range(ITEMS_PER_THREAD):
                _retry_on_internal_error(
                    lambda i=i: client.put_item(
                        TableName=table,
                        Item={
                            "pk": {"S": f"t{thread_id}-{i}"},
                            "thread": {"N": str(thread_id)},
                            "seq": {"N": str(i)},
                        },
                    )
                )
                count += 1
            return count

        with ThreadPoolExecutor(max_workers=NUM_THREADS) as pool:
            futures = [pool.submit(_insert_batch, tid) for tid in range(NUM_THREADS)]
            inserted = sum(f.result() for f in as_completed(futures))

        assert inserted == total

        # Verify via scan
        client = _make_client()
        items = _scan_all(client, table)
        assert len(items) == total
class TestAtomicCounter:
    """50 threads × 100 increments on the SAME item using ADD. Final = 5,000."""

    def test_atomic_counter(self, setup_table):
        table = setup_table
        counter_key = f"counter-{uuid.uuid4().hex[:8]}"
        client = _make_client()

        # Seed the counter item
        client.put_item(
            TableName=table,
            Item={"pk": {"S": counter_key}, "counter": {"N": "0"}},
        )

        def _increment(thread_id: int) -> int:
            c = _make_client()
            done = 0
            for _ in range(INCREMENTS_PER_THREAD):
                _retry_on_internal_error(
                    lambda: c.update_item(
                        TableName=table,
                        Key={"pk": {"S": counter_key}},
                        UpdateExpression="ADD #c :one",
                        ExpressionAttributeNames={"#c": "counter"},
                        ExpressionAttributeValues={":one": {"N": "1"}},
                    )
                )
                done += 1
            return done

        with ThreadPoolExecutor(max_workers=NUM_THREADS) as pool:
            futures = [pool.submit(_increment, tid) for tid in range(NUM_THREADS)]
            total_ops = sum(f.result() for f in as_completed(futures))

        assert total_ops == NUM_THREADS * INCREMENTS_PER_THREAD

        resp = client.get_item(
            TableName=table,
            Key={"pk": {"S": counter_key}},
        )
        final_value = int(resp["Item"]["counter"]["N"])
        assert final_value == NUM_THREADS * INCREMENTS_PER_THREAD
class TestConcurrentListAppend:
    """50 threads each append to the same list attribute."""

    def test_concurrent_list_append(self, setup_table):
        table = setup_table
        list_key = f"listitem-{uuid.uuid4().hex[:8]}"
        client = _make_client()

        # Seed with empty list
        client.put_item(
            TableName=table,
            Item={"pk": {"S": list_key}, "events": {"L": []}},
        )

        def _append(thread_id: int) -> int:
            c = _make_client()
            _retry_on_internal_error(
                lambda: c.update_item(
                    TableName=table,
                    Key={"pk": {"S": list_key}},
                    UpdateExpression="SET events = list_append(events, :items)",
                    ExpressionAttributeValues={
                        ":items": {"L": [{"S": f"thread-{thread_id}"}]},
                    },
                )
            )
            return 1

        with ThreadPoolExecutor(max_workers=NUM_THREADS) as pool:
            futures = [pool.submit(_append, tid) for tid in range(NUM_THREADS)]
            total_ops = sum(f.result() for f in as_completed(futures))

        assert total_ops == NUM_THREADS

        resp = client.get_item(
            TableName=table,
            Key={"pk": {"S": list_key}},
        )
        events = resp["Item"]["events"]["L"]
        assert len(events) == NUM_THREADS
class TestConcurrentSetUnion:
    """50 threads ADD unique tags to the same SS attribute."""

    def test_concurrent_set_union(self, setup_table):
        table = setup_table
        set_key = f"setitem-{uuid.uuid4().hex[:8]}"
        client = _make_client()

        # Seed with initial tag so the attribute exists as SS
        client.put_item(
            TableName=table,
            Item={"pk": {"S": set_key}, "tags": {"SS": ["seed"]}},
        )

        def _add_tags(thread_id: int) -> set[str]:
            c = _make_client()
            my_tags = {f"tag-{thread_id}-{i}" for i in range(3)}
            _retry_on_internal_error(
                lambda: c.update_item(
                    TableName=table,
                    Key={"pk": {"S": set_key}},
                    UpdateExpression="ADD tags :newTags",
                    ExpressionAttributeValues={":newTags": {"SS": list(my_tags)}},
                )
            )
            return my_tags

        all_expected: set[str] = {"seed"}
        with ThreadPoolExecutor(max_workers=NUM_THREADS) as pool:
            futures = [pool.submit(_add_tags, tid) for tid in range(NUM_THREADS)]
            for f in as_completed(futures):
                all_expected |= f.result()

        resp = client.get_item(
            TableName=table,
            Key={"pk": {"S": set_key}},
        )
        actual_tags = set(resp["Item"]["tags"]["SS"])
        assert actual_tags == all_expected
class TestConcurrentNestedPaths:
    """50 threads write to different nested paths on the same item."""

    def test_concurrent_nested_paths(self, setup_table):
        table = setup_table
        nested_key = f"nested-{uuid.uuid4().hex[:8]}"
        client = _make_client()

        # Seed with empty map
        client.put_item(
            TableName=table,
            Item={"pk": {"S": nested_key}, "data": {"M": {}}},
        )

        def _write_path(thread_id: int) -> str:
            c = _make_client()
            path_name = f"thread_{thread_id}"
            _retry_on_internal_error(
                lambda: c.update_item(
                    TableName=table,
                    Key={"pk": {"S": nested_key}},
                    UpdateExpression="SET #d.#t = :val",
                    ExpressionAttributeNames={"#d": "data", "#t": path_name},
                    ExpressionAttributeValues={
                        ":val": {"S": f"value-{thread_id}"},
                    },
                )
            )
            return path_name

        with ThreadPoolExecutor(max_workers=NUM_THREADS) as pool:
            futures = [pool.submit(_write_path, tid) for tid in range(NUM_THREADS)]
            expected_paths = {f.result() for f in as_completed(futures)}

        resp = client.get_item(
            TableName=table,
            Key={"pk": {"S": nested_key}},
        )
        actual_paths = set(resp["Item"]["data"]["M"].keys())
        assert actual_paths == expected_paths
        assert len(actual_paths) == NUM_THREADS
class TestParallelReads:
    """50 threads do random GetItem reads concurrently. No errors expected."""

    def test_parallel_reads(self, setup_table):
        table = setup_table
        # Read from the 50,000 items inserted by TestParallelInserts
        keys = [f"t{tid}-{i}" for tid in range(NUM_THREADS) for i in range(10)]

        errors: list[str] = []

        def _read_random(thread_id: int) -> int:
            c = _make_client()
            count = 0
            sample = random.sample(keys, min(50, len(keys)))
            for key in sample:
                try:
                    c.get_item(
                        TableName=table,
                        Key={"pk": {"S": key}},
                    )
                    count += 1
                except ClientError as e:
                    errors.append(f"Thread {thread_id} key={key}: {e}")
            return count

        with ThreadPoolExecutor(max_workers=NUM_THREADS) as pool:
            futures = [pool.submit(_read_random, tid) for tid in range(NUM_THREADS)]
            total_reads = sum(f.result() for f in as_completed(futures))

        assert not errors, f"Read errors: {errors[:10]}"
        assert total_reads == NUM_THREADS * 50
class TestParallelDeletes:
    """50 threads delete all 50,000 items in random order. Table should be empty."""

    def test_parallel_deletes(self, setup_table):
        table = setup_table
        # Build the full key list from the parallel inserts
        all_keys = [f"t{tid}-{i}" for tid in range(NUM_THREADS) for i in range(ITEMS_PER_THREAD)]
        random.shuffle(all_keys)

        # Split keys across threads
        chunk_size = len(all_keys) // NUM_THREADS
        chunks = [
            all_keys[i * chunk_size : (i + 1) * chunk_size]
            for i in range(NUM_THREADS)
        ]
        # Distribute remainder
        for i, key in enumerate(all_keys[NUM_THREADS * chunk_size :]):
            chunks[i].append(key)

        def _delete_chunk(keys: list[str]) -> int:
            c = _make_client()
            count = 0
            for key in keys:
                c.delete_item(
                    TableName=table,
                    Key={"pk": {"S": key}},
                )
                count += 1
            return count

        with ThreadPoolExecutor(max_workers=NUM_THREADS) as pool:
            futures = [pool.submit(_delete_chunk, chunk) for chunk in chunks]
            total_deleted = sum(f.result() for f in as_completed(futures))

        assert total_deleted == len(all_keys)

        # Also delete the special items from other tests
        client = _make_client()
        remaining = _scan_all(client, table)
        for item in remaining:
            client.delete_item(
                TableName=table,
                Key={"pk": item["pk"]},
            )

        # Verify table is empty
        final = _scan_all(client, table)
        assert len(final) == 0
