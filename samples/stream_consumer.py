# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Demo: DynamoDB Streams with concurrent writer and poller threads.

Creates a table with streams enabled, then runs two threads:
  - Writer: inserts items, updates them, deletes them
  - Poller: reads the stream and prints every record it sees

Usage:
    EXTENDDB_TEST_ENDPOINT=http://localhost:18443 python samples/stream_consumer.py
"""

from __future__ import annotations

import os
import sys
import threading
import time
import uuid

import boto3

ENDPOINT = os.environ.get("EXTENDDB_TEST_ENDPOINT", "http://localhost:18443").strip()
TABLE_NAME = f"stream-demo-{uuid.uuid4().hex[:8]}"
ITEM_COUNT = 5

# Shared state
stop_poller = threading.Event()
def log(thread: str, msg: str) -> None:
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] [{thread:6s}] {msg}", flush=True)
def make_client(service: str = "dynamodb"):
    return boto3.client(
        service,
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        endpoint_url=ENDPOINT,
    )
def wait_for_active(client, table_name: str, timeout: float = 60.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        resp = client.describe_table(TableName=table_name)
        if resp["Table"]["TableStatus"] == "ACTIVE":
            return
        time.sleep(0.5)
    raise TimeoutError(f"Table {table_name} did not become ACTIVE within {timeout}s")
def writer_thread() -> None:
    client = make_client()

    # Phase 1: Insert items
    for i in range(1, ITEM_COUNT + 1):
        client.put_item(
            TableName=TABLE_NAME,
            Item={
                "pk": {"S": f"item-{i}"},
                "version": {"N": "1"},
                "data": {"S": f"initial-value-{i}"},
            },
        )
        log("WRITER", f"INSERT  pk=item-{i}  version=1  data=initial-value-{i}")
        time.sleep(0.3)

    time.sleep(1)

    # Phase 2: Update items
    for i in range(1, ITEM_COUNT + 1):
        client.update_item(
            TableName=TABLE_NAME,
            Key={"pk": {"S": f"item-{i}"}},
            UpdateExpression="SET version = :v, #d = :d",
            ExpressionAttributeNames={"#d": "data"},
            ExpressionAttributeValues={
                ":v": {"N": "2"},
                ":d": {"S": f"updated-value-{i}"},
            },
        )
        log("WRITER", f"UPDATE  pk=item-{i}  version=2  data=updated-value-{i}")
        time.sleep(0.3)

    time.sleep(1)

    # Phase 3: Delete items
    for i in range(1, ITEM_COUNT + 1):
        client.delete_item(
            TableName=TABLE_NAME,
            Key={"pk": {"S": f"item-{i}"}},
        )
        log("WRITER", f"DELETE  pk=item-{i}")
        time.sleep(0.3)

    log("WRITER", "All writes complete. Waiting for poller to catch up...")
def format_image(image: dict | None) -> str:
    if not image:
        return "{}"
    parts = []
    for k, v in sorted(image.items()):
        typ = list(v.keys())[0]
        parts.append(f"{k}={v[typ]}")
    return "  ".join(parts)
def poller_thread() -> None:
    client = make_client()
    streams_client = make_client("dynamodbstreams")

    # Wait for the stream ARN to appear.
    stream_arn = None
    while not stream_arn and not stop_poller.is_set():
        try:
            desc = client.describe_table(TableName=TABLE_NAME)
            stream_arn = desc["Table"].get("LatestStreamArn")
        except Exception:
            pass
        if not stream_arn:
            time.sleep(0.5)

    if not stream_arn:
        return

    log("POLLER", f"Stream ARN: {stream_arn}")

    # Discover shards and get iterators.
    shard_iterators: dict[str, str | None] = {}

    def refresh_shards():
        resp = streams_client.describe_stream(StreamArn=stream_arn)
        for shard in resp["StreamDescription"]["Shards"]:
            sid = shard["ShardId"]
            if sid not in shard_iterators:
                it = streams_client.get_shard_iterator(
                    StreamArn=stream_arn,
                    ShardId=sid,
                    ShardIteratorType="TRIM_HORIZON",
                )
                shard_iterators[sid] = it["ShardIterator"]
                log("POLLER", f"Tracking shard {sid}")

    refresh_shards()

    records_seen = 0
    empty_polls = 0

    while not stop_poller.is_set():
        got_records = False

        for sid in list(shard_iterators.keys()):
            it = shard_iterators[sid]
            if not it:
                continue
            try:
                resp = streams_client.get_records(ShardIterator=it, Limit=25)
            except Exception as e:
                log("POLLER", f"Error reading shard {sid}: {e}")
                shard_iterators[sid] = None
                continue

            shard_iterators[sid] = resp.get("NextShardIterator")

            for rec in resp.get("Records", []):
                got_records = True
                records_seen += 1
                event = rec["eventName"]
                keys = format_image(rec["dynamodb"].get("Keys"))
                old = format_image(rec["dynamodb"].get("OldImage"))
                new = format_image(rec["dynamodb"].get("NewImage"))

                if event == "INSERT":
                    log("POLLER", f"  {event}  {keys}  →  NewImage: {new}")
                elif event == "MODIFY":
                    log("POLLER", f"  {event}  {keys}  OldImage: {old}  →  NewImage: {new}")
                elif event == "REMOVE":
                    log("POLLER", f"  {event}  {keys}  OldImage: {old}")

        if not got_records:
            empty_polls += 1
            # Periodically check for new shards
            if empty_polls % 5 == 0:
                refresh_shards()
            time.sleep(0.5)
        else:
            empty_polls = 0

    log("POLLER", f"Done. Total records seen: {records_seen}")
def main() -> None:
    client = make_client()

    log("MAIN  ", f"Creating table {TABLE_NAME} with streams (NEW_AND_OLD_IMAGES)...")
    client.create_table(
        TableName=TABLE_NAME,
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        BillingMode="PAY_PER_REQUEST",
        StreamSpecification={
            "StreamEnabled": True,
            "StreamViewType": "NEW_AND_OLD_IMAGES",
        },
    )
    wait_for_active(client, TABLE_NAME)
    log("MAIN  ", "Table ACTIVE.")

    try:
        poller = threading.Thread(target=poller_thread, name="poller", daemon=True)
        writer = threading.Thread(target=writer_thread, name="writer")

        poller.start()
        time.sleep(1)  # Let poller discover shards before writes begin
        writer.start()

        writer.join()

        # Give poller time to drain remaining records
        log("MAIN  ", "Writer done. Giving poller 5s to drain...")
        time.sleep(5)
        stop_poller.set()
        poller.join(timeout=3)

        log("MAIN  ", "Demo complete.")
    finally:
        log("MAIN  ", f"Deleting table {TABLE_NAME}...")
        try:
            client.delete_table(TableName=TABLE_NAME)
        except Exception:
            pass
if __name__ == "__main__":
    main()
