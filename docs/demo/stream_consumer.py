#!/usr/bin/env python3
# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""DynamoDB Streams consumer for the Event Ticketing demo.

Polls the Tickets table stream and prints change events as they arrive.
Demonstrates INSERT, MODIFY, and REMOVE (including TTL-triggered deletions).

Usage:
    export AWS_CA_BUNDLE=~/.extenddb/tls/cert.pem
    export AWS_ACCESS_KEY_ID=<your-key>
    export AWS_SECRET_ACCESS_KEY=<your-secret>
    python3 docs/demo/stream_consumer.py
"""

import os
import sys
import time

import boto3

ENDPOINT = os.environ.get("AWS_ENDPOINT_URL_DYNAMODB_STREAMS", "https://127.0.0.1:18443")
TABLE_NAME = "Tickets"
POLL_INTERVAL = 1  # seconds


def main():
    dynamodb = boto3.client(
        "dynamodb", endpoint_url=ENDPOINT, region_name="us-east-1"
    )
    streams = boto3.client(
        "dynamodbstreams", endpoint_url=ENDPOINT, region_name="us-east-1"
    )

    # Get stream ARN from the table.
    table = dynamodb.describe_table(TableName=TABLE_NAME)
    stream_arn = table["Table"].get("LatestStreamArn")
    if not stream_arn:
        print(f"ERROR: Table {TABLE_NAME} does not have streams enabled.")
        sys.exit(1)

    print(f"Consuming stream: {stream_arn}")
    print("Waiting for events (Ctrl+C to stop)...\n")

    # Discover shards and get iterators.
    desc = streams.describe_stream(StreamArn=stream_arn)
    shards = desc["StreamDescription"]["Shards"]
    iterators = {}
    for shard in shards:
        resp = streams.get_shard_iterator(
            StreamArn=stream_arn,
            ShardId=shard["ShardId"],
            ShardIteratorType="TRIM_HORIZON",
        )
        iterators[shard["ShardId"]] = resp["ShardIterator"]

    # Poll loop.
    try:
        while True:
            for shard_id, iterator in list(iterators.items()):
                if not iterator:
                    continue
                resp = streams.get_records(ShardIterator=iterator, Limit=100)
                for record in resp.get("Records", []):
                    event_name = record["eventName"]
                    keys = record["dynamodb"]["Keys"]
                    print(f"{event_name}: {keys}")

                    # Show old/new images for context.
                    if "NewImage" in record["dynamodb"]:
                        status = record["dynamodb"]["NewImage"].get("status", {}).get("S", "")
                        print(f"  new status: {status}")
                    if "OldImage" in record["dynamodb"]:
                        status = record["dynamodb"]["OldImage"].get("status", {}).get("S", "")
                        print(f"  old status: {status}")

                    # Highlight TTL deletions.
                    identity = record.get("userIdentity")
                    if identity:
                        print(f"  userIdentity: {identity}")

                    print()

                iterators[shard_id] = resp.get("NextShardIterator")
            time.sleep(POLL_INTERVAL)
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()
