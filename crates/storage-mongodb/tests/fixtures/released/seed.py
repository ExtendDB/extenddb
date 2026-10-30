#!/usr/bin/env python3
# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0
"""Create the table the released-catalog fixtures and the fresh comparison
carry, through the DynamoDB API:

    seed.py <endpoint> <access key id> <secret access key> [table [TTL attribute]]

The table (default `cu_orders`): hash key `pk`, range key `sk`, the GSI
`by_gpk` (`gpk`, `gsk` number), the LSI `by_lsk` (`pk`, `lsk`), a
NEW_AND_OLD_IMAGES stream, TTL on the attribute (default `expires`), and ten
items. Exits 1 on the first step that fails.
"""
import sys
import time

import boto3
import urllib3
from botocore.config import Config


def main(endpoint: str, key: str, secret: str, table: str, ttl: str) -> int:
    urllib3.disable_warnings()
    ddb = boto3.client(
        "dynamodb",
        endpoint_url=endpoint,
        region_name="us-east-1",
        aws_access_key_id=key,
        aws_secret_access_key=secret,
        verify=False,
        config=Config(retries={"max_attempts": 3}),
    )
    ddb.create_table(
        TableName=table,
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "gpk", "AttributeType": "S"},
            {"AttributeName": "gsk", "AttributeType": "N"},
            {"AttributeName": "lsk", "AttributeType": "S"},
        ],
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "by_gpk",
                "KeySchema": [
                    {"AttributeName": "gpk", "KeyType": "HASH"},
                    {"AttributeName": "gsk", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
        LocalSecondaryIndexes=[
            {
                "IndexName": "by_lsk",
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "lsk", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "KEYS_ONLY"},
            }
        ],
        BillingMode="PAY_PER_REQUEST",
        StreamSpecification={"StreamEnabled": True, "StreamViewType": "NEW_AND_OLD_IMAGES"},
    )
    for _ in range(120):
        status = ddb.describe_table(TableName=table)["Table"]["TableStatus"]
        if status == "ACTIVE":
            break
        time.sleep(0.5)
    else:
        print(f"{table} did not become ACTIVE", file=sys.stderr)
        return 1
    ddb.update_time_to_live(
        TableName=table,
        TimeToLiveSpecification={"Enabled": True, "AttributeName": ttl},
    )
    for i in range(10):
        ddb.put_item(
            TableName=table,
            Item={
                "pk": {"S": f"p{i % 3}"},
                "sk": {"S": f"s{i:02d}"},
                "gpk": {"S": f"g{i % 2}"},
                "gsk": {"N": str(i)},
                "lsk": {"S": f"l{9 - i}"},
                ttl: {"N": str(4102444800 + i)},
                "body": {"S": f"item {i}"},
            },
        )
    print(f"seeded {table}")
    return 0


if __name__ == "__main__":
    if len(sys.argv) not in (4, 5, 6):
        sys.exit(__doc__)
    table = sys.argv[4] if len(sys.argv) > 4 else "cu_orders"
    ttl = sys.argv[5] if len(sys.argv) > 5 else "expires"
    sys.exit(main(*sys.argv[1:4], table, ttl))
