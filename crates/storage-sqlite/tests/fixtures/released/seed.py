#!/usr/bin/env python3
# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0
"""Write the content every released SQLite fixture holds, through the API.

Usage: seed.py <endpoint> [--backup]

Creates `cu_orders` (pk S, sk S, GSI `by_gpk` on gpk S and gsk N, LSI
`by_lsk`, stream NEW_AND_OLD_IMAGES), enables TTL on `expires` and writes 10
items. With `--backup`, also takes an on-demand backup of the table and waits
until it is AVAILABLE, so the database holds backup rows whose items have
sort keys. Exits non-zero on the first step that fails, except TTL, which is
reported and skipped.
"""
import argparse
import sys
import time

import boto3
import botocore
from botocore.config import Config


def wait(what, check, attempts=120, pause=0.5):
    for _ in range(attempts):
        if check():
            return
        time.sleep(pause)
    print("%s: timed out" % what, file=sys.stderr)
    sys.exit(1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("endpoint")
    ap.add_argument("--backup", action="store_true")
    a = ap.parse_args()
    ddb = boto3.client(
        "dynamodb",
        endpoint_url=a.endpoint,
        region_name="us-east-1",
        aws_access_key_id="AKIAIOSFODNN7EXAMPLE",
        aws_secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        config=Config(retries={"max_attempts": 3}),
    )
    table = "cu_orders"
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
        StreamSpecification={"StreamEnabled": True, "StreamViewType": "NEW_AND_OLD_IMAGES"},
        BillingMode="PAY_PER_REQUEST",
    )

    def active():
        d = ddb.describe_table(TableName=table)["Table"]
        gsis = d.get("GlobalSecondaryIndexes", [])
        return d["TableStatus"] == "ACTIVE" and all(g["IndexStatus"] == "ACTIVE" for g in gsis)

    wait("table ACTIVE", active)
    print("create_table %s: ok" % table)
    try:
        ddb.update_time_to_live(
            TableName=table, TimeToLiveSpecification={"Enabled": True, "AttributeName": "expires"}
        )
        print("update_time_to_live: ok")
    except botocore.exceptions.ClientError as e:
        print("update_time_to_live: skipped (%s)" % e.response["Error"]["Code"])
    for i in range(10):
        ddb.put_item(
            TableName=table,
            Item={
                "pk": {"S": "p%d" % (i % 3)},
                "sk": {"S": "s%02d" % i},
                "gpk": {"S": "g%d" % (i % 2)},
                "gsk": {"N": str(i)},
                "lsk": {"S": "l%02d" % (9 - i)},
                "expires": {"N": str(4102444800 + i)},
                "payload": {"S": "item %d" % i},
            },
        )
    print("put_item x10: ok")

    def indexed():
        r = ddb.query(
            TableName=table,
            IndexName="by_gpk",
            KeyConditionExpression="gpk = :g",
            ExpressionAttributeValues={":g": {"S": "g0"}},
        )
        return r["Count"] == 5

    wait("query by_gpk g0 returns 5 items", indexed, attempts=40, pause=0.25)
    if ddb.scan(TableName=table)["Count"] != 10:
        print("scan did not return 10 items", file=sys.stderr)
        sys.exit(2)
    print("queries: ok")
    if a.backup:
        arn = ddb.create_backup(TableName=table, BackupName="cu_orders_fixture")[
            "BackupDetails"
        ]["BackupArn"]

        def available():
            d = ddb.describe_backup(BackupArn=arn)["BackupDescription"]["BackupDetails"]
            return d["BackupStatus"] == "AVAILABLE"

        wait("backup AVAILABLE", available)
        print("create_backup: ok")


if __name__ == "__main__":
    main()
