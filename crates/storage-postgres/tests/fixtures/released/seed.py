#!/usr/bin/env python3
# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0
"""Seed a released ExtendDB server through the DynamoDB API.

Usage: seed.py <endpoint> <access-key-json> <vector 0|1> <out-json>

Creates seed_tbl (sort key, GSI gsi1, LSI lsi1, a stream, TTL on `exp`),
writes ten items, takes the backup seed_backup, and creates seed_simple with
three items. With vector=1 also vec_tbl with the vector index vidx and three
vectors, through signed requests, because boto3 does not know VectorIndexes.
Every call's outcome is recorded; a failed step is recorded and the script
goes on, so a release that lacks a feature still gets a fixture.
"""
import json
import sys
import time
import warnings

import boto3
import requests
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.config import Config
from botocore.credentials import Credentials

warnings.filterwarnings("ignore")
endpoint, keyfile, vector, outfile = sys.argv[1:5]
key = json.load(open(keyfile))
ddb = boto3.client(
    "dynamodb", endpoint_url=endpoint, region_name="us-east-1",
    aws_access_key_id=key["access_key_id"], aws_secret_access_key=key["secret_access_key"],
    verify=False, config=Config(retries={"max_attempts": 2}, read_timeout=30, connect_timeout=5),
)
results = []


def step(name, fn):
    try:
        results.append({"step": name, "ok": True, "result": fn()})
    except Exception as exc:
        results.append({"step": name, "ok": False, "error": f"{type(exc).__name__}: {exc}"})


def wait_active(table, timeout=90):
    deadline = time.time() + timeout
    while time.time() < deadline:
        d = ddb.describe_table(TableName=table)["Table"]
        if d["TableStatus"] == "ACTIVE" and all(
            g.get("IndexStatus") == "ACTIVE" for g in d.get("GlobalSecondaryIndexes", [])
        ):
            return "ACTIVE"
        time.sleep(0.5)
    raise TimeoutError(f"{table} not ACTIVE")


def create_seed_tbl():
    ddb.create_table(
        TableName="seed_tbl",
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "gpk", "AttributeType": "S"},
            {"AttributeName": "lsk", "AttributeType": "N"},
        ],
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "sk", "KeyType": "RANGE"}],
        GlobalSecondaryIndexes=[{
            "IndexName": "gsi1",
            "KeySchema": [{"AttributeName": "gpk", "KeyType": "HASH"}, {"AttributeName": "sk", "KeyType": "RANGE"}],
            "Projection": {"ProjectionType": "ALL"},
        }],
        LocalSecondaryIndexes=[{
            "IndexName": "lsi1",
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "lsk", "KeyType": "RANGE"}],
            "Projection": {"ProjectionType": "ALL"},
        }],
        BillingMode="PAY_PER_REQUEST",
        StreamSpecification={"StreamEnabled": True, "StreamViewType": "NEW_AND_OLD_IMAGES"},
    )
    return wait_active("seed_tbl")


def put_items():
    for i in range(10):
        ddb.put_item(TableName="seed_tbl", Item={
            "pk": {"S": f"seed-p{i % 3}"}, "sk": {"S": f"s{i:03d}"}, "gpk": {"S": f"g{i % 2}"},
            "lsk": {"N": str(100 + i)}, "exp": {"N": str(4102444800)}, "payload": {"S": f"item {i}"},
        })
    return 10


def signed(op, body):
    data = json.dumps(body).encode()
    req = AWSRequest(method="POST", url=endpoint, data=data, headers={
        "X-Amz-Target": f"DynamoDB_20120810.{op}", "Content-Type": "application/x-amz-json-1.0"})
    SigV4Auth(Credentials(key["access_key_id"], key["secret_access_key"]), "dynamodb", "us-east-1").add_auth(req)
    r = requests.post(endpoint, data=data, headers=dict(req.headers), verify=False, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"{op} {r.status_code} {r.text[:200]}")
    return r.json() if r.content else {}


def vector_table():
    signed("CreateTable", {
        "TableName": "vec_tbl",
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "BillingMode": "PAY_PER_REQUEST",
        "VectorIndexes": [{"IndexName": "vidx", "Dimensions": 4, "DistanceFunction": "COSINE",
                           "VectorAttribute": {"AttributeName": "emb"}, "Projection": {"ProjectionType": "ALL"}}],
    })
    for _ in range(240):
        t = signed("DescribeTable", {"TableName": "vec_tbl"})["Table"]
        if t["TableStatus"] == "ACTIVE" and all(v.get("IndexStatus") == "ACTIVE" for v in t.get("VectorIndexes", [])):
            break
        time.sleep(0.5)
    for i in range(3):
        signed("PutItem", {"TableName": "vec_tbl", "Item": {
            "pk": {"S": f"v{i}"}, "emb": {"L": [{"N": str(0.1 * (i + 1))}, {"N": "0.2"}, {"N": "0.3"}, {"N": "0.4"}]}}})
    return "ACTIVE"


step("create_table seed_tbl", create_seed_tbl)
step("update_time_to_live seed_tbl", lambda: ddb.update_time_to_live(
    TableName="seed_tbl", TimeToLiveSpecification={"Enabled": True, "AttributeName": "exp"})["TimeToLiveSpecification"])
step("put_item seed_tbl x10", put_items)
time.sleep(2)  # asynchronous index propagation
step("create_backup seed_backup", lambda: ddb.create_backup(
    TableName="seed_tbl", BackupName="seed_backup")["BackupDetails"]["BackupStatus"])
step("create_table seed_simple", lambda: (ddb.create_table(
    TableName="seed_simple", AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "N"}],
    KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}], BillingMode="PAY_PER_REQUEST"),
    wait_active("seed_simple"))[1])
step("put_item seed_simple x3", lambda: [
    ddb.put_item(TableName="seed_simple", Item={"id": {"N": str(i)}, "v": {"S": "x"}}) and i for i in range(3)])
if vector == "1":
    step("create_table vec_tbl with vector index vidx", vector_table)
time.sleep(1)
json.dump(results, open(outfile, "w"), indent=1, default=str)
failed = [r["step"] for r in results if not r["ok"]]
print(f"seed: {len(results) - len(failed)} ok, {len(failed)} failed {failed}")
