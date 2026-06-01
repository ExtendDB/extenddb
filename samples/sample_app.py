#!/usr/bin/env python3
# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""extenddb sample application — full lifecycle demonstration.

Exercises the complete extenddb lifecycle against a running server with
auth_mode = builtin. Demonstrates:

  1. Table creation (simple PK, PK+SK, composite GSI keys)
  2. Polling for control plane completion (DescribeTable)
  3. Loading data (PutItem, BatchWriteItem)
  4. Querying data (Query on base table and GSIs, Scan)
  5. Updating data (UpdateItem with update expressions)
  6. Batch operations (BatchGetItem, BatchWriteItem)
  7. Transactions (TransactWriteItems, TransactGetItems)
  8. Deleting data (DeleteItem)
  9. Dropping tables (DeleteTable)
 10. Clean exit — full lifecycle from create to teardown

Usage:
    # Start extenddb with auth_mode = builtin, then:
    export EXTENDDB_ENDPOINT=http://localhost:8000
    export AWS_ACCESS_KEY_ID=<your-access-key>
    export AWS_SECRET_ACCESS_KEY=<your-secret-key>
    python3 samples/sample_app.py

    # Or with auth_mode = none:
    export EXTENDDB_ENDPOINT=http://localhost:8000
    export AWS_ACCESS_KEY_ID=test
    export AWS_SECRET_ACCESS_KEY=test
    python3 samples/sample_app.py
"""

from __future__ import annotations

import os
import sys
import time

try:
    import boto3
    from botocore.config import Config
except ModuleNotFoundError:
    print("Error: boto3 is required. Install it with: pip install boto3", file=sys.stderr)
    sys.exit(1)
# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

ENDPOINT = os.environ.get("EXTENDDB_ENDPOINT", "http://localhost:8000")
REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

# Tables created by this sample — cleaned up at the end.
USERS_TABLE = "SampleUsers"
ORDERS_TABLE = "SampleOrders"
TOURNAMENT_TABLE = "SampleTournamentMatches"
def make_client():
    """Create a boto3 DynamoDB client pointing at extenddb."""
    return boto3.client(
        "dynamodb",
        endpoint_url=ENDPOINT,
        region_name=REGION,
        config=Config(retries={"max_attempts": 0}),
    )
# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def wait_for_active(client, table_name: str, timeout: float = 30.0) -> None:
    """Poll DescribeTable until the table reaches ACTIVE status."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        resp = client.describe_table(TableName=table_name)
        status = resp["Table"]["TableStatus"]
        if status == "ACTIVE":
            print(f"  ✓ {table_name} is ACTIVE")
            return
        time.sleep(0.3)
    raise TimeoutError(f"{table_name} did not become ACTIVE within {timeout}s")
def section(title: str) -> None:
    """Print a section header."""
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}\n")
def delete_table_safe(client, table_name: str) -> None:
    """Delete a table, ignoring ResourceNotFoundException."""
    try:
        client.delete_table(TableName=table_name)
        print(f"  ✓ Deleted {table_name}")
    except client.exceptions.ResourceNotFoundException:
        pass
# ---------------------------------------------------------------------------
# Step 1: Create tables
# ---------------------------------------------------------------------------

def create_tables(client) -> None:
    """Create three tables demonstrating different key schemas."""
    section("Step 1: Create Tables")

    # Table 1: Simple PK (user profiles)
    print(f"Creating {USERS_TABLE} (simple HASH key)...")
    client.create_table(
        TableName=USERS_TABLE,
        AttributeDefinitions=[
            {"AttributeName": "userId", "AttributeType": "S"},
        ],
        KeySchema=[
            {"AttributeName": "userId", "KeyType": "HASH"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )

    # Table 2: PK + SK (orders by customer)
    print(f"Creating {ORDERS_TABLE} (HASH + RANGE key)...")
    client.create_table(
        TableName=ORDERS_TABLE,
        AttributeDefinitions=[
            {"AttributeName": "customerId", "AttributeType": "S"},
            {"AttributeName": "orderId", "AttributeType": "S"},
            {"AttributeName": "orderDate", "AttributeType": "S"},
        ],
        KeySchema=[
            {"AttributeName": "customerId", "KeyType": "HASH"},
            {"AttributeName": "orderId", "KeyType": "RANGE"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "OrderDateIndex",
                "KeySchema": [
                    {"AttributeName": "customerId", "KeyType": "HASH"},
                    {"AttributeName": "orderDate", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
        ],
        BillingMode="PAY_PER_REQUEST",
    )

    # Table 3: Composite GSI keys (DynamoDB multi-attribute key pattern)
    # https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GSI.DesignPattern.MultiAttributeKeys.html
    print(f"Creating {TOURNAMENT_TABLE} (composite GSI keys)...")
    client.create_table(
        TableName=TOURNAMENT_TABLE,
        AttributeDefinitions=[
            {"AttributeName": "matchId", "AttributeType": "S"},
            {"AttributeName": "tournamentRegion", "AttributeType": "S"},
            {"AttributeName": "roundBracketMatch", "AttributeType": "S"},
            {"AttributeName": "player1Id", "AttributeType": "S"},
            {"AttributeName": "playerMatchSort", "AttributeType": "S"},
        ],
        KeySchema=[
            {"AttributeName": "matchId", "KeyType": "HASH"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "TournamentRegionIndex",
                "KeySchema": [
                    {"AttributeName": "tournamentRegion", "KeyType": "HASH"},
                    {"AttributeName": "roundBracketMatch", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
            {
                "IndexName": "PlayerMatchHistoryIndex",
                "KeySchema": [
                    {"AttributeName": "player1Id", "KeyType": "HASH"},
                    {"AttributeName": "playerMatchSort", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
        ],
        BillingMode="PAY_PER_REQUEST",
    )
# ---------------------------------------------------------------------------
# Step 2: Wait for tables to become ACTIVE
# ---------------------------------------------------------------------------

def wait_for_tables(client) -> None:
    """Poll all tables until they reach ACTIVE status."""
    section("Step 2: Wait for Tables to Become ACTIVE")
    for name in [USERS_TABLE, ORDERS_TABLE, TOURNAMENT_TABLE]:
        wait_for_active(client, name)
# ---------------------------------------------------------------------------
# Step 3: Load data with PutItem and BatchWriteItem
# ---------------------------------------------------------------------------

def load_data(client) -> None:
    """Populate tables with sample data."""
    section("Step 3: Load Data (PutItem + BatchWriteItem)")

    # PutItem — individual user profiles
    users = [
        {"userId": "user-001", "name": "Alice", "email": "alice@example.com", "age": 30},
        {"userId": "user-002", "name": "Bob", "email": "bob@example.com", "age": 25},
        {"userId": "user-003", "name": "Charlie", "email": "charlie@example.com", "age": 35},
    ]
    for u in users:
        client.put_item(
            TableName=USERS_TABLE,
            Item={
                "userId": {"S": u["userId"]},
                "name": {"S": u["name"]},
                "email": {"S": u["email"]},
                "age": {"N": str(u["age"])},
            },
        )
    print(f"  ✓ Loaded {len(users)} users via PutItem")

    # BatchWriteItem — orders
    orders = [
        {"customerId": "user-001", "orderId": "ord-101", "orderDate": "2026-01-15", "total": "29.99", "status": "delivered"},
        {"customerId": "user-001", "orderId": "ord-102", "orderDate": "2026-02-20", "total": "149.50", "status": "shipped"},
        {"customerId": "user-001", "orderId": "ord-103", "orderDate": "2026-03-10", "total": "9.99", "status": "pending"},
        {"customerId": "user-002", "orderId": "ord-201", "orderDate": "2026-01-05", "total": "75.00", "status": "delivered"},
        {"customerId": "user-002", "orderId": "ord-202", "orderDate": "2026-03-01", "total": "200.00", "status": "shipped"},
        {"customerId": "user-003", "orderId": "ord-301", "orderDate": "2026-02-14", "total": "45.00", "status": "delivered"},
    ]
    put_requests = [
        {
            "PutRequest": {
                "Item": {
                    "customerId": {"S": o["customerId"]},
                    "orderId": {"S": o["orderId"]},
                    "orderDate": {"S": o["orderDate"]},
                    "total": {"N": o["total"]},
                    "status": {"S": o["status"]},
                }
            }
        }
        for o in orders
    ]
    client.batch_write_item(RequestItems={ORDERS_TABLE: put_requests})
    print(f"  ✓ Loaded {len(orders)} orders via BatchWriteItem")

    # BatchWriteItem — tournament matches
    matches = [
        {"matchId": "m-001", "tournamentId": "T2026-Spring", "region": "NA", "round": 1, "bracket": "A", "player1Id": "user-001", "player2Id": "user-002", "matchDate": "2026-03-01", "score": "3-1"},
        {"matchId": "m-002", "tournamentId": "T2026-Spring", "region": "NA", "round": 1, "bracket": "B", "player1Id": "user-003", "player2Id": "user-001", "matchDate": "2026-03-01", "score": "2-3"},
        {"matchId": "m-003", "tournamentId": "T2026-Spring", "region": "NA", "round": 2, "bracket": "A", "player1Id": "user-001", "player2Id": "user-003", "matchDate": "2026-03-02", "score": "3-0"},
        {"matchId": "m-004", "tournamentId": "T2026-Spring", "region": "EU", "round": 1, "bracket": "A", "player1Id": "user-002", "player2Id": "user-003", "matchDate": "2026-03-01", "score": "1-3"},
        {"matchId": "m-005", "tournamentId": "T2026-Summer", "region": "NA", "round": 1, "bracket": "A", "player1Id": "user-001", "player2Id": "user-002", "matchDate": "2026-06-15", "score": "3-2"},
    ]
    match_requests = [
        {
            "PutRequest": {
                "Item": {
                    "matchId": {"S": m["matchId"]},
                    "tournamentRegion": {"S": f"{m['tournamentId']}#{m['region']}"},
                    "roundBracketMatch": {"S": f"{m['round']:04d}#{m['bracket']}#{m['matchId']}"},
                    "playerMatchSort": {"S": f"{m['matchDate']}#{m['round']:04d}#{m['matchId']}"},
                    "tournamentId": {"S": m["tournamentId"]},
                    "region": {"S": m["region"]},
                    "round": {"N": str(m["round"])},
                    "bracket": {"S": m["bracket"]},
                    "player1Id": {"S": m["player1Id"]},
                    "player2Id": {"S": m["player2Id"]},
                    "matchDate": {"S": m["matchDate"]},
                    "score": {"S": m["score"]},
                }
            }
        }
        for m in matches
    ]
    client.batch_write_item(RequestItems={TOURNAMENT_TABLE: match_requests})
    print(f"  ✓ Loaded {len(matches)} tournament matches via BatchWriteItem")
# ---------------------------------------------------------------------------
# Step 4: Query data
# ---------------------------------------------------------------------------

def query_data(client) -> None:
    """Demonstrate Query on base tables and GSIs, plus Scan."""
    section("Step 4: Query Data (Query + Scan)")

    # Query base table — all orders for user-001
    resp = client.query(
        TableName=ORDERS_TABLE,
        KeyConditionExpression="customerId = :cid",
        ExpressionAttributeValues={":cid": {"S": "user-001"}},
    )
    print(f"  Orders for user-001: {resp['Count']} items")
    for item in resp["Items"]:
        print(f"    {item['orderId']['S']} — ${item['total']['N']} ({item['status']['S']})")

    # Query GSI — orders for user-001 sorted by date
    resp = client.query(
        TableName=ORDERS_TABLE,
        IndexName="OrderDateIndex",
        KeyConditionExpression="customerId = :cid AND orderDate BETWEEN :d1 AND :d2",
        ExpressionAttributeValues={
            ":cid": {"S": "user-001"},
            ":d1": {"S": "2026-01-01"},
            ":d2": {"S": "2026-12-31"},
        },
    )
    print(f"\n  Orders for user-001 in 2026 (via GSI): {resp['Count']} items")
    for item in resp["Items"]:
        print(f"    {item['orderDate']['S']} — {item['orderId']['S']}")

    # Query composite GSI — tournament matches in NA region, Spring tournament
    resp = client.query(
        TableName=TOURNAMENT_TABLE,
        IndexName="TournamentRegionIndex",
        KeyConditionExpression="tournamentRegion = :tr",
        ExpressionAttributeValues={
            ":tr": {"S": "T2026-Spring#NA"},
        },
    )
    print(f"\n  Spring tournament NA matches (via composite GSI): {resp['Count']} items")
    for item in resp["Items"]:
        print(f"    Match {item['matchId']['S']}: round {item['round']['N']}, bracket {item['bracket']['S']}, score {item['score']['S']}")

    # Query composite GSI — player match history
    resp = client.query(
        TableName=TOURNAMENT_TABLE,
        IndexName="PlayerMatchHistoryIndex",
        KeyConditionExpression="player1Id = :pid",
        ExpressionAttributeValues={":pid": {"S": "user-001"}},
    )
    print(f"\n  Match history for user-001 (via PlayerMatchHistoryIndex): {resp['Count']} items")
    for item in resp["Items"]:
        print(f"    {item['matchDate']['S']} — Match {item['matchId']['S']} vs {item['player2Id']['S']}: {item['score']['S']}")

    # Scan — count all users
    resp = client.scan(TableName=USERS_TABLE, Select="COUNT")
    print(f"\n  Total users (Scan COUNT): {resp['Count']}")
# ---------------------------------------------------------------------------
# Step 5: Update data
# ---------------------------------------------------------------------------

def update_data(client) -> None:
    """Demonstrate UpdateItem with update expressions."""
    section("Step 5: Update Data (UpdateItem)")

    # Update a user's age and add a new attribute
    client.update_item(
        TableName=USERS_TABLE,
        Key={"userId": {"S": "user-001"}},
        UpdateExpression="SET age = :newage, verified = :v",
        ExpressionAttributeValues={
            ":newage": {"N": "31"},
            ":v": {"BOOL": True},
        },
    )
    print("  ✓ Updated user-001: age=31, verified=true")

    # Update an order status with a condition
    client.update_item(
        TableName=ORDERS_TABLE,
        Key={
            "customerId": {"S": "user-001"},
            "orderId": {"S": "ord-103"},
        },
        UpdateExpression="SET #s = :newstatus",
        ConditionExpression="#s = :oldstatus",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={
            ":newstatus": {"S": "shipped"},
            ":oldstatus": {"S": "pending"},
        },
    )
    print("  ✓ Updated ord-103: pending → shipped (conditional)")

    # Verify the updates
    resp = client.get_item(
        TableName=USERS_TABLE,
        Key={"userId": {"S": "user-001"}},
    )
    item = resp["Item"]
    print(f"  Verified user-001: age={item['age']['N']}, verified={item['verified']['BOOL']}")
# ---------------------------------------------------------------------------
# Step 6: Batch operations
# ---------------------------------------------------------------------------

def batch_operations(client) -> None:
    """Demonstrate BatchGetItem."""
    section("Step 6: Batch Operations (BatchGetItem)")

    resp = client.batch_get_item(
        RequestItems={
            USERS_TABLE: {
                "Keys": [
                    {"userId": {"S": "user-001"}},
                    {"userId": {"S": "user-002"}},
                    {"userId": {"S": "user-003"}},
                ],
            },
        },
    )
    users = resp["Responses"][USERS_TABLE]
    print(f"  ✓ BatchGetItem returned {len(users)} users:")
    for u in sorted(users, key=lambda x: x["userId"]["S"]):
        print(f"    {u['userId']['S']}: {u['name']['S']} ({u['email']['S']})")
# ---------------------------------------------------------------------------
# Step 7: Transactions
# ---------------------------------------------------------------------------

def transaction_operations(client) -> None:
    """Demonstrate TransactWriteItems and TransactGetItems."""
    section("Step 7: Transactions (TransactWriteItems + TransactGetItems)")

    # TransactWriteItems — atomically create a new user and their first order
    client.transact_write_items(
        TransactItems=[
            {
                "Put": {
                    "TableName": USERS_TABLE,
                    "Item": {
                        "userId": {"S": "user-004"},
                        "name": {"S": "Diana"},
                        "email": {"S": "diana@example.com"},
                        "age": {"N": "28"},
                    },
                    "ConditionExpression": "attribute_not_exists(userId)",
                },
            },
            {
                "Put": {
                    "TableName": ORDERS_TABLE,
                    "Item": {
                        "customerId": {"S": "user-004"},
                        "orderId": {"S": "ord-401"},
                        "orderDate": {"S": "2026-04-18"},
                        "total": {"N": "99.99"},
                        "status": {"S": "pending"},
                    },
                },
            },
        ],
    )
    print("  ✓ TransactWriteItems: created user-004 + order ord-401 atomically")

    # TransactGetItems — read both back in a single transaction
    resp = client.transact_get_items(
        TransactItems=[
            {
                "Get": {
                    "TableName": USERS_TABLE,
                    "Key": {"userId": {"S": "user-004"}},
                },
            },
            {
                "Get": {
                    "TableName": ORDERS_TABLE,
                    "Key": {
                        "customerId": {"S": "user-004"},
                        "orderId": {"S": "ord-401"},
                    },
                },
            },
        ],
    )
    user = resp["Responses"][0]["Item"]
    order = resp["Responses"][1]["Item"]
    print(f"  ✓ TransactGetItems: {user['name']['S']} has order {order['orderId']['S']} (${order['total']['N']})")
# ---------------------------------------------------------------------------
# Step 8: Delete data
# ---------------------------------------------------------------------------

def delete_data(client) -> None:
    """Demonstrate DeleteItem."""
    section("Step 8: Delete Data (DeleteItem)")

    # Delete the transaction-created user and order
    client.delete_item(
        TableName=USERS_TABLE,
        Key={"userId": {"S": "user-004"}},
    )
    print("  ✓ Deleted user-004")

    client.delete_item(
        TableName=ORDERS_TABLE,
        Key={
            "customerId": {"S": "user-004"},
            "orderId": {"S": "ord-401"},
        },
    )
    print("  ✓ Deleted order ord-401")

    # Verify deletion
    resp = client.get_item(
        TableName=USERS_TABLE,
        Key={"userId": {"S": "user-004"}},
    )
    assert "Item" not in resp, "user-004 should be deleted"
    print("  ✓ Verified user-004 no longer exists")
# ---------------------------------------------------------------------------
# Step 9: Drop tables
# ---------------------------------------------------------------------------

def drop_tables(client) -> None:
    """Delete all tables created by this sample."""
    section("Step 9: Drop Tables (DeleteTable)")
    for name in [USERS_TABLE, ORDERS_TABLE, TOURNAMENT_TABLE]:
        delete_table_safe(client, name)
# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    """Run the full extenddb lifecycle demonstration."""
    print("extenddb Sample Application — Full Lifecycle Demo")
    print(f"Endpoint: {ENDPOINT}")
    print(f"Region:   {REGION}")

    client = make_client()

    # Clean up any leftover tables from a previous run.
    for name in [USERS_TABLE, ORDERS_TABLE, TOURNAMENT_TABLE]:
        delete_table_safe(client, name)
    # Brief pause for control plane to process deletions.
    time.sleep(1)

    try:
        create_tables(client)
        wait_for_tables(client)
        load_data(client)
        query_data(client)
        update_data(client)
        batch_operations(client)
        transaction_operations(client)
        delete_data(client)
        drop_tables(client)

        section("Done!")
        print("  All 9 steps completed successfully.")
        print("  Full lifecycle: create → load → query → update → batch → transact → delete → drop")
        return 0

    except Exception as e:
        print(f"\n  ✗ Error: {e}", file=sys.stderr)
        # Best-effort cleanup on failure.
        print("\n  Cleaning up tables...")
        for name in [USERS_TABLE, ORDERS_TABLE, TOURNAMENT_TABLE]:
            delete_table_safe(client, name)
        return 1
if __name__ == "__main__":
    sys.exit(main())
