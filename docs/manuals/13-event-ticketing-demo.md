# Event Ticketing Platform — End-to-End Demo

This guide walks you through building a complete event ticketing system on
ExtendDB (extenddb). You will create tables, load data, purchase tickets
with transactions, observe TTL-based reservation expiry, consume change events
via DynamoDB Streams, enforce IAM policies, and verify operational metrics.

**Time to complete:** 20–30 minutes (includes a 2-minute wait for TTL expiry).

**Platform:** macOS (Homebrew). Linux users: substitute your package manager.

---

## Prerequisites

Install PostgreSQL and the Rust toolchain if you haven't already:

```bash
# PostgreSQL
brew install postgresql@16
brew services start postgresql@16

# Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source "$HOME/.cargo/env"

# Python 3 + boto3 (for application scripts)
brew install python3
pip3 install boto3

# AWS CLI v2
brew install awscli
```

---

## 1. Build extenddb

```bash
cd /path/to/ExtendDB
cargo build --release --workspace
```

---

## 2. Build documentation

```bash
pip3 install -r requirements.txt
python3 docs/build-docs.py
```

This renders HTML and PDF files into `docs/rendered/`. The management console
serves them when `docs_dir` is configured.

---

## 3. Initialize extenddb

```bash
./target/release/extenddb init --config extenddb.toml
```

This creates:
- `extenddb.toml` with TLS certificates, database connection, and `docs_dir`
- A PostgreSQL catalog database
- An admin user (save the credentials printed to stdout)

Record the admin password:

```bash
export EXTENDDB_ADMIN_PASSWORD="<password-from-init-output>"
```

---

## 4. Start extenddb

```bash
./target/release/extenddb serve --config extenddb.toml
```

extenddb daemonizes automatically — no output appears in the terminal. Verify it's running:

```bash
curl --cacert ~/.extenddb/tls/cert.pem https://127.0.0.1:18443/health
# {"status":"healthy"}
```

---

## 5. Create accounts and IAM users

We'll create three users with different access levels:
- **box-office** — full DynamoDB access (the operator)
- **customer-app** — read Events, manage own Tickets only
- **auditor** — read-only access to all tables

```bash
EXTENDDB="./target/release/extenddb"
MANAGE="$EXTENDDB manage --user admin --password $EXTENDDB_ADMIN_PASSWORD"

# Create an account
$MANAGE create-account --account-id 111122223333 --account-name ticketing

# Create users
$MANAGE create-user --account-id 111122223333 \
    --user-name box-office --user-password boxoffice123
$MANAGE create-user --account-id 111122223333 \
    --user-name customer-app --user-password customer123
$MANAGE create-user --account-id 111122223333 \
    --user-name auditor --user-password auditor123
```

### Attach IAM policies

```bash
# box-office: full DynamoDB access
$MANAGE put-user-policy --account-id 111122223333 \
    --user-name box-office --policy-name FullDynamoDB \
    --policy-document '{
      "Version": "2012-10-17",
      "Statement": [{
        "Effect": "Allow",
        "Action": "dynamodb:*",
        "Resource": "*"
      }]
    }'

# customer-app: read Events, read/write Tickets only
$MANAGE put-user-policy --account-id 111122223333 \
    --user-name customer-app --policy-name CustomerAccess \
    --policy-document '{
      "Version": "2012-10-17",
      "Statement": [
        {
          "Effect": "Allow",
          "Action": ["dynamodb:GetItem", "dynamodb:Query", "dynamodb:Scan"],
          "Resource": "arn:aws:dynamodb:us-east-1:111122223333:table/Events"
        },
        {
          "Effect": "Allow",
          "Action": ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:Query",
                     "dynamodb:UpdateItem", "dynamodb:BatchGetItem"],
          "Resource": [
            "arn:aws:dynamodb:us-east-1:111122223333:table/Tickets",
            "arn:aws:dynamodb:us-east-1:111122223333:table/Tickets/index/*"
          ]
        }
      ]
    }'

# auditor: read-only on all tables
$MANAGE put-user-policy --account-id 111122223333 \
    --user-name auditor --policy-name ReadOnly \
    --policy-document '{
      "Version": "2012-10-17",
      "Statement": [{
        "Effect": "Allow",
        "Action": [
          "dynamodb:GetItem", "dynamodb:Query", "dynamodb:Scan",
          "dynamodb:BatchGetItem", "dynamodb:DescribeTable",
          "dynamodb:ListTables", "dynamodb:ListTagsOfResource",
          "dynamodbstreams:*"
        ],
        "Resource": "*"
      }]
    }'
```

### Create access keys

```bash
# box-office access key (save the output!)
$MANAGE create-access-key --account-id 111122223333 --user-name box-office
# → Save: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY

# customer-app access key
$MANAGE create-access-key --account-id 111122223333 --user-name customer-app

# auditor access key
$MANAGE create-access-key --account-id 111122223333 --user-name auditor
```

### Configure AWS CLI profiles

Create `~/.aws/credentials` entries (or export environment variables):

```bash
# Set up the box-office profile for table creation and management
export AWS_CA_BUNDLE=~/.extenddb/tls/cert.pem
export AWS_ENDPOINT_URL_DYNAMODB=https://127.0.0.1:18443
export AWS_ENDPOINT_URL_DYNAMODB_STREAMS=https://127.0.0.1:18443
export AWS_DEFAULT_REGION=us-east-1

# Use box-office credentials for setup
export AWS_ACCESS_KEY_ID=<box-office-access-key-id>
export AWS_SECRET_ACCESS_KEY=<box-office-secret-key>
```

---

## 6. Create tables

### Events table

```bash
aws dynamodb create-table \
    --table-name Events \
    --attribute-definitions \
        AttributeName=event_id,AttributeType=S \
        AttributeName=venue_id,AttributeType=S \
        AttributeName=event_date,AttributeType=S \
    --key-schema AttributeName=event_id,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST \
    --global-secondary-indexes '[{
        "IndexName": "VenueDateIndex",
        "KeySchema": [
            {"AttributeName": "venue_id", "KeyType": "HASH"},
            {"AttributeName": "event_date", "KeyType": "RANGE"}
        ],
        "Projection": {"ProjectionType": "ALL"}
    }]' \
    --tags Key=environment,Value=demo Key=app,Value=ticketing
```

### Tickets table (with Streams and TTL)

```bash
aws dynamodb create-table \
    --table-name Tickets \
    --attribute-definitions \
        AttributeName=event_id,AttributeType=S \
        AttributeName=ticket_id,AttributeType=S \
        AttributeName=customer_id,AttributeType=S \
    --key-schema \
        AttributeName=event_id,KeyType=HASH \
        AttributeName=ticket_id,KeyType=RANGE \
    --billing-mode PAY_PER_REQUEST \
    --stream-specification StreamEnabled=true,StreamViewType=NEW_AND_OLD_IMAGES \
    --global-secondary-indexes '[{
        "IndexName": "CustomerTicketsIndex",
        "KeySchema": [
            {"AttributeName": "customer_id", "KeyType": "HASH"},
            {"AttributeName": "event_id", "KeyType": "RANGE"}
        ],
        "Projection": {"ProjectionType": "ALL"}
    }]' \
    --tags Key=environment,Value=demo Key=app,Value=ticketing
```

### Customers table

```bash
aws dynamodb create-table \
    --table-name Customers \
    --attribute-definitions AttributeName=customer_id,AttributeType=S \
    --key-schema AttributeName=customer_id,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST \
    --tags Key=environment,Value=demo Key=app,Value=ticketing
```

### Enable TTL on Tickets

```bash
aws dynamodb update-time-to-live \
    --table-name Tickets \
    --time-to-live-specification Enabled=true,AttributeName=expires_at
```

### Verify tables

```bash
aws dynamodb list-tables
aws dynamodb describe-table --table-name Tickets | jq '.Table.StreamSpecification'
aws dynamodb describe-time-to-live --table-name Tickets
```

---

## 7. Load sample data

### Bulk-load events with BatchWriteItem

```bash
aws dynamodb batch-write-item --request-items '{
  "Events": [
    {"PutRequest": {"Item": {
      "event_id": {"S": "EVT-001"}, "title": {"S": "Summer Jazz Festival"},
      "venue_id": {"S": "VENUE-A"}, "event_date": {"S": "2026-07-15"},
      "capacity": {"N": "500"}, "tickets_sold": {"N": "0"},
      "ticket_price": {"N": "75"}, "status": {"S": "on_sale"}
    }}},
    {"PutRequest": {"Item": {
      "event_id": {"S": "EVT-002"}, "title": {"S": "Rock Night"},
      "venue_id": {"S": "VENUE-A"}, "event_date": {"S": "2026-07-20"},
      "capacity": {"N": "1000"}, "tickets_sold": {"N": "0"},
      "ticket_price": {"N": "120"}, "status": {"S": "on_sale"}
    }}},
    {"PutRequest": {"Item": {
      "event_id": {"S": "EVT-003"}, "title": {"S": "Classical Evening"},
      "venue_id": {"S": "VENUE-B"}, "event_date": {"S": "2026-08-01"},
      "capacity": {"N": "200"}, "tickets_sold": {"N": "0"},
      "ticket_price": {"N": "150"}, "status": {"S": "on_sale"}
    }}},
    {"PutRequest": {"Item": {
      "event_id": {"S": "EVT-004"}, "title": {"S": "Comedy Show"},
      "venue_id": {"S": "VENUE-B"}, "event_date": {"S": "2026-08-10"},
      "capacity": {"N": "150"}, "tickets_sold": {"N": "0"},
      "ticket_price": {"N": "45"}, "status": {"S": "on_sale"}
    }}},
    {"PutRequest": {"Item": {
      "event_id": {"S": "EVT-005"}, "title": {"S": "Electronic Dance Party"},
      "venue_id": {"S": "VENUE-A"}, "event_date": {"S": "2026-09-05"},
      "capacity": {"N": "2000"}, "tickets_sold": {"N": "0"},
      "ticket_price": {"N": "90"}, "status": {"S": "on_sale"}
    }}}
  ]
}'
```

### Create customers

```bash
aws dynamodb batch-write-item --request-items '{
  "Customers": [
    {"PutRequest": {"Item": {
      "customer_id": {"S": "CUST-001"}, "name": {"S": "Alice Johnson"},
      "email": {"S": "alice@example.com"}, "membership_tier": {"S": "gold"}
    }}},
    {"PutRequest": {"Item": {
      "customer_id": {"S": "CUST-002"}, "name": {"S": "Bob Smith"},
      "email": {"S": "bob@example.com"}, "membership_tier": {"S": "silver"}
    }}},
    {"PutRequest": {"Item": {
      "customer_id": {"S": "CUST-003"}, "name": {"S": "Carol Davis"},
      "email": {"S": "carol@example.com"}, "membership_tier": {"S": "bronze"}
    }}}
  ]
}'
```

---

## 8. Query operations

### Get a specific event

```bash
aws dynamodb get-item \
    --table-name Events \
    --key '{"event_id": {"S": "EVT-001"}}'
```

### Query events at a venue (GSI)

```bash
aws dynamodb query \
    --table-name Events \
    --index-name VenueDateIndex \
    --key-condition-expression "venue_id = :v AND event_date BETWEEN :d1 AND :d2" \
    --expression-attribute-values '{
      ":v": {"S": "VENUE-A"},
      ":d1": {"S": "2026-07-01"},
      ":d2": {"S": "2026-12-31"}
    }'
```

### Scan with filter (find high-capacity events)

```bash
aws dynamodb scan \
    --table-name Events \
    --filter-expression "capacity >= :min" \
    --expression-attribute-values '{":min": {"N": "500"}}'
```

---

## 9. Purchase a ticket (Transaction)

This is the core operation: atomically create a ticket and increment the
event's sold count. The ticket has a 2-minute TTL — if not confirmed, it
expires automatically.

```bash
# Calculate TTL: current time + 120 seconds
EXPIRES_AT=$(python3 -c "import time; print(int(time.time()) + 120)")

aws dynamodb transact-write-items --transact-items '[
  {
    "Put": {
      "TableName": "Tickets",
      "Item": {
        "event_id": {"S": "EVT-001"},
        "ticket_id": {"S": "TKT-001"},
        "customer_id": {"S": "CUST-001"},
        "status": {"S": "reserved"},
        "seat": {"S": "A-15"},
        "purchase_time": {"S": "2026-05-07T11:00:00Z"},
        "expires_at": {"N": "'$EXPIRES_AT'"}
      },
      "ConditionExpression": "attribute_not_exists(ticket_id)"
    }
  },
  {
    "Update": {
      "TableName": "Events",
      "Key": {"event_id": {"S": "EVT-001"}},
      "UpdateExpression": "SET tickets_sold = tickets_sold + :one",
      "ExpressionAttributeValues": {":one": {"N": "1"}}
    }
  }
]'
```

### Verify the ticket exists

```bash
aws dynamodb get-item \
    --table-name Tickets \
    --key '{"event_id": {"S": "EVT-001"}, "ticket_id": {"S": "TKT-001"}}'
```

### Verify with TransactGetItems

```bash
aws dynamodb transact-get-items --transact-items '[
  {"Get": {"TableName": "Customers", "Key": {"customer_id": {"S": "CUST-001"}}}},
  {"Get": {"TableName": "Events", "Key": {"event_id": {"S": "EVT-001"}}}}
]'
```

---

## 10. Start the streams consumer

Open a **second terminal** and run the streams consumer script. This will
print change events as they arrive — including the TTL deletion event.

```bash
export AWS_CA_BUNDLE=~/.extenddb/tls/cert.pem
export AWS_ENDPOINT_URL_DYNAMODB=https://127.0.0.1:18443
export AWS_ENDPOINT_URL_DYNAMODB_STREAMS=https://127.0.0.1:18443
export AWS_DEFAULT_REGION=us-east-1
export AWS_ACCESS_KEY_ID=<box-office-access-key-id>
export AWS_SECRET_ACCESS_KEY=<box-office-secret-key>

python3 docs/demo/stream_consumer.py
```

You should see the INSERT event for TKT-001 that was created in step 9.

---

## 11. Confirm a ticket (UpdateItem)

Back in the **first terminal**, confirm the ticket (removes the TTL):

```bash
aws dynamodb update-item \
    --table-name Tickets \
    --key '{"event_id": {"S": "EVT-001"}, "ticket_id": {"S": "TKT-001"}}' \
    --update-expression "SET #s = :confirmed REMOVE expires_at" \
    --expression-attribute-names '{"#s": "status"}' \
    --expression-attribute-values '{":confirmed": {"S": "confirmed"}}'
```

Check the streams terminal — you should see a MODIFY event showing the status
change from "reserved" to "confirmed" and the removal of `expires_at`.

---

## 12. Create a reservation that will expire (TTL demo)

Create another ticket that we intentionally leave unconfirmed:

```bash
EXPIRES_AT=$(python3 -c "import time; print(int(time.time()) + 120)")

aws dynamodb put-item \
    --table-name Tickets \
    --item '{
      "event_id": {"S": "EVT-002"},
      "ticket_id": {"S": "TKT-002"},
      "customer_id": {"S": "CUST-002"},
      "status": {"S": "reserved"},
      "seat": {"S": "B-22"},
      "purchase_time": {"S": "2026-05-07T11:05:00Z"},
      "expires_at": {"N": "'$EXPIRES_AT'"}
    }'
```

Verify it exists:

```bash
aws dynamodb get-item \
    --table-name Tickets \
    --key '{"event_id": {"S": "EVT-002"}, "ticket_id": {"S": "TKT-002"}}'
```

**Now wait approximately 2 minutes.** While waiting, proceed to step 13 to
check metrics. When the TTL fires, the streams consumer will print a REMOVE
event with `userIdentity.type: Service` — proving the deletion was automatic.

---

## 13. Check metrics (while waiting for TTL)

Verify that extenddb is recording operational metrics:

```bash
# Request counts and latencies
curl -s --cacert ~/.extenddb/tls/cert.pem https://127.0.0.1:**18443**/metrics | python3 -m json.tool

# Management console metrics (open in browser)
echo "Open: https://127.0.0.1:**18443**/console/metrics"
```

You should see non-zero counts for:
- `TransactWriteItems` requests
- `BatchWriteItem` requests
- `PutItem`, `GetItem`, `Query`, `Scan` requests
- Read/write capacity consumed
- Connection pool utilization

---

## 14. Verify TTL expiry

After ~2 minutes, verify the ticket was deleted:

```bash
aws dynamodb get-item \
    --table-name Tickets \
    --key '{"event_id": {"S": "EVT-002"}, "ticket_id": {"S": "TKT-002"}}'
# Should return empty (no Item)
```

Check the streams consumer terminal — you should see:

```
REMOVE: {'event_id': {'S': 'EVT-002'}, 'ticket_id': {'S': 'TKT-002'}}
  userIdentity: {'type': 'Service', 'principalId': 'dynamodb.amazonaws.com'}
```

This confirms TTL-based deletion generates a streams event with the service
principal, exactly matching real DynamoDB behavior.

---

## 15. Query customer's tickets (GSI)

```bash
aws dynamodb query \
    --table-name Tickets \
    --index-name CustomerTicketsIndex \
    --key-condition-expression "customer_id = :c" \
    --expression-attribute-values '{":c": {"S": "CUST-001"}}'
```

This returns all tickets for CUST-001 (the confirmed ticket TKT-001).

---

## 16. Batch operations

### BatchGetItem — fetch multiple customers at once

```bash
aws dynamodb batch-get-item --request-items '{
  "Customers": {
    "Keys": [
      {"customer_id": {"S": "CUST-001"}},
      {"customer_id": {"S": "CUST-002"}},
      {"customer_id": {"S": "CUST-003"}}
    ]
  }
}'
```

---

## 17. Update an event (UpdateItem)

Cancel an event and update its status:

```bash
aws dynamodb update-item \
    --table-name Events \
    --key '{"event_id": {"S": "EVT-004"}}' \
    --update-expression "SET #s = :cancelled" \
    --expression-attribute-names '{"#s": "status"}' \
    --expression-attribute-values '{":cancelled": {"S": "cancelled"}}'
```

---

## 18. Delete an event (DeleteItem)

```bash
aws dynamodb delete-item \
    --table-name Events \
    --key '{"event_id": {"S": "EVT-004"}}' \
    --return-values ALL_OLD
```

---

## 19. IAM enforcement demo

Switch to the **customer-app** credentials:

```bash
export AWS_ACCESS_KEY_ID=<customer-app-access-key-id>
export AWS_SECRET_ACCESS_KEY=<customer-app-secret-key>
```

### Allowed: read an event

```bash
aws dynamodb get-item \
    --table-name Events \
    --key '{"event_id": {"S": "EVT-001"}}'
# ✓ Succeeds
```

### Denied: delete an event

```bash
aws dynamodb delete-item \
    --table-name Events \
    --key '{"event_id": {"S": "EVT-001"}}'
# ✗ AccessDeniedException — customer-app cannot delete events
```

### Allowed: create a ticket

```bash
aws dynamodb put-item \
    --table-name Tickets \
    --item '{
      "event_id": {"S": "EVT-003"},
      "ticket_id": {"S": "TKT-003"},
      "customer_id": {"S": "CUST-003"},
      "status": {"S": "reserved"},
      "seat": {"S": "C-01"}
    }'
# ✓ Succeeds
```

Switch back to box-office credentials for remaining steps:

```bash
export AWS_ACCESS_KEY_ID=<box-office-access-key-id>
export AWS_SECRET_ACCESS_KEY=<box-office-secret-key>
```

---

## 20. Export table data

First, ensure the export directory exists and is configured:

```bash
mkdir -p /tmp/extenddb-exports
```

If your `extenddb.toml` doesn't have an `[export]` section, add one:

```toml
[export]
paths = ["/tmp/extenddb-exports"]
```

Then restart extenddb:

```bash
./target/release/extenddb stop --config extenddb.toml
./target/release/extenddb serve --config extenddb.toml
```

Export the Events table:

```bash
aws dynamodb export-table-to-point-in-time \
    --table-arn "arn:aws:dynamodb:us-east-1:111122223333:table/Events" \
    --s3-bucket "local" \
    --s3-prefix "/tmp/extenddb-exports/111122223333/events.json" \
    --export-format DYNAMODB_JSON
```

Export files are namespaced by account: the path sits under
`<configured export root>/<account id>/`, which is why the account id appears in
the prefix above. The server creates that directory on first use. Export also
refuses to overwrite an existing file, so re-running this step needs a new
filename (or the old file removed).

Check the exported file:

```bash
cat /tmp/extenddb-exports/111122223333/events.json | python3 -m json.tool | head -40
```

---

## 21. Tags

Verify tags were applied during table creation:

```bash
aws dynamodb list-tags-of-resource \
    --resource-arn "arn:aws:dynamodb:us-east-1:111122223333:table/Events"
```

Add another tag:

```bash
aws dynamodb tag-resource \
    --resource-arn "arn:aws:dynamodb:us-east-1:111122223333:table/Events" \
    --tags Key=owner,Value=demo-user
```

---

## 22. DescribeTable and ListTables

```bash
aws dynamodb list-tables
aws dynamodb describe-table --table-name Tickets | jq '{
  TableName: .Table.TableName,
  ItemCount: .Table.ItemCount,
  StreamArn: .Table.LatestStreamArn,
  GSIs: [.Table.GlobalSecondaryIndexes[].IndexName],
  TTL: .Table.TimeToLiveDescription
}'
```

---

## 23. Final metrics check

```bash
curl -s --cacert ~/.extenddb/tls/cert.pem https://127.0.0.1:18443/metrics | python3 -c "
import json, sys
m = json.load(sys.stdin)
print('=== extenddb Metrics Summary ===')
for key in sorted(m.keys()):
    if m[key] and m[key] != 0:
        print(f'  {key}: {m[key]}')
"
```

---

## 24. Cleanup

Stop the streams consumer (Ctrl+C in the second terminal), then:

```bash
# Delete tables
aws dynamodb delete-table --table-name Tickets
aws dynamodb delete-table --table-name Events
aws dynamodb delete-table --table-name Customers

# Remove export data
rm -rf /tmp/extenddb-exports

# Stop extenddb
./target/release/extenddb stop --config extenddb.toml

# (Optional) Destroy the catalog to start fresh
./target/release/extenddb destroy --config extenddb.toml --yes
```

---

## Application Design Notes

### Table schemas

| Table | Partition Key | Sort Key | GSIs | Streams | TTL |
|-------|--------------|----------|------|---------|-----|
| Events | `event_id` (S) | — | `VenueDateIndex` (venue_id + event_date) | No | No |
| Tickets | `event_id` (S) | `ticket_id` (S) | `CustomerTicketsIndex` (customer_id + event_id) | NEW_AND_OLD_IMAGES | `expires_at` |
| Customers | `customer_id` (S) | — | — | No | No |

### Access patterns

| Pattern | Table | Operation |
|---------|-------|-----------|
| Get event details | Events | GetItem |
| Events at a venue by date | Events | Query (VenueDateIndex) |
| Purchase ticket | Tickets + Events | TransactWriteItems |
| My tickets | Tickets | Query (CustomerTicketsIndex) |
| Confirm ticket | Tickets | UpdateItem |
| Reservation timeout | Tickets | TTL auto-delete |
| Change notifications | Tickets | Streams |
| Bulk load events | Events | BatchWriteItem |
| Fetch multiple customers | Customers | BatchGetItem |

### IAM roles

| User | Access Level | Key Restrictions |
|------|-------------|-----------------|
| box-office | Full `dynamodb:*` | None |
| customer-app | Read Events, CRUD Tickets | Cannot modify Events |
| auditor | Read-only all tables | No write access |

### Production vs. demo settings

| Setting | Demo | Production |
|---------|------|-----------|
| Reservation TTL | 120 seconds | 900 seconds (15 min) |
| Streams polling interval | 1 second | 1–5 seconds |
| Export path | `/tmp/extenddb-exports` | `/var/lib/extenddb/exports` |
