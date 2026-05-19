# First CRUD Round Trip

## 1. Purpose

Run three operations in sequence to prove the end-to-end round trip: create a table, write an item, read it back. Success on all three closes the setup phase.

## 2. Prerequisites

- AWS CLI configured per `references/01-aws-cli-config.md`.
- `aws dynamodb list-tables` returns `{"TableNames": []}` successfully. If this call fails, stop here and resolve it first. The error is almost always in CLI configuration (certificate trust, endpoint URL, access key), not in the server.

## 3. Step 1: create-table

From `docs/getting-started.md`:

```bash
aws dynamodb create-table \
    --table-name MyTable \
    --attribute-definitions AttributeName=pk,AttributeType=S \
    --key-schema AttributeName=pk,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST
```

Expected successful response shape:

```json
{
    "TableDescription": {
        "TableName": "MyTable",
        "TableStatus": "CREATING",
        "...": "..."
    }
}
```

`TableStatus` is `CREATING` immediately after the call and transitions to `ACTIVE` after the configurable control-plane delay (default 5 seconds). Wait for the transition before the next step:

```bash
aws dynamodb wait table-exists --table-name MyTable
```

`wait table-exists` returns silently with exit code 0 once the table is `ACTIVE`.

## 4. Step 2: put-item

From `docs/getting-started.md`:

```bash
aws dynamodb put-item \
    --table-name MyTable \
    --item '{"pk": {"S": "user-1"}, "name": {"S": "Alice"}, "age": {"N": "30"}}'
```

Expected successful response shape is an empty JSON object:

```json
{}
```

If `--return-values ALL_OLD` is passed, the response contains the previous item under `Attributes`. For the first write to a fresh table, no previous item exists and the response is still `{}`.

## 5. Step 3: get-item

From `docs/getting-started.md`:

```bash
aws dynamodb get-item \
    --table-name MyTable \
    --key '{"pk": {"S": "user-1"}}'
```

Expected successful response shape:

```json
{
    "Item": {
        "pk": {"S": "user-1"},
        "name": {"S": "Alice"},
        "age": {"N": "30"}
    }
}
```

The `Item` field matches what was written in Step 2. If the key does not exist, the response is `{}` with no `Item` field, which is the correct API behavior but indicates the write did not land.

## 6. Verify the round trip succeeded

The expected sequence:

- `create-table` returns JSON with the table description.
- `wait table-exists` returns silently with exit code 0.
- `put-item` returns empty JSON `{}`.
- `get-item` returns `{"Item": {...}}` matching what was written.

If any operation fails, consult ``references/troubleshooting/01-symptom-index.md``. Common failure modes include `InvalidSignatureException` (clock skew or secret key mismatch), `UnrecognizedClientException` (access key ID not found), `AccessDeniedException` (policy missing `dynamodb:*`), and `Could not connect to the endpoint URL` (server not running or wrong endpoint URL).

## 7. Teardown (optional)

```bash
aws dynamodb delete-table --table-name MyTable
```

The user runs this if they want to clean up. The skill does not run it.
