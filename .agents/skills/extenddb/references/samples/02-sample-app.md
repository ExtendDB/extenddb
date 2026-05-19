# samples/sample_app.py walkthrough

## Purpose

`samples/sample_app.py` is a Python script that exercises a broad slice of the DynamoDB API against a running extenddb instance. It walks through a nine-step lifecycle across three tables (`SampleUsers`, `SampleOrders`, `SampleTournamentMatches`): create tables, wait for ACTIVE, load data, query, update, batch-get, transact, delete items, and drop tables. Running it end to end confirms that extenddb correctly handles table creation with GSIs, the common read and write shapes, conditional updates, batch operations, ACID transactions, and table teardown.

## Environment variables

Before running, export the four environment variables the sample reads. Substitute values from the `extenddb init` output and the `create-access-key` step.

```bash
export EXTENDDB_ENDPOINT=https://127.0.0.1:<port>
export AWS_ACCESS_KEY_ID=<access-key-id>
export AWS_SECRET_ACCESS_KEY=<secret-access-key>
export AWS_CA_BUNDLE=~/.extenddb/tls/cert.pem
```

The sample reads `EXTENDDB_ENDPOINT` (not `AWS_ENDPOINT_URL_DYNAMODB`). `EXTENDDB_ENDPOINT` defaults to `http://localhost:8000` if unset. Replace `<port>` in the example below with the port from your `extenddb.toml` (default 8000). The other three are standard AWS environment variables. `AWS_CA_BUNDLE` is required only when `EXTENDDB_ENDPOINT` uses `https://`, which is the default for a `extenddb serve` started with self-signed TLS.

## Run command

With the venv activated (see `01-venv-setup.md`):

```bash
python3 samples/sample_app.py
```

## The nine lifecycle steps

| # | Step | DynamoDB API | What it demonstrates |
|---|---|---|---|
| 1 | Create tables | CreateTable | Three key schemas in one run: simple HASH (`SampleUsers`), HASH+RANGE with a GSI (`SampleOrders`), and multi-part GSI keys (`SampleTournamentMatches`). |
| 2 | Wait for ACTIVE | DescribeTable | Polls `TableStatus` every 300ms until each table reports `ACTIVE`, confirming control plane completion. |
| 3 | Load data | PutItem, BatchWriteItem | Writes three users via `PutItem`, six orders and five tournament matches via `BatchWriteItem`. |
| 4 | Query data | Query, Scan | Runs four queries (base table, single-part GSI, two multi-part GSIs) and a `Scan` with `Select="COUNT"`. |
| 5 | Update data | UpdateItem, GetItem | Updates a user with a `SET` expression and updates an order with a `ConditionExpression`, then reads the user back to verify. |
| 6 | Batch operations | BatchGetItem | Reads three users in a single request and prints them in sorted order. |
| 7 | Transactions | TransactWriteItems, TransactGetItems | Atomically creates a new user and their first order with `attribute_not_exists` guard, then reads both back in a single transactional read. |
| 8 | Delete data | DeleteItem, GetItem | Deletes the transaction-created user and order, then verifies the user is gone. |
| 9 | Drop tables | DeleteTable | Deletes all three tables; cleanup is idempotent and ignores `ResourceNotFoundException`. |

The code comments at the top of `sample_app.py` list ten items but steps 9 and 10 (`DeleteTable` and "Clean exit") both map to step 9 in `main()`. The nine steps above match the function calls in `main()` and the `Step N` section headers printed at runtime.

## Expected output

The sample prints a section header per step and a confirmation line per operation. Example output, redacted:

```
extenddb Sample Application — Full Lifecycle Demo
Endpoint: https://127.0.0.1:<port>
Region:   us-east-1

============================================================
  Step 1: Create Tables
============================================================

Creating SampleUsers (simple HASH key)...
Creating SampleOrders (HASH + RANGE key)...
Creating SampleTournamentMatches (multi-part GSI keys)...

...

============================================================
  Step 9: Drop Tables (DeleteTable)
============================================================

  ✓ Deleted SampleUsers
  ✓ Deleted SampleOrders
  ✓ Deleted SampleTournamentMatches

============================================================
  Done!
============================================================

  All 9 steps completed successfully.
  Full lifecycle: create → load → query → update → batch → transact → delete → drop
```

## Clean up

The sample drops all three tables at the end of step 9. If the sample is interrupted mid-run, the `except` block in `main()` runs best-effort cleanup. If that also fails, the tables remain. Manual cleanup:

```bash
aws dynamodb delete-table --table-name SampleUsers
aws dynamodb delete-table --table-name SampleOrders
aws dynamodb delete-table --table-name SampleTournamentMatches
```

## Troubleshooting

- `InvalidSignatureException`, `UnrecognizedClientException`, or `AccessDeniedException`: consult ``references/troubleshooting/01-symptom-index.md``.
- `Could not connect to the endpoint URL`: check that `EXTENDDB_ENDPOINT` is set to the address the server is listening on and that `extenddb status --config extenddb.toml` reports running.
- `SSL: CERTIFICATE_VERIFY_FAILED`: `AWS_CA_BUNDLE` is unset or points at the wrong file. Set it to `~/.extenddb/tls/cert.pem`.
- `ImportError: No module named 'boto3'`: venv is not activated, or `pip install -r samples/requirements.txt` has not been run. See `01-venv-setup.md`.
