# Stream Consumer Sample Walkthrough

## 1. Purpose

`samples/stream_consumer.py` demonstrates how to read DynamoDB Streams records from an extenddb table. It uses two AWS SDK clients, one for the control-plane table operations (`dynamodb`) and one for the streams read path (`dynamodbstreams`), because the DynamoDB Streams API is exposed as a separate service in the AWS SDK even though extenddb serves both on the same endpoint. The sample creates a table with streams enabled, runs a writer thread that inserts, updates, and deletes items, and runs a poller thread that reads every resulting stream record and prints it.

## 2. Environment variables

Same four variables as the sample app:

```bash
export EXTENDDB_ENDPOINT=https://127.0.0.1:<port>
export AWS_ACCESS_KEY_ID=<access-key-id>
export AWS_SECRET_ACCESS_KEY=<secret-access-key>
export AWS_CA_BUNDLE=~/.extenddb/tls/cert.pem
```

Note: `samples/stream_consumer.py` reads its endpoint from `EXTENDDB_TEST_ENDPOINT` rather than `EXTENDDB_ENDPOINT`. Set both to the same value, or export `EXTENDDB_TEST_ENDPOINT` in place of `EXTENDDB_ENDPOINT` before running this sample.

## 3. Run command

```bash
python3 samples/stream_consumer.py
```

## 4. The two-client pattern

```python
# Table operations go through the dynamodb client
ddb = boto3.client("dynamodb", endpoint_url=<endpoint>, ...)

# Streams reads go through the dynamodbstreams client
streams = boto3.client("dynamodbstreams", endpoint_url=<endpoint>, ...)
```

Both clients point at the same extenddb endpoint. extenddb dispatches based on the SigV4 service name in the request. This is identical to how the real DynamoDB Streams API works: the control plane is `dynamodb` and the stream reads are `dynamodbstreams`.

## 5. What the sample does

The sample runs three phases on a freshly created table, with a poller thread reading the stream concurrently.

1. **Create a table with streams enabled.** The main thread calls `CreateTable` with a `StreamSpecification` of `StreamEnabled=True` and `StreamViewType=NEW_AND_OLD_IMAGES`, then waits for the table to reach `ACTIVE`.
2. **Discover the stream ARN.** The poller thread calls `DescribeTable` in a loop and reads `Table.LatestStreamArn`. The ARN appears once the stream is provisioned.
3. **Iterate shards.** The poller calls `DescribeStream` on the `dynamodbstreams` client to list shards, then calls `GetShardIterator` with `ShardIteratorType=TRIM_HORIZON` for each new shard. It periodically refreshes the shard list to pick up splits.
4. **Read records.** The poller calls `GetRecords` on each shard iterator, advances to `NextShardIterator`, and prints one line per record.
5. **Writer phases.** A separate writer thread runs three sequential phases on items `item-1` through `item-5`: `PutItem` inserts (event `INSERT`), `UpdateItem` changes (event `MODIFY`), then `DeleteItem` removes (event `REMOVE`).
6. **Cleanup.** When the writer completes, the main thread gives the poller five seconds to drain, signals stop, and deletes the table.

For each stream record, the poller prints the event name, the key, and the relevant image:

- `INSERT  pk=item-1  ->  NewImage: data=initial-value-1  pk=item-1  version=1`
- `MODIFY  pk=item-1  OldImage: ...  ->  NewImage: data=updated-value-1  pk=item-1  version=2`
- `REMOVE  pk=item-1  OldImage: data=updated-value-1  pk=item-1  version=2`

## 6. Stream retention

Stream records are retained for 24 hours. If the sample is run against an older deployment where records have aged out, the poller will see empty `GetRecords` responses. This is expected. The sample creates a fresh table on every run, so the retention window only matters when adapting this code to read an existing table.

## 7. Troubleshooting

- Stream records missing on a freshly created table. Streams may not be enabled. Check the table's stream specification with `aws dynamodb describe-table --table-name <name>` and confirm `StreamSpecification.StreamEnabled` is `true`.
- Old records not visible. The 24-hour retention has expired. Stream records older than 24 hours are not readable.
- `ResourceNotFoundException` on the stream. The stream ARN may be stale if the table was recreated. Call `DescribeTable` again to get the current `LatestStreamArn`.
