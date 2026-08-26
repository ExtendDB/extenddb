# @extenddb/dev

A DynamoDB-compatible server for local development and CI, installed from npm.
One function call starts a real server on localhost: no Docker daemon, no JVM,
no AWS account, no configuration. Any AWS SDK, CLI, or tool that talks to
DynamoDB works against it unchanged.

The server is a single native binary (Rust, SQLite storage) downloaded for
your platform at install time. Linux binaries are statically linked and run
on any distribution; macOS (Intel and Apple silicon) and Windows are also
supported. Node 18 or later.

## Getting started

```bash
npm install --save-dev @extenddb/dev
```

```js
// quickstart.mjs -- run with: node quickstart.mjs
import { start } from "@extenddb/dev";
import { DynamoDBClient, ListTablesCommand } from "@aws-sdk/client-dynamodb";

const server = await start({ memory: true });
// server.endpoint    -> "http://127.0.0.1:53211" (ephemeral port)
// server.credentials -> { accessKeyId, secretAccessKey }
// server.region      -> "us-east-1"

const client = new DynamoDBClient({
  endpoint: server.endpoint,
  region: server.region,
  credentials: server.credentials,
});
await client.send(new ListTablesCommand({}));

await server.stop();
```

From CommonJS, use `const { start } = require("@extenddb/dev")` and wrap the
calls in an async function (top-level `await` is ESM-only).

## Options

`start(options)` accepts:

| Option | Default | Description |
|---|---|---|
| `storage` | `"file"` | `"file"` persists between runs; `"memory"` vanishes on stop. |
| `memory` | `false` | Shorthand for `storage: "memory"`. |
| `dbPath` | data dir | SQLite file location (file mode only). |
| `port` | ephemeral | Fixed port instead of an OS-assigned one. |
| `binary` | bundled | Path to an `extenddb` binary, overriding the bundled one. |
| `startupTimeoutMs` | `15000` | How long to wait for the server to become healthy. |

The returned object carries `endpoint`, `port`, `credentials`, `region`,
`storage`, and `stop()`, which shuts the server down and resolves when the
process has exited.

## Use in a test suite

Ephemeral ports mean parallel test shards never collide. With Vitest:

```js
// globalSetup.mjs
import { start } from "@extenddb/dev";

export default async function () {
  const server = await start({ memory: true });
  process.env.DDB_ENDPOINT = server.endpoint;
  process.env.AWS_ACCESS_KEY_ID = server.credentials.accessKeyId;
  process.env.AWS_SECRET_ACCESS_KEY = server.credentials.secretAccessKey;
  process.env.AWS_REGION = server.region;
  return () => server.stop();
}
```

Each account's data is isolated, so suites that need clean separation can use
distinct credentials rather than distinct servers.

## What it supports

The DynamoDB API surface for application development: tables, items,
expressions, queries and scans (including parallel scan), secondary indexes,
transactions, batch operations, TTL, streams, backup and restore, and vector
indexes with `SearchVectors`. Behavioral differences from the service are
documented in [Differences from DynamoDB](https://github.com/ExtendDB/extenddb/blob/main/docs/differences-from-dynamodb.md).

## Note

This package is built for local development and CI. The server speaks plain
HTTP on 127.0.0.1 and seeds AWS's documented example credential, printed at
startup; whoever holds it can do anything. Do not expose it beyond localhost
and do not keep real data in it. For a durable, TLS-terminated deployment,
use [`extenddb/extenddb-postgres`](https://hub.docker.com/r/extenddb/extenddb-postgres).

ExtendDB is an independent open source project managed by Amazon Web
Services. It is not Amazon DynamoDB and does not contain any DynamoDB source
code. "DynamoDB" is a trademark of Amazon.com, Inc. ExtendDB is a clean-room
implementation that speaks the DynamoDB wire protocol.

More at [extenddb.org](https://extenddb.org) and
[github.com/ExtendDB/extenddb](https://github.com/ExtendDB/extenddb).
Licensed under Apache-2.0.
