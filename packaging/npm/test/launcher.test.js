// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

// Launcher tests, run against a real dev binary named by EXTENDDB_BINARY.
// Plain node so the package has zero dev dependencies:
//   EXTENDDB_BINARY=/path/to/extenddb node test/launcher.test.js
//
// What is asserted, and why each assertion exists:
//   1. File mode is the DEFAULT and data survives a stop/start cycle on the
//      same path. This is the launcher's headline contract; a memory-only
//      regression would pass every single-process test, so persistence is
//      proven across two separate server lifetimes.
//   2. Memory mode writes nothing to the default data directory and loses
//      data across a restart. The loss assertion is the discriminating half:
//      if memory mode silently fell back to a file, it would fail.
//   3. dbPath + memory together are refused: silently ignoring the path a
//      caller asked to persist to would be data loss by configuration.
//   4. The startup timeout counts from the last line of catalog migration
//      progress the server prints: a dev server migrating a large database
//      reports progress for longer than the timeout and then serves. The
//      server here is test/fake-server.js, which can take 20 s to start.

"use strict";

const assert = require("node:assert");
const fs = require("node:fs");
const net = require("node:net");
const os = require("node:os");
const path = require("node:path");
const { spawn } = require("node:child_process");
const { start } = require("../index.js");
const { waitForStartup } = require("../startup.js");

if (!process.env.EXTENDDB_BINARY) {
  console.error("EXTENDDB_BINARY must point at a dev-mode extenddb binary");
  process.exit(2);
}

function health(endpoint) {
  const http = require("node:http");
  return new Promise((resolve, reject) => {
    const req = http.get(`${endpoint}/health`, (res) => {
      res.resume();
      resolve(res.statusCode);
    });
    req.on("error", reject);
  });
}

// A minimal SigV4-signed DynamoDB call in plain node, so persistence can be
// proven with real data-plane writes and reads without making an AWS SDK a
// dev dependency of the package.
function ddb(eb, action, body) {
  const crypto = require("node:crypto");
  const http = require("node:http");

  const region = "us-east-1";
  const service = "dynamodb";
  const payload = JSON.stringify(body);
  const now = new Date().toISOString().replace(/[-:]/g, "").replace(/\.\d{3}/, "");
  const date = now.slice(0, 8);
  const { hostname, port } = new URL(eb.endpoint);
  const host = `${hostname}:${port}`;

  const hash = (d) => crypto.createHash("sha256").update(d).digest("hex");
  const hmac = (k, d) => crypto.createHmac("sha256", k).update(d).digest();

  const target = `DynamoDB_20120810.${action}`;
  const canonicalHeaders =
    `host:${host}\nx-amz-date:${now}\nx-amz-target:${target}\n`;
  const signedHeaders = "host;x-amz-date;x-amz-target";
  const canonicalRequest =
    `POST\n/\n\n${canonicalHeaders}\n${signedHeaders}\n${hash(payload)}`;
  const scope = `${date}/${region}/${service}/aws4_request`;
  const stringToSign =
    `AWS4-HMAC-SHA256\n${now}\n${scope}\n${hash(canonicalRequest)}`;
  const kSigning = hmac(
    hmac(hmac(hmac(`AWS4${eb.credentials.secretAccessKey}`, date), region), service),
    "aws4_request"
  );
  const signature = crypto.createHmac("sha256", kSigning).update(stringToSign).digest("hex");

  return new Promise((resolve, reject) => {
    const req = http.request(
      {
        hostname,
        port,
        method: "POST",
        path: "/",
        headers: {
          "content-type": "application/x-amz-json-1.0",
          "x-amz-date": now,
          "x-amz-target": target,
          authorization:
            `AWS4-HMAC-SHA256 Credential=${eb.credentials.accessKeyId}/${scope}, ` +
            `SignedHeaders=${signedHeaders}, Signature=${signature}`,
        },
      },
      (res) => {
        let data = "";
        res.on("data", (d) => (data += d));
        res.on("end", () => {
          if (res.statusCode !== 200) {
            reject(new Error(`${action} -> ${res.statusCode}: ${data}`));
          } else {
            resolve(JSON.parse(data || "{}"));
          }
        });
      }
    );
    req.on("error", reject);
    req.end(payload);
  });
}

// The table used by the persistence scenarios. Tables surface a CREATING
// state briefly even on SQLite, so wait for ACTIVE before writing.
async function createProbeTable(eb) {
  await ddb(eb, "CreateTable", {
    TableName: "launcher-persist",
    AttributeDefinitions: [{ AttributeName: "pk", AttributeType: "S" }],
    KeySchema: [{ AttributeName: "pk", KeyType: "HASH" }],
    BillingMode: "PAY_PER_REQUEST",
  });
  const deadline = Date.now() + 15000;
  for (;;) {
    const d = await ddb(eb, "DescribeTable", { TableName: "launcher-persist" });
    if (d.Table && d.Table.TableStatus === "ACTIVE") return;
    if (Date.now() > deadline) throw new Error("launcher-persist never became ACTIVE");
    await new Promise((r) => setTimeout(r, 200));
  }
}

function freePort() {
  return new Promise((resolve, reject) => {
    const srv = net.createServer();
    srv.listen(0, "127.0.0.1", () => {
      const { port } = srv.address();
      srv.close((err) => (err ? reject(err) : resolve(port)));
    });
    srv.on("error", reject);
  });
}

// Wait, with the launcher's 15 s default, for a fake server that answers
// /health after 20 s and prints a progress line every `progressEveryMs`
// until then (none when 0). Resolves with the outcome and how long it took.
async function fakeStartup(progressEveryMs) {
  const port = await freePort();
  const child = spawn(process.execPath, [path.join(__dirname, "fake-server.js")], {
    env: {
      ...process.env,
      FAKE_PORT: String(port),
      FAKE_READY_AFTER_MS: "20000",
      FAKE_PROGRESS_EVERY_MS: String(progressEveryMs),
    },
    stdio: ["ignore", "ignore", "pipe"],
  });
  const started = Date.now();
  try {
    await waitForStartup(child, `http://127.0.0.1:${port}`, 15000);
    return { error: null, elapsed: Date.now() - started };
  } catch (error) {
    return { error, elapsed: Date.now() - started };
  } finally {
    child.kill();
  }
}

async function main() {
  const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "extenddb-launcher-test-"));
  const dbPath = path.join(scratch, "persisted.db");

  // 1. File mode default: no storage option given resolves to a file, and the
  //    file appears on disk after startup.
  {
    process.chdir(scratch);
    const eb = await start({ binary: process.env.EXTENDDB_BINARY });
    assert.strictEqual(await health(eb.endpoint), 200, "health check");
    assert.ok(
      eb.storage.endsWith(path.join(".extenddb", "data.db")),
      `default storage must be the project dotdir file, got ${eb.storage}`
    );
    assert.ok(
      fs.existsSync(path.join(scratch, ".extenddb", "data.db")),
      "the default database file must exist on disk"
    );
    await eb.stop();
  }

  // 2. File mode persists across two server lifetimes on the same path: an
  //    item written by the first server must be readable by the second. This
  //    is the load-bearing assertion; file existence or size alone would pass
  //    for a server that recreated its schema fresh on every boot.
  {
    const first = await start({ binary: process.env.EXTENDDB_BINARY, dbPath });
    await createProbeTable(first);
    await ddb(first, "PutItem", {
      TableName: "launcher-persist",
      Item: { pk: { S: "survivor" }, note: { S: "written by lifetime one" } },
    });
    await first.stop();

    const second = await start({ binary: process.env.EXTENDDB_BINARY, dbPath });
    const got = await ddb(second, "GetItem", {
      TableName: "launcher-persist",
      Key: { pk: { S: "survivor" } },
    });
    assert.ok(got.Item, "the item written in lifetime one must survive into lifetime two");
    assert.strictEqual(got.Item.note.S, "written by lifetime one");
    await second.stop();
  }

  // 3. Memory mode: nothing lands in the default data dir, and data written
  //    to one memory-mode server is GONE after a restart. The loss assertion
  //    is the discriminating half: if memory mode silently fell back to a
  //    file, it would fail.
  {
    const memScratch = fs.mkdtempSync(path.join(os.tmpdir(), "extenddb-mem-test-"));
    process.chdir(memScratch);
    const eb = await start({ binary: process.env.EXTENDDB_BINARY, memory: true });
    assert.strictEqual(await health(eb.endpoint), 200);
    assert.strictEqual(eb.storage, ":memory: (ephemeral)");
    await createProbeTable(eb);
    await ddb(eb, "PutItem", {
      TableName: "launcher-persist",
      Item: { pk: { S: "ephemeral" } },
    });
    assert.ok(
      !fs.existsSync(path.join(memScratch, ".extenddb")),
      "memory mode must not create the data directory"
    );
    await eb.stop();

    const again = await start({ binary: process.env.EXTENDDB_BINARY, memory: true });
    await assert.rejects(
      () =>
        ddb(again, "GetItem", {
          TableName: "launcher-persist",
          Key: { pk: { S: "ephemeral" } },
        }),
      /ResourceNotFoundException/,
      "a restarted memory-mode server must not know the previous server's table"
    );
    await again.stop();
  }

  // 4. Contradictory options are refused loudly.
  {
    await assert.rejects(
      () => start({ binary: process.env.EXTENDDB_BINARY, memory: true, dbPath }),
      /dbPath cannot be combined with memory storage/,
      "memory + dbPath must be an error, not a silent choice"
    );
    await assert.rejects(
      () => start({ binary: process.env.EXTENDDB_BINARY, storage: "sideways" }),
      /unknown storage mode/,
      "an unknown storage mode must be refused"
    );
  }

  // 5. The startup timeout applies after the last migration progress line:
  //    a server that reports progress every 2 s for 20 s is waited for, and
  //    one that prints nothing for 20 s is given up on at 15 s.
  {
    const [reporting, silent] = await Promise.all([fakeStartup(2000), fakeStartup(0)]);
    assert.strictEqual(
      reporting.error,
      null,
      `a server reporting migration progress must be waited for: ${reporting.error}`
    );
    assert.ok(
      reporting.elapsed > 15000,
      `the reporting server answered after ${reporting.elapsed}ms, within the plain timeout`
    );
    assert.ok(silent.error, "a server that prints nothing must time out");
    assert.match(silent.error.message, /did not become healthy within 15000ms$/);
    assert.ok(
      silent.elapsed >= 15000 && silent.elapsed < 18000,
      `the silent server was given up on after ${silent.elapsed}ms, not at 15 s`
    );
  }

  console.log("launcher tests: 5 scenarios passed");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
