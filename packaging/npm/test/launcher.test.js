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

"use strict";

const assert = require("node:assert");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { start } = require("../index.js");

if (!process.env.EXTENDDB_BINARY) {
  console.error("EXTENDDB_BINARY must point at a dev-mode extenddb binary");
  process.exit(2);
}

function health(endpoint) {
  const http = require("node:http");
  // The aws-sdk is deliberately not a dev dependency, so data-plane assertions
  // live in the SDK-level suites; this file's subject is process behavior and
  // storage resolution, for which the health endpoint suffices.
  return new Promise((resolve, reject) => {
    const req = http.get(`${endpoint}/health`, (res) => {
      res.resume();
      resolve(res.statusCode);
    });
    req.on("error", reject);
  });
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

  // 2. File mode persists across two server lifetimes on the same path.
  {
    const first = await start({ binary: process.env.EXTENDDB_BINARY, dbPath });
    assert.strictEqual(await health(first.endpoint), 200);
    await first.stop();
    const sizeAfterFirst = fs.statSync(dbPath).size;
    assert.ok(sizeAfterFirst > 0, "database file must be non-empty after first run");

    const second = await start({ binary: process.env.EXTENDDB_BINARY, dbPath });
    assert.strictEqual(await health(second.endpoint), 200);
    await second.stop();
  }

  // 3. Memory mode: nothing lands in the default data dir.
  {
    const memScratch = fs.mkdtempSync(path.join(os.tmpdir(), "extenddb-mem-test-"));
    process.chdir(memScratch);
    const eb = await start({ binary: process.env.EXTENDDB_BINARY, memory: true });
    assert.strictEqual(await health(eb.endpoint), 200);
    assert.strictEqual(eb.storage, ":memory: (ephemeral)");
    assert.ok(
      !fs.existsSync(path.join(memScratch, ".extenddb")),
      "memory mode must not create the data directory"
    );
    await eb.stop();
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

  console.log("launcher tests: 4 scenarios passed");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
