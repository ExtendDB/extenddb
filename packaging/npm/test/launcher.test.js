// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

// Live test for the launcher. Requires the dev binary; point EXTENDDB_BINARY
// at a build made with: cargo build --release -p extenddb \
//   --no-default-features --features sqlite-memory,dev-mode

"use strict";

const assert = require("node:assert");
const http = require("node:http");
const { start } = require("../index.js");

function get(url) {
  return new Promise((resolve, reject) => {
    http
      .get(url, (res) => {
        let body = "";
        res.on("data", (d) => (body += d));
        res.on("end", () => resolve({ status: res.statusCode, body }));
      })
      .on("error", reject);
  });
}

async function main() {
  // Two servers side by side proves ephemeral port allocation works.
  const a = await start();
  const b = await start();
  assert.notStrictEqual(a.port, b.port, "servers must get distinct ports");

  for (const eb of [a, b]) {
    const health = await get(`${eb.endpoint}/health`);
    assert.strictEqual(health.status, 200, "health must be 200");
    assert.match(health.body, /healthy/);
    assert.strictEqual(eb.credentials.accessKeyId, "AKIAIOSFODNN7EXAMPLE");
  }

  await a.stop();
  await b.stop();

  // After stop, the endpoint must be gone.
  await assert.rejects(get(`${a.endpoint}/health`), "stopped server must not respond");

  console.log("launcher test: PASS");
}

main().catch((err) => {
  console.error("launcher test: FAIL:", err.message);
  process.exit(1);
});
