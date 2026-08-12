#!/usr/bin/env node
// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

"use strict";

const { start } = require("../index.js");

function usage() {
  console.log(
    `Usage: extenddb start [--memory] [--db <path>] [--port <n>]

Starts an ExtendDB dev server (DynamoDB-compatible) on loopback.

Storage is file-based by default (./.extenddb/data.db), so your tables
survive restarts. Options:
  --memory      Ephemeral in-memory storage (nothing written to disk).
  --db <path>   Persist to this SQLite file instead of the default.
  --port <n>    Listen on a fixed port (default: an ephemeral free port).

Prints the endpoint and credentials, then runs until Ctrl-C.`
  );
}

async function main() {
  const args = process.argv.slice(2);
  if (args[0] !== "start") {
    usage();
    process.exit(args[0] === "--help" || args[0] === "-h" ? 0 : 1);
  }
  const opts = {};
  for (let i = 1; i < args.length; i += 1) {
    if (args[i] === "--memory") opts.memory = true;
    else if (args[i] === "--db") opts.dbPath = args[(i += 1)];
    else if (args[i] === "--port") opts.port = Number(args[(i += 1)]);
    else {
      usage();
      process.exit(1);
    }
  }

  const eb = await start(opts);
  console.log(`ExtendDB dev server ready`);
  console.log(`  endpoint:   ${eb.endpoint}`);
  console.log(`  region:     ${eb.region}`);
  console.log(`  accessKey:  ${eb.credentials.accessKeyId}`);
  console.log(`  storage:    ${eb.storage}`);
  console.log(`Press Ctrl-C to stop.`);
  const shutdown = async () => {
    await eb.stop();
    process.exit(0);
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

main().catch((err) => {
  console.error(err.message);
  process.exit(1);
});
