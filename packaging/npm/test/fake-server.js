// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

// A stand-in for a dev server that migrates its database before it serves,
// for the launcher's startup timeout tests. Run with node:
//   FAKE_PORT=<port> FAKE_READY_AFTER_MS=<ms> FAKE_PROGRESS_EVERY_MS=<ms> node fake-server.js
// It prints a migration progress line on stderr every FAKE_PROGRESS_EVERY_MS
// (none when 0) until FAKE_READY_AFTER_MS have passed, then answers GET
// /health with 200 on 127.0.0.1:FAKE_PORT until it is killed.

"use strict";

const http = require("node:http");

// What `extenddb serve` starts each of these lines with (PROGRESS_PREFIX
// in crates/app/src/serve_catalog.rs), written out here rather than read
// from the launcher, so a launcher that stopped matching the server's
// lines fails the test.
const PROGRESS_PREFIX = "catalog migration: ";

const port = Number(process.env.FAKE_PORT);
const readyAfter = Number(process.env.FAKE_READY_AFTER_MS);
const progressEvery = Number(process.env.FAKE_PROGRESS_EVERY_MS);
const started = Date.now();

let progress = null;
if (progressEvery > 0) {
  progress = setInterval(() => {
    const seconds = Math.floor((Date.now() - started) / 1000);
    process.stderr.write(`${PROGRESS_PREFIX}in progress for ${seconds} s\n`);
  }, progressEvery);
}

setTimeout(() => {
  if (progress) clearInterval(progress);
  http
    .createServer((req, res) => {
      res.writeHead(req.url === "/health" ? 200 : 404);
      res.end();
    })
    .listen(port, "127.0.0.1");
}, readyAfter);
