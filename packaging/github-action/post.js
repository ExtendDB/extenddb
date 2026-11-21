// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

"use strict";

// The post step of the GitHub Action in action.yml. It stops the server that
// main.js started, then prints the server log in a collapsed group.

const crypto = require("node:crypto");
const fs = require("node:fs");

const { STATE_pid: pid, STATE_endpoint: endpoint, STATE_log: log } = process.env;

// The server answers /health with 200 while it runs.
const answers = (url) =>
  fetch(`${url}/health`, { signal: AbortSignal.timeout(2000) }).then((res) => res.status === 200, () => false);

async function main() {
  // Kill the pid only while the server answers: the system can give the pid
  // of a server that exited earlier to another process.
  if (pid && (await answers(endpoint))) process.kill(Number(pid));
  if (log && fs.existsSync(log)) {
    // The log is the server's text, so the runner must not read workflow
    // commands in it.
    const token = crypto.randomUUID();
    console.log("::group::ExtendDB server log");
    console.log(`::stop-commands::${token}`);
    console.log(fs.readFileSync(log, "utf8"));
    console.log(`::${token}::`);
    console.log("::endgroup::");
  }
}

main().catch((err) => {
  // The runner stops the server when the job ends in any case.
  console.log(`could not stop the ExtendDB server: ${err.message}`);
});
