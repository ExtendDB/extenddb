// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

"use strict";

const http = require("node:http");

// The server starts each line it prints on stderr while it migrates its
// database before serving with this. A dev build applies pending catalog
// migrations at startup, and a large database can take longer than the
// startup timeout, so the timeout counts from the last such line.
const PROGRESS_PREFIX = "catalog migration: ";

/**
 * Wait for `child`, a server spawned to listen on `endpoint`, to answer
 * GET /health with 200.
 *
 * Rejects when the child exits first, with an error that names its exit
 * code or signal and the end of its stderr and carries them as `exitCode`
 * and `signal`, or when `timeoutMs` pass without an answer, counted from the
 * start or from the last migration progress line the child printed on
 * stderr, whichever is later.
 *
 * @param {import("node:child_process").ChildProcess} child  Spawned with a piped stderr.
 * @param {string} endpoint  The server's base URL.
 * @param {number} timeoutMs  How long to wait after the start or the last progress line.
 * @param {(line: string) => void} [onProgress]  Called with each progress line,
 *   as the child printed it, until the wait ends.
 * @returns {Promise<void>}
 */
function waitForStartup(child, endpoint, timeoutMs, onProgress) {
  return new Promise((resolve, reject) => {
    let tail = "";
    let partial = "";
    let lastProgressAt = Date.now();
    let progressSeen = false;
    let settled = false;
    let timer = null;

    // stderr stays drained after startup too, or a full pipe would stop
    // the server at its next log line.
    child.stderr.on("data", (d) => {
      const text = d.toString();
      tail = (tail + text).slice(-4000);
      const lines = (partial + text).split("\n");
      // The start of an unfinished line is all the prefix check needs.
      partial = lines.pop().slice(0, 4096);
      for (const line of lines) {
        if (!line.startsWith(PROGRESS_PREFIX)) continue;
        lastProgressAt = Date.now();
        progressSeen = true;
        if (onProgress && !settled) onProgress(line);
      }
    });

    // "close" comes after the child's stderr has ended, so the error holds
    // the last lines it printed; "exit" can come before them.
    const onClose = (code, signal) => {
      const how = signal ? `signal ${signal}` : `code ${code}`;
      const error = new Error(`extenddb exited during startup (${how}): ${tail}`);
      error.exitCode = code;
      error.signal = signal;
      finish(reject, error);
    };
    child.once("close", onClose);

    function finish(settle, value) {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      child.removeListener("close", onClose);
      settle(value);
    }

    function retry() {
      if (settled) return;
      if (Date.now() > lastProgressAt + timeoutMs) {
        const after = progressSeen ? " of its last catalog migration progress line" : "";
        finish(reject, new Error(`extenddb did not become healthy within ${timeoutMs}ms${after}`));
        return;
      }
      timer = setTimeout(probe, 100);
    }

    function probe() {
      if (settled) return;
      let answered = false;
      const next = () => {
        if (answered) return;
        answered = true;
        retry();
      };
      // The timeout option starts before the connect: a listener that is
      // bound but not accepting can leave a connect waiting for minutes.
      const req = http.get(`${endpoint}/health`, { timeout: 1000 }, (res) => {
        res.resume();
        if (res.statusCode === 200) {
          answered = true;
          finish(resolve);
          return;
        }
        next();
      });
      req.on("error", next);
      req.on("timeout", () => {
        req.destroy();
        next();
      });
    }

    probe();
  });
}

module.exports = { PROGRESS_PREFIX, waitForStartup };
