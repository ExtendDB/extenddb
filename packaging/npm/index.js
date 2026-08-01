// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

"use strict";

const { spawn } = require("node:child_process");
const net = require("node:net");
const http = require("node:http");
const path = require("node:path");
const fs = require("node:fs");

// AWS's documented example credential: the dev server seeds it as the
// well-known default signing credential (dev-mode builds only).
const DEFAULT_CREDENTIALS = {
  accessKeyId: "AKIAIOSFODNN7EXAMPLE",
  secretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
};

/**
 * Locate the extenddb dev binary.
 *
 * Order: explicit option, EXTENDDB_BINARY env var, a platform package
 * (optionalDependencies, wired up by the release pipeline), then PATH.
 */
function resolveBinary(explicit) {
  const candidates = [];
  if (explicit) candidates.push(explicit);
  if (process.env.EXTENDDB_BINARY) candidates.push(process.env.EXTENDDB_BINARY);
  try {
    // Platform packages ship the binary as extenddb-<os>-<cpu>/extenddb.
    const pkg = `extenddb-${process.platform}-${process.arch}`;
    candidates.push(require.resolve(`${pkg}/extenddb`));
  } catch {
    /* platform package not installed */
  }
  for (const c of candidates) {
    if (c && fs.existsSync(c)) return c;
  }
  // Fall back to PATH; spawn() will surface ENOENT with this name.
  return "extenddb";
}

/** Pick a free loopback port by binding port 0 and reading it back. */
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

function waitHealthy(endpoint, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  return new Promise((resolve, reject) => {
    const probe = () => {
      const req = http.get(`${endpoint}/health`, (res) => {
        res.resume();
        if (res.statusCode === 200) return resolve();
        retry();
      });
      req.on("error", retry);
      req.setTimeout(1000, () => {
        req.destroy();
        retry();
      });
    };
    const retry = () => {
      if (Date.now() > deadline) {
        return reject(new Error(`extenddb did not become healthy within ${timeoutMs}ms`));
      }
      setTimeout(probe, 100);
    };
    probe();
  });
}

/**
 * Start an ExtendDB dev server.
 *
 * @param {object} [options]
 * @param {string} [options.dbPath]  Persist to this SQLite file instead of memory.
 * @param {number} [options.port]    Fixed port (default: ephemeral).
 * @param {string} [options.binary]  Path to the extenddb dev binary.
 * @param {number} [options.startupTimeoutMs]  Readiness timeout (default 15000).
 * @returns {Promise<{endpoint: string, port: number, credentials: object,
 *                    region: string, stop: () => Promise<void>}>}
 */
async function start(options = {}) {
  const binary = resolveBinary(options.binary);
  const port = options.port ?? (await freePort());
  const env = {
    ...process.env,
    EXTENDDB__SERVER__PORT: String(port),
  };
  if (options.dbPath) {
    env.EXTENDDB__STORAGE__SQLITE__PATH = path.resolve(options.dbPath);
  }

  // Zero-config: the dev binary needs no init and no config file. Run from a
  // scratch cwd so a stray extenddb.toml in the project cannot leak in.
  const cwd = fs.mkdtempSync(path.join(require("node:os").tmpdir(), "extenddb-"));
  const child = spawn(binary, ["serve", "--foreground"], {
    env,
    cwd,
    stdio: ["ignore", "ignore", "pipe"],
  });
  let stderrTail = "";
  child.stderr.on("data", (d) => {
    stderrTail = (stderrTail + d.toString()).slice(-4000);
  });

  const endpoint = `http://127.0.0.1:${port}`;
  const spawned = new Promise((_, reject) => {
    child.on("error", reject);
    child.on("exit", (code) =>
      reject(new Error(`extenddb exited during startup (code ${code}): ${stderrTail}`))
    );
  });
  await Promise.race([waitHealthy(endpoint, options.startupTimeoutMs ?? 15000), spawned]);
  child.removeAllListeners("exit");

  let stopped = false;
  const stop = () =>
    new Promise((resolve) => {
      if (stopped || child.exitCode !== null) return resolve();
      stopped = true;
      child.once("exit", () => resolve());
      child.kill("SIGTERM");
      setTimeout(() => child.kill("SIGKILL"), 5000).unref();
    });
  // Best effort: do not outlive the test process.
  process.once("exit", () => child.kill("SIGKILL"));

  return {
    endpoint,
    port,
    credentials: { ...DEFAULT_CREDENTIALS },
    region: "us-east-1",
    stop,
  };
}

module.exports = { start };
