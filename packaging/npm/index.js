// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

"use strict";

const { spawn } = require("node:child_process");
const net = require("node:net");
const http = require("node:http");
const os = require("node:os");
const path = require("node:path");
const fs = require("node:fs");

// AWS's documented example credential: the dev server seeds it as the
// well-known default signing credential (dev-mode builds only).
const DEFAULT_CREDENTIALS = {
  accessKeyId: "AKIAIOSFODNN7EXAMPLE",
  secretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
};

// Where file-mode data lives when the caller names no path: a dotdir in the
// project, so data survives restarts and travels with the repo checkout, and
// one project cannot read another's tables by accident. The same default is
// used by every language launcher (pip, Maven), deliberately: the contract is
// the directory name, not anything npm-specific.
const DEFAULT_DATA_DIR = ".extenddb";
const DEFAULT_DB_FILE = "data.db";

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
    // Platform packages ship the binary as @extenddb/<os>-<cpu>/extenddb.
    const pkg = `@extenddb/${process.platform}-${process.arch}`;
    candidates.push(
      require.resolve(`${pkg}/extenddb${process.platform === "win32" ? ".exe" : ""}`)
    );
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
 * Resolve the storage location from the options.
 *
 * File-based is the DEFAULT: a database that vanishes when the process exits
 * surprises anyone who put data in it on purpose, so persistence is what you
 * get without asking. Ephemeral is one flag away for tests and CI.
 *
 * @returns {{ sqlitePath: string, description: string }}
 */
function resolveStorage(options) {
  const storage = options.storage ?? (options.memory ? "memory" : "file");
  if (storage === "memory") {
    if (options.dbPath) {
      throw new Error("dbPath cannot be combined with memory storage");
    }
    return { sqlitePath: ":memory:", description: ":memory: (ephemeral)" };
  }
  if (storage !== "file") {
    throw new Error(`unknown storage mode "${storage}" (expected "file" or "memory")`);
  }
  const dbPath = path.resolve(options.dbPath ?? path.join(DEFAULT_DATA_DIR, DEFAULT_DB_FILE));
  fs.mkdirSync(path.dirname(dbPath), { recursive: true });
  return { sqlitePath: dbPath, description: dbPath };
}

/**
 * Start an ExtendDB dev server.
 *
 * @param {object} [options]
 * @param {"file"|"memory"} [options.storage]  Storage mode (default "file").
 * @param {boolean} [options.memory]  Shorthand for storage: "memory".
 * @param {string} [options.dbPath]  SQLite file location (file mode only;
 *   default ./.extenddb/data.db).
 * @param {number} [options.port]    Fixed port (default: ephemeral).
 * @param {string} [options.binary]  Path to the extenddb dev binary.
 * @param {number} [options.startupTimeoutMs]  Readiness timeout (default 15000).
 * @returns {Promise<{endpoint: string, port: number, credentials: object,
 *                    region: string, storage: string, stop: () => Promise<void>}>}
 */
async function start(options = {}) {
  const binary = resolveBinary(options.binary);
  const port = options.port ?? (await freePort());
  const { sqlitePath, description } = resolveStorage(options);
  const env = {
    ...process.env,
    EXTENDDB__SERVER__PORT: String(port),
    EXTENDDB__STORAGE__SQLITE__PATH: sqlitePath,
  };

  // Zero-config: the dev binary needs no init and no config file. Run from a
  // scratch cwd so a stray extenddb.toml in the project cannot leak in.
  const cwd = fs.mkdtempSync(path.join(os.tmpdir(), "extenddb-"));
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
  // Best effort: do not outlive the caller.
  process.once("exit", () => child.kill("SIGKILL"));

  return {
    endpoint,
    port,
    credentials: { ...DEFAULT_CREDENTIALS },
    region: "us-east-1",
    storage: description,
    stop,
  };
}

module.exports = { start };
