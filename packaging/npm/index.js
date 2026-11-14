// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

"use strict";

const { execFile, spawn } = require("node:child_process");
const net = require("node:net");
const http = require("node:http");
const os = require("node:os");
const path = require("node:path");
const fs = require("node:fs");

// The launcher and its platform packages are published together at this
// version, and the launcher runs only a binary of the same release.
const LAUNCHER_VERSION = require("./package.json").version;

// What to point the "binary" option or EXTENDDB_BINARY at.
const WANTED_BINARY =
  `an extenddb ${LAUNCHER_VERSION} dev binary built from tag v${LAUNCHER_VERSION}`;

// The longest delay setTimeout keeps (2^31-1 ms, about 24.8 days). Node runs
// a longer delay after 1 ms.
const MAX_TIMER_MS = 2 ** 31 - 1;

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
 * Locate the extenddb dev binary, with where it came from and how to replace
 * it with one of the launcher's release.
 *
 * Order: explicit option, EXTENDDB_BINARY env var, a platform package
 * (optionalDependencies, wired up by the release pipeline), then PATH. It
 * skips a candidate that names no file, and `skipped` lists that candidate so
 * that the errors that follow can name it.
 *
 * @returns {{ path: string, source: string, fix: string,
 *             skipped: Array<{ path: string, source: string }> }}
 */
function resolveBinary(explicit) {
  const candidates = [];
  if (explicit) {
    candidates.push({
      path: explicit,
      source: `the "binary" option`,
      fix: `Pass the "binary" option ${WANTED_BINARY}.`,
    });
  }
  if (process.env.EXTENDDB_BINARY) {
    candidates.push({
      path: process.env.EXTENDDB_BINARY,
      source: "EXTENDDB_BINARY",
      fix: `Point EXTENDDB_BINARY at ${WANTED_BINARY}, or unset it to use the platform package.`,
    });
  }
  try {
    // Platform packages ship the binary as @extenddb/<os>-<cpu>/extenddb.
    const pkg = `@extenddb/${process.platform}-${process.arch}`;
    candidates.push({
      path: require.resolve(`${pkg}/extenddb${process.platform === "win32" ? ".exe" : ""}`),
      source: `the ${pkg} platform package`,
      fix:
        `Reinstall the launcher so that its platform package is the same release: ` +
        `npm install @extenddb/dev@${LAUNCHER_VERSION}.`,
    });
  } catch {
    /* platform package not installed */
  }
  const skipped = [];
  for (const c of candidates) {
    if (fs.existsSync(c.path)) return { ...c, skipped };
    skipped.push({ path: c.path, source: c.source });
  }
  // Fall back to PATH; spawn() will surface ENOENT with this name.
  return {
    path: "extenddb",
    source: "PATH",
    fix:
      `Install the launcher with its optional dependencies, which carry the platform ` +
      `binary (npm install @extenddb/dev@${LAUNCHER_VERSION}), or set EXTENDDB_BINARY ` +
      `to ${WANTED_BINARY}.`,
    skipped,
  };
}

/** One sentence for each candidate that resolveBinary skipped, or "". */
function skippedNote(binary) {
  return binary.skipped
    .map((c) => ` The launcher skipped ${c.path} (from ${c.source}), which does not exist.`)
    .join("");
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

// Probe /health until it answers 200 or the clock passes `deadline` (a
// Date.now() value). The function compares the deadline with the clock and
// never gives it to a timer, so a deadline of any size works.
function waitHealthy(endpoint, deadline, timeoutMs) {
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
        return reject(
          new Error(`extenddb did not become healthy within the ${timeoutMs}ms startup timeout`)
        );
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

function binaryNotFound(binary) {
  return new Error(
    `could not find or execute the extenddb dev binary (tried the "binary" ` +
      `option, EXTENDDB_BINARY, the @extenddb/${process.platform}-${process.arch} ` +
      `platform package, then PATH).${skippedNote(binary)} If your platform has ` +
      `no working prebuilt package, build one from tag v${LAUNCHER_VERSION} with: ` +
      `cargo build --release --no-default-features --features sqlite,dev-mode ` +
      `-p extenddb, then set EXTENDDB_BINARY to it.`
  );
}

/**
 * Refuse a binary of another release before `serve` runs. The release
 * pipeline publishes the launcher and its platform packages at one version,
 * but the "binary" option, EXTENDDB_BINARY and PATH can name any build.
 * `extenddb version` prints `extenddb <release>` as its first line
 * (print_version in crates/app/src/lib.rs). It runs with the environment and
 * working directory `serve` gets, so the check runs the binary `serve` runs.
 * It must finish by `deadline`, the Date.now() value at which start() gives up.
 */
function checkBinaryVersion(binary, env, cwd, deadline, timeoutMs) {
  const shown = binary.source === "PATH" ? "on PATH" : `${binary.path} (from ${binary.source})`;
  return new Promise((resolve, reject) => {
    let timer;
    let timedOut = false;
    const child = execFile(binary.path, ["version"], { env, cwd }, (err, stdout, stderr) => {
      clearTimeout(timer);
      let failure;
      if (err && err.code === "ENOENT") {
        reject(binaryNotFound(binary));
        return;
      }
      if (timedOut) {
        // The timer fired. This holds even when `version` exited 0 and a child
        // of it held the pipes until the timer closed them, so `serve` never
        // starts after the deadline.
        failure = `did not finish within the ${timeoutMs}ms startup timeout`;
      } else if (!err) {
        const found = /^extenddb (\S+)\s*$/m.exec(stdout);
        if (found && found[1] === LAUNCHER_VERSION) {
          resolve();
          return;
        }
        if (found) {
          reject(
            new Error(
              `the extenddb binary ${shown} is release ${found[1]}, and this ` +
                `@extenddb/dev launcher is release ${LAUNCHER_VERSION}: the launcher ` +
                `runs only a binary of its own release.${skippedNote(binary)} ${binary.fix}`
            )
          );
          return;
        }
        failure = `printed no "extenddb <release>" line`;
      } else if (typeof err.code === "number") {
        failure = `exited with code ${err.code}`;
      } else if (err.signal) {
        failure = `was stopped by ${err.signal}`;
      } else {
        // Another failure to run the binary (EACCES, say), as `serve` reports it.
        reject(err);
        return;
      }
      const output = `${stdout}${stderr}`.trim();
      reject(
        new Error(
          `could not read the release of the extenddb binary ${shown}: "extenddb ` +
            `version" ${failure}. This @extenddb/dev launcher is release ` +
            `${LAUNCHER_VERSION} and runs only a binary of its own release.` +
            `${skippedNote(binary)} ${binary.fix}${output ? `\n${output}` : ""}`
        )
      );
    });
    // The launcher's own timer: execFile's timeout option refuses a fraction,
    // a string and Infinity, and fires after 1 ms for a value above 2^31-1.
    // The launcher clamps the delay to what setTimeout keeps. A deadline of
    // Infinity or NaN gets no timer, and a past deadline, -Infinity included,
    // gets a delay of 0. At the deadline, close the pipes and
    // SIGKILL the binary, as execFile does at its timeout. Then a binary that
    // ignores SIGTERM, or a child of it that holds the pipes, cannot keep
    // start() waiting.
    const left = deadline - Date.now();
    if (left < Infinity) {
      timer = setTimeout(
        () => {
          timedOut = true;
          child.stdout?.destroy();
          child.stderr?.destroy();
          child.kill("SIGKILL");
        },
        Math.min(Math.max(left, 0), MAX_TIMER_MS)
      );
    }
  });
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
 * @param {string} [options.binary]  Path to the extenddb dev binary, of the
 *   launcher's own release.
 * @param {number} [options.startupTimeoutMs]  The time budget of start(),
 *   shared by the binary's version check and the wait for the server to
 *   become healthy (default 15000). A health probe in flight at the deadline
 *   can add up to 1 s.
 * @returns {Promise<{endpoint: string, port: number, credentials: object,
 *                    region: string, storage: string, stop: () => Promise<void>}>}
 */
async function start(options = {}) {
  // One deadline for the whole start. The version check and the health wait
  // share it, so start() takes about one startupTimeoutMs: a health probe in
  // flight at the deadline can add up to its 1 s timeout. Number() reads a
  // numeric string such as "20000" as that many ms. A fraction, Infinity or a value
  // above 2^31-1 works because start() compares the deadline with the clock
  // and gives a timer only a clamped delay.
  const timeoutMs = Number(options.startupTimeoutMs ?? 15000);
  const deadline = Date.now() + timeoutMs;
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
  await checkBinaryVersion(binary, env, cwd, deadline, timeoutMs);
  const child = spawn(binary.path, ["serve", "--foreground"], {
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
    child.on("error", (err) => {
      if (err.code === "ENOENT") {
        reject(binaryNotFound(binary));
        return;
      }
      reject(err);
    });
    child.on("exit", (code) =>
      reject(new Error(`extenddb exited during startup (code ${code}): ${stderrTail}`))
    );
  });
  // The health wait gets the time the version check left.
  await Promise.race([waitHealthy(endpoint, deadline, timeoutMs), spawned]);
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
