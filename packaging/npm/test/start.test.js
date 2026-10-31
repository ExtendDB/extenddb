// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

// start() and `extenddb start` against a stand-in for a dev server that
// migrates its database before it serves: test/fake-server.js, which
// test/stub-spawn.js runs in place of the extenddb binary. Plain node, and
// no binary is needed:
//   node test/start.test.js
//
// What is asserted:
//   1. With startupTimeoutMs unset, start() waits for a server that prints a
//      catalog migration line every 2 s and answers after 20 s, longer than
//      the 15 s default, and passes each line to onProgress.
//   2. With startupTimeoutMs unset, start() gives up at 15 s on a server
//      that prints nothing, well before that server would answer at 60 s,
//      and stops the server before it rejects.
//   3. A server that exits during startup rejects start() with an error
//      that carries its exit code as `exitCode` and names the last line it
//      printed.
//   4. A server ended by a signal rejects start() with the signal as
//      `signal`.
//   5. `extenddb start` writes the server's progress lines to its stderr
//      while it waits, and exits with the server's exit code.

"use strict";

// Before ../index.js, which takes spawn from child_process when it loads.
const stub = require("./stub-spawn.js");
const assert = require("node:assert");
const net = require("node:net");
const path = require("node:path");
const { start } = require("../index.js");

const PROGRESS_PREFIX = "catalog migration: ";

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

// start() with `options`, resolving with its error, or with null after
// stopping the server when it started.
function startError(options) {
  return start(options).then(
    async (eb) => {
      await eb.stop();
      return null;
    },
    (error) => error
  );
}

async function waitsWhileTheServerReportsProgress() {
  const port = await freePort();
  stub.fake(port, { readyAfterMs: 20000, progressEveryMs: 2000 });
  const lines = [];
  const started = Date.now();
  const eb = await start({ port, memory: true, onProgress: (line) => lines.push(line) }).catch(
    (error) => assert.fail(`a server reporting migration progress must be waited for: ${error}`)
  );
  const elapsed = Date.now() - started;
  await eb.stop();
  assert.deepStrictEqual(stub.spawned.get(port).args, ["serve", "--foreground"]);
  assert.ok(elapsed > 15000, `the reporting server answered after ${elapsed}ms, within the plain timeout`);
  assert.ok(
    lines.length >= 5 && lines.every((line) => line.startsWith(PROGRESS_PREFIX)),
    `onProgress got ${JSON.stringify(lines)}`
  );
}

async function givesUpOnASilentServerAndStopsIt() {
  const port = await freePort();
  stub.fake(port, { readyAfterMs: 60000 });
  const started = Date.now();
  const error = await startError({ port, memory: true });
  const elapsed = Date.now() - started;
  assert.ok(error, "a server that prints nothing must time out");
  assert.match(error.message, /did not become healthy within 15000ms$/);
  assert.ok(
    elapsed >= 15000 && elapsed < 25000,
    `the silent server was given up on after ${elapsed}ms, not at 15 s`
  );
  const { child } = stub.spawned.get(port);
  assert.ok(
    child.exitCode !== null || child.signalCode !== null,
    "start() rejected while the server it gave up on was still running"
  );
}

async function carriesTheExitCode() {
  const port = await freePort();
  stub.fake(port, { progressEveryMs: 200, exitAfterMs: 1500, exitCode: 6 });
  const lines = [];
  const error = await startError({ port, memory: true, onProgress: (line) => lines.push(line) });
  assert.ok(error, "a server that exits during startup must reject start()");
  assert.strictEqual(error.exitCode, 6, `exitCode of ${error.message}`);
  assert.strictEqual(error.signal, null);
  assert.match(error.message, /exited during startup \(code 6\)/);
  assert.match(error.message, /the stand-in server stops with exit code 6/);
  assert.ok(lines.length >= 1, "onProgress got no line before the server exited");
}

async function carriesTheSignal() {
  const port = await freePort();
  stub.fake(port, { readyAfterMs: 60000, progressEveryMs: 200 });
  let killed = false;
  const error = await startError({
    port,
    memory: true,
    onProgress: () => {
      if (killed) return;
      killed = true;
      stub.spawned.get(port).child.kill("SIGKILL");
    },
  });
  assert.ok(error, "a server ended by a signal must reject start()");
  assert.strictEqual(error.signal, "SIGKILL", `signal of ${error.message}`);
  assert.strictEqual(error.exitCode, null);
}

// Run `extenddb start` with the stand-in, whose behaviour `env` sets.
function runCli(args, env) {
  return new Promise((resolve) => {
    const cli = stub.realSpawn(
      process.execPath,
      [
        "-r",
        path.join(__dirname, "stub-spawn.js"),
        path.join(__dirname, "..", "bin", "extenddb-launcher.js"),
        ...args,
      ],
      { env: { ...process.env, ...env }, stdio: ["ignore", "pipe", "pipe"] }
    );
    let stdout = "";
    let stderr = "";
    cli.stdout.on("data", (d) => (stdout += d));
    cli.stderr.on("data", (d) => (stderr += d));
    const guard = setTimeout(() => cli.kill("SIGKILL"), 30000);
    cli.on("close", (code, signal) => {
      clearTimeout(guard);
      resolve({ code, signal, stdout, stderr });
    });
  });
}

async function cliShowsProgressAndExitsWithTheServersCode() {
  const port = await freePort();
  const out = await runCli(["start", "--memory", "--port", String(port)], {
    FAKE_PROGRESS_EVERY_MS: "200",
    FAKE_EXIT_AFTER_MS: "1500",
    FAKE_EXIT_CODE: "6",
  });
  assert.strictEqual(out.code, 6, `extenddb start exited ${out.code} (${out.signal}):\n${out.stderr}`);
  // The error message ends with the server's stderr, progress lines too,
  // so a line counts only when it comes before the error.
  const progressAt = out.stderr.indexOf(`${PROGRESS_PREFIX}in progress for`);
  const errorAt = out.stderr.indexOf("extenddb exited during startup");
  assert.ok(
    progressAt >= 0 && progressAt < errorAt,
    `extenddb start wrote no progress line before the error:\n${out.stderr}`
  );
}

async function main() {
  const guard = setTimeout(() => {
    console.error("start tests: still running after 90 s");
    process.exit(1);
  }, 90000);
  try {
    await Promise.all([
      waitsWhileTheServerReportsProgress(),
      givesUpOnASilentServerAndStopsIt(),
      carriesTheExitCode(),
      carriesTheSignal(),
      cliShowsProgressAndExitsWithTheServersCode(),
    ]);
  } finally {
    clearTimeout(guard);
    for (const { child } of stub.spawned.values()) child.kill("SIGKILL");
  }
  console.log("start tests: 5 scenarios passed");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
