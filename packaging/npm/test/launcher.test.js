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
//   4. The launcher runs only a binary of its own release: before `serve`
//      it runs `<binary> version` and compares the release with the version
//      in package.json. The real binary is built from the same commit, so
//      every start above passes the check. Fake binaries (node scripts that
//      record each invocation) prove the other cases: a matching release
//      starts the server, and another release, a failing `version` or output
//      with no release stops the start before `serve` runs, with an error
//      that names the releases it read and where the binary came from.

"use strict";

const assert = require("node:assert");
const { spawnSync } = require("node:child_process");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { start } = require("../index.js");
const { version: LAUNCHER_VERSION } = require("../package.json");

const LAUNCHER = path.join(__dirname, "..", "bin", "extenddb-launcher.js");

if (!process.env.EXTENDDB_BINARY) {
  console.error("EXTENDDB_BINARY must point at a dev-mode extenddb binary");
  process.exit(2);
}

function health(endpoint) {
  const http = require("node:http");
  return new Promise((resolve, reject) => {
    const req = http.get(`${endpoint}/health`, (res) => {
      res.resume();
      resolve(res.statusCode);
    });
    req.on("error", reject);
  });
}

// A minimal SigV4-signed DynamoDB call in plain node, so persistence can be
// proven with real data-plane writes and reads without making an AWS SDK a
// dev dependency of the package.
function ddb(eb, action, body) {
  const crypto = require("node:crypto");
  const http = require("node:http");

  const region = "us-east-1";
  const service = "dynamodb";
  const payload = JSON.stringify(body);
  const now = new Date().toISOString().replace(/[-:]/g, "").replace(/\.\d{3}/, "");
  const date = now.slice(0, 8);
  const { hostname, port } = new URL(eb.endpoint);
  const host = `${hostname}:${port}`;

  const hash = (d) => crypto.createHash("sha256").update(d).digest("hex");
  const hmac = (k, d) => crypto.createHmac("sha256", k).update(d).digest();

  const target = `DynamoDB_20120810.${action}`;
  const canonicalHeaders =
    `host:${host}\nx-amz-date:${now}\nx-amz-target:${target}\n`;
  const signedHeaders = "host;x-amz-date;x-amz-target";
  const canonicalRequest =
    `POST\n/\n\n${canonicalHeaders}\n${signedHeaders}\n${hash(payload)}`;
  const scope = `${date}/${region}/${service}/aws4_request`;
  const stringToSign =
    `AWS4-HMAC-SHA256\n${now}\n${scope}\n${hash(canonicalRequest)}`;
  const kSigning = hmac(
    hmac(hmac(hmac(`AWS4${eb.credentials.secretAccessKey}`, date), region), service),
    "aws4_request"
  );
  const signature = crypto.createHmac("sha256", kSigning).update(stringToSign).digest("hex");

  return new Promise((resolve, reject) => {
    const req = http.request(
      {
        hostname,
        port,
        method: "POST",
        path: "/",
        headers: {
          "content-type": "application/x-amz-json-1.0",
          "x-amz-date": now,
          "x-amz-target": target,
          authorization:
            `AWS4-HMAC-SHA256 Credential=${eb.credentials.accessKeyId}/${scope}, ` +
            `SignedHeaders=${signedHeaders}, Signature=${signature}`,
        },
      },
      (res) => {
        let data = "";
        res.on("data", (d) => (data += d));
        res.on("end", () => {
          if (res.statusCode !== 200) {
            reject(new Error(`${action} -> ${res.statusCode}: ${data}`));
          } else {
            resolve(JSON.parse(data || "{}"));
          }
        });
      }
    );
    req.on("error", reject);
    req.end(payload);
  });
}

// The table used by the persistence scenarios. Tables surface a CREATING
// state briefly even on SQLite, so wait for ACTIVE before writing.
async function createProbeTable(eb) {
  await ddb(eb, "CreateTable", {
    TableName: "launcher-persist",
    AttributeDefinitions: [{ AttributeName: "pk", AttributeType: "S" }],
    KeySchema: [{ AttributeName: "pk", KeyType: "HASH" }],
    BillingMode: "PAY_PER_REQUEST",
  });
  const deadline = Date.now() + 15000;
  for (;;) {
    const d = await ddb(eb, "DescribeTable", { TableName: "launcher-persist" });
    if (d.Table && d.Table.TableStatus === "ACTIVE") return;
    if (Date.now() > deadline) throw new Error("launcher-persist never became ACTIVE");
    await new Promise((r) => setTimeout(r, 200));
  }
}

// The body of a fake binary, run in its own process: it records its
// arguments and working directory, answers `version` as `behaviour` says, and
// for `serve` answers /health on the port the launcher names until SIGTERM.
function fakeMain(behaviour, log) {
  const fs = require("node:fs");
  const http = require("node:http");
  const args = process.argv.slice(2);
  fs.appendFileSync(log, `${JSON.stringify({ args, cwd: process.cwd() })}\n`);
  if (args[0] === "version") {
    process.stdout.write(behaviour.stdout ?? "");
    process.stderr.write(behaviour.stderr ?? "");
    process.exitCode = behaviour.exit ?? 0;
    if (behaviour.hang) {
      // Ignore SIGTERM and never exit: only SIGKILL stops this process.
      process.on("SIGTERM", () => {});
      setInterval(() => {}, 1000);
    }
    return;
  }
  http
    .createServer((req, res) => res.end())
    .listen(Number(process.env.EXTENDDB__SERVER__PORT), "127.0.0.1");
}

// Write a fake binary named `extenddb` into a directory of its own. Windows
// cannot spawn a script without a shell, so the scenarios that use one run
// on Linux and macOS only.
function fakeBinary(behaviour) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "extenddb-fake-"));
  const log = path.join(dir, "invocations.jsonl");
  const file = path.join(dir, "extenddb");
  // The script uses require(): keep it CommonJS whatever package.json is above.
  fs.writeFileSync(path.join(dir, "package.json"), '{ "type": "commonjs" }\n');
  fs.writeFileSync(
    file,
    `#!/usr/bin/env node\n(${fakeMain})(${JSON.stringify(behaviour)}, ${JSON.stringify(log)});\n`,
    { mode: 0o755 }
  );
  const invocations = () =>
    fs.existsSync(log)
      ? fs.readFileSync(log, "utf8").trim().split("\n").map((l) => JSON.parse(l))
      : [];
  return { dir, file, invocations };
}

// What `extenddb version` prints for `release` (print_version in
// crates/app/src/lib.rs).
function versionOutput(release) {
  return (
    `extenddb ${release}\ncatalog migration 7 (sqlite)\ncommit 0000000\n` +
    `built 2026-11-06T00:00:00Z\ntest-hooks disabled\n`
  );
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

  // 2. File mode persists across two server lifetimes on the same path: an
  //    item written by the first server must be readable by the second. This
  //    is the load-bearing assertion; file existence or size alone would pass
  //    for a server that recreated its schema fresh on every boot.
  {
    const first = await start({ binary: process.env.EXTENDDB_BINARY, dbPath });
    await createProbeTable(first);
    await ddb(first, "PutItem", {
      TableName: "launcher-persist",
      Item: { pk: { S: "survivor" }, note: { S: "written by lifetime one" } },
    });
    await first.stop();

    const second = await start({ binary: process.env.EXTENDDB_BINARY, dbPath });
    const got = await ddb(second, "GetItem", {
      TableName: "launcher-persist",
      Key: { pk: { S: "survivor" } },
    });
    assert.ok(got.Item, "the item written in lifetime one must survive into lifetime two");
    assert.strictEqual(got.Item.note.S, "written by lifetime one");
    await second.stop();
  }

  // 3. Memory mode: nothing lands in the default data dir, and data written
  //    to one memory-mode server is GONE after a restart. The loss assertion
  //    is the discriminating half: if memory mode silently fell back to a
  //    file, it would fail.
  {
    const memScratch = fs.mkdtempSync(path.join(os.tmpdir(), "extenddb-mem-test-"));
    process.chdir(memScratch);
    const eb = await start({ binary: process.env.EXTENDDB_BINARY, memory: true });
    assert.strictEqual(await health(eb.endpoint), 200);
    assert.strictEqual(eb.storage, ":memory: (ephemeral)");
    await createProbeTable(eb);
    await ddb(eb, "PutItem", {
      TableName: "launcher-persist",
      Item: { pk: { S: "ephemeral" } },
    });
    assert.ok(
      !fs.existsSync(path.join(memScratch, ".extenddb")),
      "memory mode must not create the data directory"
    );
    await eb.stop();

    const again = await start({ binary: process.env.EXTENDDB_BINARY, memory: true });
    await assert.rejects(
      () =>
        ddb(again, "GetItem", {
          TableName: "launcher-persist",
          Key: { pk: { S: "ephemeral" } },
        }),
      /ResourceNotFoundException/,
      "a restarted memory-mode server must not know the previous server's table"
    );
    await again.stop();
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

  if (process.platform === "win32") {
    console.log("launcher tests: 4 scenarios passed (scenarios 5 to 8 run fake binaries through their #! line)");
    return;
  }

  // 5. A binary of the launcher's release starts. The check runs `version`
  //    in the working directory `serve` then gets, so it checks the binary
  //    that serves.
  {
    const fake = fakeBinary({ stdout: versionOutput(LAUNCHER_VERSION) });
    const eb = await start({ binary: fake.file, memory: true });
    assert.strictEqual(await health(eb.endpoint), 200);
    await eb.stop();
    const runs = fake.invocations();
    assert.deepStrictEqual(
      runs.map((r) => r.args),
      [["version"], ["serve", "--foreground"]],
      "the version check runs once, before serve"
    );
    assert.strictEqual(runs[0].cwd, runs[1].cwd, "version and serve share a working directory");
  }

  // 6. Another release stops the start before serve runs. The error names
  //    both releases, where the binary came from, and the fix for that
  //    source.
  {
    const fake = fakeBinary({ stdout: versionOutput("0.0.1") });
    await assert.rejects(
      () => start({ binary: fake.file, memory: true }),
      (err) => {
        assert.ok(err.message.includes(`${fake.file} (from the "binary" option)`), err.message);
        assert.ok(err.message.includes("is release 0.0.1,"), err.message);
        assert.ok(err.message.includes(`launcher is release ${LAUNCHER_VERSION}:`), err.message);
        assert.ok(
          err.message.includes(`Pass the "binary" option an extenddb ${LAUNCHER_VERSION} dev binary.`),
          err.message
        );
        return true;
      }
    );
    assert.deepStrictEqual(
      fake.invocations().map((r) => r.args),
      [["version"]],
      "serve must not run on a binary of another release"
    );

    // The check refuses a newer release as well: it wants an exact match.
    const newer = fakeBinary({ stdout: versionOutput("99.0.0") });
    await assert.rejects(
      () => start({ binary: newer.file, memory: true }),
      /is release 99\.0\.0,/
    );
    assert.deepStrictEqual(newer.invocations().map((r) => r.args), [["version"]]);
  }

  // 7. A `version` that fails, and one that prints no release, stop the
  //    start too: a binary whose release cannot be read is not run. An
  //    extenddb before the `version` command exits 2 here, as clap exits on
  //    an unknown subcommand.
  {
    const failing = fakeBinary({ stderr: "error: unrecognized subcommand 'version'\n", exit: 2 });
    await assert.rejects(
      () => start({ binary: failing.file, memory: true }),
      (err) => {
        assert.ok(err.message.includes('"extenddb version" exited with code 2.'), err.message);
        assert.ok(err.message.includes(`launcher is release ${LAUNCHER_VERSION}`), err.message);
        assert.ok(err.message.includes("unrecognized subcommand 'version'"), err.message);
        return true;
      }
    );
    assert.deepStrictEqual(failing.invocations().map((r) => r.args), [["version"]]);

    const unreadable = fakeBinary({ stdout: "usage: extenddb <command>\n" });
    await assert.rejects(
      () => start({ binary: unreadable.file, memory: true }),
      /"extenddb version" printed no "extenddb <release>" line\./
    );
    assert.deepStrictEqual(unreadable.invocations().map((r) => r.args), [["version"]]);

    // The launcher stops a `version` that never exits and ignores SIGTERM
    // at the startup timeout. The guard fails the test instead of hanging it.
    const hanging = fakeBinary({ stdout: versionOutput(LAUNCHER_VERSION), hang: true });
    let guard;
    const deadline = new Promise((_, reject) => {
      guard = setTimeout(() => reject(new Error("start() did not return within 10 s")), 10000);
    });
    await assert.rejects(
      () => Promise.race([start({ binary: hanging.file, memory: true, startupTimeoutMs: 500 }), deadline]),
      /"extenddb version" did not finish within 500ms\./
    );
    clearTimeout(guard);
  }

  // 8. From the command line, a binary that EXTENDDB_BINARY names or that
  //    PATH finds is checked the same way, and the error names that source.
  {
    const fake = fakeBinary({ stdout: versionOutput("0.0.1") });
    const run = (env) =>
      spawnSync(process.execPath, [LAUNCHER, "start", "--memory"], {
        env,
        encoding: "utf8",
        timeout: 30000,
      });

    const viaEnv = run({ ...process.env, EXTENDDB_BINARY: fake.file });
    assert.strictEqual(viaEnv.status, 1, viaEnv.stderr);
    assert.ok(
      viaEnv.stderr.includes(`${fake.file} (from EXTENDDB_BINARY) is release 0.0.1`),
      viaEnv.stderr
    );
    assert.ok(viaEnv.stderr.includes("or unset it to use the platform package."), viaEnv.stderr);
    assert.deepStrictEqual(
      fake.invocations().map((r) => r.args),
      [["version"]],
      "serve must not run from the command line either"
    );

    // An installed platform package comes before PATH, so the PATH half
    // runs only where none is installed, as in CI.
    let platformPackage = false;
    try {
      require.resolve(`@extenddb/${process.platform}-${process.arch}/extenddb`);
      platformPackage = true;
    } catch {
      /* not installed */
    }
    if (!platformPackage) {
      // PATH holds the fake and node (for the fake's #! line), and nothing else.
      const pathEnv = {
        ...process.env,
        PATH: [fake.dir, path.dirname(process.execPath)].join(path.delimiter),
      };
      delete pathEnv.EXTENDDB_BINARY;
      const viaPath = run(pathEnv);
      assert.strictEqual(viaPath.status, 1, viaPath.stderr);
      assert.ok(
        viaPath.stderr.includes("the extenddb binary on PATH is release 0.0.1"),
        viaPath.stderr
      );
      assert.ok(viaPath.stderr.includes("or set EXTENDDB_BINARY to"), viaPath.stderr);
      assert.deepStrictEqual(fake.invocations().map((r) => r.args), [["version"], ["version"]]);
    } else {
      console.log("scenario 8: an installed platform package comes before PATH; PATH half skipped");
    }
  }

  console.log("launcher tests: 8 scenarios passed");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
