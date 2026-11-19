// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

"use strict";

// The main step of the GitHub Action in action.yml. It starts the dev server
// of this release through the npm launcher, detached so that the server runs
// until the job ends, and gives the endpoint and the credential to the later
// steps. post.js stops the server.

const crypto = require("node:crypto");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const zlib = require("node:zlib");
const { start } = require("../npm/index.js");
const { version, optionalDependencies } = require("../npm/package.json");

function input(name) {
  return (process.env[`INPUT_${name.toUpperCase()}`] ?? "").trim();
}

// Append name=value lines to a file that the runner reads after the step
// (GITHUB_OUTPUT, GITHUB_ENV or GITHUB_STATE). No value here holds a newline.
function append(file, values) {
  const lines = Object.entries(values).map(([name, value]) => `${name}=${value}\n`);
  fs.appendFileSync(process.env[file], lines.join(""));
}

async function get(url) {
  const res = await fetch(url).catch((err) => {
    throw new Error(`GET ${url} failed: ${err.cause?.code ?? err.message}`);
  });
  if (!res.ok) throw new Error(`GET ${url} answered HTTP ${res.status}`);
  return res;
}

// The file `name` from an uncompressed ustar archive, the format of npm packages.
function untar(archive, name) {
  for (let at = 0; at + 512 <= archive.length && archive[at] !== 0; ) {
    const field = (from, to) => archive.toString("utf8", at + from, at + to).replace(/\0[\s\S]*$/, "");
    const size = parseInt(field(124, 136), 8);
    const prefix = field(257, 263) === "ustar" ? field(345, 500) : "";
    if ((prefix ? `${prefix}/${field(0, 100)}` : field(0, 100)) === name) {
      return archive.subarray(at + 512, at + 512 + size);
    }
    at += 512 + Math.ceil(size / 512) * 512;
  }
  throw new Error(`the package holds no ${name}`);
}

// Download the binary of this release for the runner's platform, from the
// platform package that the launcher names in its optionalDependencies, and
// check it against the sha512 that the registry gives for the package.
async function download() {
  const pkg = `@extenddb/${process.platform}-${process.arch}`;
  if (!optionalDependencies[pkg]) {
    const platforms = Object.keys(optionalDependencies).map((name) => name.replace("@extenddb/", ""));
    throw new Error(
      `ExtendDB ${version} has no binary for ${process.platform}-${process.arch}, only for ` +
        `${platforms.join(", ")}. Set the binary input to an extenddb ${version} dev binary.`
    );
  }
  const meta = await (await get(`https://registry.npmjs.org/${pkg.replace("/", "%2f")}/${version}`)).json();
  const tarball = Buffer.from(await (await get(meta.dist.tarball)).arrayBuffer());
  const sha512 = `sha512-${crypto.createHash("sha512").update(tarball).digest("base64")}`;
  if (sha512 !== meta.dist.integrity) {
    throw new Error(`${meta.dist.tarball} has ${sha512}, and the registry gives ${meta.dist.integrity}`);
  }
  const name = process.platform === "win32" ? "extenddb.exe" : "extenddb";
  const dir = fs.mkdtempSync(path.join(process.env.RUNNER_TEMP || os.tmpdir(), "extenddb-"));
  const file = path.join(dir, name);
  fs.writeFileSync(file, untar(zlib.gunzipSync(tarball), `package/${name}`), { mode: 0o755 });
  return file;
}

async function main() {
  const port = input("port");
  if (port && !(/^\d+$/.test(port) && Number(port) >= 1 && Number(port) <= 65535)) {
    throw new Error(`the port input must be a number from 1 to 65535, got "${port}"`);
  }
  const exportEnv = input("export-env").toLowerCase() !== "false";
  const binary = input("binary") ? path.resolve(input("binary")) : await download();
  // The server refuses a request that carries a session token, so clear a
  // token that an earlier step set. Only a set one: GITHUB_ENV cannot unset a
  // variable, and the Ruby SDK sends an empty token as an empty header.
  const tokens = ["AWS_SESSION_TOKEN", "AWS_SECURITY_TOKEN"].filter((name) => process.env[name]);
  // The server inherits this environment and keeps it until the job ends.
  // Keep the runner's tokens for the action (ACTIONS_*), the inputs and the
  // job's AWS credentials out of it, keep the server on loopback whatever the
  // job sets, and put the launcher's scratch directory and the log under
  // RUNNER_TEMP, which the runner empties after each job.
  for (const name of Object.keys(process.env)) {
    if (/^(ACTIONS_|INPUT_|AWS_)/.test(name)) delete process.env[name];
  }
  delete process.env.EXTENDDB_DEV_ALLOW_ANY_BIND;
  delete process.env.EXTENDDB__SERVER__BIND_ADDR;
  if (process.env.RUNNER_TEMP) {
    process.env.TMPDIR = process.env.TEMP = process.env.TMP = process.env.RUNNER_TEMP;
  }
  const server = await start({
    binary,
    memory: true,
    port: port ? Number(port) : undefined,
    detached: true,
  });
  append("GITHUB_STATE", { pid: server.pid, endpoint: server.endpoint, log: server.log });
  const { accessKeyId, secretAccessKey } = server.credentials;
  append("GITHUB_OUTPUT", {
    endpoint: server.endpoint,
    port: server.port,
    "access-key-id": accessKeyId,
    "secret-access-key": secretAccessKey,
    region: server.region,
  });
  if (exportEnv) {
    append("GITHUB_ENV", {
      AWS_ENDPOINT_URL_DYNAMODB: server.endpoint,
      AWS_ENDPOINT_URL_DYNAMODB_STREAMS: server.endpoint,
      AWS_ACCESS_KEY_ID: accessKeyId,
      AWS_SECRET_ACCESS_KEY: secretAccessKey,
      AWS_REGION: server.region,
      AWS_DEFAULT_REGION: server.region,
      ...Object.fromEntries(tokens.map((name) => [name, ""])),
    });
  }
  console.log(`ExtendDB ${version} dev server ready at ${server.endpoint}`);
}

main().catch((err) => {
  // A workflow command is one line, so encode the line breaks of the message.
  const message = String(err.message).replace(/%/g, "%25").replace(/\r/g, "%0D").replace(/\n/g, "%0A");
  console.log(`::error::${message}`);
  process.exitCode = 1;
});
