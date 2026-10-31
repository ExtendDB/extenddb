// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

// Replaces child_process.spawn so that start() runs test/fake-server.js in
// place of the extenddb binary. Load it before ../index.js, with require()
// or `node -r`. The stand-in listens on the port start() gives the server
// (EXTENDDB__SERVER__PORT). It behaves as fake() registered for that port,
// or else as the FAKE_* variables of this process's environment say (see
// fake-server.js).

"use strict";

const childProcess = require("node:child_process");
const path = require("node:path");

const realSpawn = childProcess.spawn;
const behaviours = new Map();
// port -> { command, args, child } for each server start() spawned.
const spawned = new Map();

/**
 * Register how the stand-in for `port` behaves.
 *
 * @param {number} port
 * @param {{readyAfterMs?: number, progressEveryMs?: number,
 *          exitAfterMs?: number, exitCode?: number}} behaviour
 */
function fake(port, behaviour) {
  behaviours.set(port, behaviour);
}

childProcess.spawn = function spawnStandIn(command, args, options) {
  const port = Number(options.env.EXTENDDB__SERVER__PORT);
  const env = { ...options.env, FAKE_PORT: String(port) };
  const b = behaviours.get(port);
  if (b) {
    env.FAKE_READY_AFTER_MS = String(b.readyAfterMs ?? 0);
    env.FAKE_PROGRESS_EVERY_MS = String(b.progressEveryMs ?? 0);
    env.FAKE_EXIT_AFTER_MS = String(b.exitAfterMs ?? 0);
    env.FAKE_EXIT_CODE = String(b.exitCode ?? 0);
  }
  const child = realSpawn(process.execPath, [path.join(__dirname, "fake-server.js")], {
    ...options,
    env,
  });
  spawned.set(port, { command, args, child });
  return child;
};

module.exports = { fake, spawned, realSpawn };
