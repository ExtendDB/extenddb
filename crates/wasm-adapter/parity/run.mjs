// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

// run.mjs — the checks of `submit`'s JavaScript boundary.
//
// Loads the browser module that `wasm-pack build --target web` emits into
// `./pkg` (built by devtools/run-wasm-step before this runs), under Node. It is
// the only check of that module and of the `ExtendDb` class wasm-bindgen
// generates for it: it calls `ExtendDb.create()`, then `submit` with
// arguments that are not strings, expects a ValidationException for each, and
// expects the instance to keep serving. It makes no call on an object that is
// not a live instance: `submit` is defined only on an object
// `ExtendDb.create()` returned and that has not been freed.
//
// The parity corpus (`./corpus.json`) is run by `../tests/parity.rs`, natively
// and on wasm32 under wasm-bindgen-test. On wasm32 that test calls the Rust
// `ExtendDb::submit` directly, with string arguments, and does not load the
// emitted module.
//
// Node-hosting note: a browser always defines `globalThis.crypto`, and Node 22
// does; Node 18 does not define it in a file it runs, ES module or not, so
// the runner installs Node's Web Crypto when it is missing, before the module
// loads. The runner adds nothing else to the environment; it loads the same
// module files a browser would.

import { webcrypto } from 'node:crypto';
if (!globalThis.crypto) globalThis.crypto = webcrypto;

import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';

const PKG = resolve(dirname(fileURLToPath(import.meta.url)), 'pkg');

// Node has no fetch for file://, so the module's bytes are passed in.
const mod = await import(`${PKG}/extenddb_wasm_adapter.js`);
await mod.default({ module_or_path: await readFile(`${PKG}/extenddb_wasm_adapter_bg.wasm`) });

console.log('=== submit\'s JavaScript boundary ===');
console.log(`  module: ${PKG}/extenddb_wasm_adapter_bg.wasm`);
console.log(`  node:   ${process.version}`);

let failures = 0;
let checks = 0;
function fail(msg) {
    failures += 1;
    console.error(`  \u2717 ${msg}`);
}

const db = await mod.ExtendDb.create();

// Each call must resolve with a ValidationException error document naming
// the argument.
const badArguments = [
    ['a number operation', [123, '{}'], 'operation'],
    ['an undefined operation and body', [undefined, undefined], 'operation'],
    ['a null operation', [null, '{}'], 'operation'],
    ['an object operation', [{ op: 'ListTables' }, '{}'], 'operation'],
    ['a number body', ['ListTables', 7], 'body'],
    ['an undefined body', ['ListTables', undefined], 'body'],
    ['an object body', ['ListTables', {}], 'body'],
];
for (const [label, args, argument] of badArguments) {
    checks += 1;
    let resp;
    try {
        resp = JSON.parse(await db.submit(...args));
    } catch (e) {
        fail(`submit with ${label}: threw ${e.name}: ${e.message}`);
        continue;
    }
    const type = typeof resp.__type === 'string' ? resp.__type : '';
    if (!type.endsWith('#ValidationException') || resp.message !== `${argument} must be a string`) {
        fail(`submit with ${label}: expected a ValidationException with message "${argument} must be a string", got ${JSON.stringify(resp)}`);
    }
}

// The instance keeps serving afterwards.
checks += 1;
try {
    const after = JSON.parse(await db.submit('ListTables', '{}'));
    if (!Array.isArray(after.TableNames)) {
        fail(`ListTables after the arguments that are not strings: expected TableNames, got ${JSON.stringify(after)}`);
    }
} catch (e) {
    fail(`ListTables after the arguments that are not strings: threw ${e.name}: ${e.message}`);
}

console.log('');
if (failures === 0) {
    console.log(`RESULT: PASS (${checks} checks of submit's JavaScript boundary)`);
    process.exit(0);
} else {
    console.log(`RESULT: FAIL (${failures} of ${checks} checks of submit's JavaScript boundary)`);
    process.exit(1);
}
