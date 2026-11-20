// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

// run.mjs — the headless wasm parity runner.
//
// Drives the browser wasm module directly — `ExtendDb.create()` then
// `submit(operation, bodyJson)` — over the shared parity corpus
// (`./corpus.json`) under Node, with no browser. This is the executable gate
// the compile-only wasm build check cannot be: the clock and randomness
// primitives compile on wasm but only fail when called, so the engine must
// actually run.
//
// It runs the SAME cases with the SAME expectations as the native oracle test
// (`../tests/parity.rs`), which drives them through real sqlx + C SQLite. Both
// agreeing is what makes the port's fidelity checkable: the in-scope data-plane
// operations answer identically, and the operations the browser build cannot
// serve answer the same refusal or no-store answer on both.
//
// The module is loaded from `./pkg`, the `wasm-bindgen --target web` output
// (built by the test harness before this runner is invoked).
//
// Node-hosting note: a browser always defines `globalThis.crypto`; Node's ESM
// scope does not, so Node's Web Crypto is installed before the module loads.
// That is the only Node-vs-browser difference; the module itself is identical
// to what a browser loads.

import { webcrypto } from 'node:crypto';
if (!globalThis.crypto) globalThis.crypto = webcrypto;

import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';

const HERE = dirname(fileURLToPath(import.meta.url));
const PKG = resolve(HERE, 'pkg');
const CORPUS_PATH = resolve(HERE, 'corpus.json');

// ---- tiny assertion helpers ------------------------------------------------

let failures = 0;
let checks = 0;
function fail(msg) {
    failures += 1;
    console.error(`  \u2717 ${msg}`);
}

/** Resolve a JSON Pointer (RFC 6901), returning undefined if absent. */
function pointer(doc, path) {
    if (path === '') return doc;
    const parts = path.split('/').slice(1).map((p) => p.replace(/~1/g, '/').replace(/~0/g, '~'));
    let cur = doc;
    for (const part of parts) {
        if (cur === null || typeof cur !== 'object') return undefined;
        cur = Array.isArray(cur) ? cur[Number(part)] : cur[part];
        if (cur === undefined) return undefined;
    }
    return cur;
}

/** Short DynamoDB error name (segment after `#`, then after `.`). */
function errorShortName(resp) {
    const t = resp && resp.__type;
    if (typeof t !== 'string') return undefined;
    const afterHash = t.includes('#') ? t.slice(t.lastIndexOf('#') + 1) : t;
    return afterHash.includes('.') ? afterHash.slice(afterHash.lastIndexOf('.') + 1) : afterHash;
}

/** Apply one case's assertions to a parsed response. */
function assertCase(c, resp) {
    const got = errorShortName(resp);
    if (c.error) {
        if (got !== c.error) {
            fail(`case '${c.name}': expected error ${c.error}, got ${JSON.stringify(resp)}`);
        }
    } else if (got !== undefined) {
        fail(`case '${c.name}': expected success, got error ${JSON.stringify(resp)}`);
    }
    for (const check of c.checks || []) {
        const val = pointer(resp, check.path);
        checks += 1;
        if (check.absent === true) {
            if (val !== undefined) {
                fail(`case '${c.name}': expected ${check.path} absent, got ${JSON.stringify(val)}`);
            }
        } else if (check.present === true) {
            if (val === undefined) {
                fail(`case '${c.name}': expected ${check.path} present, got undefined`);
            }
        } else if (typeof check.contains === 'string') {
            if (typeof val !== 'string' || !val.includes(check.contains)) {
                fail(`case '${c.name}': at ${check.path} expected a string containing ${JSON.stringify(check.contains)}, got ${JSON.stringify(val)}`);
            }
        } else if (JSON.stringify(val) !== JSON.stringify(check.equals)) {
            fail(`case '${c.name}': at ${check.path} expected ${JSON.stringify(check.equals)}, got ${JSON.stringify(val)}`);
        }
    }
}

// ---- load the wasm module (pass bytes; Node has no fetch for file://) -------

const mod = await import(`${PKG}/extenddb_wasm_adapter.js`);
const bytes = await readFile(`${PKG}/extenddb_wasm_adapter_bg.wasm`);
await mod.default({ module_or_path: bytes });

console.log('=== Headless wasm parity runner ===');
console.log('  target: wasm32-unknown-unknown (in-memory SQLite, in browser/wasm)');
console.log(`  module: ${PKG}/extenddb_wasm_adapter_bg.wasm`);
console.log(`  node:   ${process.version}`);
console.log('');

// ---- run the shared parity corpus through the wasm module ------------------

const corpus = JSON.parse(await readFile(CORPUS_PATH, 'utf8'));
const db = await mod.ExtendDb.create();
for (const c of corpus.cases) {
    const bodyJson = typeof c.rawBody === 'string' ? c.rawBody : JSON.stringify(c.body);
    let resp;
    try {
        resp = JSON.parse(await db.submit(c.op, bodyJson));
    } catch (e) {
        fail(`case '${c.name}': submit threw (likely a wasm trap): ${e.message}`);
        continue;
    }
    assertCase(c, resp);
}

// ---- summary ---------------------------------------------------------------

console.log('');
if (failures === 0) {
    console.log(`RESULT: PASS (${corpus.cases.length} cases, ${checks} checks)`);
    process.exit(0);
} else {
    console.log(`RESULT: FAIL (${failures} failure(s) over ${corpus.cases.length} cases, ${checks} checks)`);
    process.exit(1);
}
