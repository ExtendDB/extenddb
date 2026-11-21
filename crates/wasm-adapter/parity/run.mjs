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
// It runs the same cases with the same assertions as the native test
// (`../tests/parity.rs`), which drives them through sqlx + C SQLite. When both
// pass, each build gave every case the error name and the checked members the
// corpus asserts; members a case does not check are not compared. Before the
// corpus, both runners judge the rows of `./runner-selftest.json` and fail if
// any row's outcome differs from the one it records.
//
// The module is loaded from `./pkg`, the `wasm-bindgen --target web` output
// (built by the test harness before this runner is invoked).
//
// Node-hosting note: a browser always defines `globalThis.crypto`; Node's ESM
// scope does not, so Node's Web Crypto is installed before the module loads.
// The runner adds nothing else to the environment; it loads the same module
// files a browser would.

import { webcrypto } from 'node:crypto';
if (!globalThis.crypto) globalThis.crypto = webcrypto;

import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';

const HERE = dirname(fileURLToPath(import.meta.url));
const PKG = resolve(HERE, 'pkg');
const CORPUS_PATH = resolve(HERE, 'corpus.json');
const SELFTEST_PATH = resolve(HERE, 'runner-selftest.json');

// ---- tiny assertion helpers ------------------------------------------------

let failures = 0;
let checks = 0;
function fail(msg) {
    failures += 1;
    console.error(`  \u2717 ${msg}`);
}

/**
 * Resolve a JSON Pointer (RFC 6901), returning undefined if absent. Follows
 * serde_json's `Value::pointer`, which the native runner uses: a pointer that
 * is not empty and does not start with `/` addresses nothing, an array index
 * is decimal digits with no leading zero, and only a document's own members
 * are addressed.
 */
function pointer(doc, path) {
    if (path === '') return doc;
    if (!path.startsWith('/')) return undefined;
    const parts = path.split('/').slice(1).map((p) => p.replace(/~1/g, '/').replace(/~0/g, '~'));
    let cur = doc;
    for (const part of parts) {
        if (Array.isArray(cur)) {
            if (!/^(0|[1-9][0-9]*)$/.test(part)) return undefined;
            cur = cur[Number(part)];
        } else if (cur !== null && typeof cur === 'object') {
            cur = Object.hasOwn(cur, part) ? cur[part] : undefined;
        } else {
            return undefined;
        }
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

// The judging rules below are the native runner's (`judge` in
// `../tests/parity.rs`); the rows of `./runner-selftest.json` hold the two to
// the same outcomes.

/** A case that is malformed, as opposed to one the response does not satisfy. */
class Reject extends Error {}

const CHECK_SHAPE = 'must have a string path and exactly one of equals, contains (a string), '
    + 'absent: true or present: true, and no other member';

function isObject(v) {
    return v !== null && typeof v === 'object' && !Array.isArray(v);
}

/** Read one check into `{ path, kind, value }`, throwing Reject on any other shape. */
function parseCheck(check) {
    const bad = () => new Reject(`check ${JSON.stringify(check)} ${CHECK_SHAPE}`);
    if (!isObject(check) || typeof check.path !== 'string') throw bad();
    const keys = Object.keys(check);
    if (keys.length !== 2) throw bad();
    const kind = keys.find((k) => k !== 'path');
    const value = check[kind];
    if (kind === 'equals') return { path: check.path, kind, value };
    if (kind === 'contains' && typeof value === 'string') return { path: check.path, kind, value };
    if ((kind === 'absent' || kind === 'present') && value === true) return { path: check.path, kind, value };
    throw bad();
}

/** JSON equality: object members regardless of order, array elements in order. */
function jsonEqual(a, b) {
    if (Array.isArray(a) || Array.isArray(b)) {
        return Array.isArray(a) && Array.isArray(b)
            && a.length === b.length && a.every((x, i) => jsonEqual(x, b[i]));
    }
    if (isObject(a) || isObject(b)) {
        if (!isObject(a) || !isObject(b)) return false;
        const keys = Object.keys(a);
        return keys.length === Object.keys(b).length
            && keys.every((k) => Object.hasOwn(b, k) && jsonEqual(a[k], b[k]));
    }
    return a === b;
}

/**
 * Judge one case against a parsed response: undefined when it holds, else a
 * failure message. Throws Reject when the case itself is malformed.
 */
function judge(c, resp) {
    let expected;
    if (c.error === undefined || c.error === null) {
        expected = undefined;
    } else if (typeof c.error === 'string' && c.error !== '') {
        expected = c.error;
    } else {
        throw new Reject(`case '${c.name}': error ${JSON.stringify(c.error)} is neither null nor a non-empty string`);
    }
    let checkList = [];
    if (c.checks !== undefined) {
        if (!Array.isArray(c.checks)) throw new Reject(`case '${c.name}': checks ${JSON.stringify(c.checks)} is not an array`);
        checkList = c.checks.map((check) => {
            try {
                return parseCheck(check);
            } catch (e) {
                if (e instanceof Reject) throw new Reject(`case '${c.name}': ${e.message}`);
                throw e;
            }
        });
    }

    const got = errorShortName(resp);
    if (expected !== undefined && got !== expected) {
        return `case '${c.name}': expected error ${expected}, got response ${JSON.stringify(resp)}`;
    }
    if (expected === undefined && got !== undefined) {
        return `case '${c.name}': expected success, got error document ${JSON.stringify(resp)}`;
    }
    for (const { path, kind, value } of checkList) {
        const val = pointer(resp, path);
        let holds;
        if (kind === 'absent') holds = val === undefined;
        else if (kind === 'present') holds = val !== undefined;
        else if (kind === 'contains') holds = typeof val === 'string' && val.includes(value);
        else holds = val !== undefined && jsonEqual(val, value);
        if (!holds) {
            return `case '${c.name}': at ${path} expected ${kind} ${JSON.stringify(value)}, response ${JSON.stringify(resp)}`;
        }
    }
    return undefined;
}

/** Apply one case's assertions to a parsed response, counting its checks. */
function assertCase(c, resp) {
    checks += Array.isArray(c.checks) ? c.checks.length : 0;
    let why;
    try {
        why = judge(c, resp);
    } catch (e) {
        why = e instanceof Reject ? `malformed case: ${e.message}` : `judging threw: ${e.message}`;
    }
    if (why !== undefined) fail(why);
}

/** Replace every string of the form `${name}` with the value an earlier case saved. */
function substitute(c, value, saved) {
    if (typeof value === 'string') {
        const m = /^\$\{(.*)\}$/.exec(value);
        if (!m) return value;
        if (!saved.has(m[1])) throw new Error(`case '${c.name}': no earlier case saved a value named ${JSON.stringify(m[1])}`);
        return saved.get(m[1]);
    }
    if (Array.isArray(value)) return value.map((v) => substitute(c, v, saved));
    if (value !== null && typeof value === 'object') {
        return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, substitute(c, v, saved)]));
    }
    return value;
}

/** Record the string members a case's `save` names, for later cases' bodies. */
function save(c, resp, saved) {
    for (const [key, path] of Object.entries(c.save || {})) {
        const val = pointer(resp, path);
        if (typeof val !== 'string') {
            fail(`case '${c.name}': save path ${path} is not a string in ${JSON.stringify(resp)}`);
            continue;
        }
        saved.set(key, val);
    }
}

// ---- judge the shared runner self-test rows before the corpus --------------

const selftest = JSON.parse(await readFile(SELFTEST_PATH, 'utf8'));
let selftestDisagreements = 0;
for (const row of selftest.rows) {
    let got;
    try {
        got = judge({ name: row.name, ...row.case }, row.response) === undefined ? 'pass' : 'fail';
    } catch (e) {
        if (!(e instanceof Reject)) throw e;
        got = 'reject';
    }
    if (got !== row.expect) {
        selftestDisagreements += 1;
        console.error(`  \u2717 self-test '${row.name}': expected ${row.expect}, got ${got}`);
    }
}
if (selftestDisagreements !== 0) {
    console.log(`RESULT: FAIL (${selftestDisagreements} of ${selftest.rows.length} runner self-test rows judged differently than expected)`);
    process.exit(1);
}

// ---- load the wasm module (pass bytes; Node has no fetch for file://) -------

const mod = await import(`${PKG}/extenddb_wasm_adapter.js`);
const bytes = await readFile(`${PKG}/extenddb_wasm_adapter_bg.wasm`);
await mod.default({ module_or_path: bytes });

console.log('=== Headless wasm parity runner ===');
console.log('  target: wasm32-unknown-unknown (in-memory SQLite, in browser/wasm)');
console.log(`  module: ${PKG}/extenddb_wasm_adapter_bg.wasm`);
console.log(`  node:   ${process.version}`);
console.log(`  runner self-test: ${selftest.rows.length} rows judged as expected`);
console.log('');

// ---- run the shared parity corpus through the wasm module ------------------

const corpus = JSON.parse(await readFile(CORPUS_PATH, 'utf8'));
const db = await mod.ExtendDb.create();
const saved = new Map();
for (const c of corpus.cases) {
    let bodyJson;
    try {
        bodyJson = typeof c.rawBody === 'string' ? c.rawBody : JSON.stringify(substitute(c, c.body, saved));
    } catch (e) {
        fail(e.message);
        continue;
    }
    let resp;
    try {
        resp = JSON.parse(await db.submit(c.op, bodyJson));
    } catch (e) {
        fail(`case '${c.name}': submit threw (likely a wasm trap): ${e.message}`);
        continue;
    }
    assertCase(c, resp);
    save(c, resp, saved);
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
