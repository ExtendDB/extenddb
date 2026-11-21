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
// The native test (`../tests/parity.rs`) runs the same corpus through sqlx and
// C SQLite. Both runners implement the same three rules: the case schema
// (`readCase`), the request a case sends (`requestText`: its rawBody as
// written, or its body as canonical JSON after each reference `${name}` in its
// strings is replaced by the value an earlier case saved) and the judging of a
// response (`judge`). Each runner first
// judges every row of `./runner-selftest.json` and fails if a row's outcome
// differs from the one the row records; the rows hold the two implementations
// of each rule to the same outcomes. When both runs pass, each build gave
// every case the error name and the checked members the corpus asserts;
// members a case does not check are not compared.
//
// A case may carry an `onWasm` expectation, with the reason the WebAssembly
// build answers differently; this runner judges the case against it in place
// of the case's `error` and `checks`.
//
// After the corpus, this runner also checks what only a JavaScript caller can
// do: it calls `submit` with arguments that are not strings and expects a
// ValidationException for each, and the instance to keep serving. It makes no
// call on an object that is not a live instance: `submit` is defined only on an
// object `ExtendDb.create()` returned and that has not been freed.
//
// The corpus and the self-test rows are read with `parseJson` below rather
// than `JSON.parse`, so that this runner sees each number as written (`1.0`
// and `1` differ) and refuses the text `serde_json` refuses: a number outside
// the double range, an unpaired surrogate, nesting of 128 levels or more.
//
// The module is loaded from `./pkg`, the `wasm-bindgen --target web` output
// (built by the test harness before this runner is invoked).
//
// Node-hosting note: a browser always defines `globalThis.crypto`, and Node 22
// does; Node 18 does not define it in an ES module, so the runner installs
// Node's Web Crypto when it is missing, before the module loads. The runner
// adds nothing else to the environment; it loads the same module files a
// browser would.

import { webcrypto } from 'node:crypto';
if (!globalThis.crypto) globalThis.crypto = webcrypto;

import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';

const HERE = dirname(fileURLToPath(import.meta.url));
const PKG = resolve(HERE, 'pkg');
const CORPUS_PATH = resolve(HERE, 'corpus.json');
const SELFTEST_PATH = resolve(HERE, 'runner-selftest.json');

// ---- reading JSON text -----------------------------------------------------

/** A number as written in JSON text, kept as its text until a rule reads it. */
class JsonNumber {
    constructor(text) {
        this.text = text;
    }
}

/** JSON text that does not parse. */
class ParseError extends Error {}

/** The characters of a string that stand for themselves, up to the next one that does not. */
const PLAIN_RUN = /[^"\\\u0000-\u001f\ud800-\udfff]*/y;
/** A JSON number. */
const NUMBER = /-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?/y;
/** The escapes that stand for one character. */
const SIMPLE_ESCAPES = { '"': '"', '\\': '\\', '/': '/', b: '\b', f: '\f', n: '\n', r: '\r', t: '\t' };

/**
 * Parse JSON text (RFC 8259) into plain values, with each number a
 * JsonNumber. Refuses what `serde_json::from_str` refuses: a number outside
 * the double range, an escape that leaves a surrogate unpaired, an unescaped
 * character below U+0020 in a string, 128 levels of nesting, and any text
 * after the value. A repeated member name keeps the last value, as
 * `serde_json` does.
 */
function parseJson(text) {
    let i = 0;
    let depthLeft = 128;
    const fail = (what) => {
        throw new ParseError(`${what} at offset ${i}`);
    };
    const ws = () => {
        while (i < text.length && ' \t\n\r'.includes(text[i])) i += 1;
    };
    const hex4 = () => {
        const h = text.slice(i, i + 4);
        if (!/^[0-9a-fA-F]{4}$/.test(h)) fail('invalid \\u escape');
        i += 4;
        return parseInt(h, 16);
    };
    const string = () => {
        i += 1; // the opening quote
        let out = '';
        for (;;) {
            PLAIN_RUN.lastIndex = i;
            const run = PLAIN_RUN.exec(text)[0];
            out += run;
            i += run.length;
            if (i >= text.length) fail('unterminated string');
            const ch = text[i];
            const code = ch.charCodeAt(0);
            if (ch === '"') {
                i += 1;
                return out;
            }
            if (code < 0x20) fail('control character in a string');
            if (code >= 0xd800 && code <= 0xdfff) {
                // Text decoded from UTF-8 holds only paired surrogates.
                const next = text.charCodeAt(i + 1);
                if (code > 0xdbff || !(next >= 0xdc00 && next <= 0xdfff)) fail('unpaired surrogate');
                out += text.slice(i, i + 2);
                i += 2;
                continue;
            }
            // A backslash.
            const esc = text[i + 1];
            i += 2;
            if (Object.hasOwn(SIMPLE_ESCAPES, esc)) {
                out += SIMPLE_ESCAPES[esc];
                continue;
            }
            if (esc !== 'u') fail('invalid escape');
            const unit = hex4();
            if (unit >= 0xdc00 && unit <= 0xdfff) fail('unpaired surrogate escape');
            if (unit >= 0xd800 && unit <= 0xdbff) {
                if (text[i] !== '\\' || text[i + 1] !== 'u') fail('unpaired surrogate escape');
                i += 2;
                const low = hex4();
                if (!(low >= 0xdc00 && low <= 0xdfff)) fail('unpaired surrogate escape');
                out += String.fromCharCode(unit, low);
                continue;
            }
            out += String.fromCharCode(unit);
        }
    };
    const number = () => {
        NUMBER.lastIndex = i;
        const m = NUMBER.exec(text);
        if (!m) fail('invalid number');
        i += m[0].length;
        if (!Number.isFinite(Number(m[0]))) fail('number out of range');
        return new JsonNumber(m[0]);
    };
    const nest = () => {
        depthLeft -= 1;
        if (depthLeft === 0) fail('nesting of 128 levels');
    };
    const value = () => {
        ws();
        const ch = text[i];
        if (ch === '{') {
            nest();
            i += 1;
            const obj = {};
            ws();
            if (text[i] === '}') {
                i += 1;
                depthLeft += 1;
                return obj;
            }
            for (;;) {
                ws();
                if (text[i] !== '"') fail('expected a member name');
                const key = string();
                ws();
                if (text[i] !== ':') fail('expected :');
                i += 1;
                const v = value();
                // defineProperty, so a member named __proto__ is an own member.
                Object.defineProperty(obj, key, { value: v, writable: true, enumerable: true, configurable: true });
                ws();
                if (text[i] === ',') {
                    i += 1;
                    continue;
                }
                if (text[i] === '}') {
                    i += 1;
                    depthLeft += 1;
                    return obj;
                }
                fail('expected , or }');
            }
        }
        if (ch === '[') {
            nest();
            i += 1;
            const arr = [];
            ws();
            if (text[i] === ']') {
                i += 1;
                depthLeft += 1;
                return arr;
            }
            for (;;) {
                arr.push(value());
                ws();
                if (text[i] === ',') {
                    i += 1;
                    continue;
                }
                if (text[i] === ']') {
                    i += 1;
                    depthLeft += 1;
                    return arr;
                }
                fail('expected , or ]');
            }
        }
        if (ch === '"') return string();
        if (ch === '-' || (ch >= '0' && ch <= '9')) return number();
        for (const [word, v] of [['true', true], ['false', false], ['null', null]]) {
            if (text.startsWith(word, i)) {
                i += word.length;
                return v;
            }
        }
        return fail('expected a value');
    };
    const result = value();
    ws();
    if (i !== text.length) fail('text after the value');
    return result;
}

/** Read a UTF-8 file as text, refusing bytes that are not UTF-8 and keeping a byte order mark. */
async function readText(path) {
    return new TextDecoder('utf-8', { fatal: true, ignoreBOM: true }).decode(await readFile(path));
}

function isObject(v) {
    return v !== null && typeof v === 'object' && !Array.isArray(v) && !(v instanceof JsonNumber);
}

/** A parsed value with every JsonNumber replaced by its Number. */
function plain(v) {
    if (v instanceof JsonNumber) return Number(v.text);
    if (Array.isArray(v)) return v.map(plain);
    if (isObject(v)) {
        const out = {};
        for (const k of Object.keys(v)) {
            Object.defineProperty(out, k, { value: plain(v[k]), writable: true, enumerable: true, configurable: true });
        }
        return out;
    }
    return v;
}

// ---- the case schema -------------------------------------------------------
// The rules are the native runner's (`read_case` in `../tests/parity.rs`); the
// rows of `./runner-selftest.json` hold the two to the same outcomes.

/** A case that does not follow the schema. */
class Reject extends Error {}

const CASE_MEMBERS = ['name', 'op', 'body', 'rawBody', 'error', 'checks', 'save', 'onWasm'];
const ON_WASM_MEMBERS = ['error', 'checks', 'reason'];
const ROW_MEMBERS = ['name', 'case', 'caseText', 'saved', 'response', 'bytes', 'expect'];
const MAX_SAFE_INTEGER = 9007199254740991n; // 2^53 - 1

/** Whether `path` is a JSON Pointer: empty, or `/`-separated tokens whose every `~` is `~0` or `~1`. */
function isJsonPointer(path) {
    return typeof path === 'string' && (path === '' || path.startsWith('/')) && !/~(?![01])/.test(path);
}

/** Whether `key` can name a saved value. */
function isSaveName(key) {
    return /^[A-Za-z_][A-Za-z0-9_]*$/.test(key);
}

/**
 * Split a body string into `{ text }` and `{ name }` pieces (see `pieces` in
 * the native runner). Every `${` begins a reference: `${`, a name, then `}`.
 * Throws Reject for a `${` that does not begin one.
 */
function pieces(s) {
    const out = [];
    let rest = s;
    for (;;) {
        const start = rest.indexOf('${');
        if (start < 0) break;
        if (start > 0) out.push({ text: rest.slice(0, start) });
        const after = rest.slice(start + 2);
        const end = after.indexOf('}');
        const name = end < 0 ? undefined : after.slice(0, end);
        if (name === undefined || !isSaveName(name)) {
            throw new Reject(`the body string ${JSON.stringify(s)} has a \${ that does not begin a reference \${name} to a saved value`);
        }
        out.push({ name });
        rest = after.slice(end + 1);
    }
    if (rest !== '') out.push({ text: rest });
    return out;
}

/** Refuse a number that is not an integer of at most 2^53 - 1 in magnitude, written without fraction or exponent. */
function checkNumbers(v) {
    if (v instanceof JsonNumber) {
        const ok = /^-?(?:0|[1-9][0-9]*)$/.test(v.text) && v.text !== '-0';
        const magnitude = ok ? BigInt(v.text.replace(/^-/, '')) : 0n;
        if (!ok || magnitude > MAX_SAFE_INTEGER) {
            throw new Reject(`the number ${v.text} is not an integer between -(2^53 - 1) and 2^53 - 1 written without a fraction or exponent`);
        }
    } else if (Array.isArray(v)) {
        v.forEach(checkNumbers);
    } else if (isObject(v)) {
        Object.values(v).forEach(checkNumbers);
    }
}

/** Refuse a body string whose `${` does not begin a reference, or whose reference names a value no earlier case saves. */
function checkReferences(v, saved) {
    if (typeof v === 'string') {
        for (const piece of pieces(v)) {
            if (piece.name !== undefined && !saved.has(piece.name)) {
                throw new Reject(`the body string ${JSON.stringify(v)} names ${JSON.stringify(piece.name)}, a value no earlier case saves`);
            }
        }
    } else if (Array.isArray(v)) {
        v.forEach((x) => checkReferences(x, saved));
    } else if (isObject(v)) {
        Object.values(v).forEach((x) => checkReferences(x, saved));
    }
}

function nonEmptyString(v) {
    return typeof v === 'string' && v !== '';
}

/** Read one check into `{ path, kind, value }`. */
function readCheck(check) {
    const bad = () => new Reject(`check ${JSON.stringify(plain(check))} must have a path that is a JSON Pointer and exactly one of equals, contains (a string), absent: true or present: true, and no other member`);
    if (!isObject(check) || !isJsonPointer(check.path)) throw bad();
    const keys = Object.keys(check);
    if (keys.length !== 2) throw bad();
    const kind = keys.find((k) => k !== 'path');
    const value = check[kind];
    if (kind === 'equals') return { path: check.path, kind, value: plain(value) };
    if (kind === 'contains' && typeof value === 'string') return { path: check.path, kind, value };
    if ((kind === 'absent' || kind === 'present') && value === true) return { path: check.path, kind, value };
    throw bad();
}

/** Read an expectation from its `error` and `checks` members (see `read_expectation`). */
function readExpectation(error, checks) {
    if (!(error === null || nonEmptyString(error))) throw new Reject('error must be null or a non-empty string');
    if (!Array.isArray(checks)) throw new Reject('checks must be an array');
    const list = checks.map(readCheck);
    const checksSomething = error === null
        ? list.some((c) => c.path !== '/__type')
        : list.some((c) => c.path === '/message' && c.kind !== 'present');
    if (!checksSomething) {
        throw new Reject(error === null
            ? 'a success case needs a check on the response body'
            : 'an error case needs an equals, contains or absent check on /message');
    }
    return { error, checks: list };
}

/** Read one case (see `read_case`); `saved` holds the names earlier cases save. */
function readCase(c, saved) {
    if (!isObject(c)) throw new Reject('a case must be an object');
    if (!nonEmptyString(c.name)) throw new Reject('name must be a non-empty string');
    const within = (e) => {
        if (e instanceof Reject) return new Reject(`case '${c.name}': ${e.message}`);
        return e;
    };
    try {
        const unknown = Object.keys(c).find((k) => !CASE_MEMBERS.includes(k));
        if (unknown !== undefined) throw new Reject(`unknown member ${JSON.stringify(unknown)}`);
        checkNumbers(c);
        if (!nonEmptyString(c.op)) throw new Reject('op must be a non-empty string');
        const hasBody = Object.hasOwn(c, 'body');
        const hasRaw = Object.hasOwn(c, 'rawBody');
        let request;
        if (hasBody && !hasRaw && isObject(c.body)) {
            checkReferences(c.body, saved);
            request = { body: plain(c.body) };
        } else if (hasRaw && !hasBody && typeof c.rawBody === 'string') {
            request = { rawBody: c.rawBody };
        } else {
            throw new Reject('a case needs exactly one of body (an object) and rawBody (a string)');
        }
        const expect = readExpectation(Object.hasOwn(c, 'error') ? c.error : undefined, c.checks);
        let save = [];
        if (Object.hasOwn(c, 'save')) {
            if (!isObject(c.save)) throw new Reject(`save ${JSON.stringify(plain(c.save))} is not an object`);
            save = Object.keys(c.save).map((key) => {
                const path = c.save[key];
                if (!isSaveName(key) || !isJsonPointer(path)) {
                    throw new Reject(`save member ${JSON.stringify(key)}: ${JSON.stringify(plain(path))} must be a JSON Pointer string under a name of ASCII letters, digits and _ that does not start with a digit`);
                }
                return [key, path];
            });
        }
        let onWasm;
        if (Object.hasOwn(c, 'onWasm')) {
            const w = c.onWasm;
            if (!isObject(w)) throw new Reject('onWasm must be an object with error, checks and reason');
            const unknownW = Object.keys(w).find((k) => !ON_WASM_MEMBERS.includes(k));
            if (unknownW !== undefined) throw new Reject(`onWasm has an unknown member ${JSON.stringify(unknownW)}`);
            if (!nonEmptyString(w.reason)) throw new Reject('onWasm needs a reason, a non-empty string');
            try {
                onWasm = readExpectation(Object.hasOwn(w, 'error') ? w.error : undefined, w.checks);
            } catch (e) {
                if (e instanceof Reject) throw new Reject(`onWasm: ${e.message}`);
                throw e;
            }
            if (Object.hasOwn(c, 'save')) throw new Reject('a case with onWasm cannot save values, as its response differs by build');
        }
        return { name: c.name, op: c.op, request, expect, onWasm, save };
    } catch (e) {
        throw within(e);
    }
}

/** Read the whole corpus (see `read_corpus`). */
function readCorpus(corpus) {
    if (!isObject(corpus)) throw new Reject('the corpus must be an object');
    const unknown = Object.keys(corpus).find((k) => k !== '_comment' && k !== 'cases');
    if (unknown !== undefined) throw new Reject(`unknown corpus member ${JSON.stringify(unknown)}`);
    if (Object.hasOwn(corpus, '_comment')
        && !(Array.isArray(corpus._comment) && corpus._comment.every((l) => typeof l === 'string'))) {
        throw new Reject('_comment must be an array of strings');
    }
    if (!Array.isArray(corpus.cases) || corpus.cases.length === 0) throw new Reject('cases must be a non-empty array');
    const saved = new Set();
    return corpus.cases.map((c, index) => {
        let read;
        try {
            read = readCase(c, saved);
        } catch (e) {
            if (e instanceof Reject) throw new Reject(`case ${index}: ${e.message}`);
            throw e;
        }
        for (const [key] of read.save) saved.add(key);
        return read;
    });
}

// ---- the request a case sends ---------------------------------------------

/** Order strings by their code points, as Rust orders `str`. */
function compareCodePoints(a, b) {
    const x = [...a];
    const y = [...b];
    for (let k = 0; k < Math.min(x.length, y.length); k += 1) {
        const d = x[k].codePointAt(0) - y[k].codePointAt(0);
        if (d !== 0) return d;
    }
    return x.length - y.length;
}

const SHORT_ESCAPES = { '"': '\\"', '\\': '\\\\', '\b': '\\b', '\f': '\\f', '\n': '\\n', '\r': '\\r', '\t': '\\t' };

/** Write `s` as a JSON string, as `write_json_string` does. */
function jsonString(s) {
    let out = '"';
    for (const ch of s) {
        if (Object.hasOwn(SHORT_ESCAPES, ch)) out += SHORT_ESCAPES[ch];
        else if (ch.codePointAt(0) < 0x20) out += `\\u${ch.codePointAt(0).toString(16).padStart(4, '0')}`;
        else out += ch;
    }
    return `${out}"`;
}

/** Write a value as canonical JSON, as `canonical_json` does. */
function canonicalJson(v) {
    if (v === null) return 'null';
    if (v === true) return 'true';
    if (v === false) return 'false';
    if (typeof v === 'number') return String(v);
    if (typeof v === 'string') return jsonString(v);
    if (Array.isArray(v)) return `[${v.map(canonicalJson).join(',')}]`;
    const names = Object.keys(v).sort(compareCodePoints);
    return `{${names.map((k) => `${jsonString(k)}:${canonicalJson(v[k])}`).join(',')}}`;
}

/** Replace every reference `${name}` in the strings of `v` with the value saved under `name`. */
function substitute(v, saved) {
    if (typeof v === 'string') {
        return pieces(v).map((piece) => {
            if (piece.name === undefined) return piece.text;
            if (!saved.has(piece.name)) throw new Error(`no value was saved under ${JSON.stringify(piece.name)}`);
            return saved.get(piece.name);
        }).join('');
    }
    if (Array.isArray(v)) return v.map((x) => substitute(x, saved));
    if (isObject(v)) {
        const out = {};
        for (const k of Object.keys(v)) {
            Object.defineProperty(out, k, { value: substitute(v[k], saved), writable: true, enumerable: true, configurable: true });
        }
        return out;
    }
    return v;
}

/** The request text a case submits (see `request_text`). */
function requestText(c, saved) {
    return c.request.rawBody !== undefined ? c.request.rawBody : canonicalJson(substitute(c.request.body, saved));
}

// ---- judging a response ----------------------------------------------------

/**
 * Resolve a JSON Pointer (RFC 6901), returning undefined if absent. Follows
 * serde_json's `Value::pointer`, which the native runner uses: an array index
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

/** JSON equality: object members regardless of order, array elements in order, numbers by value. */
function jsonEqual(a, b) {
    if (Array.isArray(a) || Array.isArray(b)) {
        return Array.isArray(a) && Array.isArray(b)
            && a.length === b.length && a.every((x, k) => jsonEqual(x, b[k]));
    }
    if (isObject(a) || isObject(b)) {
        if (!isObject(a) || !isObject(b)) return false;
        const keys = Object.keys(a);
        return keys.length === Object.keys(b).length
            && keys.every((k) => Object.hasOwn(b, k) && jsonEqual(a[k], b[k]));
    }
    return a === b;
}

/** Judge a response against an expectation: undefined when it holds, else what does not. */
function judge(expect, resp) {
    const got = errorShortName(resp);
    if (expect.error !== null && got !== expect.error) {
        return `expected error ${expect.error}, got response ${JSON.stringify(resp)}`;
    }
    if (expect.error === null && got !== undefined) {
        return `expected success, got error document ${JSON.stringify(resp)}`;
    }
    for (const { path, kind, value } of expect.checks) {
        const val = pointer(resp, path);
        let holds;
        if (kind === 'absent') holds = val === undefined;
        else if (kind === 'present') holds = val !== undefined;
        else if (kind === 'contains') holds = typeof val === 'string' && val.includes(value);
        else holds = val !== undefined && jsonEqual(val, value);
        if (!holds) return `at ${path} expected ${kind} ${JSON.stringify(value)}, response ${JSON.stringify(resp)}`;
    }
    return undefined;
}

/** Record the strings a case's `save` names: undefined, or what is not a string. */
function save(c, resp, saved) {
    for (const [key, path] of c.save) {
        const val = pointer(resp, path);
        if (typeof val !== 'string') return `save path ${path} is not a string in ${JSON.stringify(resp)}`;
        saved.set(key, val);
    }
    return undefined;
}

// ---- judge the shared runner self-test rows before the corpus --------------

/** A row's outcome `[outcome, why]`; throws an Error when the row itself is malformed. */
function rowOutcome(row) {
    if (!isObject(row)) throw new Error('a row must be an object');
    const unknown = Object.keys(row).find((k) => !ROW_MEMBERS.includes(k));
    if (unknown !== undefined) throw new Error(`unknown row member ${JSON.stringify(unknown)}`);
    let raw;
    if (Object.hasOwn(row, 'case') && !Object.hasOwn(row, 'caseText')) {
        raw = row.case;
    } else if (Object.hasOwn(row, 'caseText') && !Object.hasOwn(row, 'case') && typeof row.caseText === 'string') {
        try {
            raw = parseJson(row.caseText);
        } catch (e) {
            if (e instanceof ParseError) return ['reject', `caseText does not parse: ${e.message}`];
            throw e;
        }
    } else {
        throw new Error('a row needs exactly one of case and caseText (a string)');
    }
    const saved = new Map();
    if (Object.hasOwn(row, 'saved')) {
        if (!isObject(row.saved)) throw new Error('saved must be an object');
        for (const k of Object.keys(row.saved)) {
            if (typeof row.saved[k] !== 'string') throw new Error(`saved value ${JSON.stringify(k)} is not a string`);
            saved.set(k, row.saved[k]);
        }
    }
    let c;
    try {
        c = readCase(raw, new Set(saved.keys()));
    } catch (e) {
        if (e instanceof Reject) return ['reject', e.message];
        throw e;
    }
    if (Object.hasOwn(row, 'bytes')) {
        if (typeof row.bytes !== 'string') throw new Error('bytes must be a string');
        const got = requestText(c, saved);
        if (got !== row.bytes) return ['fail', `the request is ${got}`];
    }
    if (Object.hasOwn(row, 'response')) {
        const response = plain(row.response);
        const why = judge(c.expect, response) ?? save(c, response, new Map(saved));
        if (why !== undefined) return ['fail', why];
    }
    return ['pass', ''];
}

const selftest = parseJson(await readText(SELFTEST_PATH));
if (!isObject(selftest) || Object.keys(selftest).some((k) => k !== '_comment' && k !== 'rows')
    || !Array.isArray(selftest.rows) || selftest.rows.length === 0) {
    console.log('RESULT: FAIL (runner-selftest.json must be an object with _comment and a non-empty rows array)');
    process.exit(1);
}
let selftestDisagreements = 0;
selftest.rows.forEach((row, index) => {
    const label = isObject(row) && typeof row.name === 'string' ? row.name : '<unnamed>';
    let outcome;
    try {
        outcome = rowOutcome(row);
    } catch (e) {
        selftestDisagreements += 1;
        console.error(`  \u2717 row ${index} '${label}' is malformed: ${e.message}`);
        return;
    }
    const [got, why] = outcome;
    if (got !== row.expect) {
        selftestDisagreements += 1;
        console.error(`  \u2717 row ${index} '${label}': expected ${JSON.stringify(row.expect)}, got ${got} (${why})`);
    }
});
if (selftestDisagreements !== 0) {
    console.log(`RESULT: FAIL (${selftestDisagreements} of ${selftest.rows.length} runner self-test rows judged differently than expected)`);
    process.exit(1);
}

// ---- read the corpus --------------------------------------------------------

let cases;
try {
    cases = readCorpus(parseJson(await readText(CORPUS_PATH)));
} catch (e) {
    if (!(e instanceof Reject || e instanceof ParseError)) throw e;
    console.log(`RESULT: FAIL (corpus.json is malformed: ${e.message})`);
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

let failures = 0;
let corpusChecks = 0;
let extraChecks = 0;
function fail(msg) {
    failures += 1;
    console.error(`  \u2717 ${msg}`);
}

const db = await mod.ExtendDb.create();
const saved = new Map();
for (const c of cases) {
    const expect = c.onWasm ?? c.expect;
    corpusChecks += expect.checks.length;
    let request;
    try {
        request = requestText(c, saved);
    } catch (e) {
        fail(`case '${c.name}': ${e.message}`);
        continue;
    }
    let resp;
    try {
        resp = JSON.parse(await db.submit(c.op, request));
    } catch (e) {
        fail(`case '${c.name}': submit threw (likely a wasm trap): ${e.message}`);
        continue;
    }
    const why = judge(expect, resp) ?? save(c, resp, saved);
    if (why !== undefined) fail(`case '${c.name}': ${why}`);
}

// ---- arguments that are not strings ---------------------------------------
// JavaScript can pass any value to `submit`. Each call below must resolve with
// a ValidationException error document naming the argument, and the instance
// must keep serving afterwards. These live here rather than in corpus.json
// because the native runner passes `&str` and cannot express them.

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
    extraChecks += 1;
    let resp;
    try {
        resp = JSON.parse(await db.submit(...args));
    } catch (e) {
        fail(`submit with ${label}: threw ${e.name}: ${e.message}`);
        continue;
    }
    const why = judge({
        error: 'ValidationException',
        checks: [{ path: '/message', kind: 'equals', value: `${argument} must be a string` }],
    }, resp);
    if (why !== undefined) fail(`submit with ${label}: ${why}`);
}

extraChecks += 1;
try {
    const after = JSON.parse(await db.submit('ListTables', '{}'));
    const why = judge({ error: null, checks: [{ path: '/TableNames', kind: 'present', value: true }] }, after);
    if (why !== undefined) fail(`ListTables after the arguments that are not strings: ${why}`);
} catch (e) {
    fail(`ListTables after the arguments that are not strings: threw ${e.name}: ${e.message}`);
}

// ---- summary ---------------------------------------------------------------

console.log('');
const counts = `${cases.length} cases, ${corpusChecks} corpus checks, ${extraChecks} checks of submit's JavaScript boundary`;
if (failures === 0) {
    console.log(`RESULT: PASS (${counts})`);
    process.exit(0);
} else {
    console.log(`RESULT: FAIL (${failures} failure(s) over ${counts})`);
    process.exit(1);
}
