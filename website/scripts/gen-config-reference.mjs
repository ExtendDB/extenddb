#!/usr/bin/env node
// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

// Generates docs/configuration.md from extenddb.sample.toml.
//
// The sample is the one hand-written description of every configuration key.
// Its convention, which the Rust tests in crates/config pin to the structs:
//
//   - The first comment block of the file is the file header; it is not part
//     of the page.
//   - `[section]` (or `# [section]`) starts a section. Keys before the first
//     header are the top-level keys.
//   - A comment block directly under a header, ended by a blank line, is the
//     section's introduction and becomes prose.
//   - `# key = value` is a key line. The comment lines directly above it are
//     its description. A description containing "no default" marks a key whose
//     value is an example: the Default column shows "none", the trailing
//     "No default." sentence is dropped and the value is appended to the
//     description as an example. A value may not carry a trailing ` #`
//     comment outside a string literal; the value is the whole rest of the
//     line.
//   - Markdown links and code spans in comments pass through to the page.
//
// Usage: node scripts/gen-config-reference.mjs [--check]
//   --check  regenerate to a string and exit 1 with a unified diff when it
//            differs from the committed docs/configuration.md.

import {readFileSync, writeFileSync} from 'node:fs';
import {dirname, resolve} from 'node:path';
import {fileURLToPath} from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');
const samplePath = resolve(root, 'extenddb.sample.toml');
const outputPath = resolve(root, 'docs', 'configuration.md');

const KEY_LINE = /^# ([A-Za-z_][A-Za-z0-9_]*) = (.+)$/;
const HEADER_LINE = /^(?:# )?\[([A-Za-z0-9_.]+)\]$/;
const COMMENT_LINE = /^#(?: (.*))?$/;

/** Parse the sample into [{name, intro: string[], keys: [{name, value, description, hasDefault}]}]. */
function parseSample(text) {
  const lines = text.split('\n');
  let i = 0;
  // Skip the file header: the first comment block and the blank line after it.
  while (i < lines.length && lines[i].startsWith('#')) i++;
  while (i < lines.length && lines[i].trim() === '') i++;

  const sections = [];
  let section = {name: null, intro: [], keys: []};
  sections.push(section);
  let pending = [];
  let expectIntro = true;

  for (; i < lines.length; i++) {
    const line = lines[i];
    const lineNo = i + 1;
    const header = line.match(HEADER_LINE);
    if (header) {
      if (pending.length) throw new Error(`${samplePath}:${lineNo}: comment block before [${header[1]}] is attached to nothing`);
      section = {name: header[1], intro: [], keys: []};
      sections.push(section);
      expectIntro = true;
      continue;
    }
    const key = line.match(KEY_LINE);
    if (key) {
      const value = key[2].trim();
      if (hasInlineComment(value)) throw new Error(`${samplePath}:${lineNo}: key ${key[1]} has a trailing comment on its value line; put it in the description above`);
      let description = joinComment(pending);
      if (!description) throw new Error(`${samplePath}:${lineNo}: key ${key[1]} has no description`);
      const hasDefault = !/\bno default\b/i.test(description);
      if (!hasDefault) {
        description = description.replace(/[;,]?\s*\bNo default( of its own)?\.\s*$/i, '');
        if (!/[.!?]$/.test(description)) description += '.';
        description += ` Example: \`${key[1]} = ${value}\``;
      }
      section.keys.push({name: key[1], value, description, hasDefault});
      pending = [];
      expectIntro = false;
      continue;
    }
    if (line.trim() === '') {
      if (pending.length) {
        if (!expectIntro) throw new Error(`${samplePath}:${lineNo}: comment block is attached to nothing (not a section intro, not above a key)`);
        section.intro = pending;
        expectIntro = false;
        pending = [];
      }
      continue;
    }
    const comment = line.match(COMMENT_LINE);
    if (!comment) throw new Error(`${samplePath}:${lineNo}: unexpected line: ${line}`);
    pending.push(comment[1] ?? '');
  }
  if (pending.length) throw new Error(`${samplePath}: trailing comment block is attached to nothing`);
  return sections;
}

/** True when `value` has a ` #` outside a double-quoted string (a TOML inline comment). */
function hasInlineComment(value) {
  let inString = false;
  for (let i = 0; i < value.length; i++) {
    const c = value[i];
    if (inString) {
      if (c === '\\') i++;
      else if (c === '"') inString = false;
    } else if (c === '"') inString = true;
    else if (c === '#' && (i === 0 || value[i - 1] === ' ')) return true;
  }
  return false;
}

/** Join comment lines into one string; an empty comment line separates paragraphs. */
function joinComment(commentLines) {
  return commentLines
    .join('\n')
    .split(/\n\s*\n/)
    .map((p) => p.replace(/\s+/g, ' ').trim())
    .filter(Boolean)
    .join('\n\n');
}

/** Escape MDX-significant characters outside code spans: angle brackets (JSX), braces (expressions), asterisks (emphasis). */
function escapeMdx(text) {
  return text
    .split('`')
    .map((part, i) => (i % 2 ? part : part.replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/[{}*]/g, (c) => `\\${c}`)))
    .join('`');
}

/** A table cell: one line, pipes escaped. */
function cell(text) {
  return escapeMdx(text.replace(/\n\n/g, ' ')).replace(/\|/g, '\\|');
}

/** Section introduction prose. */
function prose(text) {
  return escapeMdx(text);
}

function render(sections) {
  const out = [];
  out.push('---');
  out.push('description: "Every key of extenddb.toml, its default and what it controls, generated from extenddb.sample.toml."');
  out.push('custom_edit_url: https://github.com/ExtendDB/extenddb/edit/main/extenddb.sample.toml');
  out.push('---');
  out.push('');
  out.push('# Configuration');
  out.push('');
  out.push('This page is generated from [`extenddb.sample.toml`](https://github.com/ExtendDB/extenddb/blob/main/extenddb.sample.toml) by `npm run gen:config` in `website/`; edit the sample, not this file. Tests in the `extenddb-config` crate and in each storage backend crate compare the sample with the configuration structs, so a key listed here exists in the code with the default shown.');
  out.push('');
  out.push('The server reads `extenddb.toml` (or the file named by `--config`) at startup. Scalar keys can also be set through an environment variable: the name is `EXTENDDB__` followed by the key path in upper case with `__` between levels, for example `EXTENDDB__SERVER__PORT=9000` or `EXTENDDB__STORAGE__POSTGRES__CONNECTION_STRING="postgresql://..."`. The list keys (`import.paths`, `export.paths`, `storage.cassandra.contact_points`) and the numeric keys of `[storage.sqlite]` and `[storage.cassandra]` are file only. Precedence, highest first: environment variables, the configuration file, built-in defaults; `extenddb serve --port` overrides `server.port` on top of all three.');
  out.push('');
  out.push('The keys below are static: a change takes effect at the next server start. Settings that change on a running server (log level, control plane delay, throttling at runtime) are set with `extenddb settings set` and are listed under [Runtime Settings](./manuals/05-admin-guide.md#runtime-settings) in the administration guide.');
  for (const section of sections) {
    out.push('');
    out.push(section.name === null ? '## Top-level keys' : `## \`[${section.name}]\``);
    if (section.intro.length) {
      out.push('');
      out.push(prose(joinComment(section.intro)));
    }
    if (!section.keys.length) continue;
    out.push('');
    out.push('| Key | Default | Description |');
    out.push('|-----|---------|-------------|');
    for (const key of section.keys) {
      // A long default (the PostgreSQL connection string) would set the column
      // width for the whole table; it moves into the description instead.
      let def = 'none';
      let desc = key.description;
      if (key.hasDefault && key.value.length > 40) {
        def = 'see description';
        desc += ` Default: \`${key.name} = ${key.value}\``;
      } else if (key.hasDefault) {
        def = `\`${key.value}\``;
      }
      out.push(`| \`${key.name}\` | ${def} | ${cell(desc)} |`);
    }
  }
  out.push('');
  return out.join('\n');
}

/** Unified diff of two texts (line based, longest common subsequence). */
function unifiedDiff(oldText, newText, oldName, newName) {
  const a = oldText.split('\n');
  const b = newText.split('\n');
  const n = a.length;
  const m = b.length;
  const lcs = Array.from({length: n + 1}, () => new Uint32Array(m + 1));
  for (let i = n - 1; i >= 0; i--) {
    for (let j = m - 1; j >= 0; j--) {
      lcs[i][j] = a[i] === b[j] ? lcs[i + 1][j + 1] + 1 : Math.max(lcs[i + 1][j], lcs[i][j + 1]);
    }
  }
  const ops = [];
  let i = 0;
  let j = 0;
  while (i < n || j < m) {
    if (i < n && j < m && a[i] === b[j]) {
      ops.push([' ', a[i]]);
      i++;
      j++;
    } else if (i < n && (j >= m || lcs[i + 1][j] >= lcs[i][j + 1])) {
      ops.push(['-', a[i]]);
      i++;
    } else {
      ops.push(['+', b[j]]);
      j++;
    }
  }
  const out = [`--- ${oldName}`, `+++ ${newName}`];
  const context = 3;
  let k = 0;
  while (k < ops.length) {
    if (ops[k][0] === ' ') {
      k++;
      continue;
    }
    const start = Math.max(0, k - context);
    let end = k;
    let lastChange = k;
    while (end < ops.length && end - lastChange <= context * 2) {
      if (ops[end][0] !== ' ') lastChange = end;
      end++;
    }
    end = Math.min(ops.length, lastChange + context + 1);
    const hunk = ops.slice(start, end);
    const oldStart = ops.slice(0, start).filter((o) => o[0] !== '+').length + 1;
    const newStart = ops.slice(0, start).filter((o) => o[0] !== '-').length + 1;
    const oldLen = hunk.filter((o) => o[0] !== '+').length;
    const newLen = hunk.filter((o) => o[0] !== '-').length;
    out.push(`@@ -${oldStart},${oldLen} +${newStart},${newLen} @@`);
    for (const [tag, text] of hunk) out.push(tag + text);
    k = end;
  }
  return out.join('\n');
}

function main() {
  const check = process.argv.includes('--check');
  const generated = render(parseSample(readFileSync(samplePath, 'utf8')));
  if (!check) {
    writeFileSync(outputPath, generated);
    console.log(`wrote ${outputPath}`);
    return;
  }
  let current = '';
  try {
    current = readFileSync(outputPath, 'utf8');
  } catch {
    // Missing file: the diff below shows the whole page as added.
  }
  if (current === generated) {
    console.log('docs/configuration.md matches extenddb.sample.toml');
    return;
  }
  console.error('docs/configuration.md is out of date; run `npm run gen:config` in website/ and commit the result.\n');
  console.error(unifiedDiff(current, generated, 'a/docs/configuration.md', 'b/docs/configuration.md'));
  process.exitCode = 1;
}

main();
