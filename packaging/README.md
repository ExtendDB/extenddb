# ExtendDB launchers

Package-manager launchers for the ExtendDB dev server: install from your
ecosystem's package manager, get a DynamoDB-compatible endpoint on loopback in
one call, point any AWS SDK at it.

npm is the first ecosystem; pip and Maven/Gradle follow. Every launcher
implements the SAME contract, defined here so the wrappers cannot drift.

## The launcher contract

Every launcher, in every language:

- Spawns the platform's prebuilt `extenddb` dev binary (dev-mode build:
  loopback-only, plain HTTP, seeded with AWS's documented example credential).
  Resolution order: explicit option, `EXTENDDB_BINARY` env var, the platform
  package installed alongside the wrapper, then `PATH`.
- Configures the server exclusively through environment variables
  (`EXTENDDB__SERVER__PORT`, `EXTENDDB__STORAGE__SQLITE__PATH`), from a scratch
  working directory, so a config file in the project cannot leak in.
- Storage is **file-based by default**: `./.extenddb/data.db` relative to the
  caller's working directory, created on demand. Tables survive restarts
  without anyone asking. The directory name is part of the contract.
- **Memory mode is opt-in** (`--memory` on the CLI, the ecosystem's idiomatic
  flag in the API): ephemeral, writes nothing to disk, for tests and CI.
- Naming a persistence path while asking for memory mode is an error, never a
  silent choice.
- Waits for `/health` to return 200 before handing back the endpoint, and
  returns: endpoint URL, port (ephemeral by default), region (`us-east-1`),
  the seeded credentials, and a stop handle that terminates the child
  (SIGTERM, then SIGKILL after 5s).
- Does not outlive the caller: best-effort kill on process exit.

## npm

```bash
npm install @extenddb/dev
npx extenddb start            # file-backed at ./.extenddb/data.db
npx extenddb start --memory   # ephemeral
```

```js
const { start } = require("@extenddb/dev");
const db = await start();                 // file-backed default
// const db = await start({ memory: true });   ephemeral
// const db = await start({ dbPath: "ci.db" }); explicit file
const client = new DynamoDBClient({
  endpoint: db.endpoint,
  region: db.region,
  credentials: db.credentials,
});
// ...
await db.stop();
```

Platform binaries ship as `optionalDependencies`
(`@extenddb/<platform>-<arch>`), so `npm install` is copy-only: no compiler,
no node-gyp, no postinstall downloads. On an unsupported platform the wrapper
falls back to `EXTENDDB_BINARY` or `PATH`.

## Testing

`test/launcher.test.js` runs against a real dev binary:

```bash
EXTENDDB_BINARY=/path/to/extenddb node test/launcher.test.js
```

It proves the contract's load-bearing clauses: file mode is the default and
persists across two server lifetimes, memory mode creates no files and is
genuinely ephemeral, and contradictory options are refused.

## Releasing

The launcher versions **with** the server, off the same `vMAJOR.MINOR.PATCH`
tag the container release uses. The workspace version in the root `Cargo.toml`
is the source of truth; the tag must match it, `npm/package.json` must match
it too, and the publish workflow refuses anything else.

The alignment is deliberate. The five platform packages ship the server
binary, so a server release necessarily republishes all six, and the launcher
pins its `optionalDependencies` to its own version. An independent launcher
number could therefore only ever answer "which launcher", never the question
users actually ask, "which server is inside this". It is the same convention
esbuild and biome use, whose launchers pin their platform packages at the tool
version. The accepted cost: a launcher-only fix rides the next server release
rather than shipping under its own number.

The binary every platform package ships is the slim dev-mode build:

```
cargo build --release --locked --no-default-features --features sqlite,dev-mode -p extenddb
```

Flow (mirrors the container release: candidate first, promotion is a
deliberate second step, each a separately approved workflow run):

1. Land the release state on `main` with the workspace version bumped and
   `npm/package.json` bumped to match, then tag that commit `vX.Y.Z` and push
   the tag. This is the same tag the container release consumes; the two
   publish workflows are dispatched independently against it.
2. Dispatch the `npm-publish` workflow from `main` with the tag and
   `action: candidate`. The gate validates shape, existence, ancestry against
   `main`, and the version match. Five native runners build, smoke test, and
   run the full launcher suite against their own binary. Only after all five
   pass does the reviewer-gated `npm` environment release the OIDC credential,
   and everything is published with provenance under the `candidate`
   dist-tag. Nothing reaches `latest`. The run then installs the candidate
   back from the public registry on all five platforms and requires both
   packages at `X.Y.Z`, verified registry signatures and provenance
   attestations on both, the binary reporting `X.Y.Z`, the launcher
   resolving its platform package on its own, and the launcher suite passing
   in both storage modes (`.github/workflows/npm-verify-matrix.yml`).
3. Dispatch `npm-publish` again with the same tag and `action: promote`. The
   gate additionally requires every package of the release to be on the
   registry at `X.Y.Z` with a provenance attestation whose signed statement
   names this repository and `npm-publish.yml`, and refuses to move `latest`
   backwards; the five verify legs run again; then, after a second reviewer
   approval, the run re-reads every `latest`, adds and removes a transient
   tag on every package (a write, which proves the OIDC credential and the
   dist-tag permission on each entry before `latest` moves), moves `latest`
   to `X.Y.Z` on each platform package and the launcher with `npm dist-tag
   add` over the same OIDC credential, and reads every tag back from the
   registry. No maintainer login or token is involved. Re-running `promote`
   for the version `latest` already holds exercises the whole path and
   leaves every durable tag as it was.

Anyone with dispatch rights dispatches either step; a reviewer of the `npm`
environment other than the dispatcher approves each run. The release does
not depend on one maintainer's npm account.

One-time registry and repo setup, in order: create the npm org `extenddb`.
The launcher ships as the scoped `@extenddb/dev` rather than an unscoped
`extenddb`, because npm's package-name similarity filter refuses the latter
against the long-established `extend` package. Publish each of the six
packages once as a `0.0.0` placeholder under the `placeholder` dist-tag from a
maintainer machine (npm needs the package to exist before a trusted publisher
can be configured). Create the GitHub environment `npm` with deployment branch
rule `main` and required reviewers; it holds no secrets. Then on npmjs.com,
for each of the six packages, add a trusted publisher for GitHub Actions
(organization `ExtendDB`, repository `extenddb`, workflow `npm-publish.yml`,
environment `npm`) with `npm publish` and "Allow npm dist-tag" both allowed;
the promote step fails naming that setting if it is off.

## Known limits

- A hard-killed caller (SIGKILL, OOM) cannot run its exit handler, so the
  spawned server survives it, holding its port and storage. `stop()` and
  normal process exit shut it down; after a hard kill, kill the `extenddb`
  process yourself.
- The prebuilt Linux binaries are glibc builds. On musl systems (Alpine,
  `node:alpine`) they fail to execute even though the file exists; build a
  musl binary and point `EXTENDDB_BINARY` at it.
- Two servers cannot share one database file: a second `start()` on the same
  path fails at startup with a lock error. Use distinct `dbPath`s or memory
  mode for parallel test workers.
