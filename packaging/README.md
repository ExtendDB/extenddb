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
npm install extenddb
npx extenddb start            # file-backed at ./.extenddb/data.db
npx extenddb start --memory   # ephemeral
```

```js
const { start } = require("extenddb");
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

The launcher versions independently of the server, under its own tag
namespace: `npm-vMAJOR.MINOR.PATCH`. The version in `npm/package.json` is the
source of truth; the tag must match it, and the publish workflow refuses
anything else.

The binary every platform package ships is the slim dev-mode build:

```
cargo build --release --locked --no-default-features --features sqlite,dev-mode -p extenddb
```

Flow (mirrors the container release: candidate first, promotion is a
deliberate second step):

1. Land the release state on `main` with the right version in
   `npm/package.json`, then tag that commit `npm-vX.Y.Z` and push the tag.
2. Dispatch the `npm-publish` workflow from `main` with the tag as input.
   The gate validates shape, existence, ancestry against `main`, and the
   version match. Five native runners build, smoke test, and run the full
   launcher suite against their own binary. Only after all five pass does the
   reviewer-gated `npm` environment release the token, and everything is
   published with `--provenance` under the `candidate` dist-tag. Nothing
   reaches `latest`.
3. Verify the candidate from a clean machine:
   `npm install extenddb@candidate` and run a smoke script in both storage
   modes.
4. Promote, one dist-tag move per package:

   ```
   npm dist-tag add extenddb@X.Y.Z latest
   npm dist-tag add @extenddb/linux-x64@X.Y.Z latest        # and the other four
   ```

One-time registry and repo setup, in order: create the npm org `extenddb`
(both the unscoped name and the scope were unclaimed as of 2026-08-12); create
a granular automation token scoped to the `extenddb` package and `@extenddb`
scope with publish permission only; create the GitHub environment `npm` with
deployment branch rule `main`, required reviewers, and the token as
`NPM_TOKEN`.
