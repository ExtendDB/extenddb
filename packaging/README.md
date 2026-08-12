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
