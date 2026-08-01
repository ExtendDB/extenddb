# ExtendDB launchers

Thin per-ecosystem launchers for the ExtendDB **dev server** (the
`sqlite-memory,dev-mode` build): a DynamoDB-compatible database for local
development and CI. Each launcher spawns the dev binary zero-config
(in-memory, loopback, seeded dev credential), waits for `/health`, and hands
back an endpoint plus credentials that any AWS SDK accepts unmodified.

| Ecosystem | Package | Usage |
|-----------|---------|-------|
| npm       | `packaging/npm` | `const eb = await require("extenddb").start()` or `npx extenddb start` |
| pip       | `packaging/python` | `with extenddb_launcher.start() as eb:` or `extenddb start` |

Both support `--db <path>` / `dbPath` / `db_path` for file-backed persistence
(same binary, one setting), fixed or ephemeral ports, and clean shutdown.

## Binary resolution

Explicit argument → `EXTENDDB_BINARY` env var → bundled platform binary →
`PATH`. The platform-binary bundling (npm `optionalDependencies`, pip platform
wheels) is wired up by the release pipeline; until then, point
`EXTENDDB_BINARY` at a build made with:

```
cargo build --release -p extenddb --no-default-features \
    --features sqlite-memory,dev-mode
```

## Tests

Live tests spawn the real binary:

```
export EXTENDDB_BINARY=$PWD/target-devmode/release/extenddb
node packaging/npm/test/launcher.test.js
python3 packaging/python/tests/test_launcher.py
```
