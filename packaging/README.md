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
deliberate second step):

1. Land the release state on `main`. Bump the workspace version and the 13
   `=X.Y.Z` pins in the root `[workspace.dependencies]`, regenerate the four
   license notices, and bump `npm/package.json` to match. Tag that commit
   `vX.Y.Z` and push the tag. The container release uses the same tag. A
   maintainer dispatches the container and npm publish workflows
   independently against it. The crates.io workflow runs last, after
   `promote-image`. For a release that ships a catalog migration, the
   release commit also raises `RELEASED_THROUGH` to the last migration. That
   commit holds the checksum lines that the checksum test prints
   (CONTRIBUTING.md, "Adding a catalog migration"). The gate jobs of the
   release workflows run `devtools/check-released-through` on the tag and
   refuse it otherwise.
2. Dispatch the `npm-publish` workflow from `main` with the tag as input.
   The gate validates shape, existence, ancestry against `main`, and the
   version match. Five native runners build, smoke test, and run the full
   launcher suite against their own binary. Only after all five pass does the
   reviewer-gated `npm` environment release the token, and everything is
   published with `--provenance` under the `candidate` dist-tag. Nothing
   reaches `latest`.
3. Verify the candidate from a clean machine:
   `npm install @extenddb/dev@candidate` and run a smoke script in both storage
   modes.
4. Promote, one dist-tag move per package:

   ```
   npm dist-tag add @extenddb/dev@X.Y.Z latest
   npm dist-tag add @extenddb/linux-x64@X.Y.Z latest        # and the other four
   ```

One-time registry and repo setup, in order: create the npm org `extenddb`.
The launcher ships as the scoped `@extenddb/dev` rather than an unscoped
`extenddb`, because npm's package-name similarity filter refuses the latter
against the long-established `extend` package. Then create a granular
automation token scoped to the `@extenddb` scope with publish permission only,
and create the GitHub environment `npm` with deployment branch rule `main`,
required reviewers, and the token as `NPM_TOKEN`.

## Releasing to crates.io

The workspace crates publish to crates.io together, at the workspace
version. Each internal dependency requires exactly that version
([ADR-0029](../docs/adr/0029-crates-io-packaging.md)). crates.io has no
candidate stage, and it never replaces a version. So the crates are the
last step of a release, after `promote-image` promotes the images.

`devtools/publish-crates` publishes the crates in two modes. The token mode
publishes the first version of each crate, and the workflow publishes each
later version:

- The `release-crates` workflow publishes new versions of the crates that
  crates.io has. It uses trusted publishing, so no API token exists.
- A maintainer publishes each new crate with an API token and
  `devtools/publish-crates --new-crates`. Trusted publishing cannot create
  a crate. This mode never publishes a crate that crates.io has. For a
  crate that requires trusted publishing, crates.io refuses a new version
  published with an API token.

### Repository settings for crates.io

The release depends on two settings of this repository. Check both before
you add a trusted publisher on crates.io. A job that names a missing
environment creates it with no protection rules.

- The environment `crates-io` has the four required reviewers of the other
  release environments, with prevent self-review. So the maintainer who
  dispatches a run cannot approve it. The environment allows deployments
  from `main` only, and it holds no secrets. Admin bypass is off for
  `crates-io`. crates.io never replaces a version, so even an administrator
  needs the approval of a second maintainer to publish.

  ```bash
  gh api repos/ExtendDB/extenddb/environments/crates-io \
    --jq '{can_admins_bypass, rules: [.protection_rules[] | {type, prevent_self_review, reviewers: [.reviewers[]? | .reviewer.login // .reviewer.slug]}]}'
  gh api repos/ExtendDB/extenddb/environments/crates-io/deployment-branch-policies \
    --jq '.branch_policies[] | "\(.type):\(.name)"'
  ```

  The first command prints `can_admins_bypass` false, and a
  `required_reviewers` rule with `prevent_self_review` true and the same
  reviewers as the environment `npm`. The second command prints only
  `branch:main`.
- The tag ruleset `protect-release-tags` is active on `refs/tags/v*`. It
  blocks the deletion and the update of those tags, and it has no bypass
  actors. So a release tag cannot move to another commit.

  ```bash
  gh api repos/ExtendDB/extenddb/rulesets \
    --jq '.[] | select(.name == "protect-release-tags") | {id, target, enforcement}'
  gh api repos/ExtendDB/extenddb/rulesets/<id from the first command> \
    --jq '{enforcement, bypass_actors, include: .conditions.ref_name.include, rules: [.rules[].type]}'
  ```

  The second command prints the enforcement `active`, an empty
  `bypass_actors` list, the pattern `refs/tags/v*`, and the rules
  `deletion` and `update`.

### Each release

After `promote-image` promotes the images of the release:

1. A maintainer dispatches the `release-crates` workflow
   (`.github/workflows/release-crates.yml`) from `main`, with the release
   tag as input.
2. The `gate` job checks that the run comes from `main`. It checks the tag
   as `npm-publish` does. The tag must have the strict shape and exist, and
   its commit must be on `main`. Its version must equal the workspace
   version.
3. The gate reads `docker.io/extenddb/extenddb-postgres:X.Y.Z` without
   credentials. The revision label of that image must be the tagged commit.
   The gate also runs `devtools/check-released-through`.
4. Then `devtools/publish-crates --dry-run` plans the run, and the gate
   writes the tag, the commit and the plan to the job summary. The plan
   fails when a crate on crates.io accepts API tokens. A new crate does not
   fail the gate, and the plan shows the stop before it.
5. The `verify` job holds no credentials. It builds every package from its
   tarball with `cargo package --workspace --locked`, and it runs
   `devtools/check-crate-packages`. It records the SHA-256 of the package of
   each crate that crates.io receives, the tagged commit and the toolchain,
   in the artifact `crate-sha256`.
6. When the plan has a crate to publish, a reviewer approves the
   `crates-io` environment. The `publish` job packages the crates again
   without compiling them, and it runs `devtools/check-crate-packages`. It
   stops unless each package has the recorded SHA-256.
7. Then the `publish` job receives a crates.io token through trusted
   publishing. `devtools/publish-crates --no-verify` plans the run again,
   and it publishes the crates in dependency order, up to the first new
   crate. It compares each uploaded package with the recorded SHA-256. The
   token expires 30 minutes after the job receives it, and the job revokes
   it when it ends.
8. When no new crate stopped the run, the `index` job compares the
   checksum of each crate's version in the crates.io index with the
   recorded SHA-256. Then the `smoke` job runs
   `cargo install extenddb --version X.Y.Z --locked` with an empty cargo
   home. It checks the version, the commit and `test-hooks disabled` in the
   output of `extenddb version`. It also checks that no
   `backup-test-hooks` line appears.
9. When the run stopped before a new crate, the `new-crate` job reports
   the stop and points at the token steps in the job summary. Then the job
   fails the run.

The plan of the `publish` job decides between steps 8 and 9, because the
script reads crates.io again before its first upload. A token run can
publish the gate's new crate while the job waits for its approval. The job
then publishes the rest of the release, and `index` and `smoke` run. When
the run skips `publish`, the gate's plan decides.

The script skips each version that crates.io has. So a run that stopped
part way can be dispatched again.

### A release that adds a crate

Trusted publishing cannot create a crate. So a release that adds a crate
takes three runs:

1. The `release-crates` run publishes the crates before the new crate, and
   it stops before the new crate.
2. A maintainer publishes the new crate with an API token, as the next
   section describes. That run skips each crate that crates.io has at this
   version, and it stops before the next crate that needs a new version.
3. A second dispatch of `release-crates` publishes the rest of the release.
   Its gate fails until the new crate requires trusted publishing.

When a crate that crates.io has comes between two new crates, steps 2 and
3 repeat for the second new crate.

### Publishing new crates with an API token

The first publish of the workspace uses these steps for all 14 crates. A
later release uses them for each new crate. Both come after
`promote-image` for the release.

You need Python 3.11 or later, rustup and the GitHub CLI. You also need a
crates.io account, from a sign-in with GitHub, with a verified email
address. crates.io refuses a publish from an account without one. Before
the first publish, an owner of the ExtendDB GitHub organization allows
crates.io's GitHub OAuth app for the organization. A GitHub team can then
own the crates.

1. Use the `release-crates` run that stopped before the new crate. For the
   first publish, dispatch it as in "Each release". Its `verify` job records
   the SHA-256 list, the tagged commit and the toolchain in the artifact
   `crate-sha256`. Note the ID of the run. The run keeps the artifact for 7
   days. After that, dispatch `release-crates` again for a new artifact.
2. On crates.io, create an API token (Account Settings, API Tokens, New
   Token):
   - Scopes: `publish-new` only. So the token cannot publish a new version
     of a crate that crates.io has.
   - Crates: the pattern `extenddb*` for the first publish, or the name of
     the new crate for a later one.
   - Expiration: "Custom...", with the date of the next day.
3. Fetch the tag into a new directory, build the packages there, and
   compare them with the list of the `verify` job. The block fetches the
   tag by its full ref and checks it out detached. Before any command from
   the new directory runs, the block stops unless the checked-out commit is
   the commit in the artifact. So the code of a branch with the name of the
   tag never runs. The block takes the toolchain from the artifact, so it
   packages with the toolchain of the `verify` job. It accepts only a
   toolchain of the form X.Y.Z. No build of dependency code runs in the new
   directory. The comparison stops the procedure before any upload when
   this machine writes other bytes.

   The block packages with an empty cargo home, so cargo reads no stored
   token. Cargo still reads .cargo/config.toml in the new directory and in
   each parent directory, which includes ~/.cargo/config.toml when the new
   directory is under your home directory.

   ```bash
   TAG=vX.Y.Z
   RUN_ID=<the ID of the run from step 1>
   SUMS="$(mktemp -d)" && SRC="$(mktemp -d)" \
     && gh run download "$RUN_ID" --repo ExtendDB/extenddb --name crate-sha256 --dir "$SUMS" \
     && grep -qxE '[0-9]+\.[0-9]+\.[0-9]+' "$SUMS/TOOLCHAIN" \
     && git init -q "$SRC" && cd "$SRC" \
     && git fetch --depth 1 https://github.com/ExtendDB/extenddb.git "refs/tags/$TAG:refs/tags/$TAG" \
     && git checkout -q --detach "refs/tags/$TAG" \
     && test "$(git rev-parse HEAD)" = "$(cat "$SUMS/COMMIT")" \
     && export RUSTUP_TOOLCHAIN="$(cat "$SUMS/TOOLCHAIN")" \
     && rustup toolchain install "$RUSTUP_TOOLCHAIN" \
     && export CARGO_HOME="$(mktemp -d)" \
     && cargo package --workspace --no-verify --locked \
     && devtools/check-crate-packages \
     && (cd target/package && sha256sum --check --strict "$SUMS/SHA256SUMS") \
     && devtools/publish-crates --new-crates --dry-run --version "${TAG#v}" --sha256sums "$SUMS/SHA256SUMS" \
     && echo "Ready: $TAG is commit $(git rev-parse HEAD), and the packages have the bytes of the verify job."
   ```

4. Run the next line only after the block prints its last line, in the
   same shell. The command is one line, so `read` takes the token from the
   keyboard and never from a pasted line. The line asks for the token,
   gives it to the publish command only, and then removes it from the
   shell.

   ```bash
   printf 'crates.io token: ' && read -rs CARGO_REGISTRY_TOKEN && echo && CARGO_REGISTRY_TOKEN="$CARGO_REGISTRY_TOKEN" devtools/publish-crates --new-crates --no-verify --version "${TAG#v}" --sha256sums "$SUMS/SHA256SUMS"; unset CARGO_REGISTRY_TOKEN
   ```

   With `--no-verify`, cargo publishes without a build. So no build script
   or procedural macro runs while the token is in the environment. The
   script compares each uploaded package with the list. By default,
   crates.io accepts 5 new crates at once and then one every 10 minutes. So
   the 14 crates of the first publish take about 90 minutes. The script
   waits after each 429 answer, up to 2 hours in total.
5. Revoke the token on crates.io at once. The next steps use the browser
   session.
6. Check the settings in "Repository settings for crates.io".
7. On the Settings tab of each new crate on crates.io:
   - Add a second named owner. That maintainer signed in to crates.io
     before, and accepts the invitation.
   - Add the team `github:extenddb:extendb-committers`. A team owner can
     publish and yank, but cannot change the owners. The maintainer who adds
     the team is a member of `extendb-committers` or an owner of the
     ExtendDB organization. That maintainer also grants crates.io the GitHub
     scope `read:org`.
   - Add a GitHub trusted publisher: repository owner `ExtendDB`,
     repository name `extenddb`, workflow filename `release-crates.yml`,
     and `crates-io` in the field "Environment name (optional)". The form
     marks the environment as optional. Fill it in, because a publisher
     without it gives a token to a run of `release-crates.yml` from any
     branch.
   - Select "Require trusted publishing for all new versions", so that
     crates.io refuses a new version published with an API token.
8. After the first publish, check that docs.rs built the library crates.
9. Dispatch `release-crates` again with the release tag. The run publishes
   the rest of the release, and its `index` and `smoke` jobs check the
   whole release.

### A failure after the upload

A version on crates.io is permanent. A trusted publishing token can only
publish, so an owner yanks a version.

- The `index` job fails when the checksum of a version in the crates.io
  index differs from the verified SHA-256. crates.io then serves other bytes
  than the `verify` job built. An owner yanks every version of the release,
  on the page of each crate, or with `cargo yank` and an API token of the
  scope `yank`. The next patch release replaces the release.
- When `devtools/publish-crates` stops because an uploaded package differs
  from the verified list, the same steps apply. Do not run the script again
  for that release. A later run with `--sha256sums` stops before its first
  upload, because the index then lists that version with another checksum.
- The `index` job also fails when the index does not list a version of the
  release after 20 tries. Rerun the job first, because the index can lag
  behind an upload. If the version is still missing, check the log of the
  `publish` job for that crate.
- The `smoke` job fails when `cargo install` fails, or when
  `extenddb version` prints a wrong line. A rerun helps only when the
  install failed, because the crates.io index can lag. A wrong version,
  commit or test-hooks line comes from the published bytes, and a rerun
  prints the same line. Then decide whether to yank the release.

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
