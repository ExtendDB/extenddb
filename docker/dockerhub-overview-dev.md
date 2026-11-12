# ExtendDB dev

> **Local development and CI only. Not for shared networks.** This image serves plain HTTP, its authorization is open, and by default it accepts AWS's public example credential, so anyone who can reach its port can read, change and delete every table. Always publish it on loopback, `-p 127.0.0.1:18080:18080`; a bare `-p 18080:18080` publishes it on every network the host is on. Do not keep real data in it. On a shared host or CI runner, with an image released after 0.1.14, also set `EXTENDDB_DEV_DISABLE_EXAMPLE_CREDENTIAL=1` with your own `EXTENDDB_DEV_ACCESS_KEY_ID` and `EXTENDDB_DEV_SECRET_ACCESS_KEY` ([details](https://github.com/ExtendDB/extenddb/blob/main/docs/dev-image.md#a-shared-host-or-ci-runner)). Releases through 0.1.14 ignore that variable and always accept the example credential; whatever the version, check that the credentials the server prints at startup do not include `AKIAIOSFODNN7EXAMPLE`.

ExtendDB dev is a single-container, DynamoDB-compatible endpoint that enables developers to develop and test applications against the DynamoDB API in their own development environment.

## Benefits of using ExtendDB dev

The image has everything built in: the API, its storage, and a seeded credential. There is no external database to run, no init step, no bootstrap sidecar, and no certificate to trust, so it drops into a containerized build or a continuous integration suite as one service.

ExtendDB dev works with your existing DynamoDB API calls. Any AWS SDK, CLI, or tool that talks to DynamoDB talks to it unchanged, so you point your client at a different endpoint and nothing else about your code changes.

It needs no internet connection, and there are no provisioned throughput, data storage, or data transfer costs.

Data is durable across restarts by default, or entirely in memory when you want a pristine database per test run.

## Getting started with ExtendDB dev on Docker

Run:

```bash
docker run -d -p 127.0.0.1:18080:18080 -v extenddb:/var/lib/extenddb extenddb/extenddb-dev
```

Then use it like DynamoDB. The server seeds AWS's documented example credential and prints it at startup:

```bash
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE \
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
aws dynamodb list-tables --region us-east-1 --endpoint-url http://127.0.0.1:18080
```

For an in-memory database that vanishes with the container, drop the volume and add `-e EXTENDDB__STORAGE__SQLITE__PATH=:memory:`.

To learn how to configure ExtendDB dev, see [the `extenddb-dev` image](https://github.com/ExtendDB/extenddb/blob/main/docs/dev-image.md).

For a durable, TLS-terminated deployment, use [`extenddb/extenddb-postgres`](https://hub.docker.com/r/extenddb/extenddb-postgres).

## Verification

Every published image is signed with Sigstore cosign, with a Rekor transparency-log entry. Releases through 0.1.14 use a committed release key, [`extenddb-signing.pub.pem`](https://github.com/ExtendDB/extenddb/blob/main/extenddb-signing.pub.pem):

```console
cosign verify --key extenddb-signing.pub.pem docker.io/extenddb/extenddb-dev:X.Y.Z
```

From the first release after the keyless migration (named in the release notes), signatures are keyless, bound to the workflow that signed the image:

```console
cosign verify \
  --certificate-identity-regexp '^https://github\.com/ExtendDB/extenddb/\.github/workflows/(release-dev-image|sign-image)\.yml@refs/heads/main$' \
  --certificate-oidc-issuer 'https://token.actions.githubusercontent.com' \
  docker.io/extenddb/extenddb-dev:X.Y.Z
```

The routine signer is `release-dev-image.yml`; the break-glass signer `sign-image.yml` can sign any tier, so drop it from the pattern to require the routine dev path.

## Note

ExtendDB is an open source project maintained by AWS. It is not Amazon DynamoDB and does not contain any DynamoDB source code. "DynamoDB" is a trademark of Amazon.com, Inc. ExtendDB is a clean-room implementation that speaks the DynamoDB wire protocol; behavioral differences from the service are documented in [Differences from DynamoDB](https://github.com/ExtendDB/extenddb/blob/main/docs/differences-from-dynamodb.md).

More at [extenddb.org](https://extenddb.org) and [github.com/ExtendDB/extenddb](https://github.com/ExtendDB/extenddb). Licensed under Apache-2.0.
