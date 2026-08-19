# ExtendDB dev

ExtendDB dev is a single-container, DynamoDB-compatible endpoint that enables developers to develop and test applications against the DynamoDB API in their own development environment.

## Benefits of using ExtendDB dev

The image has everything built in: the API, its storage, and a seeded credential. There is no external database to run, no init step, no bootstrap sidecar, and no certificate to trust, so it drops into a containerized build or a continuous integration suite as one service.

ExtendDB dev works with your existing DynamoDB API calls. Any AWS SDK, CLI, or tool that talks to DynamoDB talks to it unchanged, so you point your client at a different endpoint and nothing else about your code changes.

It needs no internet connection, and there are no provisioned throughput, data storage, or data transfer costs.

Data is durable across restarts by default, or entirely in memory when you want a pristine database per test run.

## Getting started with ExtendDB dev on Docker

Run:

```bash
docker run -d -p 127.0.0.1:18443:18443 -v extenddb:/var/lib/extenddb extenddb/extenddb-dev
```

Then use it like DynamoDB. The server seeds AWS's documented example credential and prints it at startup:

```bash
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE \
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
aws dynamodb list-tables --region us-east-1 --endpoint-url http://127.0.0.1:18443
```

For an in-memory database that vanishes with the container, drop the volume and add `-e EXTENDDB__STORAGE__SQLITE__PATH=:memory:`.

To learn how to configure ExtendDB dev, see [the `extenddb-dev` image](https://github.com/ExtendDB/extenddb/blob/main/docs/dev-image.md).

For a durable, TLS-terminated deployment, use [`extenddb/extenddb-postgres`](https://hub.docker.com/r/extenddb/extenddb-postgres).

## Note

This image is built for local development and CI. It serves plain HTTP and its authorization is open, so whoever holds the credential can do anything. Publish it to loopback only, as the command above does, and do not keep real data in it.

ExtendDB is an independent open source project managed by Amazon Web Services. It is not Amazon DynamoDB and does not contain any DynamoDB source code. "DynamoDB" is a trademark of Amazon.com, Inc. ExtendDB is a clean-room implementation that speaks the DynamoDB wire protocol; behavioral differences from the service are documented in [Differences from DynamoDB](https://github.com/ExtendDB/extenddb/blob/main/docs/differences-from-dynamodb.md).

More at [extenddb.org](https://extenddb.org) and [github.com/ExtendDB/extenddb](https://github.com/ExtendDB/extenddb). Licensed under Apache-2.0.
