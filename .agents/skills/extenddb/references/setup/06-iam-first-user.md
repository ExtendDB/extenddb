# Stage 5: IAM First User

## Purpose

After `extenddb serve` confirms the server is running, create the first IAM user, attach a policy, and create an access key. These three steps produce the credentials needed to make signed requests against extenddb. Skip this stage only if the user already has a working access key from a previous onboarding run.

## Inputs required

Before the commands below work, confirm three inputs are in hand.

1. The account ID printed by `extenddb init`. The value was displayed once at init time alongside the admin credentials.
2. The admin password printed by `extenddb init`. The password cannot be retrieved later. If it is lost, destroy and re-initialize per `references/04-init-stage.md`.
3. The self-signed TLS certificate at `~/.extenddb/tls/cert.pem`. Verify with:

```bash
test -f ~/.extenddb/tls/cert.pem && echo "present" || echo "missing"
```

If the certificate is missing, return to the init or serve stage. The access key commands authenticate over HTTPS and will fail without the cert.

## Create IAM user

Run the `manage create-user` command. Substitute the account ID from init and the admin password from init. Choose any `--user-name`; this reference uses `alice` as the placeholder.

```bash
./target/release/extenddb manage --user admin --password <admin-pw> \
    --config extenddb.toml \
    create-user --account-id <account-id> \
    --user-name alice --user-password secret
```

The `--user-password` value lets the new user authenticate to the management API for self-service operations. The next two commands use it.

## Attach full-access policy

Run the `manage put-user-policy` command with the full-access policy document. The policy grants `dynamodb:*` on all resources.

```bash
./target/release/extenddb manage --user admin --password <admin-pw> \
    --config extenddb.toml \
    put-user-policy --account-id <account-id> --user-name alice \
    --policy-name FullAccess \
    --policy-document '{
      "Version": "2012-10-17",
      "Statement": [{
        "Effect": "Allow",
        "Action": "dynamodb:*",
        "Resource": "*"
      }]
    }'
```

> The default policy grants `dynamodb:*`, which allows all DynamoDB actions on all resources. This is appropriate for local development and the onboarding walkthrough. Tighten the policy before production use. See `docs/manuals/10-security-model.md` for production IAM patterns.

## Create access key

Run the `manage create-access-key` command using the self-service `<account-id>/<user-name>` authentication format. The `--password` is the `--user-password` set in the create-user step.

```bash
./target/release/extenddb manage --user <account-id>/alice --password secret \
    --config extenddb.toml \
    create-access-key
```

## Capture the access key

> IMPORTANT. The access key ID and secret access key are printed to stdout exactly once. The secret key cannot be retrieved later. Copy both values to a password manager or `.env` file before continuing.

The access key ID starts with `AKIAEXTENDDB` and the secret access key starts with `extenddb`. These prefixes distinguish extenddb credentials from real AWS credentials.

## Verify the access key

Run a minimal signed request to confirm the access key works. Substitute the values captured in the previous step. Use the port configured in `extenddb.toml` (default 8000).

```bash
export AWS_CA_BUNDLE=~/.extenddb/tls/cert.pem
export AWS_ACCESS_KEY_ID=<access-key-id>
export AWS_SECRET_ACCESS_KEY=<secret-access-key>
aws dynamodb list-tables --endpoint-url https://127.0.0.1:<port> --region us-east-1
```

A successful response returns a JSON object with a `TableNames` array. An empty array is expected and correct; no tables exist yet. If the command returns `InvalidSignatureException`, `UnrecognizedClientException`, or a TLS error, consult ``references/troubleshooting/01-symptom-index.md`` with the exact error code.

Full AWS CLI configuration, including the profile and per-command options, is covered in ``references/first-request/01-aws-cli-config.md``.

## Handoff

When the user has an access key ID and secret access key saved, consult ``references/first-request/01-aws-cli-config.md`` to configure the AWS CLI and run the first CRUD round trip.
