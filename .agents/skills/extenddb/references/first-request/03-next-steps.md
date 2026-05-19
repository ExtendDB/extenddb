# Post-CRUD Next Steps

## Purpose

After the three-operation CRUD round trip (`create-table`, `put-item`, `get-item`) succeeds, the user has a working extenddb deployment with a working access key. Onboarding is complete. This reference lists three recommended next steps and a short further-reading table.

Pick any one. They are independent. Most users run the sample application first because it exercises the broadest surface area in a single command.

## Next step 1: Run the sample applications

Run the included sample against the running extenddb instance.

```bash
python3 samples/sample_app.py
```

The sample exercises a nine-stage lifecycle: create table, describe table, put item, batch write, query, scan, update, stream read, and delete. It demonstrates the broader API surface beyond the three CRUD operations.

To walk through the sample, consult ``references/samples/01-venv-setup.md``. That skill covers the Python virtual environment setup, the four environment variables `samples/sample_app.py` reads, and the stream consumer sample.

## Next step 2: Explore the management console

Visit `https://127.0.0.1:<port>/console/` in a browser. Accept the self-signed certificate warning on first visit. Sign in with the admin credentials captured during `extenddb init`.

The console provides:

- Account and user administration
- Policy and role management
- Access key creation and rotation
- Table browsing (read-only)

The console is the self-service alternative to `extenddb manage` CLI commands. New users benefit from seeing the admin UI even if they prefer the CLI for automation.

## Next step 3: Read the differences doc

`docs/differences-from-dynamodb.md` lists every behavioral difference between extenddb and the real DynamoDB service. Read this before porting production code from DynamoDB to extenddb or before writing code that targets both. Key categories:

- Unsupported operations
- Semantic differences (for example, index propagation timing, stream ordering)
- Limits that differ

The differences doc is the source of truth for compatibility. If application code behaves differently against extenddb than against DynamoDB, check this doc before filing a bug.

## Further reading

| Document | When to read |
|---|---|
| `docs/manuals/01-architecture-guide.md` | Before deploying extenddb in a non-laptop environment |
| `docs/manuals/05-admin-guide.md` | When the CLI commands the user has learned are not enough |
| `docs/manuals/10-security-model.md` | Before tightening the full-access IAM policy for production |
| `docs/manuals/11-deployment-guide.md` | Before self-hosting, multi-cloud, or air-gapped deployments |
