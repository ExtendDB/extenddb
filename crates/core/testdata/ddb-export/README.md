# DynamoDB export fixture

A small export in the layout Amazon DynamoDB writes for
`ExportTableToPointInTime` with `DYNAMODB_JSON` output.

## Contents

- `manifest-summary.json`: the summary manifest, one compact JSON document.
- `manifest-files.json`: the files manifest, JSON lines, one entry.
- `data/ka2sswm5ha4uejfqcmnjcbg6ru.json.gz`: one gzip data file holding three
  items as newline-delimited `{"Item": ...}` objects.

## Provenance

Constructed, not captured from the live service. The fixture follows the
output format documented at
<https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/S3DataExport.Output.html>:
member names, the `2020-06-30` summary version, ISO 8601 timestamps, the
explicit `null` for `s3SseKmsKeyId`, the absence of `exportType` on a full
export, base64 `md5Checksum` per data file, and the marshalled DynamoDB JSON
item encoding. The summary field values reuse the documented example
(ProductCatalog); item counts and checksums are computed from the data file
in this directory, so the three files are mutually consistent. The data file
was gzipped with mtime 0 for reproducibility. Regenerating any file requires
recomputing `md5Checksum`, `etag`, and `itemCount` in the manifests.
