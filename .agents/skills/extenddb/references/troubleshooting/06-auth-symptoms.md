# Auth symptoms

This file holds verbatim Cause and Fix entries for SigV4 signature and IAM authorization errors. These are the most common symptoms users hit when configuring AWS CLI or SDKs to talk to extenddb.

### InvalidSignatureException

<a name="invalidsignatureexception"></a>

**Error text:**
```
InvalidSignatureException: The request signature we calculated does not match the signature you provided
```

**Cause:** The secret key used to sign the request does not match the secret key stored in extenddb. This can happen if the secret key was copied incorrectly (even a single character difference causes a completely different signature).

**Fix:**
Verify you are using the exact secret key returned when the access key was created. Secret keys cannot be retrieved after creation — if lost, delete the access key and create a new one.

**Not to be confused with:**
- `UnrecognizedClientException` — the access key ID is unknown to extenddb. An unknown or mistyped access key ID does NOT produce `InvalidSignatureException`; it produces `UnrecognizedClientException`. Do not tell the user that a wrong access key ID could surface as `InvalidSignatureException`.
- `AccessDeniedException` — the user is authenticated but not authorized. If the signature is valid and the access key ID is known, the request reaches IAM policy evaluation; a missing Allow there produces `AccessDeniedException`, not `InvalidSignatureException`.
- Clock skew, expired session tokens, region mismatches, and proxy header rewriting are NOT documented extenddb causes for this error. Do not add them as secondary diagnostics.

**Source:** `docs/troubleshooting.md`, section "`InvalidSignatureException: The request signature we calculated does not match the signature you provided`", last synced 2026-05-12.

### UnrecognizedClientException

<a name="unrecognizedclientexception"></a>

**Error text:**
```
UnrecognizedClientException: The security token included in the request is invalid
```

**Cause:** The access key ID in the request does not exist in extenddb's credential store. Either the key was never created, was deleted, or is misspelled.

**Fix:**
Verify the access key ID with `extenddb manage list-access-keys`. Create a new access key if needed.

**Not to be confused with:**
- `InvalidSignatureException` — the access key ID is known but the signature does not verify. That is a secret key problem, not an access key ID problem.
- `AccessDeniedException` — the access key ID is known and the signature verifies, but IAM policy denies the action.
- Session tokens: extenddb does not implement federated `AssumeRoleWithSAML` or `AssumeRoleWithWebIdentity` flows at the request level. Missing or expired session tokens from external federation providers are not a documented cause of this error.

**Source:** `docs/troubleshooting.md`, section "`UnrecognizedClientException: The security token included in the request is invalid`", last synced 2026-05-12.

### AccessDeniedException

<a name="accessdeniedexception"></a>

**Error text:**
```
AccessDeniedException: User: <ARN> is not authorized to perform: <action>
```

**Cause:** The authenticated user does not have an IAM policy granting the requested DynamoDB action. This can be an implicit deny (no matching Allow statement) or an explicit Deny.

**Fix:**
Attach a policy granting the required action to the user, or to a group the user belongs to. Use `extenddb manage list-user-policies` to check current policies. Remember that explicit Deny always overrides Allow.

**Not to be confused with:**
- `InvalidSignatureException` and `UnrecognizedClientException` — those are credential-verification failures that happen before IAM policy evaluation. If the user is seeing `AccessDeniedException`, the credentials are valid; the missing piece is a policy grant.
- Resource not found: a missing table returns `ResourceNotFoundException`, not `AccessDeniedException`. A 404-like symptom is a different branch.

**Source:** `docs/troubleshooting.md`, section "`AccessDeniedException: User: <ARN> is not authorized to perform: <action>`", last synced 2026-05-12.
