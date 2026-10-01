# Producer authentication contract

## Boundary

An authenticator converts an Authorization header into a trusted `Principal`:
tenant ID plus the source IDs that producer may claim. The upload API accepts a
`source_id` only if it appears in that principal. Image status is scoped by the
principal's tenant. No request body, filename, S3 metadata, or unsigned token
claim may create or expand this source authorization.

The local bearer token is bound to one configured source ID for development.
The shipped production default is `UnconfiguredAuthenticator`, which returns
503 without reading or writing upload state. A production authenticator must
be selected and deployed explicitly; this repository does not assume a cloud
identity provider or token issuer yet. Supplying only an authenticator does not
enable uploads outside local mode: an explicit non-local upload service must
also be configured, so production cannot silently fall back to Floci.

## Production implementation requirements

1. Verify the credential using the chosen provider's trusted keys or
   introspection endpoint. Validate issuer, audience, expiry, not-before,
   algorithm/key selection, and revocation behavior. Never decode an unsigned
   token into a `Principal`.
2. Bind the verified subject to one tenant and an allowlist of stable source
   IDs in a server-controlled registry. Reject unknown, disabled, or
   cross-tenant sources. Treat changes to that mapping as auditable operations.
3. Keep source claims narrow: a producer may upload without provenance as a
   separate observation, but it cannot assert another producer's source ID.
   Enforce capture-key uniqueness within each source at the producer boundary.
4. Define key rotation, credential lifetime, cache TTL, outage behavior, and
   incident revocation. Authentication and source-registry outages fail closed
   without creating an upload session.
5. Exercise valid, expired, revoked, wrong-audience, wrong-tenant, wrong-source,
   and provider-outage cases before enabling production uploads.

The provider, issuer, credential format, and producer/source registry are open
deployment decisions. The `Authenticator` interface isolates those decisions
from upload and dedup logic; it does not implement them.

## Historical-state preflight

`make audit-dedup` is read-only and limited to a loopback local AWS endpoint.
It paginates the image table and checks completed duplicate references against
their representative image's tenant, scope, and group. `complete_scan` means
pagination reached the end, **not** that DynamoDB provided a point-in-time
snapshot. The command neither changes policy markers nor verifies Kafka or
downstream search state.

On 2026-10-01, the local development table contained 124 images: 92 completed
unversioned, 31 completed v3, and one incomplete. All 36 duplicate owner links
checked were structurally consistent. Nine records had partial provenance from
earlier development. This is not a production migration or proof that their
published events and downstream mappings agree. Those require an explicit
event/index reconciliation and a reviewed backfill/rollback procedure.
