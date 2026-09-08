# ADR-0015: Security Foundation Architecture

Status: Accepted

## Context

Through `v0.1.0`/`EXTERNAL-REVIEW READY`, ChronicleDB deliberately
deferred authentication, transport encryption, authorization, and
audit logging as a documented scope decision
([`docs/non-goals.md`](../non-goals.md) §Authentication and TLS,
[`docs/failure-model.md`](../failure-model.md) §6, `SECURITY.md`):
`internal/transport` dials/accepts plaintext TCP for Raft peer traffic,
and every `cmd/chronicledb-node` HTTP endpoint (`/status`, `/propose`,
`/outcome`, `/fault`, `/metrics`, `/health`) is open to anyone who can
reach the port. `docs/enterprise-v1-plan.md` §5 ("Security Foundation")
is the designated resolution point for that gap, planned as the first
phase of the Enterprise V1 roadmap because every later phase adds a new
administrative surface that would otherwise ship unauthenticated. This
ADR records the architecture actually implemented for that phase,
targeted at release `v0.2.0` (not yet tagged).

## Decision

Eight layers, each depending only on the previous, implemented as
described in `docs/enterprise-v1-plan.md` §5 and `docs/security.md`:

1. **Node identity** (`internal/identity`): a node's identity is its TLS
   certificate's `CommonName` (or a matching SAN `DNSName`), not merely
   the `-id` flag. `internal/node.Open` calls
   `identity.BindNodeIdentity` and refuses to start on a mismatch. V1
   does not build a certificate authority — operators bring their own
   (`docs/security.md`).
2. **Peer mTLS** (`internal/transport/tls.go`): `Transport.NewTLS`
   wraps the listener in `tls.NewListener` with
   `ClientAuth: RequireAndVerifyClientCert`; outbound dials use a
   `VerifyPeerCertificate` callback that both verifies the presented
   chain against the trusted CA pool and confirms the leaf's identity
   matches the specific peer being dialed
   (`internal/identity.VerifyPeerIdentity`) — strictly more
   verification than `crypto/tls`'s own hostname check, not less, since
   ChronicleDB's operator-managed-CA convention does not guarantee a
   DNS-SAN-to-dial-address correspondence. Every message received on an
   inbound connection is additionally checked against that connection's
   own TLS-verified identity (`Message.From` must equal the
   handshake-verified `CommonName`) — a valid certificate for one node
   can never be used to impersonate a different node's messages on the
   same connection. There is no plaintext fallback: `New` (plaintext)
   and `NewTLS` (mTLS) are separate constructors: a Transport is either
   fully protected or not configured for peer TLS at all, never
   partially.
3. **Client TLS** (`cmd/chronicledb-node`): the control-plane HTTP
   server terminates TLS via `crypto/tls` (stdlib only, per
   [`docs/dependencies.md`](../dependencies.md)). Client mTLS is
   supported (`-auth-mode=mtls`) but not required — server-only TLS
   plus a bearer token (`-auth-mode=token`) is an equally valid V1
   configuration.
4. **Authentication** (`internal/authn`): an `Authenticator` interface
   with two implementations — `TokenAuthenticator` (constant-time
   comparison via `crypto/subtle`, so response timing cannot help guess
   a valid token byte-by-byte) and `MTLSAuthenticator` (reads the
   already-verified client certificate's identity as the principal).
5. **RBAC** (`internal/authz`): three fixed roles (`admin`, `operator`,
   `read-only`) and a single hardcoded decision table
   (`endpoint -> role -> bool`) — no role hierarchy computed in code,
   so the table itself is the complete, auditable authorization
   surface. Credential-to-role mapping is a static, operator-provided
   JSON file (`-rbac-mapping-file`), not a dynamic grants database.
6. **Audit logging** (`internal/audit`): a dedicated, hash-chained,
   checksummed, framed log — deliberately reusing
   `internal/storage.Segment`'s append-only primitive (the same layer
   `internal/wal` itself is built on) rather than `internal/wal`
   directly, since the audit log is a separate, non-authoritative-for-
   state record (see Correctness Implications below), never a second
   copy of committed database history. Each record's `recordHash`
   covers its own header/payload/checksum, and the *next* record embeds
   the *previous* record's `recordHash` as `prevHash` — tampering with
   any one record, even if its own checksum/hash is also forged to
   match, breaks the chain link the following record's `prevHash` still
   references, unless every subsequent record is also rewritten.
7. **Certificate rotation** (`internal/identity.Holder`): certificate/
   key/CA material is held behind an `atomic.Pointer`, read fresh via
   `GetConfigForClient` on every new TLS handshake (both the peer
   transport and the control-plane HTTP server use this same
   mechanism). `Reload()` re-reads from the same file paths and
   validates the new material fully before swapping it in — a
   malformed replacement never replaces working material. Because only
   *new* handshakes ever observe the swap, an already-established
   connection is never forcibly dropped by rotation alone; an "overlap
   window" during a CA rotation specifically is achieved operationally
   (the CA bundle file transiently contains both old and new CA certs),
   not by inventing a second, parallel trust-pool mechanism. Triggered
   by `SIGHUP` or the `admin`-only `/admin/reload-tls` endpoint.
8. **Secure admin endpoints**: `/fault` is registered on the control
   plane's `http.ServeMux` *conditionally* — only when
   `-enable-fault-endpoint` is passed — so a default `go build` binary
   run with default flags has no route for it at all (proven by
   asserting `mux.Handler` returns an empty pattern, not merely that
   calling the URL returns an error status). When registered, it is
   additionally gated by `admin`-only RBAC like every other
   administrative endpoint.

The full request path is a middleware chain —
`TLS termination -> authn -> authz -> audit -> handler`
(`cmd/chronicledb-node/auth.go`'s `security.wrap`) — with exactly one
audit record written per decision (allow, deny-unauthenticated, or
deny-unauthorized) *before* the handler ever runs, and an audit-write
failure rejecting the request outright rather than letting the handler
proceed unrecorded.

`-auth-mode=none` (the flag default) reproduces `v0.1.0`'s exact
plaintext/unauthenticated behavior byte-for-byte, so an in-place binary
swap does not silently break an existing trusted-network deployment —
`v0.2.0` is secure-**by-configuration**, not yet secure-by-default (see
`docs/enterprise-v1-plan.md` §5's Compatibility implications; flipping
the default is deferred to the `v1.0.0` gate). Running without TLS/auth
prints a loud, repeated (every 30s) warning for the lifetime of the
process.

## Alternatives Considered

1. **A built-in certificate authority / PKI service.** Rejected per
   `docs/enterprise-v1-plan.md` §5's explicit non-goal: operators
   already have mature tooling (`openssl`, `step-ca`, cloud-managed
   CAs) for certificate issuance; building a parallel, V1-scoped one
   would be exactly the kind of "infrastructure ahead of a concrete,
   evidenced need" `docs/non-goals.md`'s general policy warns against.
2. **A dynamic grants/permissions database instead of three fixed
   roles.** Rejected as V1 scope: `docs/enterprise-v1-plan.md` §5
   explicitly scopes RBAC to endpoint-level gating with static role
   assignment; per-row/per-column SQL authorization is named as a
   separate, not-yet-justified future extension.
3. **Storing the audit log as WAL records via `internal/wal` directly**
   (reusing its exact record type set). Rejected: `internal/wal` is the
   single logical history for committed database state
   (`CONSISTENT LOG RESPONSIBILITY`, `docs/invariants.md`); audit
   records are not committed state and must remain readable/verifiable
   even by an operator with WAL access revoked (and vice versa) — a
   genuinely separate physical file with its own, simpler format was
   the correct application of "reuse mechanism, not files."
   `internal/storage.Segment` (the layer *beneath* `internal/wal`,
   with no opinion about record meaning) is exactly the right reuse
   boundary.
4. **Hostname-based (`ServerName`) verification for peer mTLS
   identity**, matching `crypto/tls`'s default client behavior.
   Rejected: ChronicleDB's operator-managed-CA convention identifies a
   node by its certificate `CommonName`, not necessarily a DNS name
   resolvable to its dial address (e.g. a NAT'd or container-networked
   deployment) — an explicit `VerifyPeerCertificate` callback checking
   chain trust plus `CommonName`-equals-expected-peer-ID is a strictly
   stronger and more portable check than hostname matching would be
   here.
5. **Secure-by-default at `v0.2.0`** (no `-auth-mode=none`, TLS
   mandatory). Rejected for this phase specifically per
   `docs/enterprise-v1-plan.md` §5's Compatibility implications: an
   in-place binary upgrade of an existing trusted-network `v0.1.0`
   deployment must not silently stop working; the flip to
   secure-by-default is an explicit, separately evidenced `v1.0.0` gate
   requirement (§13.2), not something this phase is authorized to
   claim.

## Consequences

- Every administrative HTTP endpoint now has a defined `401`/`403`
  status space that did not previously exist; every response body in
  that space is a fixed, generic string (no information leak
  distinguishing failure reasons).
- A peer-mTLS-configured cluster requires all three of
  `-peer-tls-cert`/`-peer-tls-key`/`-peer-tls-ca` — a partially
  specified configuration is refused at startup
  (`node.Config.validate`), not silently downgraded.
- `cmd/chronicledb-node`'s existing real-process test helper
  (`newRealCluster`, shared by `main_test.go` and `chaos_test.go`) now
  passes `-enable-fault-endpoint` explicitly, since `/fault` is no
  longer unconditionally reachable — an intentional, documented
  behavior change to those tests' fixture, not a weakening of any
  assertion they make.
- `docs/wal.md`'s framing pattern gained a second, independent consumer
  (`internal/audit`) beyond `internal/wal` itself, validating that
  pattern as genuinely reusable rather than accidentally
  WAL-specific.

## Correctness Implications

- **No changes to any existing Raft/WAL/MVCC/Snapshot-Isolation/
  RequestID/snapshot-compaction invariant.** `internal/raft.Core` and
  `internal/fsm.Apply` are untouched by this phase; peer mTLS is
  implemented entirely in `internal/transport`'s driver-side
  connection-handling code, never inside the deterministic core
  (`DETERMINISM BOUNDARY`, `docs/invariants.md`).
- New invariants added to `docs/invariants.md`: `NO UNAUTHENTICATED
  ADMIN ACTION`, `NO PLAINTEXT PEER REPLICATION`, `FAULT SURFACE OFF BY
  DEFAULT`, `AUDIT COMPLETENESS`.
- The audit log is explicitly **not** part of `CONSISTENT LOG
  RESPONSIBILITY`'s "one logical ordered history" — it is a second,
  deliberately independent, non-authoritative-for-state record, whose
  entire purpose (tamper-evidence of administrative activity) requires
  it to survive independently of WAL access, not to be another copy of
  committed state.
- Peer identity verification happens per-connection at the transport
  layer (TLS handshake) and per-message (`Message.From` bound to that
  connection's verified identity) — `internal/raft.Core` still receives
  `raft.Message` values exactly as before; it has no awareness that
  identity verification occurred underneath it, preserving the existing
  `internal/raft`/`internal/transport` dependency boundary
  (`docs/architecture.md` §5).

## Testing and Proof Obligations

- `internal/identity`: `BindNodeIdentity` match/mismatch (CommonName
  and SAN), `Holder.Reload` success and malformed-material-rejection.
- `internal/transport/tls_test.go`: valid mTLS round-trip; expired,
  wrong-CA, self-signed, no-certificate, and plaintext-to-TLS-listener
  rejection; a same-CA-issued-but-wrong-identity connection cannot
  spoof another node's `Message.From`; certificate rotation under a
  live connection with zero drops, proven against both the
  already-open connection and a fresh post-rotation one.
- `internal/authn`: token valid/invalid/missing/malformed-header, a
  same-length near-miss-prefix sweep as an indirect proof of
  constant-time comparison, mTLS principal extraction with/without a
  presented certificate.
- `internal/authz`: the full role x endpoint decision table, unknown-
  endpoint/unknown-role fail-closed defaults, token-file and
  RBAC-mapping-file parsing including malformed-input rejection.
- `internal/audit`: encode/decode round-trip, exhaustive single-byte-
  flip tamper detection across an entire log file, forged-but-
  internally-consistent single-record tamper detection (the actual
  chain-link property, not just per-record checksums), torn-tail crash
  recovery, write-failure-after-underlying-close, and a
  `FuzzDecodeFrame` fuzz target run in CI.
- `internal/node/tls_test.go`: a real three-node cluster with peer mTLS
  proving replication and leader failover are unaffected; an untrusted-
  CA rogue node's presence never disrupts the healthy cluster's own
  quorum; identity-mismatch and partial-peer-TLS-configuration startup
  refusal.
- `cmd/chronicledb-node/auth_test.go`: the RBAC decision table exercised
  through the full HTTP middleware chain for every role x endpoint,
  unauthenticated-denied-for-every-endpoint, generic-error-message
  equality across distinct failure reasons, `/fault` route-registration
  under every flag combination (structural, via `mux.Handler`, not just
  status-code checks), exactly-one-audit-record-per-decision, and
  audit-write-failure blocking the triggering action.
- `cmd/chronicledb-node/security_integration_test.go` (`integration`
  build tag): real OS processes, real certificates, peer mTLS + client
  TLS + token auth + RBAC + audit together end-to-end; `/fault`
  confirmed unreachable (404, not 403) on a real binary invocation
  without the enable flag; a real `SIGHUP`-triggered certificate
  reissue under continuous authenticated write load with zero dropped
  commits.
