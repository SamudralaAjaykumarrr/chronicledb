# Security

Status: `v0.2.0` (Security Foundation, `docs/enterprise-v1-plan.md` §5;
not yet tagged/released — see `docs/roadmap.md`). This document is the
authoritative operational guide for TLS, authentication, RBAC, and
audit logging as actually implemented, and supersedes the "no auth/TLS"
posture described in `SECURITY.md`'s "Deployment assumptions" and
`docs/non-goals.md`'s former §Authentication and TLS entry (both
updated to point here).

## 1. What changed from `v0.1.0`

Through `v0.1.0`, ChronicleDB had **no** transport encryption,
authentication, authorization, or audit trail: anyone who could reach a
node's ports could read/write cluster state and inject network faults.
That was a documented, deliberate scope decision for the phases proving
engine correctness, not an oversight.

`v0.2.0` resolves it — but **not by defaulting to secure**. Every new
flag defaults to the exact `v0.1.0` behavior:

| Flag | Default | `v0.1.0`-equivalent when left at default |
|---|---|---|
| `-tls-cert`/`-tls-key`/`-tls-ca` | `""` | plaintext control-plane HTTP |
| `-peer-tls-cert`/`-peer-tls-key`/`-peer-tls-ca` | `""` | plaintext peer Raft transport |
| `-auth-mode` | `none` | no authentication/authorization/audit at all |
| `-enable-fault-endpoint` | `false` | `/fault` was always reachable in `v0.1.0`; now it is off unless explicitly enabled |

Running with any of TLS/auth left at its default prints a loud warning
at startup and again every 30 seconds for the life of the process. This
is a **required migration**, not a permanent supported configuration —
see §7.

## 2. Threat model

Extends `docs/failure-model.md` §6. Through `v0.1.0`, the assumed
adversary was "an untrusted network, but only a benign/faulty actor" —
malformed or corrupted bytes, not a deliberately hostile one. Once
TLS/auth are configured, the threat model expands to a genuinely
**adversarial network**: an attacker who can observe or inject packets
on the network between clients/nodes but does **not** possess a valid
certificate or credential.

**Still out of scope** (unchanged, and not something this phase or any
future one changes without its own ADR):

- **Byzantine node behavior.** TLS/auth protect against *unauthorized*
  access; they do not protect against an *authorized-but-malicious*
  node — Raft itself provides no Byzantine fault tolerance
  (`docs/failure-model.md` §5), and neither does this phase.
- **A compromised operator-managed CA.** ChronicleDB trusts whatever CA
  the operator configures; CA compromise is outside ChronicleDB's
  control (see §3's non-goal on building a CA).
- **Side-channel attacks against the host** (memory disclosure, timing
  attacks against the OS/kernel network stack, etc.) beyond the
  constant-time token comparison this phase itself implements
  (§4.1).

## 3. Node identity and certificates

A node's identity is its TLS certificate's `Subject.CommonName` (or a
matching `DNSNames` SAN) — not merely its `-id` flag. At startup,
`internal/node.Open` verifies these match and **refuses to start** on a
mismatch, whenever peer TLS is configured.

ChronicleDB does **not** build or ship a certificate authority. Bring
your own (`openssl`, `step-ca`, your cloud provider's private CA, an
internal PKI team) and issue one leaf certificate per node, with
`CommonName` equal to that node's `-id` value. A minimal `openssl`
example for a self-managed test/dev CA:

```bash
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 \
  -keyout ca-key.pem -out ca-cert.pem -days 3650 -nodes \
  -subj "/CN=chronicledb-test-ca"

openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 \
  -keyout n1-key.pem -out n1.csr -nodes -subj "/CN=n1"
openssl x509 -req -in n1.csr -CA ca-cert.pem -CAkey ca-key.pem \
  -CAcreateserial -out n1-cert.pem -days 365
```

Repeat per node ID. Distribute `ca-cert.pem` to every node as the
trusted CA pool.

## 4. Client TLS and authentication

### 4.1 Authentication modes (`-auth-mode`)

| Mode | Mechanism | Requires |
|---|---|---|
| `none` (default) | No authentication at all | Nothing — matches `v0.1.0` |
| `token` | Static bearer token, compared constant-time (`crypto/subtle`) | `-auth-token-file`, `-rbac-mapping-file` |
| `mtls` | Client TLS certificate identity | `-tls-cert`/`-tls-key`, `-tls-ca`, `-rbac-mapping-file` |

`-auth-token-file` format: one `<token>:<principal>` pair per line
(`#`-prefixed lines and blank lines ignored):

```
op-token-a1b2c3:alice
readonly-token-d4e5f6:bob
```

`-rbac-mapping-file` format: a JSON object mapping principal name
(a token file's principal, or an mTLS certificate's `CommonName`) to
one of the three fixed roles:

```json
{"alice": "operator", "bob": "read-only", "n1": "admin"}
```

`no OIDC/SSO/LDAP/SAML integration` — see §9.

### 4.2 RBAC (fixed roles)

Three fixed roles; no dynamic grants database. `admin` implies every
`operator`/`read-only` permission, encoded explicitly per endpoint
(`internal/authz`'s decision table), not via a computed hierarchy:

| Endpoint | `admin` | `operator` | `read-only` |
|---|---|---|---|
| `/status` | ✅ | ✅ | ✅ |
| `/metrics` | ✅ | ✅ | ✅ |
| `/health` | ✅ | ✅ | ✅ |
| `/propose` | ✅ | ✅ | ❌ |
| `/outcome` | ✅ | ✅ | ❌ |
| `/fault` | ✅ | ❌ | ❌ |
| `/admin/reload-tls` | ✅ | ❌ | ❌ |

`/fault` additionally requires the `-enable-fault-endpoint` flag; its
route is never registered on the HTTP mux without it, regardless of
role (§6).

Every response to a failed authentication or authorization check is a
fixed, generic body (`401 unauthorized` / `403 forbidden`) — never one
that reveals *why* (wrong token vs. unknown principal vs. insufficient
role).

### 4.3 Enabling client TLS

```bash
./chronicledb-node ... \
  -tls-cert=n1-cert.pem -tls-key=n1-key.pem \
  -auth-mode=token -auth-token-file=tokens.txt -rbac-mapping-file=rbac.json
```

Add `-tls-ca=ca-cert.pem` to additionally accept (optional, unless
`-auth-mode=mtls`) client certificates. `-auth-mode=mtls` requires
`-tls-ca` (to verify presented client certificates) in addition to
`-tls-cert`/`-tls-key`.

## 5. Peer mTLS

```bash
./chronicledb-node ... \
  -peer-tls-cert=n1-cert.pem -peer-tls-key=n1-key.pem -peer-tls-ca=ca-cert.pem
```

All three flags must be set together — a partial peer-TLS configuration
is refused at startup, never silently downgraded to plaintext
(`NO PLAINTEXT PEER REPLICATION`, `docs/invariants.md`). Once
configured: every inbound peer connection must present a certificate
verified against the trusted CA pool before any Raft message is read;
every outbound dial presents this node's own certificate and
independently verifies the specific peer it intended to reach
(not merely "some validly-signed certificate"). A connection whose
certificate identity does not match the `Message.From` field it then
sends is rejected.

## 6. `/fault` — off by default

`/fault` (real fault injection: `block`/`unblock`/`blocksend`/
`blockrecv`/etc. against this process's live peer connections) is
**not registered on the HTTP mux at all** unless `-enable-fault-endpoint`
is passed — a request to it gets `404`, not `403`, proving the route is
structurally absent, not merely access-controlled. When enabled, it
additionally requires `admin` authentication whenever `-auth-mode` is
not `none`. Never enable this flag on a production deployment; it
exists for the adversarial-testing/chaos-engineering harness
(`docs/testing-strategy.md`, `docs/adversarial-testing.md`).

## 7. Migration from an existing `v0.1.0` deployment

1. Generate a CA and per-node certificates (§3).
2. Roll out `-peer-tls-cert`/`-peer-tls-key`/`-peer-tls-ca` to every
   node — since there is no plaintext fallback, all nodes must switch
   together (a rolling mixed plaintext/TLS peer deployment is not
   supported in `v0.2.0`; that is exactly the kind of mixed-version
   rollout `docs/enterprise-v1-plan.md` §7 Compatibility/Rolling
   Upgrades is designed to handle for a *future* phase, not this one).
3. Add `-tls-cert`/`-tls-key` and choose an `-auth-mode`
   (`token` or `mtls`), distributing tokens/RBAC mappings to clients.
4. Confirm the "running without TLS/auth" warning has stopped appearing
   in the log.
5. Consider `-enable-fault-endpoint` only in non-production test
   environments.

`-auth-mode=none`/no-TLS remains available for local development —
it is **not** a supported permanent production configuration, and the
default is planned to flip in a future `v1.0.0` gate
(`docs/enterprise-v1-plan.md` §5's Compatibility implications, §13.2).

## 8. Audit logging

Every authenticated administrative decision (allow, deny-unauthenticated,
deny-unauthorized) is appended, once, to a hash-chained, checksummed
audit log — `<datadir>/audit` by default, or `-audit-log-dir`. A write
failure (disk full, permission error) **blocks the triggering action**
(fails closed) rather than letting it proceed unrecorded.

Format: `internal/audit`, deliberately WAL-like in mechanism (framed,
checksummed records, built on `internal/storage.Segment`) but a
physically separate file/format from the WAL — readable/verifiable even
by an operator who has audit-log access but not WAL access, and vice
versa. Each record's hash is chained to the previous record's hash;
tampering with any record (even one that also forges its own stored
hash/checksum) breaks the chain the next record still references. A
read-only operator CLI for querying the audit log is planned for a
future Operations phase (`docs/enterprise-v1-plan.md` §11); today, use
`internal/audit.ReadAll`/`Verify` as a library, or inspect it with the
package's own tests as a reference.

## 9. Non-goals (unchanged from `docs/enterprise-v1-plan.md` §5)

- OIDC/SSO/LDAP/SAML integration.
- A built-in certificate authority/PKI service.
- Per-row/per-column SQL-level authorization (RBAC here gates
  *endpoints*, not SQL rows/columns).
- HSM-backed private key storage.
- Built-in external secret-manager integration (mounting
  operator-managed secrets is an operational pattern, documented, not a
  built-in integration — see `docs/enterprise-v1-plan.md` §12,
  Production Deployment, for future *operational guidance*).
- Secure-by-default (deferred to the `v1.0.0` gate).

## 10. Related documents

- `docs/invariants.md` — `NO UNAUTHENTICATED ADMIN ACTION`,
  `NO PLAINTEXT PEER REPLICATION`, `FAULT SURFACE OFF BY DEFAULT`,
  `AUDIT COMPLETENESS`.
- `docs/failure-model.md` §6 — the extended adversarial-network threat
  model.
- `docs/configuration.md` — the full flag reference.
- `docs/adr/0015-security-foundation.md` — the architecture decision
  record for this phase.
- `SECURITY.md` — vulnerability reporting process (unchanged) and
  updated deployment assumptions.
