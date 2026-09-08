# ChronicleDB Enterprise V1 Architecture Roadmap

Status: **planning only**. Nothing in this document has been
implemented. No production code, test, ADR, or other doc has been
changed to produce it, and no maturity claim in
[`docs/roadmap.md`](roadmap.md) changes as a result of it existing.
Written against baseline commit `b850b1e` (`docs: add Break
ChronicleDB external review process`) — `main` clean, CI green, current
maturity `EXTERNAL-REVIEW READY` (Phase 12a; no external review has
concluded — see [`docs/roadmap.md`](roadmap.md)).

This document follows the same discipline as
[`docs/phase-12-plan.md`](phase-12-plan.md) and
[`docs/vision.md`](vision.md) §Guiding principle: it plans a path to a
claim, it does not make the claim. It does not authorize starting
`v0.2.0` implementation. That authorization, when it comes, is a
separate, later decision referencing this document.

## 0. What this document is, and is not

**Is**: the dependency-ordered engineering plan from the current
`EXTERNAL-REVIEW READY` engine to a specifically scoped release,
`v1.0.0`, called **`ChronicleDB Enterprise-Grade / Open-Source
Production-Oriented V1`**. It defines, for each of 14 work areas: the
current gap, the target architecture, new/extended invariants, formats
and APIs touched, failure semantics, expected packages/files, the test
strategy at every level, compatibility and security implications,
observability and documentation obligations, acceptance criteria, the
release gate that criteria feeds, dependencies on other areas, and
explicit non-goals. It proposes and justifies a release ladder
(`v0.2.0` through `v1.0.0`) and defines the exact evidence `v1.0.0`
requires.

**Is not**: an implementation of any of it. Every "packages/files
expected" list below is a plan, not a promise of exact shape — the
engineering session that actually builds a phase may reasonably deviate
from file/package names here, the same way Phases 1-12's actual
implementation sometimes named things differently than
[`docs/architecture.md`](architecture.md) §5's Phase-0 plan (e.g.
`internal/protocol` was never built as its own package; the client
surface stayed HTTP/JSON in `cmd/chronicledb-node`). What must not
deviate without a new ADR is the *invariant* and *failure-semantics*
content, per this repository's standing rule
([`docs/invariants.md`](invariants.md), [`docs/vision.md`](vision.md)
§Guiding principle).

### 0.1 What this roadmap does **not** claim, anywhere, ever

Per the task that produced this document, and consistent with
[`docs/non-goals.md`](non-goals.md) §Staff/Principal validation and
equivalence claims:

- **Not** "enterprise-proven" — no production deployment history exists
  or is claimed by reaching any gate below.
- **Not** "battle-tested" — no adversarial real-world traffic history
  exists or is claimed.
- **Not** "production-proven" — internal qualification (soak,
  black-box, chaos) is evidence of *readiness*, not of *operation*.
- **Not** a full CockroachDB/PostgreSQL replacement — no broad SQL
  compatibility, no PostgreSQL wire protocol, no arbitrary workload
  guarantee is added by this plan.
- **Not** multi-shard distributed SQL — explicitly out of scope; see
  §14.
- **Not** external validation that has not happened — any reference to
  "black-box testing" or "independent verification" below means
  *ChronicleDB-authored* tooling structurally forbidden from importing
  ChronicleDB internals (§10), which is evidence of testability and
  honesty about internal coupling, **not** a substitute for the actual
  external human review [`docs/break-chronicledb.md`](break-chronicledb.md)
  already solicits. `STAFF/PRINCIPAL DISCUSSION READY`
  ([`docs/roadmap.md`](roadmap.md) §Maturity Model) remains gated on
  that real external review, independent of and in addition to
  everything in this document.

§13 states precisely what evidence is required before `v1.0.0`, and
draws the `ENTERPRISE-GRADE` vs. `ENTERPRISE-PROVEN` line explicitly.

### 0.2 Explicit standing constraints inherited from the current roadmap

- No new phase in this plan is exempt from
  [`docs/invariants.md`](invariants.md): every existing invariant must
  keep holding; every new invariant added by a phase below must be
  added to that catalog (not a shadow catalog) using its documented
  format (statement, scope, why it matters, mechanism, threatened by,
  proof/test obligations) when that phase is actually implemented.
- No phase below reintroduces sharding, cross-shard transactions, or
  dynamic *shard* reconfiguration — that remains
  [ADR-0001](adr/0001-v1-single-shard-static-cluster-scope.md)'s scope
  boundary; §4's Dynamic Membership phase changes *cluster membership*
  (which nodes host the one Raft group), never the *number of shards*
  (still exactly one). See §14.
- Per [`docs/roadmap.md`](roadmap.md) Phase 12: "SQL and deployment
  work is not moved ahead of the correctness foundations... without a
  new ADR providing a strong, specific architectural reason." This
  plan's ordering (§3) is itself that justification exercise, applied
  to the 13 new work areas rather than repeating the original one.

## 1. Current-state baseline (facts, not aspiration)

Summarized from [`docs/roadmap.md`](roadmap.md),
[`docs/non-goals.md`](non-goals.md), and
[`docs/support-matrix.md`](support-matrix.md) — this section only
restates what already exists; see those documents for the authoritative
account.

**Implemented and evidenced**: durable segment storage + checksummed
WAL with crash recovery (`internal/storage`, `internal/wal`); MVCC
under Snapshot Isolation with first-committer-wins conflict detection
(`internal/mvcc`, `internal/txn`); a deterministic `internal/fsm.Apply`
boundary and durable `RequestID` idempotency; a deterministic Raft core
(`internal/raft`) and a real three-node TCP/disk deployment
(`internal/transport`, `internal/node`) with quorum commit, leader
failover, and `ReadIndex` strong reads; snapshots and log compaction
(`internal/snapshot`); combined randomized chaos testing
(`internal/fault`); a constrained SQL frontend over the same engine
(`internal/sql`); measured benchmarks and `/metrics`/`/health`
observability (`internal/metrics`, `internal/benchutil`); an
independent reference-model adversarial pass (`internal/oracle`); open
source packaging and a published `v0.1.0` tag; a published external
review process with zero findings recorded so far.

**Explicitly not implemented — the gap this plan closes**: no
authentication, no TLS anywhere (client or peer), no RBAC, no audit
log; no backup/restore format, no PITR; no rolling-upgrade story
(single binary version, no wire/format version negotiation); a
**static** three-node cluster only, no member add/remove; no admission
control (a slow/adversarial client can propose without bound); no MVCC
GC implementation (rule defined, [`docs/mvcc.md`](mvcc.md) §6, not
built) and no tombstone/disk reclamation; no structured logging, no
diagnostics bundle, no operator CLI, no alerts/SLOs/runbooks; no
Docker/systemd/Kubernetes/Helm packaging; no SBOM/signing/provenance;
no black-box (non-internal-importing) test harness; no nightly/soak
qualification; no formal performance-regression gate. Every one of
these gaps is either already named in
[`docs/non-goals.md`](non-goals.md) with its own "revisit when"
trigger, or is new scope this plan introduces because the "revisit
when" trigger has now been reached (`EXTERNAL-REVIEW READY`, published
review process, zero known correctness gaps).

## 2. Enterprise V1 target definition

**`ChronicleDB Enterprise-Grade / Open-Source Production-Oriented V1`**
means: a secure-by-default, recoverable, upgradeable in place,
observable, dynamically maintainable, single-shard replicated
transactional database, with objective engineering evidence
(deterministic tests, chaos tests, black-box tests, soak results,
measured performance, supply-chain artifacts) for every claim it makes
— at the same evidence-over-assertion discipline
[`docs/roadmap.md`](roadmap.md) §Maturity Model already holds Phases
1-12 to, extended to enterprise operational concerns.

It is explicitly **not** a claim of proven production usage,
third-party validation beyond whatever Phase 12 (`docs/break-chronicledb.md`)
actually yields by `v1.0.0`, or feature parity with an established
multi-shard distributed SQL system. §13 is the binding definition;
§0.1 is the binding list of disallowed claims.

## 3. Phase ordering rationale

The 13 build-phases below (§4 skips straight to the release ladder;
§5-§17 are the phases; §18 is out of scope) are ordered by a strict
dependency rule: **a phase may depend only on phases with a lower
number, and every dependency edge is justified below, not asserted.**
This is the same ordering discipline
[`docs/architecture.md`](architecture.md) §5 already applies to package
dependency direction, extended to phase sequencing.

```
Security Foundation (1)
        |
        v
Backup/DR (2) --------------------+
        |                         |
        v                         |
Compatibility/Rolling Upgrade (3) |
        |                         |
        v                         |
Dynamic Membership (4)            |
        |                         |
        v                         |
Admission Control (5) --+         |
        |                |        |
        v                v        v
Storage Lifecycle (6) <--+--------+
        |
        v
Operations/Observability (7)
        |
        v
Production Deployment (8)
        |
        v
Software Supply Chain (9)
        |
        v
Independent Black-Box Testing (10)
        |
        v
Long-Duration Qualification (11)
        |
        v
Performance Qualification (12)
        |
        v
Enterprise V1 acceptance gate (13)
```

Justification for each edge:

- **Security (1) first.** Every later phase adds a new admin surface
  (backup/restore triggers, upgrade prechecks, membership-change
  commands, admission-control tuning knobs, diagnostics bundles,
  deployment secrets). Building any of those before an authentication/
  authorization/audit foundation exists means either leaving them
  unauthenticated (a regression the moment they ship) or bolting auth
  onto each one separately later (repeated, inconsistent work). This
  mirrors why Phase 1 of the *original* roadmap (durable storage) came
  before everything that durably persists through it.
- **Backup/DR (2) before Compatibility (3).** A rolling-upgrade
  precheck/finalize/rollback design (§7) needs a safety net: if a
  finalize step turns out to be wrong, the documented recovery path is
  "restore from a pre-upgrade backup," which requires backup/restore to
  already exist and already be trusted. Building upgrade machinery
  before backup exists would leave upgrade's own worst-case failure
  mode without a tested recovery story.
- **Compatibility (3) before Dynamic Membership (4).** Membership
  changes (§8) add new Raft message types and new FSM commands
  (`AddServer`/`RemoveServer`/promotion). Those are exactly the kind of
  wire/FSM-command-format change Compatibility's version negotiation
  (§7) is designed to gate safely across a mixed-version cluster. If
  Dynamic Membership shipped first, its own new command type would be
  the first thing to break an N/N+1 mixed-version rolling upgrade,
  with no version-negotiation mechanism yet in place to catch it.
- **Dynamic Membership (4) before Admission Control (5).** Admission
  control's "control-plane/consensus priority" requirement (§9) must
  reason about *which* proposals are control-plane once membership
  changes are themselves a kind of control-plane proposal distinct
  from client `CommitTxn` proposals. Defining that priority split is
  cleaner once the full set of control-plane command kinds (including
  membership changes) is fixed.
- **Admission Control (5) and Storage Lifecycle (6) may build in
  parallel** (both depend only on 1-4) but Storage Lifecycle is listed
  after because MVCC GC's `GCWatermark` computation (§10, extending
  [`docs/mvcc.md`](mvcc.md) §6) should account for admission-control-
  induced backpressure state (a rejected/queued proposal must not skew
  the watermark) — a one-directional dependency of *design review*, not
  of code.
- **Operations/Observability (7) after 1-6.** The production metric
  catalog, diagnostics bundle, and operator CLI need something real to
  report on: auth/audit state (1), backup/restore/PITR status (2),
  upgrade precheck/finalize state (3), membership/learner state (4),
  admission-control queue depth/rejection counts (5), and GC/tombstone/
  disk-pressure state (6) are all things §11's metric catalog and
  diagnostics bundle must surface. Building observability first would
  mean re-deriving its own catalog once each of those phases lands.
- **Production Deployment (8) after Operations (7).** Kubernetes
  liveness/readiness probes, graceful-shutdown signal handling, and
  Helm-exposed configuration all consume the health/status/metrics
  surface Operations defines. Deployment packaging before observability
  exists would guess at probe endpoints instead of wiring to real ones.
- **Software Supply Chain (9) after Deployment (8).** SBOM and
  provenance attestation are most naturally generated per release
  *artifact* — and Deployment (8) is what defines the full artifact set
  (binaries, container images, Helm chart) that needs an SBOM/signature
  each, not just the existing five `scripts/build-release.sh` binary
  archives.
- **Black-Box Testing (10) after 1-9.** A black-box harness exercises
  the system exactly as an operator/client would: over TLS with real
  auth (1), against backup/restore (2), across a rolling upgrade (3),
  through a membership change (4), under admission pressure (5), with
  GC running (6), reading real metrics/diagnostics (7), against a real
  deployment topology (8), using signed/verified artifacts (9). It
  cannot meaningfully test surfaces that do not exist yet.
- **Long-Duration Qualification (11) after Black-Box (10).** Soak
  testing multiplies whatever the black-box harness already proves
  correct over a single run across time and iteration count; running it
  before the black-box harness exists would mean soaking untested
  scenarios, discovering both correctness bugs and duration-only bugs
  in the same undifferentiated signal.
- **Performance Qualification (12) after Long-Duration (11).**
  Performance regression thresholds are only meaningful once the system
  is known to be *correct* under load for a meaningful duration —
  chasing a throughput number on a system whose long-duration
  correctness is still unverified risks optimizing a code path a soak
  run would have shown was wrong.
- **Enterprise V1 acceptance gate (13) last, by definition** — it is
  the AND of every phase's own acceptance criteria plus the additional
  cross-cutting evidence in §13.2.
- **Enterprise-Scale V2 (14) is explicitly not on this critical path**
  — see §14; nothing in §5-§13 depends on it, and nothing in §14 may be
  started under this plan's authorization.

This ordering **validates** the task-provided release ladder (§4) as
technically sound rather than arbitrary; no reordering is proposed.

## 4. Release ladder

| Release | Phase(s) | Rationale for grouping |
|---|---|---|
| `v0.2.0` | §5 Security Foundation | Large enough to be its own release; every later phase depends on it (§3). |
| `v0.3.0` | §6 Backup / DR / PITR | Standalone capability; the recovery story compatibility (§7) needs. |
| `v0.4.0` | §7 Compatibility / Rolling Upgrades | Must land before any phase that changes wire/FSM-command format (Dynamic Membership). |
| `v0.5.0` | §8 Dynamic Membership | The highest-risk correctness-adjacent phase (touches Raft directly) — isolated in its own release for focused review/soak, not bundled with lower-risk work. |
| `v0.6.0` | §9 Admission Control + §10 Storage Lifecycle | Both are resource-protection/lifecycle concerns, buildable in parallel (§3), naturally reviewed together; neither individually is release-sized. |
| `v0.7.0` | §11 Operations + §12 Deployment + §13 Supply Chain | All three are "make it operable by someone who isn't the author" concerns, each individually smaller than a full release, and each consumes the previous one's output within this same release (§3). |
| `v0.8.0` | §14→ renumbered: Black-Box (§10 of task list) + Soak (§11) + Performance (§12) | The qualification trio — deliberately bundled because none of the three is a standalone shippable capability; together they are the evidence base for the RC. |
| `v0.9.0` | Enterprise V1 Release Candidate / hardening | Not a new phase — a stabilization pass fixing whatever `v0.8.0`'s qualification work found, with no new capability added. See §13.3. |
| `v1.0.0` | Enterprise V1 acceptance gate (§13) satisfied | First release entitled to the `ENTERPRISE-GRADE` claim, precisely scoped per §13.2. |

Note on numbering: the task's own §-list numbers Security..V2-out-of-scope
as items 1-14. This document keeps that numbering for §5-§18 below
(§5=item 1 Security Foundation ... §18=item 14 V2). The release-ladder
table above maps those items onto the task's proposed `v0.2.0`-`v1.0.0`
tags unchanged — the grouping is validated (§3), not altered.

---

## 5. Security Foundation

**Target release**: `v0.2.0`. **Depends on**: nothing (first phase).

### Current gap

No TLS on the client HTTP surface or the peer Raft TCP transport
(`internal/transport/transport.go` dials/accepts plaintext); no
authentication or authorization on any `cmd/chronicledb-node` HTTP
endpoint (`/status`, `/propose`, `/outcome`, `/fault`, `/metrics`,
`/health` are all open); no node identity beyond a configured string
`NodeID`; no audit trail of administrative actions; `/fault` (real
fault injection against a live process) is reachable by anyone who can
reach the port, in every build. Documented and accepted as a scope
decision through `EXTERNAL-REVIEW READY`
([`docs/non-goals.md`](non-goals.md) §Authentication and TLS,
[`docs/failure-model.md`](failure-model.md) §6, `SECURITY.md`) — this
phase is that gap's designated resolution point.

### Architecture

Layered, each layer depending only on the previous:

1. **Node identity** — a node's identity becomes its TLS certificate's
   subject (not just the free-text `-node-id` flag), issued by an
   operator-managed CA (V1 does not build a built-in CA/PKI service —
   see Non-goals). `internal/node` binds `NodeID` to the certificate at
   startup and refuses to start on a mismatch.
2. **Peer mTLS** — `internal/transport` requires and verifies a client
   certificate on every inbound peer connection and presents one on
   every outbound dial; connections without a valid peer certificate
   are rejected before any Raft message is read. No plaintext
   fallback: a misconfigured peer fails closed (connection refused),
   never silently downgrades.
3. **Client TLS** — `cmd/chronicledb-node`'s HTTP server terminates TLS
   using `crypto/tls` (stdlib only, per
   [`docs/dependencies.md`](dependencies.md)'s zero-external-dependency
   policy). Client mTLS is supported but not required (server-only TLS
   plus a separate auth token is also a valid V1 configuration).
4. **Authentication** — a pluggable check (interface, not a hardcoded
   mechanism) with two V1-shipped implementations: static bearer token
   (operator-provisioned, config-file/flag-supplied, compared with
   constant-time comparison) and mTLS client-certificate identity
   (reuses layer 3's already-verified certificate as the authenticated
   principal). No OIDC/SSO/LDAP integration — see Non-goals.
5. **RBAC** — three fixed roles (`admin`, `operator`, `read-only`);
   `admin` may call any endpoint including `/fault` and future
   backup/membership/upgrade endpoints; `operator` may call
   operational endpoints (`/status`, `/propose`, `/outcome`, future
   backup-trigger) but not `/fault` or membership changes; `read-only`
   may call `/status`, `/metrics`, `/health`, and SQL `SELECT`-only
   paths. Role assignment is static per credential (token-to-role or
   cert-subject-to-role mapping), not a dynamic grants database — a
   full grants/permissions model is Non-goal scope for V1's SQL layer.
6. **Audit logging** — every authenticated administrative action
   (auth success/failure, `/fault` invocation, future backup/restore/
   membership/upgrade actions) is appended to a dedicated,
   hash-chained (each record includes the previous record's hash) audit
   log file, physically separate from the WAL — auditability must
   survive even if a reader has WAL access but not audit-log access,
   and vice versa.
7. **Certificate rotation** — reloading TLS material (server cert/key,
   trusted CA pool) without a full process restart, triggered by
   `SIGHUP` or a new `admin`-only `/admin/reload-tls` endpoint; an
   overlap window during which both old and new peer certificates
   validate is required so a rolling rotation across a live cluster
   never causes a connectivity gap.
8. **Secure admin endpoints** — `/fault` specifically requires both
   `admin` role **and** an explicit `-enable-fault-endpoint` build/run
   flag defaulting to `false`; the flag's absence must make the
   handler structurally unreachable (registered conditionally, not
   just checked-and-rejected), so a production binary run with default
   flags cannot serve `/fault` even to an authenticated admin.

### Invariants (new, added to `docs/invariants.md` when implemented)

- **NO UNAUTHENTICATED ADMIN ACTION** — every state-changing
  administrative endpoint requires a successful authentication check
  before any side effect, with no code path that performs the side
  effect first and authenticates after.
- **NO PLAINTEXT PEER REPLICATION** — once peer TLS is configured for a
  node, `internal/transport` never sends or accepts an unencrypted Raft
  message on that node; there is no runtime toggle to disable it
  without a restart with different flags.
- **FAULT SURFACE OFF BY DEFAULT** — `/fault` is unreachable unless
  both the build/run flag and `admin` authentication are satisfied;
  proven by a test asserting the route is unregistered, not merely
  that calling it returns 403.
- **AUDIT COMPLETENESS** — every action gated by RBAC produces exactly
  one audit record (never zero, never more than one for a single
  logical action), and audit-write failure blocks the action rather
  than silently succeeding without a record.

### Formats/APIs affected

- New CLI flags: `-tls-cert`, `-tls-key`, `-tls-ca`, `-peer-tls-cert`,
  `-peer-tls-key`, `-peer-tls-ca`, `-auth-mode` (`token`|`mtls`|`none`
  — `none` only permitted with a loud startup warning, see
  Compatibility below), `-auth-token-file`, `-rbac-mapping-file`,
  `-enable-fault-endpoint` (default `false`, replacing today's
  unconditional registration).
- New HTTP status codes on existing endpoints: `401` (no/invalid
  credential), `403` (authenticated but insufficient role) — currently
  undefined since no auth exists.
- New audit log on-disk format: framed, checksummed, hash-chained
  records (deliberately WAL-like in mechanism, per
  [`docs/wal.md`](wal.md)'s existing framing approach — reuse, not
  reinvent, per [`docs/vision.md`](vision.md)'s "smallest technically
  real design" principle), versioned like every other durable format
  ([`docs/failure-model.md`](failure-model.md) §6).

### Failure semantics

- Invalid/expired/untrusted certificate → connection rejected at the
  TLS handshake; never falls back to plaintext or "trust anyway."
- Auth failure → `401`/`403` with a generic message (no information
  leak distinguishing "wrong token" from "unknown user" from "disabled
  account").
- Audit log write failure (disk full, permission error) → the
  triggering administrative action is itself rejected (fails closed);
  this is a deliberate availability-over-silent-blind-spot trade-off,
  consistent with `docs/observability.md`'s existing rule that
  diagnostic state is *not* a correctness dependency — audit logging is
  the one deliberate exception, because its entire purpose is a
  tamper-evident record, and a "best-effort" audit log is not one.
- Certificate rotation mid-flight → in-flight connections using the old
  certificate continue to validate until the overlap window ends; no
  live connection is forcibly dropped by rotation alone.

### Packages/files expected

- `internal/identity` (new): node identity binding, certificate
  loading/reload.
- `internal/transport/tls.go` (extends existing `transport.go`): mTLS
  dial/accept.
- `internal/authn` (new): pluggable authentication interface + token/
  mTLS implementations.
- `internal/authz` (new): RBAC role table and per-endpoint decision.
- `internal/audit` (new): hash-chained audit log writer/reader, layered
  on `internal/storage` framing primitives.
- `cmd/chronicledb-node/main.go`, new `cmd/chronicledb-node/auth.go`:
  flag wiring, HTTP middleware chain (TLS termination → authn → authz
  → audit → handler), conditional `/fault` registration.
- New doc: `docs/security.md` (threat model, configuration guide,
  supersedes the "no auth/TLS" section of `SECURITY.md` and
  [`docs/non-goals.md`](non-goals.md) §Authentication and TLS,
  replacing "deferred" with "implemented, `-auth-mode none` retained
  only for local development").

### Deterministic tests

TLS handshake accept/reject fixtures (valid, expired, wrong-CA,
self-signed, no-cert-when-required); RBAC decision-table tests (every
role × every endpoint); audit hash-chain integrity test (tamper with
one record, assert detection); token comparison timing-safety test;
`/fault` route-registration test under every flag combination.

### Integration tests

Real three-node cluster with peer mTLS enabled end-to-end (genuine TCP,
genuine certs) proving replication still works and a missing/invalid
peer cert is rejected by a real listener, not a mock; real subprocess
certificate rotation under continuous write load with zero dropped
commits across the rotation.

### Chaos tests

Certificate rotation initiated during an active network partition;
audit-log disk-full injected mid-administrative-action, asserting the
action is rejected and no partial audit record exists; expired
certificate discovered mid-session (connection must be torn down, not
grandfathered).

### Compatibility implications

Breaking for existing plaintext, unauthenticated `v0.1.0`-style
deployments. `v0.2.0` ships **secure-by-configuration, not yet
secure-by-default**: `-auth-mode none`/no-TLS remains available and is
the flag-default at `v0.2.0` (matching `v0.1.0` behavior exactly, so an
in-place binary swap does not silently break an existing trusted-network
deployment), but startup prints a loud, repeated warning when running
without TLS/auth, and `docs/security.md` documents this as a **required
migration**, not a permanent option. Flipping the default to
secure-by-default is itself a required §13.2 gate for `v1.0.0` — see
§13.

### Security implications

This phase *is* the security phase. Threat model addition to
[`docs/failure-model.md`](failure-model.md) §6: adversarial network
(not just untrusted-network-but-benign-actor) becomes an in-scope
threat once TLS/auth exist to defend against it; Byzantine node
behavior remains explicitly out of scope (§5 of
[`docs/failure-model.md`](failure-model.md), unchanged — TLS/auth
protect against unauthorized access, not against an authorized-but-
malicious node, which Raft itself does not defend against either).

### Observability

New metrics: auth success/failure counts (no credential value in any
label, per [`docs/observability.md`](observability.md) §9's existing
no-high-cardinality-label rule), certificate-expiry-remaining gauge per
configured certificate, audit-write-failure counter. Audit log itself
is queryable via the future operator CLI (§11), read-only, `admin`
role required.

### Documentation

`docs/security.md` (new); `SECURITY.md` operational section updated
(reporting process untouched); `docs/non-goals.md` §Authentication and
TLS updated from "deferred" to "resolved, see docs/security.md";
`docs/failure-model.md` §6 extended with the adversarial-network threat
model addition above; `docs/configuration.md` gains the new flags.

### Acceptance criteria

- Every administrative HTTP endpoint requires authentication; every
  role boundary is enforced and tested (RBAC decision-table test
  passes for all role × endpoint pairs).
- Peer replication traffic is encrypted and mutually authenticated when
  configured; a real three-node cluster runs the full existing
  scenario corpus ([`docs/scenario-corpus.md`](scenario-corpus.md))
  with mTLS enabled, unchanged pass rate versus plaintext.
- `/fault` is unreachable by default in a plain `go build`; reachable
  only with the explicit flag plus `admin` auth, proven by test.
- Audit log hash-chain integrity is proven under fuzzing/tampering
  tests; audit-write failure demonstrably blocks the triggering action.
- Certificate rotation proven with zero replication interruption in a
  real-cluster integration test.

### Release gate

`v0.2.0`.

### Dependencies

None — first phase.

### Explicit non-goals

OIDC/SSO/LDAP/SAML integration; a built-in certificate authority/PKI
service (operators bring their own CA, documented in
`docs/security.md`); per-row/per-column SQL-level authorization
(RBAC here gates *endpoints*, not SQL rows/columns — a future,
separately-ADR'd extension if ever needed); HSM-backed private key
storage; external secret-manager integration (deferred to §12
Production Deployment's "TLS/secret handling" as *operational
guidance* — mounting operator-managed secrets — not a built-in
integration); secure-by-default at `v0.2.0` itself (explicitly deferred
to the `v1.0.0` gate, §13.2, with the migration path documented here).

---

## 6. Backup / Disaster Recovery

**Target release**: `v0.3.0`. **Depends on**: §5 Security Foundation
(backup/restore triggers are administrative actions requiring RBAC +
audit).

### Current gap

No backup mechanism exists. The only current recovery path is Raft/WAL
replay from surviving nodes' own persisted state
([`docs/recovery.md`](recovery.md)); simultaneous loss of a majority of
nodes' storage is explicitly out of guarantee scope
([`docs/failure-model.md`](failure-model.md) §5). There is no way to
recover from that scenario, no portable export format, and no
point-in-time recovery.

### Architecture

A backup is a **self-describing, versioned export of a consistent
snapshot boundary plus the log suffix needed to reach a chosen point in
time**, deliberately reusing existing mechanism rather than inventing a
new one:

- **Full backup** = the existing `internal/snapshot` format (already
  checksummed, versioned, atomically created — Snapshot Safety
  invariant) plus a manifest describing it.
- **Incremental/PITR backup** = a full backup plus the WAL segment
  range from that snapshot's boundary forward (already durable,
  checksummed, ordered — reusing `internal/wal`'s own record format
  rather than a parallel log), up to an operator-chosen index/time.
- **Manifest**: a small JSON/binary document recording: backup format
  version, ChronicleDB version that produced it, cluster ID, snapshot's
  `lastIncludedIndex`/`lastIncludedTerm`, included WAL segment range and
  their checksums, a manifest-level checksum, and creation timestamp
  (diagnostic only — never a correctness input, consistent with
  [`docs/architecture.md`](architecture.md) §4's "never wall-clock
  timestamps" rule for `CommitSeq`/`StartSeq`; the backup manifest
  timestamp is purely for operator/human bookkeeping).
- **Restore into a clean cluster**: a new node/cluster started with
  `-restore-from=<backup-dir>` validates the manifest, loads the
  snapshot via the existing `internal/snapshot` load-with-validation
  path, replays the included WAL suffix via the existing
  `internal/wal.Replay`, and only then joins/forms a Raft group —
  restore is architecturally "recovery from a portable source" reusing
  the exact same validated code paths as restart-from-local-disk
  recovery, not a new decode path.
- **RPO/RTO model**: documented per backup schedule choice — RPO is
  bounded by "time since last included WAL segment" (as low as the
  segment-close interval if continuous WAL archiving is configured, or
  the full snapshot interval otherwise); RTO is bounded by "restore
  time" = manifest validation + snapshot load + WAL suffix replay,
  measured and published the same way §12 measures restart-recovery
  time today ([`docs/benchmarks.md`](benchmarks.md) §7).

### Invariants (new)

- **BACKUP INTEGRITY** — a restore never trusts a backup whose manifest
  checksum, per-segment checksums, or snapshot checksum fail
  validation; corruption is rejected outright (mirrors `RECOVERY
  NON-INVENTION`, applied to an external input instead of local disk).
- **BACKUP CONSISTENCY** — a restored cluster's state is exactly the
  state a legitimate node would have reached by replaying the identical
  committed history through the same boundary — no partial-transaction,
  no reordered-command restore is ever possible (mirrors `ATOMICITY`/
  `RECOVERY NON-INVENTION` for the restore path specifically).
- **DESTRUCTIVE RESTORE ISOLATION** — restoring into a cluster never
  silently overwrites an existing, live cluster's data; restore targets
  a explicitly clean data directory only, refusing to run against one
  containing existing WAL/snapshot state without an explicit
  `-force-overwrite` flag requiring `admin` auth and producing an audit
  record.

### Formats/APIs affected

New backup manifest format (versioned, per
[`docs/failure-model.md`](failure-model.md) §6's "every durable format
carries an explicit version field" rule); new CLI:
`chronicledb-node -backup-to=<dir>` (or a new `chronicledb-ctl backup`
subcommand — see §11), `-restore-from=<dir>`, `-restore-until=<index|
time>` for PITR; new `admin`-gated HTTP endpoints `/admin/backup` and
`/admin/restore` (audited per §5).

### Failure semantics

- Backup creation crash mid-export → partial backup directory is never
  trusted; a `.manifest.tmp`-then-atomic-rename pattern (reusing
  `internal/snapshot`'s existing atomic-creation mechanism, §3 there)
  ensures a manifest only exists once the backup is complete and valid.
- Corrupted backup (bad checksum anywhere in the manifest, snapshot, or
  WAL segment range) → restore refuses to start, exits non-zero with a
  specific diagnostic, never guesses or partially restores.
- Restore interrupted mid-way (process killed during replay) →
  restarting the restore from the same backup directory is safe/
  idempotent (replay is already idempotent by construction — replaying
  the same committed history twice into a from-scratch data directory
  produces the same result, since nothing was acknowledged to an
  external client mid-restore).
- Disk-full during backup export → the backup is treated as failed
  (never "trusted" — no manifest is written), the partial directory is
  left for operator inspection or cleaned up by an explicit `-force`
  retry, matching §1.9's existing out-of-space handling.

### Packages/files expected

- `internal/backup` (new): manifest encode/decode, export (snapshot +
  WAL-range copy + checksum), restore (validate + load + replay),
  built on `internal/snapshot` and `internal/wal` — never duplicating
  their decode logic.
- `cmd/chronicledb-node/backup.go`: CLI flag wiring, HTTP handlers.
- New doc: `docs/backup.md` (format, RPO/RTO model, restore runbook).

### Deterministic tests

Manifest encode/decode round-trip and fuzz test (mirrors
`internal/snapshot/fuzz_test.go`'s existing pattern); corruption-
injection tests at every layer (bad manifest checksum, bad snapshot
checksum, bad WAL segment checksum, truncated segment) each asserting
restore refusal, not partial success; PITR boundary tests (restore to
an index that lands exactly on/before/after a transaction boundary).

### Integration tests

Real backup of a live three-node cluster under concurrent write load;
real restore of that backup into a fresh, empty cluster on real disk,
proving byte-identical committed state to what the source cluster
actually held at the backup boundary (checked against
`internal/oracle`'s existing reference-model approach, reused not
reinvented).

### Chaos tests

**Destructive restore drills** (explicitly required by the task): kill
the source cluster entirely (simulate total loss — delete all three
nodes' data directories after taking a backup), restore from backup
alone, and prove the restored cluster reaches a state consistent with
everything that was durably backed up (and only that — nothing
manufactured, nothing lost within the backup's own RPO boundary).
Backup taken during an active chaos schedule (partitions/crashes
in-flight) must still produce a valid, restorable artifact reflecting
some real consistent prior point, never a torn or contradictory one.

### Compatibility implications

New, additive on-disk backup format — does not change the existing
WAL/snapshot on-disk format (reuses it as-is via copy + manifest, not a
new encoding), so this phase does not itself trigger a `docs/wal.md`/
`docs/snapshots.md` format version bump.

### Security implications

Backup/restore are `admin`-only, audited actions (§5). A backup
directory contains the same sensitive data the live cluster does and
must be documented as requiring equivalent access control/encryption-
at-rest by the operator — ChronicleDB does not encrypt the backup
artifact itself in V1 (see Non-goals).

### Observability

Backup/restore duration, size, and success/failure counters; last-
successful-backup timestamp gauge (diagnostic only, per
`docs/observability.md`'s existing rule).

### Documentation

`docs/backup.md` (new); `docs/failure-model.md` gains a new §
"Simultaneous majority storage loss" cross-referencing backup/restore
as the resolution path for the scenario currently listed as "explicitly
outside V1 guarantee scope" (§5 there) — narrowing that scope
explicitly, with an ADR, rather than silently.

### Acceptance criteria

- A real destructive restore drill (full cluster data loss, restore
  from backup alone) succeeds and is reproducible.
- Corrupted/truncated backups are rejected in every injected corruption
  case, never partially trusted.
- Measured RPO/RTO numbers are published for at least two backup
  schedules (snapshot-only vs. continuous WAL archiving).
- PITR restores to an arbitrary committed boundary produce state
  matching the reference model exactly.

### Release gate

`v0.3.0`.

### Dependencies

§5 Security Foundation (RBAC + audit on backup/restore endpoints).

### Explicit non-goals

Encryption of backup artifacts at rest (operator responsibility,
documented, not built); backup to a specific cloud object-store API
(V1 ships local-filesystem-path backup/restore only — an operator
copies the directory to S3/GCS/etc. themselves; no built-in cloud SDK
integration, consistent with the zero-external-dependency policy);
continuous/streaming replication to a warm standby (a different feature
from point-in-time backup, out of scope here); automatic scheduled
backups (operators use `cron`/their own scheduler against the CLI —
V1 does not build an in-process scheduler).

---

## 7. Compatibility / Rolling Upgrades

**Target release**: `v0.4.0`. **Depends on**: §5 Security Foundation
(upgrade-precheck endpoint is administrative), §6 Backup/DR (rollback
safety net).

### Current gap

`internal/version` reports a build-time version string but nothing
reads it to make a compatibility decision; there is no wire-protocol
version, no WAL/snapshot/FSM-command format version negotiation, and no
tested mixed-version behavior. Every current test runs one binary
version against itself. [`docs/versioning.md`](versioning.md)
documents SemVer policy for *what counts as breaking* but not *how a
live cluster survives a breaking change during rollout* — this phase
builds that mechanism.

### Architecture

Five independently versioned surfaces, each gaining an explicit version
field checked at the point it is interpreted (extending the existing
`docs/failure-model.md` §6 rule that "every durable format carries an
explicit version field" from WAL/snapshot to all five):

1. **Wire protocol version** — `internal/transport`'s connection
   handshake exchanges each peer's supported version range before any
   Raft message; a peer outside the supported range is rejected with a
   diagnostic, not silently misinterpreted.
2. **WAL format version** — already has a version field per record
   framing ([`docs/wal.md`](wal.md) §3); this phase adds an explicit
   **format generation** concept so a new record kind or field can be
   introduced without breaking N-1 readers during rollout (N+1 writes
   old-format records until finalize, §below).
3. **Snapshot format version** — already versioned
   ([`docs/snapshots.md`](snapshots.md) §5); same generation treatment.
4. **FSM command format version** — `internal/fsm`'s command encoding
   gains an explicit version tag per command; an N-version node
   applying an N+1-version command it does not understand fails closed
   (refuses to apply, halts) rather than silently misinterpreting bytes
   — this is the highest-risk surface (a `STATE MACHINE SAFETY`
   invariant boundary) and the primary reason this phase exists before
   Dynamic Membership.
5. **Metadata/schema version** — SQL DDL-produced schema encoding
   (`internal/sql/schema.go`) and cluster metadata (membership list,
   once §8 exists) both gain the same version tag.
6. **Cluster version / finalize** — a cluster-wide, Raft-replicated
   "minimum understood version" value (itself an FSM command, subject
   to rule 4 above). `precheck` confirms every live node reports
   support for the target version; `finalize` is an explicit,
   `admin`-gated, audited action that raises the cluster version once
   every node has actually been upgraded — **before finalize**, new
   nodes must still write old-generation formats (so an old node can
   still be rolled back and rejoin); **after finalize**, new-format
   writes are permitted and rollback below the finalized version is no
   longer supported (this is the rollback boundary, documented, not
   silent).

### Invariants (new)

- **NO SILENT FORMAT MISINTERPRETATION** — a node never applies,
  replays, or installs a record/command/snapshot/schema whose version
  it does not recognize; it fails closed with a diagnostic.
- **ROLLBACK BOUNDARY HONESTY** — the system never claims rollback
  safety past the last `finalize` boundary; `docs/upgrades.md` and the
  CLI both surface exactly where that boundary currently is.
- **MIXED-VERSION QUORUM SAFETY** — during an N/N+1 rolling upgrade
  window, Raft quorum safety (`RAFT ELECTION SAFETY`, `QUORUM SAFETY`)
  continues to hold regardless of which subset of the cluster is on
  which version — an N+1 leader with an N follower (or vice versa)
  never produces a state divergence, because pre-finalize the wire
  protocol constrains both sides to the lowest commonly-understood
  command generation.

### Formats/APIs affected

Wire handshake (new); WAL/snapshot/FSM-command/schema encodings all
gain a version/generation field (additive — existing `v0.1.0`-produced
files remain readable, since generation 0 is defined as today's exact
existing format, never redefined); new `admin` endpoints
`/admin/upgrade/precheck` and `/admin/upgrade/finalize`; new CLI flag
`-upgrade-precheck` (dry-run check against the running cluster).

### Failure semantics

- A node encountering a command/record generation it does not
  understand halts applying (does not crash the process outright,
  where avoidable, but refuses further progress on that log) and
  surfaces a clear operator diagnostic — this is a deliberate
  availability-over-silent-corruption trade-off, the same posture
  `RECOVERY NON-INVENTION` already takes for corrupted (as opposed to
  merely newer-format) data.
- `finalize` called while any node still reports an old version fails
  the finalize action outright (checked against the same live
  membership/version state `precheck` reports) rather than partially
  finalizing.
- A crash mid-rolling-upgrade (some nodes upgraded, some not, no
  finalize yet) is safe by construction: the cluster continues
  operating in the lower generation's semantics until finalize,
  regardless of which physical binary version each node happens to be
  running.

### Packages/files expected

- `internal/version` (extends existing): generation constants, a
  version-negotiation helper shared by transport/WAL/snapshot/FSM.
- `internal/transport`: handshake extension.
- `internal/wal`, `internal/snapshot`, `internal/fsm`, `internal/sql/schema.go`:
  each gains generation-aware encode/decode, backward-compatible by
  construction (a decoder for generation N+1 must still read every
  prior generation it has not yet dropped support for, per
  [`docs/versioning.md`](versioning.md)'s own N/N+1 policy applied to
  binary formats, not just APIs).
- `internal/node`: cluster-version FSM command, precheck/finalize
  orchestration.
- New doc: `docs/upgrades.md` (the upgrade runbook: precheck → roll
  nodes one at a time → verify → finalize; rollback procedure before
  finalize).

### Deterministic tests

Version-negotiation handshake tests (compatible range, incompatible
range, one-sided-only-support); generation-aware encode/decode fuzz
tests for each of the five formats, including "old generation still
decodes under the new binary" as an explicit assertion; finalize-
rejected-with-stale-node test.

### Integration tests

Real mixed-binary cluster: build two actual `chronicledb-node` binaries
(current `main` and a synthetic "N+1" test build with a deliberately
bumped generation and one new field/command), run them together in a
real three-node deployment, exercise the full existing scenario corpus
against the mixed cluster, roll all three to N+1, finalize, confirm
N+1-only behavior activates only after finalize.

### Chaos tests

Crash a node mid-rolling-upgrade (between individual node restarts,
before finalize) and confirm the cluster continues serving correctly
in the lower generation; partition during a rolling upgrade; kill the
process that issued `finalize` mid-call and confirm finalize is either
fully applied (as a committed FSM command) or not applied at all, never
partially.

### Compatibility implications

This phase **is** the compatibility mechanism — it is what makes every
later phase's format changes (membership commands in §8, admission-
control state in §9, GC/tombstone metadata in §10) safely rollable
rather than requiring full-cluster downtime. `v0.1.0`'s and
`EXTERNAL-REVIEW READY`'s current formats become "generation 0" under
this scheme, unchanged.

### Security implications

Precheck/finalize are `admin`-gated, audited actions (§5). Version-
handshake information (what versions a node supports) is not considered
sensitive but is logged for audit/diagnostic purposes.

### Observability

Cluster version gauge, per-node reported-version gauge, precheck
pass/fail history, finalize event in the audit log and as a distinct
metric (elections/leader-changes-style counter, per
[`docs/observability.md`](observability.md)'s existing pattern).

### Documentation

`docs/upgrades.md` (new); `docs/versioning.md` extended with the binary
format compatibility policy (currently scoped to "API surfaces," not
on-disk formats); `docs/wal.md`/`docs/snapshots.md` each gain a
"generation" section describing the versioning scheme as implemented.

### Acceptance criteria

- A real mixed-binary N/N+1 cluster passes the full existing scenario
  corpus throughout a rolling upgrade, before finalize.
- Finalize is proven atomic (committed FSM command) under crash
  injection.
- Rollback (redeploying the N binary onto an N+1-upgraded-but-not-yet-
  finalized node) is proven safe by real test; rollback after finalize
  is proven to be correctly refused/documented as unsupported.
- Every one of the five format surfaces has a passing generation-aware
  fuzz/round-trip test.

### Release gate

`v0.4.0`.

### Dependencies

§5 Security Foundation, §6 Backup/DR.

### Explicit non-goals

Automatic/unattended rolling upgrades (an operator or external
orchestrator — e.g. a future Kubernetes operator, itself out of scope
per §12's non-goals — drives node-by-node restart; ChronicleDB provides
the safety mechanism, not the orchestration); N/N+2 (skip-version)
upgrade support (only N/N+1 adjacent-version mixed operation is
supported and tested, matching most production database upgrade
policies); live schema migration tooling beyond the existing SQL DDL
(`ALTER TABLE`-style evolution is separate, unscoped SQL-surface work,
not a compatibility-mechanism concern).

---

## 8. Dynamic Membership

**Target release**: `v0.5.0`. **Depends on**: §5, §6, §7 (especially
§7's FSM-command versioning, since membership changes add new command
kinds).

### Current gap

The Raft cluster is static — three nodes, fixed at creation
([ADR-0001](adr/0001-v1-single-shard-static-cluster-scope.md),
[`docs/architecture.md`](architecture.md) §1). There is no add/remove/
promote mechanism, no learner/catch-up state, and no tested crash/
partition behavior during a membership change (because none can happen
today).

### Architecture

Single-server membership changes (Raft paper §6's simpler, safer
alternative to joint consensus — appropriate for V1's three-to-five-
node scale where changing one voter at a time is never itself
unavailable-inducing, and avoids joint consensus's added complexity
without a demonstrated need for it):

1. **Learner/catch-up** — a new node joins as a **learner**: it
   receives log replication (and, if far behind, a snapshot install via
   the existing `internal/snapshot` follower-catch-up path,
   [`docs/snapshots.md`](snapshots.md) §7) but does not count toward
   quorum and cannot vote. This reuses the *existing* snapshot-install
   and log-replication mechanism unchanged — a learner is architecturally
   "a follower that doesn't vote yet," not new replication mechanism.
2. **Promotion** — once a learner's `matchIndex` reaches within a
   configurable threshold of the leader's log, an `admin`-gated
   `PromoteToVoter` command (a new FSM/Raft configuration-change
   command, versioned per §7) is proposed and committed through the
   *existing* Raft log — membership changes are themselves log entries,
   applied via the same `Apply` path as any other command, preserving
   `STATE MACHINE SAFETY`.
3. **Removal/decommission** — `RemoveServer` similarly proposed and
   committed as a log entry; a removed voter stops counting toward
   quorum the moment the entry is applied (single-server-change safety:
   the cluster is never simultaneously reasoning about two overlapping
   quorum definitions, which is exactly what joint consensus exists to
   handle for *concurrent* multi-node changes — V1 deliberately
   disallows starting a second membership change before the first
   commits, sidestepping that problem instead of solving it).
   `decommission` additionally drains the node (stops accepting new
   client connections, confirms no longer needed for quorum) before the
   operator is told it is safe to stop the process.
4. **Restart persistence** — current membership (voters + learners) is
   part of durable state (already implicitly true — it's a sequence of
   committed log entries — but this phase adds an explicit "current
   configuration" derived view, snapshotted like any other FSM state so
   restart/catch-up doesn't require replaying the entire membership
   history from index 0).
5. **Crash/partition safety during changes**: a leader crash mid-
   membership-change is safe because the change is just a log entry —
   either it committed (and the new leader's log reflects it, per
   `LEADER COMPLETENESS`) or it didn't (and it's as if the change never
   happened). A partition during a change cannot produce two
   simultaneously-valid quorum definitions because single-server
   changes are serialized (rule 3 above) and the change itself must
   reach the *old* quorum to commit, per standard Raft membership-
   change safety reasoning.

### Invariants (new)

- **SERIALIZED MEMBERSHIP CHANGE** — at most one membership-change
  command may be outstanding (proposed but not yet committed/aborted)
  at a time; a second is rejected until the first resolves.
- **QUORUM CONTINUITY** — at every point in the committed log, there is
  a well-defined, single quorum requirement (majority of the *current*
  voter set as of that log position) — no ambiguity about which
  configuration a given commit decision used.
- **LEARNER NON-INTERFERENCE** — a learner never counts toward quorum,
  never votes, and its absence/crash/slowness never blocks or delays
  commit progress for the voting set.

### Formats/APIs affected

New FSM/Raft commands: `AddLearner`, `PromoteToVoter`, `RemoveServer`
(each versioned per §7); new `admin` HTTP endpoints
`/admin/membership/add`, `/admin/membership/promote`,
`/admin/membership/remove`, `/admin/membership/status`; membership
configuration becomes part of snapshotted state
([`docs/snapshots.md`](snapshots.md) format gains a membership-config
section, itself versioned).

### Failure semantics

- Learner crash before promotion → simply a slow/absent learner; no
  effect on voter quorum (Learner Non-Interference).
- Leader crash with a membership change committed-but-not-yet-applied →
  new leader applies it from the log normally, identical to any other
  committed-not-yet-applied entry (§2.3 of
  [`docs/failure-model.md`](failure-model.md), extended to cover this
  command kind explicitly).
- Removing the current leader → handled as a special case: the leader
  steps down (or the removal doesn't take effect for the node issuing
  it until after a new leader is elected among the remaining voters) —
  documented precisely, not left implicit, since naively applying "you
  removed yourself" mid-leadership is a known Raft membership-change
  footgun.
- Partition isolates a node with a pending (uncommitted) membership
  change proposal → the proposal is not committed until it reaches the
  old quorum; the isolated minority cannot unilaterally change
  membership, consistent with `QUORUM SAFETY`.

### Packages/files expected

- `internal/raft`: new message/command types for configuration change,
  extending `messages.go`/`types.go`; `Core`'s commit-rule logic
  extended to reason about "quorum as of the configuration active at
  each log position" rather than a single fixed `N/2+1` constant.
  Revisits [ADR-0001](adr/0001-v1-single-shard-static-cluster-scope.md)
  with a new ADR narrowing "static" to "static-unless-changed-via-this-
  mechanism," not removing the single-shard constraint (§14).
- `internal/node`: learner lifecycle, promotion threshold logic, drain-
  before-decommission sequencing.
- `internal/snapshot`: membership-config section in the snapshot
  format.
- New doc: `docs/membership.md`.

### Deterministic tests

Single-server-change safety property tests (simulator-level, extending
`internal/fault`'s existing chaos harness): add/promote/remove combined
with elections, partitions, crashes, checked against `QUORUM
CONTINUITY` and `SERIALIZED MEMBERSHIP CHANGE` after every step;
learner-never-counts-toward-quorum test; self-removal-of-current-leader
scenario test.

### Integration tests

Real cluster: start with three voters, add a real fourth node as
learner over real TCP/disk, prove it catches up via replication (small
lag) and via snapshot install (large lag, reusing the existing SN-\*
test pattern), promote it, remove an original voter, confirm the
cluster continues serving with the new membership and the removed
node's process can be safely stopped per `decommission`.

### Chaos tests

Crash the leader mid-membership-change; partition the learner during
catch-up; partition a voter being removed before removal commits;
combined randomized schedule (elections + crashes + partitions +
membership changes together, extending `internal/fault/chaos_test.go`'s
existing combined-fault-schedule pattern) run for the same seed-count
discipline Phase 7 established
([`docs/testing-strategy.md`](testing-strategy.md) §3.3).

### Compatibility implications

New FSM/Raft command kinds — must be introduced through §7's
generation mechanism; a mixed-version cluster mid-upgrade must not
accept a membership-change command a not-yet-upgraded node cannot
understand (enforced by §7's cluster-version gate: membership commands
require the cluster to already be at the generation that defines them).

### Security implications

All membership endpoints are `admin`-gated and audited (§5); a newly
added node's identity is verified via mTLS (§5) before it is ever
allowed to become a learner — an unauthenticated process cannot join
the cluster and receive replicated data.

### Observability

Current voter/learner set exposed via `/status`; membership-change
history (add/promote/remove events with outcome) in metrics and audit
log; learner catch-up progress (lag behind leader) gauge.

### Documentation

`docs/membership.md` (new, membership-change runbook); `docs/raft.md`
extended with the configuration-change mechanism and its safety
argument; `docs/architecture.md` §1's "static three-node cluster"
statement updated to "static unless changed via the documented
membership mechanism," cross-referencing the new ADR.

### Acceptance criteria

- Real add/promote/remove cycle proven end-to-end on a real cluster,
  including via snapshot-based catch-up for a far-behind learner.
- Combined chaos schedule (membership changes + partitions + crashes)
  run at the same seed-count discipline as existing Phase 7 chaos,
  clean.
- Self-removal-of-leader and removal-during-partition scenarios
  explicitly tested and documented.
- New ADR revisiting ADR-0001 published and internally consistent with
  every other document referencing "static three-node cluster."

### Release gate

`v0.5.0`.

### Dependencies

§5, §6, §7.

### Explicit non-goals

Joint consensus / arbitrary concurrent multi-node membership changes
(single-server-change only, per the architecture rationale above);
automatic membership rebalancing or failure-triggered auto-replacement
(purely operator/admin-triggered in V1 — no automatic "detect a dead
node and replace it" control loop, which would itself require policy
decisions, e.g. how long to wait, explicitly out of V1 scope); changing
the number of *shards* (still exactly one, always — see §14); cluster
sizes beyond a small, explicitly bounded range (this phase targets
three-to-seven-voter clusters, consistent with typical Raft deployment
sizing; arbitrarily large membership is not a design goal).

---

## 9. Admission Control / Resource Protection

**Target release**: `v0.6.0` (bundled with §10). **Depends on**: §5,
§6, §7, §8 (needs the full set of control-plane command kinds,
including membership changes, fixed first — §3).

### Current gap

`internal/node.Node.Propose` and the SQL execution path
(`internal/sql/exec.go`) accept client work with no bound on concurrent
in-flight proposals/transactions and no queue depth limit; a client
(or many clients) issuing unbounded concurrent requests can exhaust
memory or starve the Raft control-plane's own message processing.
Nothing in the current codebase distinguishes "Raft heartbeat/vote
traffic" from "client proposal traffic" for scheduling priority.

### Architecture

- **Bounded queues** — every entry point that accepts client work
  (`Node.Propose`, SQL statement execution, future backup/restore
  triggers) is fronted by a bounded queue/semaphore; a full queue
  rejects new work immediately (explicit backpressure) rather than
  growing unboundedly or blocking indefinitely.
- **Bounded concurrent proposals/transactions** — a configurable cap on
  in-flight (proposed-but-not-yet-applied) proposals and open
  transactions per node; exceeding it rejects new work with a specific,
  retryable error (distinct from a conflict abort — this is capacity,
  not correctness).
- **Overload rejection/backpressure** — rejection is explicit and fast
  (fail fast under overload, per standard admission-control practice)
  rather than accepting work and letting latency degrade unboundedly;
  a rejected client can retry (its `RequestID`, if any, was never
  consumed, since rejection happens before `Propose`, per `IDEMPOTENCY`
  — a rejected request was never applied, so retry is always safe).
- **Control-plane/consensus priority** — Raft's own internal traffic
  (heartbeats, `AppendEntriesRPC`/`RequestVoteRPC` processing, and, per
  §8, membership-change commands) is processed on a priority path never
  blocked behind a full client-work queue — starving Raft's own
  liveness because of client overload would itself become an
  availability bug (`RAFT ELECTION SAFETY` in practice depends on
  timely heartbeat processing, `docs/raft.md` §2).
- **Memory/CPU/storage pressure** — a resource-pressure signal
  (available disk headroom below a configured threshold, informed by
  `internal/storage`'s existing knowledge of segment sizes; memory
  pressure via Go runtime stats) feeds into admission decisions:
  approaching disk-full tightens admission before an actual `ENOSPC`
  (proactively addressing the "possible future enhancement" §1.9 of
  [`docs/failure-model.md`](failure-model.md) already named).
- **Snapshot/backup resource limits** — snapshot creation (§`internal/snapshot`)
  and backup export (§6) are themselves rate/priority-limited relative
  to normal client traffic — a running backup must not starve the
  cluster's own commit path, and vice versa.

### Invariants (new)

- **BOUNDED ADMITTED WORK** — the number of concurrently admitted
  (proposed-but-unapplied) proposals never exceeds the configured
  bound; enforced structurally (a semaphore/counter checked before
  `Propose`, not a best-effort convention).
- **CONTROL-PLANE NON-STARVATION** — Raft heartbeat/vote/replication
  message processing latency is never a function of client queue depth
  — proven by a dedicated priority-lane implementation, not by "usually
  fast enough."
- **REJECTION SAFETY** — a rejected-for-capacity request has never been
  proposed, replicated, or applied; rejection is always safe to retry
  and never produces a duplicate effect (extends `IDEMPOTENCY`'s
  existing guarantee to the new rejection path explicitly).

### Formats/APIs affected

New rejection error kind (distinct HTTP status, e.g. `503` with a
`Retry-After` hint) on the client-facing proposal/SQL path; new CLI
flags: `-max-inflight-proposals`, `-max-concurrent-transactions`,
`-admission-queue-depth`, `-disk-pressure-threshold`.

### Failure semantics

- Queue full → immediate, explicit rejection; never silent drop, never
  unbounded block.
- Disk pressure crosses threshold → admission tightens (lower effective
  concurrency cap) before actual `ENOSPC`; client sees explicit
  capacity rejection instead of eventually hitting a write failure deep
  in the pipeline.
- Admission-control component itself failing (a bug in the limiter) —
  must fail toward *rejecting* new work, never toward silently bypassing
  the limit (fail-safe direction matches §5's audit-log fail-closed
  posture: an availability cost is preferred over a correctness/
  resource-exhaustion cost).

### Packages/files expected

- `internal/admission` (new): bounded queue/semaphore primitives,
  priority-lane dispatch, resource-pressure signal aggregation.
- `internal/node`: `Propose` path wraps through `internal/admission`
  before entering Raft; Raft's own internal message loop is wired to
  bypass the client-facing admission gate entirely (separate code path,
  not a priority flag on a shared queue — structurally cannot be
  starved by client queue depth).
- `internal/sql/exec.go`: statement execution gated the same way.
- New doc: `docs/admission-control.md`.

### Deterministic tests

Queue-full rejection test; concurrent-cap boundary test (exactly N
admitted, N+1th rejected); priority-lane test proving Raft heartbeat
processing latency is unaffected by a saturated client queue (both
paths driven concurrently in `internal/fault`'s deterministic
simulator); disk-pressure-threshold admission-tightening test.

### Integration tests

Real cluster under a synthetic high-concurrency client load generator
(reusing/extending `internal/benchutil`) proving the node stays
responsive (bounded memory, bounded latency for admitted work,
immediate rejection for excess work) rather than degrading unboundedly;
real disk-pressure test (fill a real disk toward the configured
threshold) proving admission tightens before a real write failure.

### Chaos tests

Overload combined with leader failover (does admission-control state
survive/reset correctly across a leadership change, and does the new
leader immediately apply its own limits rather than inheriting a stale
count); overload combined with a membership change in progress (§8) —
control-plane priority must hold even under simultaneous client
overload and an in-flight configuration change.

### Compatibility implications

New rejection error kind is additive to the client-facing API surface
([`docs/versioning.md`](versioning.md)'s "an addition that does not
change existing behavior is a PATCH or MINOR bump" — existing clients
that don't expect a `503` must be documented to handle it, a MINOR
compatibility note, not a breaking wire-format change).

### Security implications

Admission control is itself a defense against a form of denial-of-
service from an authenticated-but-abusive client; per-credential rate
limiting (as opposed to global node-wide limiting) is a natural
extension but not required for V1 — see Non-goals.

### Observability

Queue depth gauge, admitted/rejected counters (by reason: capacity,
disk-pressure), control-plane-lane message latency histogram (the
first histogram-shaped metric in the catalog — see §11's "production
metric catalog" for where the deferred-since-Phase-9 histogram
decision, [`docs/observability.md`](observability.md) §9, is finally
revisited).

### Documentation

`docs/admission-control.md` (new); `docs/failure-model.md` §1.9 updated
from "a possible future enhancement" to "implemented, see
docs/admission-control.md."

### Acceptance criteria

- Bounded-work invariant proven under sustained overload (real
  integration test, not just unit-level).
- Control-plane non-starvation proven under simultaneous client
  overload in both the deterministic simulator and a real cluster.
- Disk-pressure-based admission tightening proven with a real
  near-full disk.
- Zero duplicate-effect incidents across a rejection-and-retry stress
  test (rejection safety).

### Release gate

`v0.6.0` (bundled with §10).

### Dependencies

§5, §6, §7, §8.

### Explicit non-goals

Per-credential/per-tenant rate limiting or fair-share scheduling across
multiple distinct clients (V1's admission control is node-global, not
multi-tenant-aware — ChronicleDB V1 has no tenant concept at all,
consistent with [`docs/non-goals.md`](non-goals.md)'s sharding
deferral); a fully general priority/QoS class system for client traffic
(only the two-lane control-plane-vs-client split above); predictive/
adaptive admission control (fixed, operator-configured thresholds only,
not a self-tuning controller).

---

## 10. Storage Lifecycle

**Target release**: `v0.6.0` (bundled with §9). **Depends on**: §5,
§6, §7, §8 (design-only ordering after §9, per §3 — no code
dependency).

### Current gap

MVCC version garbage collection is explicitly deferred — the rule is
defined (`GCWatermark`, [`docs/mvcc.md`](mvcc.md) §6) but not
implemented; old versions accumulate unboundedly. There is no
tombstone reclamation, no disk-reclamation policy beyond existing WAL
segment compaction after a snapshot ([`docs/wal.md`](wal.md) §11), no
defined disk-full semantics beyond "the write fails explicitly" (§1.9,
[`docs/failure-model.md`](failure-model.md)), no documented fsync-
failure *recovery* policy (only that it must not be silently treated as
success), and no storage integrity verification tool (a scrub/check
utility) independent of the checks that happen automatically at
read/replay time.

### Architecture

- **MVCC garbage collection** — implements exactly the rule already
  specified in [`docs/mvcc.md`](mvcc.md) §6, unchanged: a version is
  reclaimable once a strictly newer version covers every `StartSeq` an
  active snapshot could still legally use, and the newest version of
  any key is never reclaimed. GC runs as a background, low-priority
  process (consuming `internal/admission`'s resource-pressure signals
  from §9 to avoid contending with foreground write/read paths under
  load) that computes the current safe watermark from the live
  transaction-manager's active-snapshot set (`internal/txn`) and walks
  version chains (`internal/mvcc`) removing eligible entries.
- **Safe GC watermark** — computed identically to the already-specified
  `GCWatermark`; this phase's only new work is *implementing* the
  watermark computation as a live, continuously-updated value (driven
  by transaction begin/end events) rather than the mvcc.md §6
  specification remaining unimplemented.
- **Tombstone lifecycle** — a tombstone (a deleted key's newest
  version) is retained exactly as long as `GCWatermark` §6's "the
  single newest version... is never removed" rule already requires,
  even though it carries no value — reclaimed only once superseded by a
  new write for that key, same as any other version.
- **Disk reclamation** — extends the existing WAL segment compaction
  (`CompactBefore`, [`docs/wal.md`](wal.md) §11) to also trigger
  storage-layer file-space reclamation for fully-superseded MVCC data
  once GC removes it from the in-memory index and the next snapshot
  boundary makes that removal durable.
- **Disk-full semantics** — formalizes §1.9's existing "treated as a
  disk write failure" into an explicit, tested state machine: detect
  approaching-full (feeds §9's admission tightening), detect actual
  `ENOSPC` (explicit write failure, no acknowledgment given, per
  existing `DURABILITY` invariant), and recovery-on-space-freed
  (resuming normal operation once an operator frees space or GC
  reclaims enough — no special "stuck forever" state).
- **fsync failure handling** — a failed `Sync()` call is treated as
  fatal to that write (never silently ignored, matching §1.4/§1.8's
  existing rule) **and** this phase adds an explicit node-health
  transition: repeated fsync failures on the same node mark it
  unhealthy (surfaced via `/health`, §5's audit log, and §11's
  diagnostics) rather than continuing to accept writes against
  apparently-failing local storage.
- **Storage integrity verification** — a new, explicit `admin`-gated
  scrub operation that walks every durable segment and validates every
  checksum without relying on the read/replay path encountering the
  bad data first (proactive verification vs. today's reactive-only
  detection), reporting corruption found without attempting silent
  repair (consistent with `RECOVERY NON-INVENTION`).

### Invariants (new)

- **GC SAFETY** — a version is reclaimed by GC only when
  `docs/mvcc.md` §6's precise rule permits it; no active transaction's
  snapshot ever observes a version that GC has removed (proven by
  property test: no reachable execution has an active snapshot whose
  `StartSeq` falls below the watermark GC used).
- **DISK-FULL EXPLICITNESS** — an out-of-space condition is always
  surfaced as an explicit, distinguishable failure to the caller, never
  conflated with an ordinary transaction conflict or a generic I/O
  error a client might retry blindly against.
- **SCRUB NON-DESTRUCTIVE** — the integrity-verification scrub never
  modifies on-disk state; it only reads and reports, so running it is
  always safe against a live node.

### Formats/APIs affected

No new durable-record format (GC operates on the existing MVCC
in-memory structure and existing WAL/snapshot formats); new `admin`
endpoint `/admin/storage/scrub`; new CLI flag `-gc-interval` (or
disabled by default with an explicit opt-in, given this is new,
previously-never-run machinery touching a `STATE MACHINE SAFETY`-
adjacent invariant — see Compatibility below).

### Failure semantics

- GC crash mid-reclamation → safe by construction: GC only removes
  already-superseded, already-durable versions; a crash mid-GC leaves
  some reclaimable versions unreclaimed (fine — they get GC'd on the
  next pass) and never removes a version prematurely (the watermark
  check happens before each removal, not as a batch precomputed once
  and blindly applied).
- Scrub finds corruption → reported via diagnostics/audit, operator-
  actionable (matches existing `docs/recovery.md` §4's "operator
  intervention" posture for corruption the automatic paths cannot
  safely resolve themselves); scrub itself never attempts automatic
  repair.
- Disk-full during GC's own bookkeeping (rare — GC mostly frees space)
  → GC's own writes (if any persistent bookkeeping is needed) follow
  the same explicit-failure rule as any other write; GC does not get a
  special exemption from `DURABILITY`.

### Packages/files expected

- `internal/mvcc`: GC implementation of the existing §6 rule (new
  code; the *rule* file, `mvcc.go`, already documents the target
  behavior).
- `internal/txn`: exposes the live active-snapshot set needed to
  compute the watermark.
- `internal/storage`: file-space reclamation triggered post-GC/post-
  compaction; new scrub walk.
- `internal/wal`: disk-full/fsync-failure health-state transitions.
- New doc: `docs/storage-lifecycle.md`; `docs/mvcc.md` §6 updated from
  "not implemented in V1" to "implemented, see
  docs/storage-lifecycle.md."

### Deterministic tests

Property test: randomized version-chain generation + randomized active-
snapshot sets, GC run, assert no active snapshot's required version was
removed (extends the existing MVCC visibility property-test pattern,
[`docs/roadmap.md`](roadmap.md) Phase 2); tombstone-retained-until-
superseded test; disk-full-then-space-freed recovery test; scrub-
detects-injected-corruption test (reusing existing corruption-injection
fixtures from `internal/wal`'s test suite).

### Integration tests

Real long-running workload against a real cluster with GC enabled,
confirming disk usage stabilizes (does not grow unboundedly) while
correctness (via the existing scenario corpus) is unaffected; real
disk-full injection (a small, real, deliberately-sized filesystem) on a
live node proving the documented disk-full state machine.

### Chaos tests

GC running concurrently with the existing chaos schedule (crashes,
partitions, snapshots, now membership changes from §8) — extends
`internal/fault`'s combined-fault-schedule suites with "GC active" as a
new dimension, checked against `GC SAFETY` after every action, same
seed-count discipline as Phase 7.

### Compatibility implications

GC is new machinery touching MVCC's core data structure — ships
**disabled by default** at `v0.6.0` (`-gc-interval=0` default,
explicit opt-in) specifically because it is new, not because it is
architecturally optional; enabling it by default is a `v1.0.0` gate
item (§13.2), consistent with §5's TLS/auth phase-in approach.

### Security implications

Scrub is `admin`-gated and audited (§5) since it reads potentially
sensitive full-database content, even though it only reports metadata
(corruption locations), not values.

### Observability

Disk-usage-over-time gauge, GC reclaimed-version count, GC watermark
value, fsync-failure counter and node-health-transition events, scrub
run history (last run, findings count).

### Documentation

`docs/storage-lifecycle.md` (new); `docs/mvcc.md` §6, `docs/wal.md`,
`docs/failure-model.md` §1.8-1.9 all updated from "future"/"deferred"
language to "implemented, see docs/storage-lifecycle.md."

### Acceptance criteria

- GC property test passes at the same seed-count discipline as existing
  MVCC property tests; zero `GC SAFETY` violations found.
- Long-running real-cluster test shows stabilized disk usage under
  continuous write load with GC enabled.
- Disk-full and fsync-failure state machines proven against real
  injected conditions, not just simulated return codes.
- Scrub proven to detect every corruption case the existing WAL/
  snapshot corruption-injection test suite already covers, without
  modifying on-disk state.

### Release gate

`v0.6.0` (bundled with §9).

### Dependencies

§5, §6, §7, §8 (design ordering only, per §3).

### Explicit non-goals

Compression of retained/archived data; tiered/cold storage; automatic
disk-space-based data eviction beyond MVCC-correct GC (V1 never deletes
data a `SELECT`/read could legitimately still need — GC is purely
about *redundant* old versions, never about capacity-driven deletion of
live data); a general-purpose defragmentation/compaction scheme beyond
what already exists (WAL segment compaction, §`internal/wal` §11) and
this phase's GC-triggered reclamation.

---

## 11. Operations / Observability

**Target release**: `v0.7.0` (bundled with §12, §13). **Depends on**:
§5 through §10 (this phase's catalog surfaces state every prior phase
introduces — §3).

### Current gap

[`docs/observability.md`](observability.md) documents a real but
intentionally minimal Phase-9 catalog: node status, election/leader-
change counters, proposal-outcome counters, `RequestID` duplicate
counts, snapshot counters, `/metrics` (Prometheus text) and `/health`
(JSON). Explicitly not built at Phase 9: replication lag per follower,
a live per-request latency histogram, structured logging (current
logging is ad hoc, not `RequestID`-correlated), a diagnostics bundle,
and an operator CLI. This phase closes those gaps plus adds the metric
surfaces every phase §5-§10 introduces.

### Architecture

- **Production metric catalog** — extends `internal/metrics`
  (currently `Counter`/`Gauge` only, deliberately, per
  [`docs/observability.md`](observability.md) §9) with a bounded
  histogram primitive (the concrete trigger §9's admission-control
  control-plane-lane latency measurement now provides, closing the
  Phase-9-deferred decision) and consolidates every counter/gauge
  introduced by §5-§10 into one documented catalog, keeping the
  existing no-high-cardinality-label rule.
- **Structured logs** — replaces ad hoc `log.Printf` call sites with a
  structured logger (`log/slog`, stdlib, per the zero-dependency
  policy) emitting consistent fields (timestamp, node ID, component,
  level, and, wherever applicable, `RequestID`) — never used as a
  correctness input (mirrors the existing "diagnostic state is never a
  correctness dependency" rule).
- **RequestID correlation** — every log line touching a specific
  request's lifecycle (propose → replicate → apply → outcome) carries
  its `RequestID`, letting an operator `grep` one request's full path
  across a multi-node log set.
- **Cluster status** — extends the existing `/status` endpoint with
  aggregate cluster-wide view (each node's own status plus, where
  knowable, peers' last-known status) — carefully avoiding the
  Phase-9-rejected "heuristic disguised as fact" trap
  ([`docs/roadmap.md`](roadmap.md) Phase 9's explicit reasoning for
  why `/health` omits a cluster-wide quorum boolean) by labeling
  aggregate fields explicitly as "as last observed by this node,"
  never as a guaranteed-current fact.
- **Diagnostics bundle** — a new `admin`-gated action producing a
  single tarball: current `/status`, `/metrics`, recent structured log
  tail, active configuration (secrets redacted), membership state
  (§8), version/generation state (§7), audit log summary (§5) — the
  single artifact an operator attaches to a support request or an
  external reviewer requests when investigating a report against
  [`docs/break-chronicledb.md`](break-chronicledb.md).
- **Admin/operator CLI** — a new `chronicledb-ctl` binary (separate
  from `chronicledb-node`, consistent with existing "diagnostic/admin
  tooling is not the data-path binary" separation) wrapping every
  `admin`-gated HTTP endpoint introduced by §5-§11 (auth/cert
  management, backup/restore, upgrade precheck/finalize, membership
  add/promote/remove, scrub, diagnostics-bundle) behind one consistent,
  scriptable interface, rather than requiring an operator to hand-craft
  HTTP requests for each.
- **Alerts / SLOs / runbooks** — documentation, not code: a curated set
  of example alert rules (Prometheus alerting-rule YAML, since
  `/metrics` is already Prometheus-format) for the highest-signal
  conditions (quorum-loss risk, disk-pressure, certificate-expiry-
  approaching, fsync-failure-node-unhealthy, GC watermark stalled), a
  small set of example SLOs (e.g. "p99 commit latency," using the
  measurement methodology `docs/benchmarks.md` already established,
  not a new invented one), and runbooks (step-by-step operator
  procedures for the alert conditions above, plus the restore/upgrade/
  membership-change procedures already documented per-phase).

### Invariants (new)

- **DIAGNOSTIC NON-INTERFERENCE** (extends the existing Phase-9
  principle explicitly into the invariant catalog): no metric, log
  line, diagnostics-bundle generation, or CLI query ever affects a
  correctness decision or blocks the commit path it observes.
- **SECRET REDACTION** — the diagnostics bundle and structured logs
  never include credential material (tokens, private key bytes,
  certificate private keys) even when the surrounding context (e.g. a
  failed-auth log line) would naturally otherwise include it.

### Formats/APIs affected

New histogram metric type in `internal/metrics`; new
`/admin/diagnostics-bundle` endpoint; new `chronicledb-ctl` CLI
(subcommands mirroring every admin endpoint across §5-§11); structured
log line schema (documented, versioned informally via
`docs/observability.md`, not a durable format subject to §7's
generation mechanism since logs are never replayed as input to any
decision).

### Failure semantics

Diagnostics-bundle generation failing partway (e.g. disk pressure while
writing the tarball) → the bundle generation fails explicitly, no
partial/corrupt bundle is left claiming completeness (small, low-risk
echo of `BACKUP INTEGRITY`'s posture, applied to a diagnostic artifact
instead of a recovery-critical one). `chronicledb-ctl` losing
connectivity mid-command → reports the ambiguous outcome explicitly
(does this look like a `RequestID`-bearing mutating call, e.g.
`membership promote`? if so it inherits that call's own idempotency/
retry story, per §4.4 of
[`docs/failure-model.md`](failure-model.md); a pure read command simply
retries).

### Packages/files expected

- `internal/metrics`: histogram primitive.
- `internal/log` (new, thin wrapper around `log/slog` establishing the
  field schema) or direct `log/slog` usage wired through `internal/node`/
  `cmd/chronicledb-node` — exact shape decided at implementation time
  (§0's "packages are a plan, not a promise" caveat applies here most
  directly).
- `cmd/chronicledb-node`: diagnostics-bundle handler.
- `cmd/chronicledb-ctl` (new binary): admin CLI.
- `deploy/alerts/*.yml` (new): example Prometheus alerting rules.
- New docs: `docs/runbooks.md`, `docs/slos.md`; `docs/observability.md`
  extended with the full catalog, histogram design, structured-log
  schema, diagnostics-bundle contents, CLI reference.

### Deterministic tests

Histogram bucket/percentile correctness tests; secret-redaction test
(inject a fake credential into a log-triggering path, assert it never
appears in captured log output or a generated diagnostics bundle);
`chronicledb-ctl` command-to-HTTP-call mapping tests (every subcommand
against a mock/local server).

### Integration tests

Real cluster: generate a diagnostics bundle, confirm every claimed
section is present and accurate against the live cluster's actual
state; drive every `chronicledb-ctl` subcommand against a real
three-node cluster end-to-end (superset of the individual per-phase
integration tests already required in §5-§10, exercised through the
CLI specifically, not just raw HTTP).

### Chaos tests

Diagnostics-bundle generation during an active chaos schedule (must
still produce a valid, if perhaps incomplete-but-honestly-labeled,
bundle — never a misleading one); `chronicledb-ctl` issuing a mutating
command against a cluster mid-leader-failover, confirming the existing
`RequestID` retry story (already proven in Phases 5/8) covers this
new client, since the CLI is just another client of the same API.

### Compatibility implications

Additive only (new metric type, new endpoints, new binary); no change
to any existing wire/durable format.

### Security implications

`chronicledb-ctl` authenticates exactly like any other client (§5); the
diagnostics bundle is the single highest-value target for accidental
secret leakage in the whole system, hence the dedicated `SECRET
REDACTION` invariant and its own fuzz/injection test.

### Observability

This phase *is* the observability phase — see Architecture above for
the full surface.

### Documentation

`docs/observability.md` (extended, becomes the full production
catalog); `docs/runbooks.md` (new); `docs/slos.md` (new);
`docs/configuration.md` extended with `chronicledb-ctl` flags/env vars.

### Acceptance criteria

- Full metric catalog documented and every metric proven to move on a
  real event, exactly matching Phase 9's existing evidence bar
  ([`docs/roadmap.md`](roadmap.md) Phase 9).
- `SECRET REDACTION` proven by a dedicated fuzz/injection test with
  zero leaks found.
- Every admin action from §5-§10 reachable via `chronicledb-ctl`.
- At least one example alert per named high-signal condition, each with
  a corresponding runbook section.

### Release gate

`v0.7.0` (bundled with §12, §13).

### Dependencies

§5, §6, §7, §8, §9, §10.

### Explicit non-goals

A built-in metrics time-series database or dashboard (ChronicleDB
exposes `/metrics`; Prometheus/Grafana or equivalent is the operator's
own stack, not shipped here); log aggregation/shipping (structured
logs go to stdout/a configured file; shipping to a log platform is
operator-side configuration, no built-in integration); a web-based
admin UI (`chronicledb-ctl` is a CLI; a GUI is unscoped, separate,
future work with its own justification if ever pursued).

---

## 12. Production Deployment

**Target release**: `v0.7.0` (bundled with §11, §13). **Depends on**:
§11 (probes/health surface must exist first — §3).

### Current gap

No Docker image, no systemd unit, no Kubernetes manifests, no Helm
chart. `scripts/demo-local-cluster.sh` (Phase 11) starts three real OS
processes directly — useful for a demo/quickstart, not a packaged
deployment artifact. [`docs/non-goals.md`](non-goals.md) §Kubernetes /
cloud infrastructure explicitly defers this, with the trigger condition
("the engine reaches `PORTFOLIO READY` or `OPEN-SOURCE READY` maturity
and deployment ergonomics become the limiting factor") already met.

### Architecture

- **Docker** — a minimal, reproducible container image (multi-stage
  build: the existing `go build` toolchain, then a small runtime base)
  running `chronicledb-node`; image build reuses
  `scripts/build-release.sh`'s existing reproducible-build discipline
  rather than a separate, divergent Dockerfile build process.
- **systemd** — a documented unit file template (`Type=notify` or
  `Type=simple` with `ExecStop` sending `SIGTERM`, matching the
  graceful-shutdown behavior below) plus a hardening profile
  (`ProtectSystem=strict`, dedicated user, capability restriction) as a
  reference for bare-metal/VM deployment.
- **Kubernetes StatefulSet** — ordinal-indexed pod identity mapped to
  stable node identity (§5's per-node TLS certificate), a persistent
  volume claim template per pod (durable WAL/snapshot/backup storage
  survives pod rescheduling), and a headless service for stable peer
  DNS names `internal/transport` peers dial.
- **Helm** — a chart parameterizing cluster size (interacting correctly
  with §8's membership mechanism — scaling the StatefulSet is not, by
  itself, a membership change; the chart documents that a scale-up
  requires an explicit `chronicledb-ctl membership add` afterward, not
  an automatic one), TLS/secret references, resource requests/limits
  feeding §9's admission-control thresholds.
- **Persistent volumes**: one PVC per pod, sized/documented against
  §10's disk-reclamation/GC behavior so operators can reason about
  steady-state usage.
- **Anti-affinity / topology spread**: pod anti-affinity (or
  `topologySpreadConstraints`) rules in the chart default to spreading
  the three-to-N voters across distinct nodes/zones, since colocating
  a Raft majority on one physical host defeats the fault-tolerance
  model entirely.
- **PodDisruptionBudget**: `minAvailable` set so voluntary disruptions
  (node drains, cluster upgrades) never evict enough pods
  simultaneously to lose quorum — directly protects `QUORUM SAFETY` at
  the orchestration layer.
- **Graceful shutdown**: `cmd/chronicledb-node` gains explicit `SIGTERM`
  handling — stop accepting new client work (via §9's admission
  control), finish in-flight admitted work up to a bounded grace
  period, step down as leader if currently leading (proactively
  triggering an election rather than waiting for the new leader's
  election timeout to fire), then exit — minimizing unavailability
  windows from routine orchestrated restarts (directly serves §8's
  rolling-upgrade and Kubernetes' own rolling-update/eviction model).
- **TLS/secret handling**: the Helm chart mounts operator-provided
  Kubernetes `Secret` objects for §5's certificate/token material
  (never generates or stores secrets itself) — cert-manager integration
  is documented as a *pattern*, not a built-in dependency (keeps the
  zero-external-Go-dependency policy intact; this is deployment
  YAML, not Go code).
- **Multi-AZ guidance**: a documentation section (not code) describing
  how to combine anti-affinity/topology-spread with §8's membership
  sizing to survive a single-AZ outage, and the latency trade-off of
  spreading a Raft group across AZs (cross-AZ round-trip cost affects
  commit latency, same physics [`docs/non-goals.md`](non-goals.md)
  already cites for why cross-*region* is out of scope — multi-AZ
  within a region is the supported ceiling).

### Invariants (new)

- **GRACEFUL SHUTDOWN COMPLETENESS** — a `SIGTERM`'d node either
  finishes or safely abandons (never partially commits) every
  in-flight request before exiting; no request is left in an undefined
  state by an orchestrated restart specifically (as opposed to an
  ungraceful crash, already covered by existing durability invariants).
- **QUORUM-AWARE DISRUPTION BUDGET** — the shipped Helm chart's default
  `PodDisruptionBudget` never permits a voluntary eviction that would
  drop the voter set below quorum, for the chart's own default
  cluster-size values.

### Formats/APIs affected

No wire/durable format changes. New artifacts: `deploy/docker/Dockerfile`,
`deploy/systemd/chronicledb-node.service`,
`deploy/k8s/statefulset.yaml` (+ supporting manifests),
`deploy/helm/chronicledb/` (chart).

### Failure semantics

Pod eviction beyond the PDB's allowance is refused by Kubernetes itself
(the PDB is the enforcement mechanism, not application code) — this
phase's job is shipping a *correct* PDB value, not building new
runtime logic. A `SIGKILL` (bypassing graceful shutdown, e.g.
`terminationGracePeriodSeconds` exceeded) falls back to exactly the
existing ungraceful-crash recovery story (§1.2-§1.6 of
[`docs/failure-model.md`](failure-model.md)) — graceful shutdown is a
latency/availability optimization, never a correctness dependency.

### Packages/files expected

`deploy/docker/Dockerfile`, `deploy/systemd/*.service`,
`deploy/k8s/*.yaml`, `deploy/helm/chronicledb/{Chart.yaml,values.yaml,
templates/}`; `cmd/chronicledb-node/main.go` gains `SIGTERM` handling;
new doc `docs/deployment.md`.

### Deterministic tests

Helm chart lint/template-render tests (`helm template` output validated
against expected manifest shape, no live cluster needed); PDB value
computed-correctly-for-N-voters unit test.

### Integration tests

Real `docker build` + `docker run` three-container cluster proving the
image actually starts a working cluster; a real (or `kind`-based)
Kubernetes cluster running the chart, proving StatefulSet pod
identity/PVC persistence across a pod restart, and that a real rolling
update (via §7's mechanism, orchestrated by `kubectl rollout` or Helm
upgrade) completes without quorum loss.

### Chaos tests

Simulated node/AZ failure in the `kind`-based test cluster (delete a
pod out-of-band, not via a graceful rollout) proving the remaining
quorum continues serving and the replacement pod rejoins correctly
(exercising §8's membership/catch-up path in a real orchestrated
environment, not just the bespoke integration harness).

### Compatibility implications

None to wire/durable formats; the Helm chart's own values-schema
versioning follows [`docs/versioning.md`](versioning.md)'s existing
policy, extended to cover chart versions explicitly.

### Security implications

Secrets are never baked into the container image or chart defaults;
the chart's documentation explicitly warns against committing rendered
manifests containing real secret values to source control (mirrors
[`docs/failure-model.md`](failure-model.md) §6's "no committed
secrets" rule, now operationally relevant for the first time).

### Observability

Kubernetes liveness/readiness probes wired to §11's `/health`; the
diagnostics bundle (§11) documented as the first thing to attach to a
Kubernetes-specific support request alongside `kubectl describe`/pod
logs.

### Documentation

`docs/deployment.md` (new: Docker, systemd, Kubernetes/Helm, PVs,
anti-affinity/PDB, graceful shutdown, secret handling, multi-AZ
guidance — the full task-required list); `docs/non-goals.md`
§Kubernetes/cloud infrastructure updated from "deferred" to "delivered,
see docs/deployment.md" (the Kubernetes *operator* pattern — automated
day-2 operations beyond what Helm/StatefulSet provide out of the box —
remains deferred, see Non-goals below).

### Acceptance criteria

- A real `kind`-based (or equivalent) Kubernetes cluster running the
  shipped chart survives a pod deletion and a rolling update without
  quorum loss or data loss, proven by test.
- Docker image builds reproducibly and starts a working real cluster.
- PDB, anti-affinity, and graceful shutdown are each proven by a
  dedicated test, not asserted by inspection alone.
- `docs/deployment.md` covers every task-required subtopic with a
  concrete, runnable example (matching the existing
  `docs/quickstart.md` discipline of "every command actually run").

### Release gate

`v0.7.0` (bundled with §11, §13).

### Dependencies

§11.

### Explicit non-goals

A Kubernetes *operator* (a custom controller automating day-2
operations — scale, upgrade, backup scheduling — beyond what a
StatefulSet/Helm chart plus the CLI/runbooks from §11 already provide)
— explicitly still deferred, per
[`docs/non-goals.md`](non-goals.md)'s original entry, now with a
sharper trigger: revisit only if manual/scripted day-2 operations via
`chronicledb-ctl` + runbooks prove insufficient in practice; managed-
cloud-specific integrations (AWS/GCP/Azure-specific load balancer
annotations, managed-disk classes) beyond generic Kubernetes primitives;
a public container registry publishing pipeline (image build is
provided; where it is published is a project/maintainer decision
outside this plan's scope).

---

## 13. Software Supply Chain

**Target release**: `v0.7.0` (bundled with §11, §12). **Depends on**:
§12 (needs the full release-artifact set — binaries, image, chart — to
attest, per §3).

### Current gap

`.github/workflows/release.yml` produces checksummed archives
(Phase 11) but no SBOM, no artifact signing, no provenance attestation,
no dependency vulnerability scanning, and no static-analysis workflow
(CI runs `go vet`/`gofmt`/tests per `.github/workflows/ci.yml`, but no
CodeQL or equivalent). `docs/dependencies.md` documents a zero-external-
Go-dependency policy, which simplifies (but does not eliminate — the Go
toolchain/stdlib itself, plus any future CDN/tooling dependency in
`deploy/`, still need scanning) this phase's scope.

### Architecture

- **SBOM** — a CycloneDX or SPDX-format software bill of materials
  generated per release artifact (each binary, the container image,
  the Helm chart) via a stdlib/`go list -m all`-based generator or a
  well-known, pinned SBOM tool invoked in CI (not vendored into the
  zero-dependency Go module itself — a CI-time tool dependency is a
  different category from a compiled-in Go dependency, and
  `docs/dependencies.md` is updated to state that distinction
  explicitly rather than leave it ambiguous).
- **Artifact signing** — release binaries, checksums, the container
  image, and the Helm chart are signed (e.g. via `cosign` keyless
  signing against the GitHub Actions OIDC identity, avoiding a
  long-lived private signing key the project would need to protect)
  so a consumer can verify an artifact actually came from this
  repository's release workflow.
- **Provenance** — a SLSA-style provenance attestation (build
  workflow identity, source commit, builder environment) generated and
  attached alongside each signed artifact, extending the existing
  tag-triggered `release.yml` rather than a parallel workflow.
- **Vulnerability scanning** — `govulncheck` (the standard Go
  toolchain vulnerability scanner, no new external dependency) run in
  CI against every push and on a schedule (catching a newly disclosed
  vulnerability in a dependency even between pushes — relevant even
  under the zero-Go-dependency policy, since it also scans the
  standard library itself); container image scanning (e.g. `trivy` or
  equivalent, CI-tool-only, same category distinction as SBOM
  generation above) for the Docker artifact from §12.
- **Dependency review** — Dependabot already covers `gomod`/
  `github-actions` (Phase 11); this phase adds a `docker`-ecosystem
  Dependabot config for the base image introduced by §12, and a
  documented manual review step (given `docs/dependencies.md`'s policy
  that Go dependency additions are rare, deliberate decisions, not
  routine) for any future actual Go dependency.
- **Static analysis / CodeQL** — a new `.github/workflows/codeql.yml`
  running CodeQL's Go query pack on every push/PR, on top of (not
  replacing) the existing `go vet`/race-detector/fuzz-target discipline
  already in `.github/workflows/ci.yml`.

### Invariants (new)

- **ARTIFACT PROVENANCE** — every artifact published by
  `release.yml` from this phase onward carries a verifiable signature
  and provenance attestation traceable to a specific source commit and
  build workflow run; an unsigned or attestation-less artifact is never
  published.
- **SCAN-BEFORE-RELEASE** — the release workflow does not publish an
  artifact whose vulnerability scan (Go stdlib/toolchain, container
  image) reports a known-fixed, currently-unpatched critical
  vulnerability without an explicit, documented, time-bounded
  exception.

### Formats/APIs affected

No product wire/durable format changes — purely CI/release-pipeline
and published-artifact metadata (SBOM files, `.sig`/`.att` files
alongside existing release archives).

### Failure semantics

A signing or SBOM-generation step failing in CI fails the release
workflow outright (no artifact is published without its accompanying
attestation) — mirrors this phase's own `ARTIFACT PROVENANCE`
invariant: "best-effort" supply-chain evidence is not evidence.
`govulncheck`/CodeQL finding a genuine issue fails the relevant CI job
(blocking merge/release) rather than merely warning, for any finding
above a documented severity threshold; below-threshold findings are
tracked, not silently ignored, per the existing correctness-bug issue
template's discipline (Phase 11) extended to vulnerability findings.

### Packages/files expected

`.github/workflows/release.yml` (extended: SBOM, signing, provenance
steps); `.github/workflows/codeql.yml` (new); `.github/workflows/vuln-scan.yml`
(new, or folded into `ci.yml`); `.github/dependabot.yml` (extended:
`docker` ecosystem); new doc `docs/supply-chain.md`;
`docs/dependencies.md` extended with the CI-tool-vs-compiled-dependency
distinction and the new scanning/signing policy.

### Deterministic tests

CI-workflow-level tests are inherently integration-shaped (there is no
meaningful unit test for "did cosign sign the artifact") — this
phase's "deterministic tests" are the CI jobs' own pass/fail signal
against a fixture release build, run in a PR-triggered dry-run mode
before being wired to the real tag-triggered `release.yml`.

### Integration tests

A full dry-run release (tag a throwaway pre-release-style tag against a
disposable fork/branch, or use `workflow_dispatch` against a test
input) proving the entire signed-artifact-plus-SBOM-plus-provenance
pipeline actually produces artifacts a real verifier (`cosign verify`,
an SBOM validator) accepts.

### Chaos tests

Not applicable in the traditional sense — the closest analogue is a
**negative test**: deliberately tamper with a built artifact after
signing and confirm verification fails (proving the signature actually
constrains something, rather than being present but unchecked
anywhere).

### Compatibility implications

Purely additive to the release process; existing `v0.1.0`-style
archives remain valid, just without retroactive SBOM/signature (not
re-issued for past releases — this phase changes the process for
future releases from its adoption point forward, documented explicitly
as such, matching how `docs/versioning.md` already treats policy
changes as forward-only).

### Security implications

This phase directly hardens the software supply chain — reduces the
risk of a compromised build/release pipeline producing a tampered
artifact undetected. Signing key management (the GitHub OIDC keyless
approach) is chosen specifically to avoid adding a new long-lived
secret for the project to protect, itself a security property worth
stating explicitly.

### Observability

Release workflow run status/history is already visible via GitHub
Actions; this phase adds no new runtime (node-process) observability —
its "observability" is entirely about the release pipeline's own
auditability (which is the point).

### Documentation

`docs/supply-chain.md` (new: what's generated, how to verify a release
artifact, the vulnerability-response policy); `docs/dependencies.md`
extended; `docs/releasing.md` extended with the new signing/SBOM steps
in the release checklist.

### Acceptance criteria

- Every artifact `release.yml` produces from this phase forward is
  signed and carries a valid SBOM and provenance attestation, verified
  by an actual `cosign verify` (or equivalent) run as part of CI, not
  merely asserted.
- `govulncheck` and container-image scanning both run on every push and
  block on a documented severity threshold.
- CodeQL runs clean (zero unaddressed findings above the documented
  threshold) at the point this phase is marked complete.
- A deliberately tampered artifact demonstrably fails verification.

### Release gate

`v0.7.0` (bundled with §11, §12).

### Dependencies

§12.

### Explicit non-goals

A full SLSA Build Level 4 (hermetic, fully reproducible builds with
two-party review) — this phase targets a realistic, achievable
provenance level (signed, attested, traceable to source) rather than
the highest formal SLSA tier, which would require build-hermeticity
work far beyond this phase's scope; a private/internal package
registry or mirror; license-compliance scanning beyond what
`docs/dependencies.md`'s existing zero-dependency policy already makes
close to moot (revisit if a real Go dependency is ever actually added).

---

## 14. Independent Black-Box Correctness Testing

**Target release**: `v0.8.0` (bundled with §15, §16 below — task items
11 and 12). **Depends on**: §5-§13 (needs every surface it tests to
actually exist — §3).

### Current gap

Every current correctness suite — `internal/fault`'s chaos suites,
`internal/node`'s model tests, `internal/oracle`'s reference-model
checks, `cmd/chronicledb-node`'s `-tags=integration` real-process
tests — is **ChronicleDB-authored and imports ChronicleDB internals**
(or, for the `cmd/chronicledb-node` integration tests, at minimum lives
inside the same module and is written by the same project). This is
honestly documented as exactly what it is throughout
[`docs/testing-strategy.md`](testing-strategy.md),
[`docs/adversarial-testing.md`](adversarial-testing.md), and
[`docs/break-chronicledb.md`](break-chronicledb.md) — thorough, but not
independent of the implementation's own assumptions. No harness
exists that is *structurally* prevented from importing ChronicleDB
internals, speaks only the external client protocol, and drives real
process kill/restart, real network partitions, and real disk
corruption from entirely outside the system under test.

### Architecture

- **External process-level client** — a new harness (`tools/blackbox`,
  its **own Go module** with its own `go.mod`, structurally unable to
  `import "chronicledb/internal/..."` regardless of package-visibility
  discipline, since Go's `internal/` rule already forbids cross-module
  imports outright) that speaks exclusively the documented external
  surfaces: the HTTP control-plane JSON API and the SQL grammar
  ([`docs/sql.md`](sql.md)), plus `chronicledb-ctl` (§11) as a black-box
  client itself.
- **External process kill/restart** — the harness manages real
  `chronicledb-node`/`chronicledb-ctl` OS processes (spawned via `os/
  exec`, killed via real signals) exactly the way an external operator
  or `docs/break-chronicledb.md`'s reviewers would, without any
  in-process shortcut `cmd/chronicledb-node`'s own existing
  `-tags=integration` tests can (because those still live in-repo and
  can, in principle, import test-only internal hooks like `/fault`).
- **External network partitions** — driven by real OS-level network
  control (Linux network namespaces + `iptables`/`nft`, or `tc netem`
  for asymmetric/lossy conditions) rather than the existing `/fault`
  endpoint's `BlockSend`/`BlockRecv` hook — deliberately not reusing
  `/fault`, since that endpoint is itself part of the system under test
  and a "black-box" partition test that used it would not be
  independent of ChronicleDB's own fault-injection code being correct.
- **Disk failure/corruption scenarios** — corrupts real on-disk
  WAL/snapshot/backup files by direct byte manipulation *from the
  outside* (a shell/Go-stdlib file operation in the black-box module,
  never calling into `internal/wal`'s own encode/decode helpers to
  construct the corruption, unlike some of today's in-repo corruption
  tests which do reuse internal helpers for convenience) against a real
  running/restarting node, verified purely through the external
  protocol's response (does the node refuse to start, does it report
  the documented error, per [`docs/failure-model.md`](failure-model.md)
  §1.7).
- **Independent history verification** — a from-scratch, minimal
  reference model living in the black-box module (deliberately *not*
  importing `internal/oracle`, even though the *approach* —
  independent-model verification — is the same idea Phase 10 already
  validated works; this is a second, independently-written
  implementation of "what should the visible history look like,"
  reducing the risk that a bug in `internal/oracle` itself masks a real
  ChronicleDB bug) that predicts client-visible outcomes (`SELECT`
  results, `RequestID` outcomes, `/status` fields) purely from the
  sequence of external requests issued and responses observed, flagged
  against any divergence.
- **No reliance on ChronicleDB internals** — enforced structurally (a
  separate Go module, no `internal/` import path available to it) and
  by review discipline (the harness must not special-case behavior
  based on knowledge only available by reading `internal/` source, only
  from documented external contracts in `docs/`).

### Invariants

This phase does not add new product invariants — it is a second,
independent proof mechanism for the *existing* invariant catalog
([`docs/invariants.md`](invariants.md)), specifically the client-
observable ones: `DURABILITY`, `ATOMICITY`, `ABORT SAFETY`, `MVCC
VISIBILITY`, `CONFLICT CORRECTNESS`, `IDEMPOTENCY`, `REQUEST OUTCOME
STABILITY`. A finding here that contradicts the in-repo suites'
existing "clean" result is exactly the kind of signal
[`docs/adversarial-testing.md`](adversarial-testing.md)'s own honesty
discipline exists to surface, not paper over.

### Formats/APIs affected

None — the black-box harness is a *consumer* of existing documented
external surfaces only (HTTP JSON API, SQL grammar,
`chronicledb-ctl`); if it needs a surface that doesn't yet exist to
observe something it needs (e.g. a way to confirm quorum state
externally without an internal hook), that is itself a finding — either
the surface should be added to the documented external API (a
follow-up, scoped change, not silently worked around) or the check is
out of black-box scope.

### Failure semantics

The black-box harness's own failures (e.g. it can't reach a node) are
distinguished from a ChronicleDB-under-test failure by the same
discipline `internal/fault`'s existing chaos suites already apply to
"harness bug vs. product bug" (per
[`docs/adversarial-testing.md`](adversarial-testing.md)'s "Bugs found"
section precedent, which already reports harness bugs found separately
from product bugs, honestly).

### Packages/files expected

`tools/blackbox/` (new, separate Go module: `go.mod`, its own client
package speaking only the documented external protocol, its own
minimal reference model, process/network/disk fault-injection helpers,
scenario runner); `tools/blackbox/scenarios/` (scenario definitions,
one per §Deterministic/Integration/Chaos test bullet below); new doc
`docs/black-box-testing.md`.

### Deterministic tests

Scenario definitions are deterministic where the underlying operation
is (e.g. "corrupt byte N of the WAL, restart, expect refusal" is
deterministic given a fixed byte offset); a seeded-random mode
(mirroring `internal/fault`'s existing seed-reproducibility discipline,
[`docs/testing-strategy.md`](testing-strategy.md) §3) drives randomized
request sequences against the independent reference model.

### Integration tests

The bulk of this phase's own test suite *is* integration-level by
nature (real processes, real network, real disk) — see Architecture
above; "integration tests" here specifically means: full transaction
lifecycles (`BEGIN`/mutate/`COMMIT`/`ROLLBACK`) driven purely over SQL
against a real cluster, cross-checked against the independent model.

### Chaos tests

Combined real kill + real partition + real disk corruption schedules,
run purely from outside the system, at a meaningful seed/iteration
count (the exact count is an implementation-time decision, but must be
published, per this repository's evidence discipline — "a benchmark
command existing does not prove performance" applied here as "a
black-box harness existing does not prove independence or coverage;
its actual run counts and findings do").

### Compatibility implications

None to product formats. The black-box module's own external-protocol
usage must track [`docs/versioning.md`](versioning.md)'s documented
public-surface list — a breaking change to that surface is itself
something this harness would (and should) fail to build against,
functioning as an incidental compatibility check.

### Security implications

The black-box harness authenticates like any other client (§5) — it
must not require or use any internal bypass of authentication, which
would itself defeat the purpose of testing the real, secured external
surface.

### Observability

The harness produces its own run reports (pass/fail per scenario, seed,
findings) — retained as machine-readable evidence feeding §15's
long-duration qualification record and §16's regression tracking.

### Documentation

`docs/black-box-testing.md` (new: what "black-box" means here
precisely, module boundary enforcement, scenario catalog, how this
differs from and complements
[`docs/adversarial-testing.md`](adversarial-testing.md) and
[`docs/break-chronicledb.md`](break-chronicledb.md) — this phase is
*ChronicleDB-authored* independent-of-internals testing, explicitly
**not** a substitute for the real external human review Phase 12
solicits, per §0.1).

### Acceptance criteria

- `tools/blackbox` builds as a genuinely separate module with zero
  `internal/` import capability (proven by the module boundary itself,
  not just convention).
- Full scenario catalog (process kill/restart, real network partition,
  real disk corruption, independent history verification) run clean
  against the current engine, with results published.
- Any divergence found between the black-box independent model and
  ChronicleDB's actual behavior is triaged exactly like any other
  correctness bug (fix, regression test, honest documentation of root
  cause) before this phase is considered satisfied.

### Release gate

`v0.8.0` (bundled with §15, §16).

### Dependencies

§5-§13.

### Explicit non-goals

Recruiting or claiming actual third-party/external human reviewers
(that is Phase 12's separate, already-published, ongoing process,
[`docs/break-chronicledb.md`](break-chronicledb.md) — this phase builds
tooling, not a claim of external validation, per §0.1); formal
verification/model checking (TLA+ or similar) — a different, heavier
technique with its own separate justification, not attempted here;
fuzzing the SQL/HTTP surface for security vulnerabilities specifically
(that is closer to §13's/`security-review`-style scope; this phase's
fuzz-shaped work, where it exists, targets correctness, not security
bug classes, though a security finding surfaced incidentally would of
course still be reported through the normal process).

---

## 15. Long-Duration Qualification

**Target release**: `v0.8.0` (bundled with §14, §16). **Depends on**:
§14 (soaks the black-box harness's own scenario catalog, extended in
time — §3).

### Current gap

Existing chaos suites run for bounded iteration/seed counts in CI
(smaller default) and locally (larger, e.g. Phase 10's 200-3,000 seeds,
[`docs/adversarial-testing.md`](adversarial-testing.md)) but never for
sustained wall-clock duration (hours), never nightly on a recurring
schedule, and never with dedicated resource-growth-over-time detection
(goroutine leaks, memory growth, unbounded disk growth beyond what
§10's GC is supposed to prevent). No release-candidate soak gate
exists — nothing today prevents tagging a release the moment CI is
green.

### Architecture

- **Nightly chaos** — a new scheduled (`cron`-triggered) GitHub Actions
  workflow running the full existing `internal/fault` chaos suite plus
  §14's black-box chaos scenarios at a larger seed count than the
  per-push CI default, every night against `main`, independent of any
  particular release candidate.
- **Multi-hour stress** — a dedicated workflow (or a manually/schedule-
  triggered long-running job, since GitHub-hosted runners have job time
  limits that may require self-hosted or chunked execution) running
  sustained mixed read/write load (via `internal/benchutil`/§14's
  black-box client) for a period measured in hours, not the
  seconds-to-minutes existing tests run for.
- **Release-candidate soak testing** — a specific, named gate: before
  any `vX.Y.0` tag (starting with `v0.9.0`, the RC release itself, and
  every release from then on per the updated `docs/releasing.md`
  checklist), the exact commit intended for tagging must complete a
  defined soak window (the exact duration is an implementation-time
  operational decision, published in `docs/releasing.md` once set —
  this document deliberately does not invent a specific number without
  operational data to justify it) with zero invariant violations and no
  unbounded resource growth.
- **Memory/goroutine/disk growth detection** — periodic sampling
  (`runtime.NumGoroutine()`, Go memory stats, `du`-style disk usage)
  during every long-duration run, with growth-rate analysis (not just
  an absolute threshold, since some growth — e.g. the WAL before a
  snapshot boundary — is expected and bounded by design) flagging any
  trend inconsistent with the documented steady-state behavior §10's
  GC/compaction is supposed to produce.
- **Retained machine-readable evidence** — every long-duration run's
  results (pass/fail, seed, duration, resource-growth samples,
  findings) are retained as structured (JSON) artifacts — uploaded as
  CI artifacts and additionally committed to a dated results directory
  for the specific runs backing each release's soak claim, mirroring
  [`docs/benchmarks.md`](benchmarks.md)'s existing discipline of citing
  real, reproducible, retained numbers rather than "trust me" claims.

### Invariants

Like §14, this phase does not add new product invariants — it extends
the *duration and continuity* over which the existing full catalog
(`docs/invariants.md`) is checked, specifically adding two new
qualification-process requirements (not database invariants, but
process invariants this plan defines):

- **NO UNBOUNDED GROWTH** — over a qualifying soak window, memory,
  goroutine count, and disk usage each either stabilize or grow at a
  rate consistent with genuinely bounded, documented causes (e.g. audit
  log growth is expected and unbounded *unless* a future retention
  policy is added — documented explicitly, not silently ignored).
- **RC EVIDENCE COMPLETENESS** — no `vX.Y.0` tag (from `v0.9.0` onward)
  is created without a corresponding retained soak-evidence artifact
  for the exact tagged commit.

### Formats/APIs affected

None to the product. New: the JSON soak-report schema; a
`docs/soak-results/` directory convention (dated, per-run reports).

### Failure semantics

A soak run finding an invariant violation is treated exactly like any
other correctness bug found by any existing suite — reported, fixed,
regression-tested, documented — and blocks the RC/release it was
running for until resolved. A soak run finding unbounded resource
growth without a correctness violation is still a release blocker
(operational unfitness, distinct from but as serious as a correctness
bug for an "enterprise-grade" claim).

### Packages/files expected

`.github/workflows/nightly-chaos.yml` (new); `scripts/soak.sh` (new,
orchestrates the multi-hour run + sampling + report generation, reusing
`internal/fault`'s and §14's `tools/blackbox`'s existing scenario
runners rather than a third, separate implementation); `internal/benchutil`
extended with resource-growth sampling helpers if not already
sufficient; `docs/soak-results/` (new directory, dated JSON + a short
human-readable summary per run); new doc `docs/qualification.md`.

### Deterministic tests

The soak *process* itself (sampling, report generation, growth-rate
analysis) is unit-testable deterministically against synthetic sample
sequences (e.g. a fabricated monotonically-growing memory series must
be flagged; a fabricated bounded-oscillation series must not).

### Integration tests

A short-duration (minutes, not hours — for CI feasibility) "smoke"
version of the soak run, exercised on every push to prove the soak
*mechanism* itself works, distinct from the real multi-hour/nightly
runs which validate the *product*.

### Chaos tests

The nightly/soak runs **are** the chaos tests at this phase's scope —
see Architecture above; no additional new fault-injection mechanism is
introduced, this phase is purely about duration/scheduling/evidence
retention layered on §14's (and the existing Phase 7's) mechanisms.

### Compatibility implications

None.

### Security implications

None beyond what §14/§5 already establish for the harnesses being run
longer.

### Observability

Soak-run dashboards/summaries (could be as simple as the retained JSON
reports plus a small generated summary page — no new product-runtime
observability surface, since this is qualification tooling, not a
`chronicledb-node` feature).

### Documentation

`docs/qualification.md` (new: what qualifies as a passing soak, the
schedule, how to read a retained report); `docs/releasing.md` extended
with the RC soak-evidence-required checklist item.

### Acceptance criteria

- Nightly chaos workflow running clean for a sustained period (e.g. two
  consecutive weeks with zero unaddressed findings) before this phase
  is considered satisfied.
- At least one full multi-hour stress run completed and its report
  retained, with no unbounded growth detected.
- `docs/releasing.md`'s checklist demonstrably includes and is followed
  for the soak-evidence gate on the `v0.9.0` RC itself (a real
  self-application of this phase's own gate, not just a documented
  intention).

### Release gate

`v0.8.0` (bundled with §14, §16); the RC-soak-gate *process* itself
first actually applies to `v0.9.0`.

### Dependencies

§14.

### Explicit non-goals

Multi-day/multi-week continuous production-like operation (that would
start to resemble an actual production deployment claim, explicitly
disallowed per §0.1 — soak windows here are qualification-scale,
measured in hours to at most a few days, not an open-ended "runs
forever" claim); chaos-engineering-as-a-service style continuous
production fault injection (out of scope — this is pre-release
qualification, not a production operational practice this plan
prescribes).

---

## 16. Performance Qualification

**Target release**: `v0.8.0` (bundled with §14, §15). **Depends on**:
§15 (performance numbers are only meaningful once long-duration
correctness is established — §3).

### Current gap

[`docs/benchmarks.md`](benchmarks.md) has real, measured Phase-9
numbers for standalone/replicated write latency, `ReadIndex` read
latency, WAL append latency, recovery time, snapshot cost, and CPU/
allocation profiles — but explicitly *not measured*: throughput under
sustained concurrent multi-client load, and a dedicated failover-time-
as-a-latency-number (§1 of `docs/benchmarks.md`'s own "not measured"
list). There is no regression-threshold mechanism (a number is measured
once and reported; nothing today fails a build/PR if a later change
regresses it), no slow-follower-behavior measurement, and no snapshot/
backup-under-load measurement (backup didn't exist before §6).
`cmd/chronicledb-bench` was explicitly not built at Phase 9, with a
named trigger for revisiting that decision
([`docs/benchmarks.md`](benchmarks.md) §9): "a configurable,
multi-client concurrent-load generator" being needed — which this
phase's throughput-under-load requirement now is.

### Architecture

- **Reproducible workloads** — a small, documented set of named
  workload profiles (e.g. "80/20 read/write mixed," "write-heavy
  single-key hot spot," "wide multi-key transaction") each with a fixed
  definition (key distribution, transaction shape, concurrency level)
  so a "throughput number" is always reproducible against a stated
  workload, not an ambiguous one-off. Reuses/extends
  [`docs/benchmarks.md`](benchmarks.md)'s existing named-workload
  precedent (it already names "an explicitly named 80% read / 20%
  write mixed workload" for its macro-benchmarks).
- **Throughput** — sustained committed-transactions/second under each
  named workload at increasing concurrency, using the
  `cmd/chronicledb-bench` load generator this phase finally builds
  (the concrete trigger Phase 9 named has now been reached — §14's
  black-box client and this phase's needs converge on wanting the same
  configurable multi-client generator, so this tool may reasonably
  share code with `tools/blackbox`'s client, decided at implementation
  time).
- **p50/p95/p99** — extends `internal/benchutil`'s existing latency-
  percentile recorder (already used for Phase 9's replicated-write
  macro-benchmark) to every named workload, under load, not just at
  low/no concurrency.
- **CPU/RAM/disk/network** — resource-usage measurement alongside
  throughput/latency for each workload (via the same `go tool pprof`
  discipline Phase 9 already used for profiling, applied here as a
  *measurement*, not a one-off *investigation*).
- **Failover performance** — the specific, previously-unmeasured
  latency number: from leader-crash-injection to new-leader-serving-
  writes-again, measured directly (not merely proven-to-eventually-
  happen, which Phase 7's chaos suites already do) — a dedicated timer
  around the existing chaos-proven failover mechanism.
- **Slow follower behavior** — throughput/latency impact on the rest of
  the cluster when one follower is artificially slowed (via §14's
  external network-shaping tools, `tc netem`, or the existing `/fault`
  hooks where appropriate) — proving §10's "never blocks cluster-wide
  commit progress" failure-model claim (§2.9 of
  [`docs/failure-model.md`](failure-model.md)) with a real measured
  number, not just a qualitative "it doesn't block."
- **Snapshot/backup-under-load** — extends the existing
  `TestSnapshotLatencyImpact` (Phase 9) pattern to also measure §6's
  backup export running concurrently with normal write load, and to
  measure it against §9's admission-control resource limits (proving
  those limits actually bound the impact, per §9's own architecture).
- **Regression thresholds** — every measured number above gets a
  stored baseline (checked into the repository or a dedicated
  benchmark-history store) and a CI job (likely scheduled/nightly
  rather than per-push, given benchmark noise sensitivity) that fails
  or flags when a new commit regresses a metric beyond a documented
  tolerance — the first time this repository has an automated
  performance gate rather than only point-in-time measurement.

### Invariants

No new product invariants (performance is explicitly never allowed to
be achieved by weakening a correctness invariant — this document
reaffirms [`docs/roadmap.md`](roadmap.md)'s existing Phase 9 rule:
"Any performance optimization proposed in or after Phase 9 must be
checked against `docs/invariants.md` before adoption"). This phase adds
one new *process* invariant:

- **NO SILENT REGRESSION** — a measured performance regression beyond
  the documented tolerance is never merged without an explicit,
  reviewed justification (the same discipline
  [`docs/benchmarks.md`](benchmarks.md) §10 already applies to Phase
  9's own WAL-replay optimization, generalized into an automated gate
  instead of a one-time manual check).

### Formats/APIs affected

None to the product. New: the benchmark-baseline storage format (JSON,
versioned per workload/metric name) and `cmd/chronicledb-bench`'s own
CLI surface.

### Failure semantics

A regression-threshold CI job failing blocks merge/release exactly like
any other CI gate — never silently reported-only. A workload
definition itself changing (not a code regression, a deliberate
workload redefinition) requires an explicit baseline reset, documented
in the commit, never a silent baseline drift.

### Packages/files expected

`cmd/chronicledb-bench` (new, finally built per Phase 9's own named
trigger); `internal/benchutil` (extended: resource-usage sampling
alongside latency percentiles); `.github/workflows/perf-regression.yml`
(new, likely scheduled); `docs/benchmarks.md` (extended with every new
measured number, the regression-threshold mechanism, and the named
workload catalog).

### Deterministic tests

Regression-detection logic itself is unit-testable against synthetic
baseline/current-measurement pairs (a fabricated 2x latency regression
must be flagged; a fabricated 2% noise-level difference must not).

### Integration tests

Full named-workload runs against a real three-node cluster (already the
existing macro-benchmark pattern from Phase 9, extended to run under
concurrent multi-client load via `cmd/chronicledb-bench` rather than a
single benchmark goroutine).

### Chaos tests

Failover-performance and slow-follower measurements are, by
definition, chaos-adjacent (they require injecting the same
crash/slowdown conditions Phase 7's chaos suites already prove safe,
now measured for latency impact rather than only checked for safety).

### Compatibility implications

None.

### Security implications

None.

### Observability

The regression-tracking dashboard/history (could be as simple as the
retained JSON baselines plus a generated trend summary, mirroring
§15's soak-evidence retention pattern) — again qualification tooling,
not a `chronicledb-node` runtime feature.

### Documentation

`docs/benchmarks.md` (extended substantially: every new measured
number, workload catalog, regression-threshold policy,
`cmd/chronicledb-bench` usage).

### Acceptance criteria

- Every task-required metric (throughput, p50/p95/p99, CPU/RAM/disk/
  network, failover performance, slow-follower behavior, snapshot/
  backup-under-load) has at least one real, published, reproducible
  measurement.
- A regression-threshold CI job exists, is exercised (a deliberately
  introduced synthetic regression in a test branch is caught by it),
  and is documented.
- `cmd/chronicledb-bench` exists and its `docs/benchmarks.md` §9
  "not built" note is updated to reflect that its own previously-
  documented trigger condition was met and acted on.

### Release gate

`v0.8.0` (bundled with §14, §15).

### Dependencies

§15.

### Explicit non-goals

Comparative benchmarking against CockroachDB, PostgreSQL, or any other
system (per §0.1 — no equivalence claim, and a fair comparative
benchmark is a substantial, separate methodological undertaking this
plan does not scope); performance tuning/optimization work itself
beyond what a genuine regression fix requires (this phase measures and
gates, it does not chase speed for its own sake — matches Phase 9's own
"no optimization forced where profiling evidence did not justify one"
precedent, [`docs/roadmap.md`](roadmap.md) Phase 9); capacity-planning
guidance for arbitrary customer workloads (the named-workload catalog
is representative, not exhaustive or customer-specific).

---

## 17. Enterprise V1 final acceptance gate

**Target release**: `v1.0.0`. **Depends on**: §5-§16 (all thirteen
build phases) plus §13.3's `v0.9.0` hardening pass.

### 17.1 Exact evidence required before `v1.0.0`

`v1.0.0` may be tagged only when **every** item below is true and
checkable, not asserted:

1. Every acceptance-criteria list in §5 through §16 is satisfied, with
   the evidence for each (test names, retained reports, measured
   numbers) linked from a single tracking document (this document's own
   §17.4 template, filled in at implementation time — not written
   speculatively here).
2. Secure-by-default is live: `-auth-mode none` and no-TLS are no
   longer the shipped default (§5's deferred item), with a documented,
   explicit opt-out for local development only, loudly warned.
3. MVCC GC (§10) runs by default (§10's deferred item), with a real
   long-duration (§15) run proving stable disk usage under continuous
   load with GC enabled by default.
4. The full existing scenario corpus
   ([`docs/scenario-corpus.md`](scenario-corpus.md)) plus every new
   scenario class introduced by §5-§16 passes, in CI, reproducibly.
5. At least one full nightly-chaos cycle (§15) and one RC soak window
   (§15, first real self-application at `v0.9.0`) complete clean against
   the exact commit being tagged `v1.0.0`.
6. The black-box harness (§14) full scenario catalog passes against the
   exact commit being tagged, with results retained.
7. Every published release artifact for `v1.0.0` carries a valid SBOM,
   signature, and provenance attestation (§13), verified in CI.
8. `docs/roadmap.md` §Maturity Model is updated with a new
   `ENTERPRISE-GRADE` row using this document's own evidence-gate
   discipline — written and reviewed at the time `v1.0.0` is actually
   prepared, not pre-written here (this document does not itself
   perform that edit — see §0's "Do NOT... change current maturity").
9. `docs/external-review-findings.md` (Phase 12) reflects the actual,
   current state of external review at the time of tagging — whatever
   that honestly is (this plan does not require external review to have
   found zero issues, only that its real, current status is accurately
   reported, per Phase 12's own existing discipline).

### 17.2 `ENTERPRISE-GRADE` vs. `ENTERPRISE-PROVEN` — the binding distinction

- **`ENTERPRISE-GRADE`** (what `v1.0.0` under this plan is entitled to
  claim): the system's *design and internal qualification evidence*
  meet enterprise operational expectations — secure by default,
  recoverable via tested backup/restore, upgradeable without downtime,
  observable, dynamically maintainable, resource-protected, internally
  qualified via independent (module-boundary-enforced) black-box
  testing, soaked, performance-measured, and supply-chain-attested.
  This is a claim about **what has been built and how rigorously it has
  been checked by ChronicleDB's own (and its own tooling's)
  processes.**
- **`ENTERPRISE-PROVEN`** (what `v1.0.0` under this plan is **not**
  entitled to claim, and what no future version may claim without new,
  separate evidence): that the system has actually operated correctly
  under real, adversarial, sustained production traffic, at scale,
  operated by parties other than this project, over a meaningful
  duration, with a track record of incidents (or lack thereof) to
  point to. That evidence does not exist by construction at `v1.0.0` —
  it can only be accumulated *after* a real production deployment
  history exists, which this plan does not simulate, fabricate, or
  claim on anyone's behalf.
- The same distinction, restated in this repository's existing
  vocabulary
  ([`docs/roadmap.md`](roadmap.md) §Maturity Model's own principle,
  "a benchmark command existing does not prove performance"):
  **an enterprise *feature* existing and being *tested* does not prove
  it is enterprise-*proven* — only real operational history does, and
  none is claimed here.**

### 17.3 `v0.9.0` — Release Candidate / hardening

Not a fourteenth build phase — `v0.9.0` is a stabilization release: it
adds no new capability beyond §5-§16, and exists specifically to (a)
apply the RC soak gate (§15) to a real candidate commit for the first
time, (b) fix whatever §14/§15/§16's qualification work surfaces, and
(c) produce the §17.4 evidence-tracking document in full before
`v1.0.0` is tagged. If §14/§15/§16 find nothing to fix, `v0.9.0` is
still cut (never skipped) specifically to prove the RC-soak-gate
*process itself* works end-to-end on a real tag before it becomes the
`v1.0.0` gate.

### 17.4 Evidence-tracking document (template only — not filled in by this plan)

At `v0.9.0`/`v1.0.0` preparation time, a maintainer produces a table
(in `docs/releasing.md` or a dedicated `docs/enterprise-v1-evidence.md`)
with one row per §17.1 item, each linking to the actual test names,
CI run URLs, and retained report paths that satisfy it — mirroring
exactly the evidence-citation discipline
[`docs/adversarial-testing.md`](adversarial-testing.md) and
[`docs/benchmarks.md`](benchmarks.md) already use for Phases 9-10, not
a new format invented for this purpose.

### Acceptance criteria (of this gate itself)

Everything in §17.1, all nine items, simultaneously true and evidenced.

### Release gate

`v1.0.0`.

### Dependencies

§5, §6, §7, §8, §9, §10, §11, §12, §13, §14, §15, §16 — all of them.

### Explicit non-goals

Every item in §0.1 restated as binding here specifically: `v1.0.0`
under this plan never claims enterprise-*proven*, battle-tested,
production-proven, CockroachDB/PostgreSQL-equivalence, multi-shard
distributed SQL, or external validation beyond whatever
[`docs/external-review-findings.md`](external-review-findings.md)
honestly contains at tagging time.

---

## 18. Enterprise-Scale V2 — explicitly OUT OF SCOPE

This section exists to draw the line this plan does **not** cross, not
to design V2. Nothing below is architected, scheduled, or authorized by
this document.

### Out of scope, explicitly

- **Multi-sharding** — partitioning the keyspace across more than one
  Raft group. Everything in §5-§17 operates within the existing
  single-shard model ([ADR-0001](adr/0001-v1-single-shard-static-cluster-scope.md));
  §8's Dynamic Membership changes *who hosts the one shard*, never *how
  many shards exist*.
- **Range directory** — the metadata service a multi-shard system needs
  to map keys/ranges to shards; has no reason to exist under a
  single-shard model.
- **Range split/merge** — dividing or combining key ranges across
  shards; not applicable without sharding.
- **Replica placement / rebalancing** — automated, policy-driven
  placement of replicas across a multi-shard, multi-node fleet;
  distinct from and far more complex than §8's single-group membership
  change, which has no placement *policy* to speak of (an operator
  explicitly chooses which node joins).
- **Per-shard Raft** — running many independent Raft groups
  simultaneously, with the coordination and resource-isolation
  challenges that implies; §5-§17 assume exactly one Raft group
  throughout.
- **Distributed transactions** — transactions spanning multiple shards
  (requiring cross-shard coordination, e.g. two-phase commit or a
  Percolator/Spanner-style protocol) — meaningless without sharding,
  and already named as deferred in
  [`docs/non-goals.md`](non-goals.md) §Sharding / multi-shard.

### Why

Unchanged from [`docs/non-goals.md`](non-goals.md)'s existing
reasoning, now doubly reinforced: "a distributed database that has not
proven correctness for one shard cannot be trusted to coordinate many"
— and Enterprise V1 (§5-§17) is precisely the process of proving that
one shard is not just *correct* (already substantially evidenced
through Phase 12) but *operable* at enterprise-grade rigor. Sharding
before that operability story exists would compound an unproven axis
onto a still-stabilizing one.

### Revisit when

[`docs/non-goals.md`](non-goals.md)'s existing sharding entry's
trigger ("the single-shard replicated engine has reached at least
`STRONG DISTRIBUTED V1` maturity... with the scenario corpus for
Phases 1-7 passing") is superseded by a sharper one once this plan is
actually executed: **not before `v1.0.0`
(`ENTERPRISE-GRADE`) is reached**, and even then, only via a dedicated
V2 architecture-planning phase (this document's own §0 discipline
applied recursively — a plan before implementation) with its own ADR
explicitly re-examining every item above, informed by real operational
evidence this plan's qualification phases (§14-§16) and any actual
production usage by then will have produced.

### Non-goals (of this section itself)

This section is not a V2 design document and must not be read as one —
no interface, package name, wire format, or data model for any item
above is proposed anywhere in this plan. Any future document that does
propose one is a distinct piece of work requiring its own planning
discipline, not an extension of this one.
