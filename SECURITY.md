# Security Policy

ChronicleDB is a from-scratch, evidence-driven distributed database
project (see [`docs/vision.md`](docs/vision.md)). Its current maturity
is documented in [`README.md`](README.md) and
[`docs/roadmap.md`](docs/roadmap.md) — please read the "Deployment
assumptions" section below before relying on it for anything you did
not build yourself. If you're reviewing ChronicleDB specifically to
try to find correctness violations (not a security vulnerability), see
[`docs/break-chronicledb.md`](docs/break-chronicledb.md) instead — the
reporting process below is unchanged.

## Supported versions

ChronicleDB is pre-1.0 (see [`docs/versioning.md`](docs/versioning.md)
for the full versioning policy). There is no long-term support branch:
only the latest tagged release and the `main` branch receive security
fixes. If a vulnerability is found in an older tag, the fix lands in a
new release rather than being backported.

| Version | Supported |
|---|---|
| Latest tagged release | Yes |
| `main` (unreleased) | Yes, best-effort |
| Older tagged releases | No — upgrade to the latest release |

## Reporting a vulnerability

**Please do not open a public GitHub issue for a suspected security
vulnerability.** Instead, use GitHub's private vulnerability reporting
for this repository:

1. Go to the repository's **Security** tab.
2. Click **Report a vulnerability** to open a private GitHub Security
   Advisory.
3. Include: a description of the issue, the affected version/commit,
   reproduction steps, and — if it is a correctness/data-safety issue
   rather than a classic security vulnerability — whether you believe
   data loss, a durability violation, or an incorrect commit/read
   result is involved (see [`docs/invariants.md`](docs/invariants.md)
   for the invariant catalog a report like this might be violating).

If private advisory reporting is unavailable to you for some reason,
open a regular issue asking a maintainer to contact you, without
describing the vulnerability itself in public.

We do not currently operate a bug bounty program. Reports are handled
on a best-effort basis by the project maintainer(s); there is no SLA.

## Deployment assumptions (please read before deploying)

As of `v0.2.0` (Security Foundation, `docs/enterprise-v1-plan.md` §5;
not yet tagged/released), ChronicleDB *supports* TLS (client and peer
mTLS), authentication (static bearer token or mTLS identity), RBAC, and
audit logging — see [`docs/security.md`](docs/security.md) for the full
operational guide. **None of it is on by default.** Every relevant flag
defaults to the exact `v0.1.0` behavior described below, so an in-place
binary upgrade of an existing trusted-network deployment does not
silently change behavior:

- **A trusted network remains the default assumption** between clients
  and the cluster, and among cluster nodes, until an operator
  explicitly configures `-tls-cert`/`-peer-tls-cert`/`-auth-mode`. With
  those left unset, there is still no authentication, authorization, or
  transport encryption on the Raft transport or the HTTP control plane
  (`cmd/chronicledb-node`'s `/propose`, `/status`, `/outcome`, `/fault`
  when explicitly enabled, `/metrics`, `/health` endpoints), and anyone
  who can reach these ports can propose mutations and read cluster
  state. A node running this way prints a loud, repeated warning.
- **No input sanitization boundary for a hostile client.** The
  constrained SQL frontend (`internal/sql`, [`docs/sql.md`](docs/sql.md))
  is fuzz-tested against malformed/adversarial *syntax* (it never
  panics on bad input), but this is a correctness/robustness property,
  not a claim that ChronicleDB is safe to expose directly to an
  untrusted network.
- **Do not expose ChronicleDB directly to the public internet**, even
  with TLS/auth configured — Byzantine node behavior and CA compromise
  remain out of scope (`docs/security.md` §2). Run it behind your own
  network boundary as you would any database.

Configuring TLS/auth is a **required migration**, not optional
hardening — see [`docs/security.md`](docs/security.md) §7. Flipping the
*default* to secure-by-default (rather than merely secure-by-
configuration) remains tracked as a prerequisite for any future
production-readiness claim — see [`docs/roadmap.md`](docs/roadmap.md)
§Maturity Model and `docs/enterprise-v1-plan.md` §5/§13.2.

## What we do not claim

Consistent with [`docs/non-goals.md`](docs/non-goals.md) and
[`README.md`](README.md)'s maturity statement, ChronicleDB does not
claim to be production-ready, hardened for internet-facing deployment,
or externally security-audited. No such audit has occurred as of this
writing.
