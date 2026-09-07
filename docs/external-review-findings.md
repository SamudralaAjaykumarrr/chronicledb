# External Review Findings Ledger

Status: Phase 12 evidence ledger
([`roadmap.md`](roadmap.md) Phase 12,
[`break-chronicledb.md`](break-chronicledb.md)). This document records
every report received against the Break ChronicleDB challenge, whether
confirmed or not, using the fixed schema in §Schema below. It is
updated honestly regardless of outcome — a report that turns out to be
working-as-documented is recorded as such, not omitted; an untriaged
report stays visible as "Not yet triaged," not silently dropped.

**As of this writing, zero reports have been received.** No external
review has occurred. This is stated plainly rather than omitted, in the
same style [`adversarial-testing.md`](adversarial-testing.md) §10
reported Phase 10 finding zero new production defects, and
[`docs/roadmap.md`](roadmap.md)'s general principle that a maturity or
evidence claim is only as good as what actually happened, not what was
predicted or hoped for. This ledger will be updated the moment a real
report arrives — never backdated, never pre-populated.

## Schema

One row per report:

| Field | Notes |
|---|---|
| Report ID | GitHub issue number, or "private advisory — see below" (with only the resolution summarized publicly once appropriate, never the advisory's private content). |
| Date received | ISO date. |
| Reviewer / handle | As given, or "anonymous" if the reporter declined. |
| Environment / commit | Exact commit hash, OS/arch, Go version, deployment mode. |
| Deterministic seed or reproduction | Seed+command, or a description of the manual/real-cluster sequence. |
| Expected invariant | Cited from [`invariants.md`](invariants.md) (or [`sql.md`](sql.md)/[`failure-model.md`](failure-model.md) for non-catalog properties). |
| Observed result | What actually happened. |
| Classification | One of: safety / liveness / durability-recovery / isolation / idempotency / raft / snapshot-compaction / sql-frontend / observability-benchmark / security (routed away, not detailed here) / not-a-bug (working as documented). |
| Confirmed? | Yes / No / Not yet triaged. |
| Root cause (if confirmed) | One paragraph, matching the style of [`testing-strategy.md`](testing-strategy.md) §7's existing bug write-ups. |
| Regression test | Test name + file, or "N/A — see reasoning" if genuinely not applicable. |
| Fix commit | Hash, once merged. |
| Release containing fix | Version, once released, or "unreleased." |

## Reports

_No reports recorded yet._

## Running tally

**0 reports received, 0 confirmed, 0 fixed** (as of this writing).

This tally is maintained by hand alongside the per-row detail above and
must never be allowed to drift from it — the same discipline
[`scenario-corpus.md`](scenario-corpus.md) and
[`adversarial-testing.md`](adversarial-testing.md)'s own closing
summary sections already hold themselves to.

A null result (this challenge running for a stated period with zero
confirmed findings) is a legitimate, honestly-reportable outcome — see
[`non-goals.md`](non-goals.md) §Staff/Principal validation and
equivalence claims and [`roadmap.md`](roadmap.md) §Maturity Model
(`STAFF/PRINCIPAL DISCUSSION READY`) for what a real review's actual
outcome, including a documented null result, would need to look like
before any further maturity claim is made on top of it.
