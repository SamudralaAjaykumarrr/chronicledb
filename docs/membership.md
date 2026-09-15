# Dynamic Membership Operator Runbook

Implemented in `internal/raft` (the consensus mechanism),
`internal/fsm` (the membership `RequestID` outcome table),
`internal/snapshot`/`internal/backup` (durable format and restore
behavior), and `internal/node`/`cmd/chronicledb-node` (the admin API).
Full design and safety proof: [`docs/dynamic-membership-plan.md`
](dynamic-membership-plan.md). Architecture decision:
[`ADR-0018`](adr/0018-dynamic-membership-architecture.md).

This document is the operator-facing "how do I actually do this"
companion; it does not re-derive why the mechanism is safe.

## 1. Prerequisites

Dynamic membership requires the cluster to have finalized to generation
2 (`docs/upgrades.md` §8a). Since generation stepping is N/N+1-only, a
fresh cluster needs **two** `/admin/upgrade/finalize` calls to get
there from generation 0. Any membership call made before that returns
`412` with `reason: "generation-too-low"`.

## 2. Adding a node

1. Start the new node process: a genuinely empty data directory, no
   `-peers`/`-cluster` flags (it joins by replication, not by
   bootstrap), listening on its own address, with a valid mTLS identity
   certificate already issued for its intended node ID if peer TLS is
   configured.
2. `POST /admin/membership/add` against the current leader:
   ```json
   {"requestId": "<opaque>", "nodeId": "n4", "address": "host:port"}
   ```
3. The call returns once the add is durably committed (`status:
   "committed"`) — **not** once the new node has caught up. Poll `GET
   /admin/membership/status` (readable by every role) for
   `learners[].lag` reaching `0` before promoting.

## 3. Promoting a learner to voter

```json
POST /admin/membership/promote
{"requestId": "<opaque>", "nodeId": "n4"}
```

**`425` is normal and retryable, not a failure.** The promotion gate
requires the learner's replication position to exactly match the
leader's at the instant of the call; on a cluster under continuous
write load this is a narrow, moving target, so the shipped default
makes a single-shot promote unreliable by construction. Retry with the
**same** `requestId` — it is a pre-proposal refusal that records
nothing, so retrying is always safe — either in a client-side loop or
during a quiet moment. Treating `425` as a terminal failure is the
single most common operator mistake with this endpoint.

A `412` with `reason: "peer-generation-too-old"` means the target
learner's own binary has not been confirmed to understand the
cluster's current agreed format generation; upgrade its binary first.

## 4. Removing a node

```json
POST /admin/membership/remove
{"requestId": "<opaque>", "nodeId": "n2"}
```

Works identically whether the target is a voter or a learner, and
whether or not the target is currently reachable (an unreachable
voter's own absence never blocks its own removal from committing). The
leader may also remove **itself** — the call still returns the
committed outcome normally; the (former) leader steps down the instant
the removal commits, and the remaining voters elect a successor.

### Sub-three-voter confirmation

Any add/promote/remove whose **resulting** voter count would drop below
three is refused with `409` and `reason: "confirmation-required"`,
naming the exact resulting count in `resultingVoterCount`. Resubmit the
**same** `requestId` with `confirmVoterCount` set to that exact number:

```json
{"requestId": "<opaque>", "nodeId": "n2", "confirmVoterCount": 2}
```

The count must be **stated**, not merely `force: true` — this fails
safe if the cluster's size changed between when the operator last
looked and when the request lands. This is an operator-policy guard,
not a safety mechanism: quorum mathematics itself refuses a removal
that would leave zero voters unconditionally, with no confirmation
field able to override it.

**A 2-voter cluster has zero fault tolerance** (`majority(2) = 2`: the
loss of either node stalls the cluster). **A 1-voter cluster has no
fault tolerance and no remaining purpose for Raft** — the loss of that
single node's disk is unrecoverable except from backup
(`docs/backup.md`). Both are permitted by the mechanism when explicitly
confirmed; neither is recommended for anything but transient
maintenance windows.

## 5. Even-voter-count warning

A response whose resulting voter count is even carries a non-blocking
`warning` field: an even count never provides more fault tolerance than
the next smaller odd count, at the cost of a strictly larger required
quorum. This never blocks the operation — it is purely advisory.

## 6. The post-election "not ready" window

Immediately after any leader election, a brief window exists where
`/admin/membership/status` reports `changesReady: false` with a
`notReadyReason` (`"inherited-suffix-uncommitted"` or
`"no-current-term-commit"`) — the new leader has not yet committed
anything in its own term. This resolves on its own, automatically,
within one ordinary replication round on a healthy cluster; a
membership call made during this window returns `503` with
`Retry-After` set. **Poll `changesReady`, or simply retry with the same
`requestId` after the indicated delay — never treat a `503` here as a
failure.**

## 7. Decommissioning a node

Removal is **authoritative cluster-side**: a node is removed the instant
the configuration entry removing it commits under a majority of the new
configuration. That is never conditioned on the removed node hearing
about it. A removed node is retired whether or not its own disk ever
learns — but retirement is not self-announcing, so steps 2–5 are
yours, not the cluster's.

1. Remove it via §4 above.
2. **Stop the removed process.** A removed follower does **not** learn
   of its own removal: replication to it stops the instant the removal
   entry is *appended*, so the entry that would tell it is never sent
   to it. Left running it continues to answer client requests as an
   ordinary follower (`NotLeaderError`, **not** `ErrNodeRemoved`) and
   continues to campaign on its stale configuration. That is harmless
   to the remaining cluster — every active member denies its
   `RequestVote`, and it can never win an election even with no
   filtering at all (`docs/invariants.md`'s
   `MEMBERSHIP-SCOPED VOTE ACCEPTANCE` and
   `CONFIGURATION BRANCH CONFINEMENT`) — but it is not
   self-correcting, and it will generate useless election traffic and
   connection attempts indefinitely. `ErrNodeRemoved` is returned only
   by a node that has *observed* its own removal, which in practice
   means a leader that removed itself.
3. **Wipe or archive its data directory** before reusing the node
   operationally in any form — including restarting it, re-adding it
   under a different ID, or repurposing the hardware. Its durable log
   still asserts the pre-removal configuration and will be re-adopted
   verbatim on any restart, putting the stale process straight back
   into the campaigning-forever state of step 2. Archive rather than
   wipe if you may need the directory for forensics; either way it must
   not be started again as-is.
4. **Revoke or let expire its mTLS certificate via your CA tooling.**
   This is recommended defense in depth and to stop wasted background
   connection attempts — it is **not** required for cluster safety,
   which never depended on it. ChronicleDB does not implement an
   OCSP/CRL mechanism of its own; certificate lifecycle is an operator/
   CA responsibility, unchanged from `v0.2.0`.
5. **Do not reuse a removed node's ID for a genuinely different
   physical node without first decommissioning the original** — steps
   2, 3 and 4 in full. Reuse is permitted by the mechanism but is not
   separately detected, and mTLS identity binding plus this
   confirmation is the only guard against a mistaken reuse.

## 8. HTTP status codes and reason vocabulary

Every error response body carries a machine-readable `reason` field —
never distinguish conditions by status code alone, since two distinct
conditions share `412`.

| Status | Meaning | `reason` values | Retryable with same `requestId`? |
|---|---|---|---|
| `400` | Malformed request or illegal transition | `invalid-transition`, `last-voter-removal` | No — the request itself is wrong |
| `409` | Not leader, leadership lost mid-call, change already in progress, confirmation required, or this node was removed | `not-leader` (see `leaderHint`), `leadership-lost` (see `Retry-After`), `change-in-progress`, `confirmation-required` (see `resultingVoterCount`), `node-removed`, `request-id-conflict` | No — but see below |
| `412` | Capability not permitted | `generation-too-low`, `peer-generation-too-old` | No (fix the underlying condition first) |
| `425` | Learner not caught up | `learner-not-caught-up` (see `lag`) | **Yes** |
| `503` | Transiently not ready | `not-ready-inherited-suffix`, `not-ready-no-current-term-commit` | **Yes** (see `Retry-After`) |

Only `425` and `503` are pre-proposal refusals that record nothing —
every other refusal is terminal for that specific submission, though a
fresh submission (same or new `requestId`, once the underlying
condition is fixed) is always possible.

**`not-leader` and `leadership-lost` are the one pair where "fresh
submission" means specifically the *same* `requestId`, not a new one.**
Both mean "retry against whoever leads next," and `leadership-lost` in
particular can occur *after* the proposal was already accepted for
replication — an election can race a membership proposal and cost the
leader its role before the entry commits. Whether that entry went on to
commit anyway is genuinely unknown to the caller at that point, which
is exactly what makes reusing the same `requestId` required rather than
merely convenient: it is what lets §9's idempotency table resolve to
the real outcome (committed or not) instead of risking a second,
independent attempt at the same change.

## 9. RBAC and audit

`/admin/membership/add`, `/promote`, and `/remove` require the `admin`
role, like `/admin/upgrade/finalize`. `/admin/membership/status` is
readable by every role (`admin`, `operator`, `read-only`), mirroring
`/status`'s existing openness — it is pure observability. Every call to
any of the four endpoints produces an audit record, including refused
calls, carrying the same `reason` string as the HTTP response body.

## 10. Related documents

- [`docs/dynamic-membership-plan.md`](dynamic-membership-plan.md) —
  the full design and safety proof this runbook operationalizes.
- [`ADR-0018`](adr/0018-dynamic-membership-architecture.md) —
  architecture decision record.
- [`docs/raft.md`](raft.md) §12 — the consensus mechanism summary.
- [`docs/backup.md`](backup.md) §10a — restore membership bootstrap.
- [`docs/upgrades.md`](upgrades.md) §8a — the generation-2 prerequisite.
- [`docs/invariants.md`](invariants.md) — the "Dynamic Membership
  invariants" section.
- [`docs/security.md`](security.md) — the RBAC/audit/mTLS model these
  endpoints build on.
