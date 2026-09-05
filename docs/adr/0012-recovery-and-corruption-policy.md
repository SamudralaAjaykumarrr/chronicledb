# ADR-0012: Recovery and Corruption Policy

Status: Accepted

## Context

Restart/recovery is where a durability model either holds up or gets
silently violated. ChronicleDB needs an explicit, unambiguous policy
for what corruption is safe to auto-repair versus what must halt the
node — and for how to determine the true committed boundary rather
than trusting "what's on disk" at face value.

## Decision

- **Torn final record**: automatically truncated and recovery proceeds
  (see [`docs/wal.md`](../wal.md) §6.1).
- **Fully framed record with a bad checksum, anywhere**: startup
  refused unconditionally; requires operator intervention (see
  [`docs/wal.md`](../wal.md) §6.2, [`docs/recovery.md`](../recovery.md) §4).
- **Committed boundary** is never inferred from log presence alone; it
  is reconstructed from the snapshot boundary plus legitimate
  leader-confirmed `commitIndex` information (or, in standalone mode,
  from the `Sync()`-confirmed durability boundary directly) — see
  [`docs/recovery.md`](../recovery.md) §2.
- The full 14-step recovery sequence in
  [`docs/recovery.md`](../recovery.md) §1 is binding.

## Alternatives Considered

1. **Best-effort recovery: skip corrupted records and continue
   replaying what comes after.** Rejected: this is precisely the
   "invented committed state" failure mode
   `RECOVERY-NON-INVENTION` exists to prevent — skipping a corrupted
   record could silently discard a transaction's effects while leaving
   later, dependent-looking records in place, producing a
   database that looks internally consistent but is factually wrong.
2. **Treat any bad checksum (including mid-log) the same as a torn
   tail (auto-truncate everything from that point).** Rejected: a
   mid-log bad checksum could be discarding already-acknowledged,
   already-replicated (in Raft mode) committed data — silently
   truncating it would violate `DURABILITY` for data the client was
   told was safe.
3. **Trust `commitIndex` as persisted directly, rather than
   reconstructing it.** Rejected: see
   [ADR-0008](0008-raft-architecture-and-persistent-state.md)
   alternative 3 — a cached `commitIndex` can be based on
   since-superseded information.
4. **Automatic quorum-based self-healing of a corrupted node's local
   WAL (ask peers to resend the correct data automatically).**
   Deferred, not rejected — a plausible future enhancement once the
   manual re-provisioning procedure (discard local data, rejoin as a
   fresh node via snapshot install) is proven reliable; automating it
   prematurely risks the automation itself becoming a new source of
   `RECOVERY-NON-INVENTION` bugs.

## Consequences

- A single bit-flip in the middle of a node's WAL, in V1, takes that
  node offline until an operator re-provisions it — a real
  availability cost, accepted because the alternative (guessing) risks
  silent correctness violations, which are worse than a clear,
  actionable failure.
- Recovery logic must be written and tested to distinguish "the last
  record is incomplete because we crashed mid-write" from "a record
  earlier in the file is corrupted" — these look similar at a glance
  (both are "checksum mismatch") but require materially different
  code paths.

## Correctness Implications

- Directly implements `RECOVERY-NON-INVENTION`,
  `APPLIED-PREFIX-SAFETY`, and `ABORT-SAFETY`
  ([`docs/invariants.md`](../invariants.md)).

## Testing and Proof Obligations

- LD-4 (torn tail, auto-recovers), LD-5/LD-6 (corruption, startup
  refused) in [`docs/scenario-corpus.md`](../scenario-corpus.md)
  §Local Durability.
- Restart-with-uncommitted-durable-suffix scenario (extension of
  RF-4/RF-9) verifying the committed-boundary reconstruction rule, not
  log-presence, governs what gets applied.
