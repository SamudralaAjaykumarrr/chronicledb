package node

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/fsm"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/raft"
)

// This file carries the three cross-package compatibility proofs
// dynamic-membership plan §19 gate 5 requires and §18's matrix names:
// the entry-payload sentinel's non-collision, and the two independent
// old-binary-fails-closed paths (wire, §2.5; disk, §6.1a).
//
// internal/node is the smallest package that legitimately imports both
// internal/raft and internal/fsm (internal/raft deliberately does not
// import internal/fsm — §2.4's dependency direction — so neither of
// those packages can host a test that crosses the boundary at all).

// realEntryConfigWALPayload commits a genuine EntryConfig entry through
// the ordinary production path — a real three-node cluster, finalized to
// generation 2, then a real AddLearner — and returns the leader's raw,
// durable WAL payload bytes for that entry.
//
// Nothing here is synthesized: the bytes under test are the bytes
// encodeEntryPayload actually wrote to disk for an entry
// Core.ProposeConfigChange actually produced, which is what makes the
// two fails-closed proofs below statements about the shipped format
// rather than about a test's own idea of it.
func realEntryConfigWALPayload(t *testing.T) []byte {
	t.Helper()

	tc := newTestCluster(t, 3)
	leader := tc.leaderNode(5 * time.Second)
	mustFinalizeToMax(t, tc, leader)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	outcome, err := leader.AddLearner(ctx, fsm.RequestID("compat-add-n4"), raft.NodeID("n4"), "127.0.0.1:1")
	if err != nil {
		t.Fatalf("AddLearner: %v", err)
	}
	if outcome.Status != fsm.StatusCommitted {
		t.Fatalf("AddLearner outcome = %+v, want Committed", outcome)
	}
	return readWALRecordPayload(t, leader, raft.Index(outcome.CommitSeq))
}

// TestEntryPayloadSentinelNeverCollides pins the assumption
// entryPayloadTypeSentinel's own doc comment rests on (§6.1a): 0xFF
// occupies a namespace that does not exist at payload offset 8 in any
// format this project has ever written, so decodeEntryPayload can treat
// "byte 8 is the sentinel" as an unambiguous discriminator.
//
// The structural half mirrors internal/fsm's existing
// ControlCommandMarker/commitTxnCommandVersion guard one layer up, at
// the payload-framing level. commitTxnCommandVersion is unexported, so
// this reads the version byte back off the production encoder's own
// output rather than restating the constant — a mechanical bump of that
// version toward 0xFF in some future phase must fail this test, which it
// could not do against a copied literal.
//
// The behavioral half is what actually matters: every command form this
// codebase produces must survive encodeEntryPayload/decodeEntryPayload
// as EntryNormal. A command whose first byte were the sentinel would
// make its own untyped payload decode as the typed form and be rejected
// (or worse, misread) on replay, which is exactly the silent format
// misinterpretation the sentinel exists to prevent.
func TestEntryPayloadSentinelNeverCollides(t *testing.T) {
	if entryPayloadTypeSentinel == fsm.ControlCommandMarker {
		t.Fatalf("entryPayloadTypeSentinel (%#x) collides with fsm.ControlCommandMarker (%#x)",
			entryPayloadTypeSentinel, fsm.ControlCommandMarker)
	}

	commitTxn := fsm.EncodeCommitTxn(fsm.CommitTxnCommand{RequestID: "r1", TxnID: 1, StartSeq: 0})
	setGen := fsm.EncodeSetClusterVersion(fsm.SetClusterVersionCommand{RequestID: "g1", TargetGeneration: 1})

	if commitTxn[0] == entryPayloadTypeSentinel {
		t.Fatalf("CommitTxn's command-version byte (%#x) equals entryPayloadTypeSentinel — an ordinary "+
			"CommitTxn payload would decode as the typed form", commitTxn[0])
	}

	for _, tc := range []struct {
		name string
		data []byte
	}{
		{"CommitTxn", commitTxn},
		{"SetClusterVersion", setGen},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			payload := encodeEntryPayload(7, raft.EntryNormal, tc.data)

			// ROLLBACK BOUNDARY HONESTY: the untyped form carries no
			// header at all, so it stays byte-identical to what every
			// release through v0.4.0 wrote for the same command.
			if len(payload) != 8+len(tc.data) {
				t.Fatalf("untyped payload is %d bytes, want %d (8-byte term + %d data) — a header leaked "+
					"into the EntryNormal form", len(payload), 8+len(tc.data), len(tc.data))
			}

			term, typ, got, err := decodeEntryPayload(payload)
			if err != nil {
				t.Fatalf("decodeEntryPayload: %v", err)
			}
			if typ != raft.EntryNormal {
				t.Fatalf("decoded type = %d, want EntryNormal (%d) — this payload's first data byte (%#x) "+
					"was mistaken for the typed-entry sentinel", typ, raft.EntryNormal, tc.data[0])
			}
			if term != 7 {
				t.Fatalf("decoded term = %d, want 7", term)
			}
			if string(got) != string(tc.data) {
				t.Fatalf("decoded data = %#v, want %#v", got, tc.data)
			}
		})
	}
}

// TestOldBinaryFailsClosedOnEntryConfigWirePayload is the wire-path half
// of NO SILENT FORMAT MISINTERPRETATION (§2.5, §19 gate 5).
//
// internal/transport gob-encodes raft.Message, and gob silently drops a
// field the decoder's struct does not have, so a pre-v0.5.0 peer that
// receives an EntryConfig entry loses Entry.Type entirely and sees an
// ordinary EntryNormal entry carrying this exact Data. Its unmodified
// applyCommitted then dispatches on fsm.IsControlCommand — which matches,
// because §2.5 deliberately reuses 0xF0 — into fsm.DecodeSetClusterVersion,
// the only control-command decoder that binary has.
//
// That decoder must fail closed on the membership kind byte rather than
// misread the entry. The fsm and raft control-kind ranges are asserted
// disjoint by fsm.TestControlKindRangesNeverCollide; this test proves the
// consequence end to end, against a real committed entry, which is the
// part that cross-package constant arithmetic alone cannot establish.
func TestOldBinaryFailsClosedOnEntryConfigWirePayload(t *testing.T) {
	payload := realEntryConfigWALPayload(t)

	// Entry.Data as it travels on the wire. Read back through the
	// production decoder so the bytes are the field's real contents, not
	// an offset this test asserted for itself.
	_, typ, data, err := decodeEntryPayload(payload)
	if err != nil {
		t.Fatalf("decodeEntryPayload: %v", err)
	}
	if typ != raft.EntryConfig {
		t.Fatalf("decoded type = %d, want EntryConfig (%d)", typ, raft.EntryConfig)
	}

	if !fsm.IsControlCommand(data) {
		t.Fatalf("EntryConfig Data[0] = %#x: an old binary would route this to DecodeCommitTxn, not the "+
			"control-command decoder this test's premise (§2.5) depends on", data[0])
	}
	if _, err := fsm.DecodeSetClusterVersion(data); !errors.Is(err, fsm.ErrUnknownControlCommand) {
		t.Fatalf("a pre-v0.5.0 binary's DecodeSetClusterVersion on a real EntryConfig payload: err = %v, "+
			"want ErrUnknownControlCommand (it must fail closed, never misread the membership kind byte)", err)
	}
}

// TestOldBinaryFailsClosedOnTypedEntryConfigWALPayload is the disk-path
// half of NO SILENT FORMAT MISINTERPRETATION (§6.1a, §19 gate 5), and is
// independent of the wire path above: it exercises the typed-entry header
// the wire never carries.
//
// A pre-v0.5.0 binary's decodeEntryPayload knows nothing of the header
// and returns the whole remainder after the 8-byte term — so what its
// applyCommitted receives begins with the sentinel itself. That byte is
// not fsm.ControlCommandMarker, so the entry routes to DecodeCommitTxn,
// where the sentinel lands in the command-version position and must be
// refused. This is the property TestEntryPayloadSentinelNeverCollides
// guards structurally, proven here against a genuine durable record.
func TestOldBinaryFailsClosedOnTypedEntryConfigWALPayload(t *testing.T) {
	payload := realEntryConfigWALPayload(t)

	if !hasTypedEntryHeader(payload) {
		t.Fatalf("durable EntryConfig payload carries no typed-entry header: % x", payload[:min(12, len(payload))])
	}

	// Exactly what v0.1.0-v0.4.0's decodeEntryPayload returns as Data.
	oldData := payload[8:]

	if fsm.IsControlCommand(oldData) {
		t.Fatalf("old binary's Data[0] = %#x matched IsControlCommand — the typed header must not be "+
			"mistaken for a control command", oldData[0])
	}
	if _, err := fsm.DecodeCommitTxn(oldData); !errors.Is(err, fsm.ErrUnsupportedCommandVersion) {
		t.Fatalf("a pre-v0.5.0 binary's DecodeCommitTxn on a real typed EntryConfig WAL payload: err = %v, "+
			"want ErrUnsupportedCommandVersion (it must fail closed on the sentinel in the version position)", err)
	}
}
