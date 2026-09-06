package fault

import (
	"sort"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/raft"
)

// ClusterOptions configures NewCluster. Zero values fall back to
// reasonable, well-separated defaults (heartbeat well below election
// timeout, per docs/raft.md §2).
type ClusterOptions struct {
	ElectionTimeoutTicks       int
	ElectionTimeoutJitterTicks int
	HeartbeatTimeoutTicks      int
	// Seed drives every randomized choice in the cluster (election
	// jitter). The same Seed plus the same sequence of Cluster calls
	// always reproduces the same run (docs/testing-strategy.md §3.2).
	Seed int64
}

func (o *ClusterOptions) setDefaults() {
	if o.ElectionTimeoutTicks <= 0 {
		o.ElectionTimeoutTicks = 10
	}
	if o.ElectionTimeoutJitterTicks < 0 {
		o.ElectionTimeoutJitterTicks = 0
	}
	if o.HeartbeatTimeoutTicks <= 0 {
		o.HeartbeatTimeoutTicks = 2
	}
}

// Cluster is a deterministic multi-node Raft test harness
// (docs/testing-strategy.md §3): real, unmodified raft.Core instances,
// one per peer, wired together through a Transport, driven by explicit
// logical-time advancement rather than time.Sleep. Every method is a
// simple, explicit action a test script calls in whatever order and
// combination it needs — there is no hidden background goroutine.
type Cluster struct {
	nodes     map[raft.NodeID]*Node
	order     []raft.NodeID
	transport *Transport
	rnd       *Rand
	tick      int64
	seed      int64
}

// NewCluster constructs a Cluster with one Node per entry in peers,
// all sharing the same randomized-timeout Rand source (seeded from
// opts.Seed) the way separate real processes would each have their own
// independent RNG stream in production, but reproducibly here.
func NewCluster(peers []raft.NodeID, opts ClusterOptions) *Cluster {
	opts.setDefaults()
	cl := &Cluster{
		nodes:     make(map[raft.NodeID]*Node, len(peers)),
		transport: NewTransport(),
		rnd:       NewRand(opts.Seed),
		seed:      opts.Seed,
	}
	cl.order = append([]raft.NodeID(nil), peers...)
	sort.Slice(cl.order, func(i, j int) bool { return cl.order[i] < cl.order[j] })

	allPeers := append([]raft.NodeID(nil), peers...)
	for _, id := range peers {
		cfg := raft.Config{
			ID:                         id,
			Peers:                      append([]raft.NodeID(nil), allPeers...),
			ElectionTimeoutTicks:       opts.ElectionTimeoutTicks,
			ElectionTimeoutJitterTicks: opts.ElectionTimeoutJitterTicks,
			HeartbeatTimeoutTicks:      opts.HeartbeatTimeoutTicks,
			Rand:                       cl.rnd,
		}
		cl.nodes[id] = newNode(cfg)
	}
	return cl
}

// Node returns the Node for id, or nil if id is not a cluster member.
func (cl *Cluster) Node(id raft.NodeID) *Node { return cl.nodes[id] }

// NodeIDs returns every cluster member, in a stable (sorted) order.
func (cl *Cluster) NodeIDs() []raft.NodeID { return append([]raft.NodeID(nil), cl.order...) }

// Transport returns the cluster's Transport, for direct fault-
// injection control beyond the convenience wrappers below.
func (cl *Cluster) Transport() *Transport { return cl.transport }

// LogicalTick returns the cluster's current logical time.
func (cl *Cluster) LogicalTick() int64 { return cl.tick }

// Seed returns the seed this cluster was constructed with
// (docs/testing-strategy.md §3.2's reproduction triple: configuration +
// seed + explicit call sequence) — a failing chaos run logs this so it
// can be replayed exactly.
func (cl *Cluster) Seed() int64 { return cl.seed }

// Compact performs a node's own local log compaction against a
// snapshot boundary at uptoIndex (docs/snapshots.md §3's "a node's own
// local compaction," the same operation internal/node.Node.maybeSnapshot
// performs against a real snapshot; the simulator has no FSM/snapshot
// content of its own — see internal/fault's package doc comment — so
// this only exercises raft.Core's log-retention side of
// LOG-COMPACTION-SAFETY, which is transport/FSM-independent by
// construction). It reports whether the compaction actually advanced
// anything, mirroring raft.Core.Compact.
func (cl *Cluster) Compact(id raft.NodeID, uptoIndex raft.Index) bool {
	n := cl.nodes[id]
	if n == nil || n.Crashed() {
		return false
	}
	return n.core.Compact(uptoIndex)
}

func (cl *Cluster) flush(n *Node) {
	for _, m := range n.DrainOutbox() {
		cl.transport.Send(m)
	}
}

// Tick advances logical time by exactly one step across every
// non-crashed node (docs/testing-strategy.md §3.1's single global
// logical clock), then enqueues whatever messages that produced into
// the Transport. It does not itself deliver anything — call
// DeliverEligible (or a more targeted Deliver) to actually move
// messages between nodes.
func (cl *Cluster) Tick() {
	cl.tick++
	cl.transport.advanceTick(cl.tick)
	for _, id := range cl.order {
		n := cl.nodes[id]
		n.Tick()
		cl.flush(n)
	}
}

// AdvanceTicks calls Tick n times.
func (cl *Cluster) AdvanceTicks(n int) {
	for i := 0; i < n; i++ {
		cl.Tick()
	}
}

func (cl *Cluster) deliverToNode(m raft.Message) {
	n, ok := cl.nodes[m.To]
	if !ok || n.Crashed() {
		return // unknown or down destination: the message is simply lost
	}
	n.Step(raft.Input{Kind: raft.InputMessage, Message: m})
	cl.flush(n)
}

// DeliverEligible delivers every currently eligible pending message
// (respecting partitions/isolation/delay), repeating until a full pass
// produces no further eligible messages — so a delivery that itself
// produces an immediately-eligible reply (e.g. a heartbeat prompting an
// instant AppendEntriesResponse) cascades within one call. It returns
// the total number of messages delivered.
func (cl *Cluster) DeliverEligible() int {
	delivered := 0
	for {
		msgs := cl.transport.TakeEligible()
		if len(msgs) == 0 {
			return delivered
		}
		for _, m := range msgs {
			cl.deliverToNode(m)
			delivered++
		}
	}
}

// Deliver force-delivers one specific pending message by id (see
// Transport.Take), regardless of partition state.
func (cl *Cluster) Deliver(id uint64) bool {
	m, ok := cl.transport.Take(id)
	if !ok {
		return false
	}
	cl.deliverToNode(m)
	return true
}

// Propose submits data as a new command directly to the named node
// (test convenience: Phase 4 has no client-facing routing layer — see
// docs/raft.md §8/§Phase boundary). If leader is not currently Leader,
// the proposal is rejected exactly as raft.Core.Step documents.
func (cl *Cluster) Propose(leader raft.NodeID, data []byte) {
	n := cl.nodes[leader]
	if n == nil {
		return
	}
	n.Step(raft.Input{Kind: raft.InputPropose, ProposeData: data})
	cl.flush(n)
}

// Crash simulates an ungraceful crash of id: its volatile Core is
// discarded; its durable Storage survives untouched.
func (cl *Cluster) Crash(id raft.NodeID) {
	if n := cl.nodes[id]; n != nil {
		n.Crash()
	}
}

// Restart brings a crashed node back, reconstructing its Core from
// durable Storage via the real recovery path (docs/recovery.md).
func (cl *Cluster) Restart(id raft.NodeID) {
	if n := cl.nodes[id]; n != nil {
		n.Restart()
	}
}

// Partition blocks every link between groupA and groupB, in both
// directions (docs/replication.md §5's network partition contract).
func (cl *Cluster) Partition(groupA, groupB []raft.NodeID) { cl.transport.Partition(groupA, groupB) }

// IsolateNode fully disconnects id from the rest of the cluster.
func (cl *Cluster) IsolateNode(id raft.NodeID) { cl.transport.IsolateNode(id) }

// HealNode reconnects a node previously isolated by IsolateNode.
func (cl *Cluster) HealNode(id raft.NodeID) { cl.transport.HealNode(id) }

// HealAll fully reconnects the simulated network (clears every
// partition and isolation control).
func (cl *Cluster) HealAll() { cl.transport.HealAll() }

// Leaders returns every currently non-crashed node whose Core believes
// itself to be Leader right now — the direct oracle for
// RAFT-ELECTION-SAFETY ("at most one leader" — note that at most one
// per *term* is the actual invariant; a test asserting cluster-wide
// uniqueness should pair this with each returned leader's CurrentTerm).
func (cl *Cluster) Leaders() []raft.NodeID {
	var out []raft.NodeID
	for _, id := range cl.order {
		n := cl.nodes[id]
		if n.Crashed() {
			continue
		}
		if n.Core().Role() == raft.Leader {
			out = append(out, id)
		}
	}
	return out
}

// SettleElection alternates advancing one logical tick and delivering
// every eligible message, up to maxRounds times, stopping as soon as
// exactly one leader exists. It returns whether a single leader was
// reached. This is a convenience for tests that don't need to
// hand-drive every individual message — hand-driving remains available
// via Deliver/Transport for tests that do.
func (cl *Cluster) SettleElection(maxRounds int) bool {
	for i := 0; i < maxRounds; i++ {
		cl.AdvanceTicks(1)
		cl.DeliverEligible()
		if len(cl.Leaders()) == 1 {
			return true
		}
	}
	return false
}
