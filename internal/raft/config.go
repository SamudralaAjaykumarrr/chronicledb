package raft

import "fmt"

// Config configures a Core. It is supplied fresh at every construction
// (including reconstruction after a restart) — Config itself carries
// no persistent state.
type Config struct {
	// ID is this node's identity.
	ID NodeID
	// Bootstrap is the starting Configuration, consulted ONLY when a
	// Core is constructed with no prior log entries and no snapshot at
	// all (a brand-new, never-before-run cluster, or one produced by
	// -restore-from, whose staged snapshot deliberately carries no
	// Configuration — dynamic-membership plan §1.1/§1.8/§7.6). Every
	// later restart derives the active Configuration from durable state
	// instead (ConfigAt), never from this field again.
	Bootstrap Configuration

	// ElectionTimeoutTicks is the minimum number of logical ticks a
	// Follower/Candidate waits, without a valid contact from a current
	// leader, before starting an election.
	ElectionTimeoutTicks int
	// ElectionTimeoutJitterTicks is the width of the additional
	// randomized window added on top of ElectionTimeoutTicks
	// (docs/raft.md §2: randomization avoids split-vote livelock).
	// Actual timeout = ElectionTimeoutTicks + Rand.Intn(ElectionTimeoutJitterTicks+1).
	ElectionTimeoutJitterTicks int
	// HeartbeatTimeoutTicks is how often a Leader re-arms its heartbeat
	// timer. Must be well below ElectionTimeoutTicks so followers do
	// not spuriously time out a healthy leader.
	HeartbeatTimeoutTicks int

	// Rand supplies election-timeout jitter (docs/adr/0009). Required.
	Rand Rand
}

// validate is a bootstrap-seed well-formedness check ("if you are
// seeding a cluster, you must be in it"), not a membership check
// (dynamic-membership plan §1.1): a node joining an already-running
// cluster as a learner is constructed with a deliberately empty
// Bootstrap and never takes this branch at all — an entirely empty
// Bootstrap is valid and is the required state for that case. Only a
// non-empty Bootstrap (a genuine fresh-cluster seed, or a restored
// directory's operator-supplied peer set) must include Config.ID.
func (c Config) validate() error {
	if c.ID == "" {
		return fmt.Errorf("raft: Config.ID must not be empty")
	}
	if len(c.Bootstrap.Voters) > 0 || len(c.Bootstrap.Learners) > 0 {
		if !c.Bootstrap.IsMember(c.ID) {
			return fmt.Errorf("raft: non-empty Config.Bootstrap must include Config.ID (%q)", c.ID)
		}
	}
	if c.ElectionTimeoutTicks <= 0 {
		return fmt.Errorf("raft: Config.ElectionTimeoutTicks must be > 0")
	}
	if c.ElectionTimeoutJitterTicks < 0 {
		return fmt.Errorf("raft: Config.ElectionTimeoutJitterTicks must be >= 0")
	}
	if c.HeartbeatTimeoutTicks <= 0 {
		return fmt.Errorf("raft: Config.HeartbeatTimeoutTicks must be > 0")
	}
	if c.Rand == nil {
		return fmt.Errorf("raft: Config.Rand must not be nil")
	}
	return nil
}

func (c Config) electionTimeout() int {
	if c.ElectionTimeoutJitterTicks == 0 {
		return c.ElectionTimeoutTicks
	}
	return c.ElectionTimeoutTicks + c.Rand.Intn(c.ElectionTimeoutJitterTicks+1)
}
