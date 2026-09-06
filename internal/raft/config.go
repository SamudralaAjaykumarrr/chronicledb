package raft

import "fmt"

// Config configures a Core. It is supplied fresh at every construction
// (including reconstruction after a restart) — Config itself carries
// no persistent state.
type Config struct {
	// ID is this node's identity. Must be a member of Peers.
	ID NodeID
	// Peers lists every voting member of the cluster, including ID
	// itself (docs/architecture.md §1: static membership in V1).
	Peers []NodeID

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

func (c Config) validate() error {
	if c.ID == "" {
		return fmt.Errorf("raft: Config.ID must not be empty")
	}
	found := false
	for _, p := range c.Peers {
		if p == c.ID {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("raft: Config.Peers must include Config.ID (%q)", c.ID)
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

// majority returns the smallest count that constitutes a majority of
// len(c.Peers).
func (c Config) majority() int {
	return len(c.Peers)/2 + 1
}
