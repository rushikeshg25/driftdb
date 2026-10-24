// Package hlc implements a hybrid logical clock.
//
// A hybrid logical clock combines physical wall-clock time with a logical
// counter so that timestamps remain monotonic even when the system clock
// jumps backwards or when events arrive from a peer whose clock is ahead.
// Timestamps are totally ordered by (wall, counter, node), which makes them
// safe to use as last-writer-wins conflict resolution timestamps.
package hlc

import (
	"fmt"
	"sync"
	"time"
)

// Timestamp is a totally ordered hybrid logical clock value.
type Timestamp struct {
	// Wall is the physical component in Unix milliseconds.
	Wall int64 `json:"wall"`
	// Counter disambiguates events that share the same wall time.
	Counter uint64 `json:"counter"`
	// Node breaks ties between replicas deterministically.
	Node string `json:"node"`
}

// Compare returns -1, 0, or 1 when t is before, equal to, or after other.
func (t Timestamp) Compare(other Timestamp) int {
	switch {
	case t.Wall < other.Wall:
		return -1
	case t.Wall > other.Wall:
		return 1
	case t.Counter < other.Counter:
		return -1
	case t.Counter > other.Counter:
		return 1
	case t.Node < other.Node:
		return -1
	case t.Node > other.Node:
		return 1
	default:
		return 0
	}
}

// After reports whether t is strictly ordered after other.
func (t Timestamp) After(other Timestamp) bool { return t.Compare(other) > 0 }

// IsZero reports whether the timestamp has not been set.
func (t Timestamp) IsZero() bool {
	return t.Wall == 0 && t.Counter == 0 && t.Node == ""
}

func (t Timestamp) String() string {
	return fmt.Sprintf("%d.%d@%s", t.Wall, t.Counter, t.Node)
}

// Clock produces monotonic timestamps for one node.
type Clock struct {
	mu   sync.Mutex
	node string
	now  func() time.Time
	last Timestamp
}

// New returns a clock for node using the system wall clock.
func New(node string) *Clock {
	return NewWithTime(node, time.Now)
}

// NewWithTime returns a clock with an injectable time source, which keeps
// tests deterministic.
func NewWithTime(node string, now func() time.Time) *Clock {
	return &Clock{node: node, now: now}
}

// Node returns the replica identifier this clock stamps onto timestamps.
func (c *Clock) Node() string { return c.node }

// Now returns a timestamp that is strictly greater than every timestamp this
// clock has produced or observed.
func (c *Clock) Now() Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()

	wall := c.now().UnixMilli()
	if wall > c.last.Wall {
		c.last = Timestamp{Wall: wall, Counter: 0, Node: c.node}
	} else {
		c.last = Timestamp{Wall: c.last.Wall, Counter: c.last.Counter + 1, Node: c.node}
	}
	return c.last
}

// Observe folds a remote timestamp into the clock and returns a new local
// timestamp that is strictly after both the local state and the remote value.
// Call this for every received event so that later local writes win over
// everything the node has already seen.
func (c *Clock) Observe(remote Timestamp) Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()

	wall := c.now().UnixMilli()
	maxWall := max(wall, c.last.Wall, remote.Wall)

	var counter uint64
	switch {
	case maxWall == c.last.Wall && maxWall == remote.Wall:
		counter = max(c.last.Counter, remote.Counter) + 1
	case maxWall == c.last.Wall:
		counter = c.last.Counter + 1
	case maxWall == remote.Wall:
		counter = remote.Counter + 1
	}

	c.last = Timestamp{Wall: maxWall, Counter: counter, Node: c.node}
	return c.last
}

// Last returns the most recent timestamp produced by the clock.
func (c *Clock) Last() Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.last
}
