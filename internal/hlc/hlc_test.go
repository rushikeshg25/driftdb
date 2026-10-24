package hlc

import (
	"testing"
	"time"
)

func fixedClock(node string, ms int64) *Clock {
	t := time.UnixMilli(ms)
	return NewWithTime(node, func() time.Time { return t })
}

func TestNowIsStrictlyMonotonic(t *testing.T) {
	c := fixedClock("a", 1_000)
	prev := c.Now()
	for i := 0; i < 100; i++ {
		next := c.Now()
		if !next.After(prev) {
			t.Fatalf("timestamp %v is not after %v", next, prev)
		}
		prev = next
	}
}

func TestNowFollowsWallClock(t *testing.T) {
	now := time.UnixMilli(1_000)
	c := NewWithTime("a", func() time.Time { return now })
	first := c.Now()
	now = now.Add(5 * time.Second)
	second := c.Now()

	if second.Wall != 6_000 {
		t.Fatalf("wall = %d, want 6000", second.Wall)
	}
	if second.Counter != 0 {
		t.Fatalf("counter = %d, want 0 after wall advance", second.Counter)
	}
	if !second.After(first) {
		t.Fatal("second timestamp should be after first")
	}
}

func TestWallClockGoingBackwardsKeepsMonotonicity(t *testing.T) {
	now := time.UnixMilli(10_000)
	c := NewWithTime("a", func() time.Time { return now })
	first := c.Now()
	now = now.Add(-time.Hour)
	second := c.Now()

	if second.Wall != first.Wall {
		t.Fatalf("wall went backwards: %d -> %d", first.Wall, second.Wall)
	}
	if second.Counter != first.Counter+1 {
		t.Fatalf("counter = %d, want %d", second.Counter, first.Counter+1)
	}
}

func TestObserveAdvancesPastRemote(t *testing.T) {
	c := fixedClock("a", 1_000)
	remote := Timestamp{Wall: 5_000, Counter: 7, Node: "b"}
	got := c.Observe(remote)

	if got.Wall != 5_000 || got.Counter != 8 {
		t.Fatalf("observe = %v, want wall 5000 counter 8", got)
	}
	if got.Node != "a" {
		t.Fatalf("node = %q, want a", got.Node)
	}
}

func TestObserveCombinesLocalAndRemoteLogicalTime(t *testing.T) {
	c := fixedClock("a", 5_000)
	c.Now() // wall 5000, counter 0
	got := c.Observe(Timestamp{Wall: 5_000, Counter: 4, Node: "b"})

	if got.Wall != 5_000 || got.Counter != 5 {
		t.Fatalf("got %v, want wall 5000 counter 5", got)
	}
}

func TestObserveRemoteInThePast(t *testing.T) {
	c := fixedClock("a", 5_000)
	local := c.Now()
	got := c.Observe(Timestamp{Wall: 1_000, Counter: 99, Node: "b"})

	if !got.After(local) {
		t.Fatalf("got %v, want after local %v", got, local)
	}
}

func TestCompareOrdersByWallThenCounterThenNode(t *testing.T) {
	base := Timestamp{Wall: 10, Counter: 2, Node: "b"}
	cases := []struct {
		name string
		a, b Timestamp
		want int
	}{
		{"wall", Timestamp{Wall: 9, Counter: 99, Node: "z"}, base, -1},
		{"counter", Timestamp{Wall: 10, Counter: 1, Node: "z"}, base, -1},
		{"node", Timestamp{Wall: 10, Counter: 2, Node: "a"}, base, -1},
		{"equal", base, base, 0},
		{"greater", Timestamp{Wall: 11}, base, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.a.Compare(tc.b); got != tc.want {
				t.Fatalf("Compare() = %d, want %d", got, tc.want)
			}
			if got := tc.b.Compare(tc.a); got != -tc.want {
				t.Fatalf("reverse Compare() = %d, want %d", got, -tc.want)
			}
		})
	}
}

func TestConcurrentUseIsSafe(t *testing.T) {
	c := New("a")
	done := make(chan Timestamp, 4)
	for i := 0; i < 4; i++ {
		go func() {
			var last Timestamp
			for j := 0; j < 250; j++ {
				last = c.Now()
			}
			done <- last
		}()
	}
	seen := map[string]bool{}
	for i := 0; i < 4; i++ {
		ts := <-done
		seen[ts.String()] = true
	}
	if len(seen) != 4 {
		t.Fatalf("expected 4 distinct final timestamps, got %d", len(seen))
	}
}
