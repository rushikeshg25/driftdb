package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rushikeshg25/driftdb/internal/crdt"
	"github.com/rushikeshg25/driftdb/internal/hlc"
)

func openStore(t *testing.T, dir, node string) *Store {
	t.Helper()
	s, err := Open(dir, node)
	if err != nil {
		t.Fatalf("open %s: %v", node, err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func mustSet(t *testing.T, s *Store, doc, field, value string) {
	t.Helper()
	if _, err := s.Set(doc, field, value); err != nil {
		t.Fatalf("set: %v", err)
	}
}

func TestNodeIdentityIsPinnedToDirectory(t *testing.T) {
	dir := t.TempDir()
	first := openStore(t, dir, "a")
	if _, err := first.Set("d", "f", "v"); err != nil {
		t.Fatal(err)
	}
	first.Close()

	if _, err := Open(dir, "b"); !errors.Is(err, ErrNodeMismatch) {
		t.Fatalf("err = %v, want ErrNodeMismatch", err)
	}
	again := openStore(t, dir, "a")
	if got := again.Stats().Ops; got != 1 {
		t.Fatalf("ops = %d, want 1", got)
	}
}

func TestOpenRequiresNode(t *testing.T) {
	if _, err := Open(t.TempDir(), ""); !errors.Is(err, ErrNoNode) {
		t.Fatalf("err = %v, want ErrNoNode", err)
	}
}

func TestOpenRejectsUnsafeNodeIDs(t *testing.T) {
	for _, node := range []string{"a/b", "a b", " a", "a\tb", strings.Repeat("x", 129)} {
		if _, err := Open(t.TempDir(), node); !errors.Is(err, ErrInvalidNode) {
			t.Fatalf("node %q: err = %v, want ErrInvalidNode", node, err)
		}
	}
	for _, node := range []string{"node-1", "a:b", "replica_2"} {
		st, err := Open(t.TempDir(), node)
		if err != nil {
			t.Fatalf("valid node %q rejected: %v", node, err)
		}
		st.Close()
	}
}

func TestLocalWritesCannotSwitchFieldType(t *testing.T) {
	s := openStore(t, t.TempDir(), "a")
	mustSet(t, s, "doc", "title", "hello")

	if _, err := s.SetAdd("doc", "title", "x"); !errors.Is(err, ErrTypeConflict) {
		t.Fatalf("sadd on register: err = %v, want ErrTypeConflict", err)
	}
	if _, err := s.SetRemove("doc", "title", "x"); !errors.Is(err, ErrTypeConflict) {
		t.Fatalf("srem on register: err = %v, want ErrTypeConflict", err)
	}
	if _, err := s.Set("doc", "title", "still-a-register"); err != nil {
		t.Fatalf("put on register: %v", err)
	}

	if _, err := s.SetAdd("doc", "tags", "x"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Set("doc", "tags", "scalar"); !errors.Is(err, ErrTypeConflict) {
		t.Fatalf("put on set: err = %v, want ErrTypeConflict", err)
	}
	if _, err := s.Delete("doc", "tags"); !errors.Is(err, ErrTypeConflict) {
		t.Fatalf("del on set: err = %v, want ErrTypeConflict", err)
	}
	if _, err := s.SetAdd("doc", "tags", "y"); err != nil {
		t.Fatalf("sadd on set: %v", err)
	}
}

func TestFailedAppendDoesNotAdvanceState(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, "a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Set("d", "f", "before"); err != nil {
		t.Fatal(err)
	}

	// Simulate an I/O failure after the store is open by closing the log file
	// underneath it. The in-memory state must not move ahead of the log.
	if err := s.f.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Set("d", "f", "after"); err == nil {
		t.Fatal("expected write error")
	}
	fields, _ := s.Doc("d")
	if got := fields["f"].Value; got != "before" {
		t.Fatalf("value = %q, want before", got)
	}
	s.f = nil

	reopened := openStore(t, dir, "a")
	fields, _ = reopened.Doc("d")
	if got := fields["f"].Value; got != "before" {
		t.Fatalf("value after reopen = %q, want before", got)
	}
}

func TestLocalMutationsAreVisible(t *testing.T) {
	s := openStore(t, t.TempDir(), "a")
	mustSet(t, s, "note", "title", "hello")

	fields, ok := s.Doc("note")
	if !ok {
		t.Fatal("document missing")
	}
	if got := fields["title"].Value; got != "hello" {
		t.Fatalf("value = %q, want hello", got)
	}

	if _, err := s.SetAdd("note", "tags", "urgent"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SetAdd("note", "tags", "later"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SetRemove("note", "tags", "urgent"); err != nil {
		t.Fatal(err)
	}
	fields, _ = s.Doc("note")
	if got := fields["tags"].Elements; len(got) != 1 || got[0] != "later" {
		t.Fatalf("elements = %v, want [later]", got)
	}
}

func TestDeleteTombstonesField(t *testing.T) {
	s := openStore(t, t.TempDir(), "a")
	mustSet(t, s, "note", "title", "hello")
	if _, err := s.Delete("note", "title"); err != nil {
		t.Fatal(err)
	}
	fields, _ := s.Doc("note")
	if !fields["title"].Deleted {
		t.Fatal("expected tombstone")
	}
}

func TestEmptyKeysRejected(t *testing.T) {
	s := openStore(t, t.TempDir(), "a")
	if _, err := s.Set("", "f", "v"); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("err = %v, want ErrEmptyKey", err)
	}
	if _, err := s.Set("d", "", "v"); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("err = %v, want ErrEmptyKey", err)
	}
}

func TestClockOnlyAdvancesOverContiguousOps(t *testing.T) {
	a := openStore(t, t.TempDir(), "a")
	mustSet(t, a, "d", "f", "one")
	mustSet(t, a, "d", "f", "two")
	mustSet(t, a, "d", "f", "three")

	b := openStore(t, t.TempDir(), "b")
	all := a.Ops()
	// Simulate a partial import: b receives only a's third operation.
	if _, err := b.Apply([]crdt.Op{all[2]}); err != nil {
		t.Fatal(err)
	}
	if got := b.Clock()["a"]; got != 0 {
		t.Fatalf("clock[a] = %d, want 0 while ops 1-2 are missing", got)
	}

	// A full sync must therefore resend the whole prefix.
	n, err := b.Apply(a.Missing(b.Clock()))
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("applied = %d, want the 2 missing ops", n)
	}
	if got := b.Clock()["a"]; got != 3 {
		t.Fatalf("clock[a] = %d, want 3 after filling the gap", got)
	}
}

func TestLocalSeqSkipsImportedCollisions(t *testing.T) {
	a := openStore(t, t.TempDir(), "a")
	mustSet(t, a, "d", "f", "remote-1")
	remoteOp := a.Ops()[0]

	b := openStore(t, t.TempDir(), "b")
	// Give b an op that claims b/1 even though b has not minted anything.
	foreign := remoteOp
	foreign.Node = "b"
	foreign.HLC.Node = "b"
	if _, err := b.Apply([]crdt.Op{foreign}); err != nil {
		t.Fatal(err)
	}
	op, err := b.Set("d", "f", "local")
	if err != nil {
		t.Fatal(err)
	}
	if op.ID() == "b/1" {
		t.Fatal("local sequence collided with an imported op")
	}
}

func TestOpsSurviveReopen(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir, "a")
	mustSet(t, s, "note", "title", "hello")
	mustSet(t, s, "note", "body", "world")
	first, _ := s.CanonicalJSON()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	reopened := openStore(t, dir, "a")
	second, _ := reopened.CanonicalJSON()
	if !bytes.Equal(first, second) {
		t.Fatalf("state changed after reopen:\n%s\n%s", first, second)
	}
	if got := reopened.Stats().Ops; got != 2 {
		t.Fatalf("ops = %d, want 2", got)
	}
}

func TestSequenceContinuesAfterReopen(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir, "a")
	mustSet(t, s, "d", "f", "one")
	mustSet(t, s, "d", "f", "two")
	s.Close()

	reopened := openStore(t, dir, "a")
	op, err := reopened.Set("d", "f", "three")
	if err != nil {
		t.Fatal(err)
	}
	if op.Seq != 3 {
		t.Fatalf("seq = %d, want 3", op.Seq)
	}
	if got := reopened.Clock()["a"]; got != 3 {
		t.Fatalf("clock[a] = %d, want 3", got)
	}
}

func TestTornTrailingWriteIsDiscarded(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir, "a")
	mustSet(t, s, "d", "f", "good")
	s.Close()

	path := filepath.Join(dir, logName)
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(`{"node":"a","seq":99,"kind":"set"`); err != nil {
		t.Fatal(err)
	}
	f.Close()

	reopened := openStore(t, dir, "a")
	if got := reopened.Stats().Ops; got != 1 {
		t.Fatalf("ops = %d, want 1; torn write should be dropped", got)
	}
	fields, _ := reopened.Doc("d")
	if got := fields["f"].Value; got != "good" {
		t.Fatalf("value = %q, want good", got)
	}

	// The log must be usable for new writes after truncation.
	if _, err := reopened.Set("d", "f", "after"); err != nil {
		t.Fatalf("write after recovery: %v", err)
	}
}

func TestRemoteOpsPersistAndDeduplicate(t *testing.T) {
	a := openStore(t, t.TempDir(), "a")
	bDir := t.TempDir()
	b := openStore(t, bDir, "b")
	mustSet(t, b, "note", "title", "from-b")

	ops := b.Ops()
	n, err := a.Apply(ops)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("applied = %d, want 1", n)
	}
	n, err = a.Apply(ops)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("reapplied = %d, want 0", n)
	}

	fields, _ := a.Doc("note")
	if got := fields["title"].Value; got != "from-b" {
		t.Fatalf("value = %q, want from-b", got)
	}
	if got := a.Clock()["b"]; got != 1 {
		t.Fatalf("clock[b] = %d, want 1", got)
	}

	a.Close()
	reopened := openStore(t, a.Dir(), "a")
	if got := reopened.Stats().Ops; got != 1 {
		t.Fatalf("ops after reopen = %d, want 1", got)
	}
}

func TestApplyRejectsConflictingDuplicateID(t *testing.T) {
	s := openStore(t, t.TempDir(), "a")
	mustSet(t, s, "d", "f", "one")
	op := s.Ops()[0]

	clone := op
	clone.Value = "tampered"
	if _, err := s.Apply([]crdt.Op{clone}); err == nil {
		t.Fatal("expected an error for conflicting duplicate id")
	}
}

func TestMissingUsesVectorClock(t *testing.T) {
	a := openStore(t, t.TempDir(), "a")
	b := openStore(t, t.TempDir(), "b")
	mustSet(t, a, "d", "x", "a1")
	mustSet(t, a, "d", "x", "a2")
	mustSet(t, b, "d", "x", "b1")

	missingOnB := a.Missing(b.Clock())
	if len(missingOnB) != 2 {
		t.Fatalf("a missing for b = %d, want 2", len(missingOnB))
	}
	if missing := a.Missing(a.Clock()); len(missing) != 0 {
		t.Fatalf("self missing = %d, want 0", len(missing))
	}
	partial := map[string]uint64{"a": 1}
	if got := len(a.Missing(partial)); got != 1 {
		t.Fatalf("partial missing = %d, want 1", got)
	}
}

func TestMissingIsSorted(t *testing.T) {
	a := openStore(t, t.TempDir(), "a")
	b := openStore(t, t.TempDir(), "b")
	mustSet(t, b, "d", "f", "1")
	mustSet(t, a, "d", "f", "2")
	mustSet(t, b, "d", "f", "3")

	ops := a.Missing(map[string]uint64{})
	last := ""
	for _, op := range ops {
		key := op.Node + "/" + strconv.FormatUint(op.Seq, 10)
		if last != "" && key < last {
			t.Fatalf("ops not sorted: %s before %s", key, last)
		}
		last = key
	}
}

func TestCanonicalJSONIsStable(t *testing.T) {
	s := openStore(t, t.TempDir(), "a")
	mustSet(t, s, "b", "f", "1")
	mustSet(t, s, "a", "f", "2")
	mustSet(t, s, "a", "g", "3")

	first, err := s.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	var pretty bytes.Buffer
	if err := json.Indent(&pretty, first, "", "  "); err != nil {
		t.Fatal(err)
	}
	second, _ := s.CanonicalJSON()
	if !bytes.Equal(first, second) {
		t.Fatal("canonical output is not stable")
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	s, err := Open(t.TempDir(), "a")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	if _, err := s.Set("d", "f", "v"); !errors.Is(err, ErrClosed) {
		t.Fatalf("err = %v, want ErrClosed", err)
	}
}

func TestLocalClockSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	st := openStore(t, dir, "a")
	future := time.Now().Add(24 * time.Hour)
	st.clock = hlc.NewWithTime("a", func() time.Time { return future })
	previous, err := st.Set("d", "f", "before")
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	st = openStore(t, dir, "a")
	next, err := st.Set("d", "f", "after")
	if err != nil {
		t.Fatal(err)
	}
	fields, _ := st.Doc("d")
	if !next.HLC.After(previous.HLC) || fields["f"].Value != "after" {
		t.Fatalf("acknowledged write lost: previous=%v next=%v value=%q", previous.HLC, next.HLC, fields["f"].Value)
	}
}

func TestImportedOwnOperationsAdvanceClock(t *testing.T) {
	source := openStore(t, t.TempDir(), "a")
	future := time.Now().Add(24 * time.Hour)
	source.clock = hlc.NewWithTime("a", func() time.Time { return future })
	op, err := source.Set("d", "f", "imported")
	if err != nil {
		t.Fatal(err)
	}
	restored := openStore(t, t.TempDir(), "a")
	if _, err := restored.Apply([]crdt.Op{op}); err != nil {
		t.Fatal(err)
	}
	next, err := restored.Set("d", "f", "new")
	if err != nil {
		t.Fatal(err)
	}
	fields, _ := restored.Doc("d")
	if !next.HLC.After(op.HLC) || fields["f"].Value != "new" {
		t.Fatalf("own imported timestamp was not observed: %v -> %v", op.HLC, next.HLC)
	}
}
