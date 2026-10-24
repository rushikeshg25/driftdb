package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/rushikeshg25/driftdb/internal/crdt"
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
