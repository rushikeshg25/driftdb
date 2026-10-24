package crdt

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/rushikeshg25/driftdb/internal/hlc"
)

func testOp(node string, seq uint64, wall int64, kind OpKind, doc, field string) Op {
	return Op{
		Node:  node,
		Seq:   seq,
		HLC:   hlc.Timestamp{Wall: wall, Counter: 0, Node: node},
		Kind:  kind,
		Doc:   doc,
		Field: field,
	}
}

func applyAll(t *testing.T, ops ...Op) *State {
	t.Helper()
	s := NewState()
	for _, op := range ops {
		if err := s.Apply(op); err != nil {
			t.Fatalf("apply %s: %v", op.ID(), err)
		}
	}
	return s
}

func fieldOf(t *testing.T, s *State, doc, field string) FieldView {
	t.Helper()
	fields, ok := s.Doc(doc)
	if !ok {
		t.Fatalf("doc %q missing", doc)
	}
	f, ok := fields[field]
	if !ok {
		t.Fatalf("field %q missing", field)
	}
	return f
}

func TestRegisterLastWriterWins(t *testing.T) {
	old := testOp("a", 1, 100, OpSet, "note", "title")
	old.Value = "first"
	newer := testOp("b", 1, 200, OpSet, "note", "title")
	newer.Value = "second"

	forward := applyAll(t, old, newer)
	backward := applyAll(t, newer, old)
	for name, s := range map[string]*State{"forward": forward, "backward": backward} {
		if got := fieldOf(t, s, "note", "title").Value; got != "second" {
			t.Fatalf("%s: value = %q, want second", name, got)
		}
	}
}

func TestRegisterDeleteTombstone(t *testing.T) {
	set := testOp("a", 1, 100, OpSet, "note", "title")
	set.Value = "hello"
	del := testOp("a", 2, 200, OpDelete, "note", "title")

	s := applyAll(t, set, del)
	got := fieldOf(t, s, "note", "title")
	if !got.Deleted || got.Value != "" {
		t.Fatalf("expected deleted empty register, got %+v", got)
	}
}

func TestRegisterResurrectionWithNewerWrite(t *testing.T) {
	set := testOp("a", 1, 100, OpSet, "note", "title")
	set.Value = "hello"
	del := testOp("a", 2, 200, OpDelete, "note", "title")
	revive := testOp("b", 1, 300, OpSet, "note", "title")
	revive.Value = "again"

	got := fieldOf(t, applyAll(t, del, set, revive), "note", "title")
	if got.Deleted || got.Value != "again" {
		t.Fatalf("expected resurrected value, got %+v", got)
	}
}

func TestORSetAddRemove(t *testing.T) {
	addA := testOp("a", 1, 100, OpSetAdd, "board", "tags")
	addA.Element = "urgent"
	addB := testOp("a", 2, 110, OpSetAdd, "board", "tags")
	addB.Element = "later"

	rm := testOp("a", 3, 120, OpSetRemove, "board", "tags")
	rm.Element = "urgent"
	rm.Tags = []string{addA.Tag()}

	got := fieldOf(t, applyAll(t, addA, addB, rm), "board", "tags")
	if len(got.Elements) != 1 || got.Elements[0] != "later" {
		t.Fatalf("elements = %v, want [later]", got.Elements)
	}
}

func TestORSetConcurrentAddWinsOverRemove(t *testing.T) {
	addOld := testOp("a", 1, 100, OpSetAdd, "board", "tags")
	addOld.Element = "x"
	// Replica a removes what it has seen.
	rm := testOp("a", 2, 200, OpSetRemove, "board", "tags")
	rm.Element = "x"
	rm.Tags = []string{addOld.Tag()}
	// Replica b concurrently adds the same element with a fresh tag.
	addNew := testOp("b", 1, 200, OpSetAdd, "board", "tags")
	addNew.Element = "x"

	got := fieldOf(t, applyAll(t, addOld, rm, addNew), "board", "tags")
	if len(got.Elements) != 1 || got.Elements[0] != "x" {
		t.Fatalf("concurrent add should survive, elements = %v", got.Elements)
	}
}

func TestORSetReAddAfterRemove(t *testing.T) {
	add := testOp("a", 1, 100, OpSetAdd, "board", "tags")
	add.Element = "x"
	rm := testOp("a", 2, 200, OpSetRemove, "board", "tags")
	rm.Element = "x"
	rm.Tags = []string{add.Tag()}
	readd := testOp("a", 3, 300, OpSetAdd, "board", "tags")
	readd.Element = "x"

	got := fieldOf(t, applyAll(t, add, rm, readd), "board", "tags")
	if len(got.Elements) != 1 || got.Elements[0] != "x" {
		t.Fatalf("re-add should be visible, elements = %v", got.Elements)
	}
}

func TestRemovingUnknownTagIsHarmless(t *testing.T) {
	add := testOp("a", 1, 100, OpSetAdd, "board", "tags")
	add.Element = "x"
	rm := testOp("a", 2, 200, OpSetRemove, "board", "tags")
	rm.Element = "x"
	rm.Tags = []string{"does/not/exist"}

	got := fieldOf(t, applyAll(t, add, rm), "board", "tags")
	if len(got.Elements) != 1 {
		t.Fatalf("unknown tag should not remove element, elements = %v", got.Elements)
	}
}

func TestMixedFieldTypesConverge(t *testing.T) {
	reg := testOp("a", 1, 100, OpSet, "weird", "f")
	reg.Value = "scalar"
	set := testOp("b", 1, 200, OpSetAdd, "weird", "f")
	set.Element = "member"

	a := applyAll(t, reg, set)
	b := applyAll(t, set, reg)
	if got := fieldOf(t, a, "weird", "f").Kind; got != "set" {
		t.Fatalf("newer set should win, kind = %q", got)
	}
	ca, _ := a.CanonicalJSON()
	cb, _ := b.CanonicalJSON()
	if !bytes.Equal(ca, cb) {
		t.Fatalf("states diverged:\n%s\n%s", ca, cb)
	}
}

func TestApplyIsOrderIndependent(t *testing.T) {
	ops := []Op{
		testOp("a", 1, 100, OpSet, "d", "title"),
		testOp("b", 1, 150, OpSet, "d", "title"),
		testOp("a", 2, 200, OpSetAdd, "d", "tags"),
		testOp("b", 2, 250, OpSetRemove, "d", "tags"),
		testOp("a", 3, 300, OpDelete, "d", "body"),
		testOp("c", 1, 120, OpSet, "e", "n"),
	}
	ops[0].Value = "first"
	ops[1].Value = "second"
	ops[2].Element = "one"
	ops[3].Element = "one"
	ops[3].Tags = []string{ops[2].Tag()}

	want, err := applyAll(t, ops...).CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}

	rng := rand.New(rand.NewSource(7))
	for i := 0; i < 50; i++ {
		shuffled := append([]Op(nil), ops...)
		rng.Shuffle(len(shuffled), func(i, j int) {
			shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
		})
		got, err := applyAll(t, shuffled...).CanonicalJSON()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("order %d diverged:\nwant %s\ngot  %s", i, want, got)
		}
	}
}

func TestApplyIsIdempotent(t *testing.T) {
	op := testOp("a", 1, 100, OpSet, "d", "title")
	op.Value = "v"

	a := applyAll(t, op, op, op)
	want, _ := applyAll(t, op).CanonicalJSON()
	got, _ := a.CanonicalJSON()
	if !bytes.Equal(got, want) {
		t.Fatalf("idempotency failed:\nwant %s\ngot  %s", want, got)
	}
}

func TestValidateRejectsMalformedOps(t *testing.T) {
	valid := testOp("a", 1, 100, OpSet, "d", "f")
	cases := map[string]func(Op) Op{
		"no node":    func(o Op) Op { o.Node = ""; return o },
		"no seq":     func(o Op) Op { o.Seq = 0; return o },
		"no hlc":     func(o Op) Op { o.HLC = hlc.Timestamp{}; return o },
		"wrong node": func(o Op) Op { o.HLC.Node = "b"; return o },
		"bad kind":   func(o Op) Op { o.Kind = "nope"; return o },
		"no doc":     func(o Op) Op { o.Doc = ""; return o },
		"no field":   func(o Op) Op { o.Field = ""; return o },
		"no element": func(o Op) Op { o.Kind = OpSetAdd; return o },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			if err := mutate(valid).Validate(); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid op rejected: %v", err)
	}
}
