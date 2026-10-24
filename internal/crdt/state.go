package crdt

import (
	"encoding/json"
	"sort"

	"github.com/rushikeshg25/driftdb/internal/hlc"
)

// FieldView is the materialized, JSON-friendly form of a document field.
type FieldView struct {
	Kind     string        `json:"kind"`
	Value    string        `json:"value,omitempty"`
	Deleted  bool          `json:"deleted,omitempty"`
	Elements []string      `json:"elements,omitempty"`
	HLC      hlc.Timestamp `json:"hlc"`
}

// Field holds both possible CRDT shapes for a key. In normal use a field is
// only ever a register or a set; keeping both cells means that even a
// type-conflicting pair of operations converges deterministically. The visible
// cell is the one with the newest timestamp.
type Field struct {
	Reg *Register
	Set *Set
}

func (f *Field) view() FieldView {
	switch {
	case f.Reg == nil:
		return f.Set.view()
	case f.Set == nil:
		return f.Reg.view()
	case f.Reg.HLC.Compare(f.Set.HLC) >= 0:
		return f.Reg.view()
	default:
		return f.Set.view()
	}
}

// Doc is a named collection of CRDT fields.
type Doc struct {
	ID     string
	Fields map[string]*Field
}

func newDoc(id string) *Doc {
	return &Doc{ID: id, Fields: map[string]*Field{}}
}

func (d *Doc) field(name string) *Field {
	f := d.Fields[name]
	if f == nil {
		f = &Field{}
		d.Fields[name] = f
	}
	return f
}

// State is the materialized view of every applied operation.
type State struct {
	Docs map[string]*Doc
}

// NewState returns an empty state.
func NewState() *State {
	return &State{Docs: map[string]*Doc{}}
}

// Apply folds one operation into the state. Applying the same operation twice
// is harmless; applying a set of operations in any order produces the same
// state.
func (s *State) Apply(op Op) error {
	if err := op.Validate(); err != nil {
		return err
	}
	d := s.Docs[op.Doc]
	if d == nil {
		d = newDoc(op.Doc)
		s.Docs[op.Doc] = d
	}
	f := d.field(op.Field)

	switch op.Kind {
	case OpSet, OpDelete:
		if f.Reg == nil {
			f.Reg = &Register{}
		}
		f.Reg.apply(op)
	case OpSetAdd, OpSetRemove:
		if f.Set == nil {
			f.Set = newSet()
		}
		f.Set.apply(op)
	}
	return nil
}

// ObservedTags returns the live OR-Set tags this replica has seen for an
// element. A removal op carries these tags so that only the observed adds are
// cancelled.
func (s *State) ObservedTags(doc, field, element string) []string {
	d := s.Docs[doc]
	if d == nil {
		return nil
	}
	f := d.Fields[field]
	if f == nil || f.Set == nil {
		return nil
	}
	return f.Set.tags(element)
}

// Doc returns the materialized fields of one document. The second return value
// reports whether the document exists.
func (s *State) Doc(id string) (map[string]FieldView, bool) {
	d := s.Docs[id]
	if d == nil {
		return nil, false
	}
	out := make(map[string]FieldView, len(d.Fields))
	for name, f := range d.Fields {
		out[name] = f.view()
	}
	return out, true
}

// DocIDs returns the document identifiers in sorted order.
func (s *State) DocIDs() []string {
	ids := make([]string, 0, len(s.Docs))
	for id := range s.Docs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// View returns the whole state as nested maps, sorted by encoding/json.
func (s *State) View() map[string]map[string]FieldView {
	out := make(map[string]map[string]FieldView, len(s.Docs))
	for id, d := range s.Docs {
		fields := make(map[string]FieldView, len(d.Fields))
		for name, f := range d.Fields {
			fields[name] = f.view()
		}
		out[id] = fields
	}
	return out
}

// CanonicalJSON serializes the state deterministically. It is the basis for
// convergence checks: two replicas that have seen the same operations must
// produce byte-identical output.
func (s *State) CanonicalJSON() ([]byte, error) {
	return json.Marshal(s.View())
}
