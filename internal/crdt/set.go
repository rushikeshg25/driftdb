package crdt

import (
	"sort"

	"github.com/rushikeshg25/driftdb/internal/hlc"
)

// Set is an observed-remove set (OR-Set). Every add mints a unique tag.
// Removing an element records the tags the remover had observed; concurrent
// adds mint new tags that survive, which gives the set add-wins semantics.
type Set struct {
	Adds    map[string]map[string]bool `json:"adds"`
	Removed map[string]bool            `json:"removed"`
	// HLC tracks the newest operation applied to this cell. It is used to
	// decide which cell is visible when a field has held different CRDT types.
	HLC hlc.Timestamp `json:"hlc"`
}

func newSet() *Set {
	return &Set{
		Adds:    map[string]map[string]bool{},
		Removed: map[string]bool{},
	}
}

func (s *Set) apply(op Op) {
	if s.HLC.Compare(op.HLC) < 0 {
		s.HLC = op.HLC
	}
	switch op.Kind {
	case OpSetAdd:
		tags := s.Adds[op.Element]
		if tags == nil {
			tags = map[string]bool{}
			s.Adds[op.Element] = tags
		}
		tags[op.Tag()] = true
	case OpSetRemove:
		for _, tag := range op.Tags {
			s.Removed[tag] = true
		}
	}
}

// element reports whether an element has at least one tag that has not been
// removed. Tags belonging to elements that were never added are ignored.
func (s *Set) element(elem string) bool {
	for tag := range s.Adds[elem] {
		if !s.Removed[tag] {
			return true
		}
	}
	return false
}

// members returns the live elements in sorted order.
func (s *Set) members() []string {
	out := make([]string, 0, len(s.Adds))
	for elem := range s.Adds {
		if s.element(elem) {
			out = append(out, elem)
		}
	}
	sort.Strings(out)
	return out
}

// tags returns the live tags currently observed for an element.
func (s *Set) tags(elem string) []string {
	out := make([]string, 0, len(s.Adds[elem]))
	for tag := range s.Adds[elem] {
		if !s.Removed[tag] {
			out = append(out, tag)
		}
	}
	sort.Strings(out)
	return out
}

func (s *Set) view() FieldView {
	return FieldView{
		Kind:     "set",
		Elements: s.members(),
		HLC:      s.HLC,
	}
}
