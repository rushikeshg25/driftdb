package crdt

import "github.com/rushikeshg25/driftdb/internal/hlc"

// Register is a last-writer-wins string cell. Deletes are represented as
// tombstones carrying the timestamp of the delete so that a concurrent write
// with a newer timestamp can still resurrect the field.
type Register struct {
	Value   string        `json:"value"`
	HLC     hlc.Timestamp `json:"hlc"`
	Deleted bool          `json:"deleted,omitempty"`
}

// apply folds a register operation into the cell.
func (r *Register) apply(op Op) {
	if r.HLC.After(op.HLC) {
		return
	}
	// A strictly newer operation wins. Equal timestamps cannot occur for one
	// node, and cross-node ties are broken by node id inside HLC.Compare.
	if r.HLC.Compare(op.HLC) == 0 {
		return
	}
	r.HLC = op.HLC
	switch op.Kind {
	case OpDelete:
		r.Deleted = true
		r.Value = ""
	default:
		r.Deleted = false
		r.Value = op.Value
	}
}

// view projects the register into its wire representation.
func (r *Register) view() FieldView {
	return FieldView{
		Kind:    "reg",
		Value:   r.Value,
		Deleted: r.Deleted,
		HLC:     r.HLC,
	}
}
