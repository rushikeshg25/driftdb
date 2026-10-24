// Package crdt implements the conflict-free replicated data types that back
// driftdb: last-writer-wins registers and observed-remove sets, folded from an
// append-only operation log.
package crdt

import (
	"errors"
	"fmt"
	"strings"

	"github.com/rushikeshg25/driftdb/internal/hlc"
)

// OpKind identifies the mutation carried by an operation.
type OpKind string

const (
	// OpSet writes a string value into a document field (LWW register).
	OpSet OpKind = "set"
	// OpDelete tombstones a document field (LWW register).
	OpDelete OpKind = "del"
	// OpSetAdd adds an element to an OR-Set field.
	OpSetAdd OpKind = "sadd"
	// OpSetRemove removes the observed tags of an element from an OR-Set field.
	OpSetRemove OpKind = "srem"
)

// Valid reports whether the kind is known.
func (k OpKind) Valid() bool {
	switch k {
	case OpSet, OpDelete, OpSetAdd, OpSetRemove:
		return true
	default:
		return false
	}
}

// Op is the smallest unit of replication. Every replica applies the same full
// set of operations, so state converges regardless of delivery order.
type Op struct {
	Node  string        `json:"node"`
	Seq   uint64        `json:"seq"`
	HLC   hlc.Timestamp `json:"hlc"`
	Kind  OpKind        `json:"kind"`
	Doc   string        `json:"doc"`
	Field string        `json:"field"`
	Value string        `json:"value,omitempty"`
	// Element is set for sadd/srem.
	Element string `json:"element,omitempty"`
	// Tags records the add-tags observed by a removal, which is what makes
	// OR-Set removal commutative.
	Tags []string `json:"tags,omitempty"`
}

// ID returns the globally unique identifier of the operation.
func (o Op) ID() string { return fmt.Sprintf("%s/%d", o.Node, o.Seq) }

// Tag returns the OR-Set tag minted by this operation.
func (o Op) Tag() string { return o.ID() }

// Validate checks the structural invariants of an operation. It does not
// require a particular kind to be semantically valid for the target field.
func (o Op) Validate() error {
	if strings.TrimSpace(o.Node) == "" {
		return errors.New("crdt: op has no node")
	}
	if o.Seq == 0 {
		return errors.New("crdt: op has zero sequence")
	}
	if o.HLC.IsZero() {
		return errors.New("crdt: op has no hlc timestamp")
	}
	if o.HLC.Node != o.Node {
		return fmt.Errorf("crdt: op %s has hlc node %q", o.ID(), o.HLC.Node)
	}
	if !o.Kind.Valid() {
		return fmt.Errorf("crdt: op %s has unknown kind %q", o.ID(), o.Kind)
	}
	if o.Doc == "" {
		return fmt.Errorf("crdt: op %s has empty document", o.ID())
	}
	if o.Field == "" {
		return fmt.Errorf("crdt: op %s has empty field", o.ID())
	}
	if (o.Kind == OpSetAdd || o.Kind == OpSetRemove) && o.Element == "" {
		return fmt.Errorf("crdt: op %s has empty element", o.ID())
	}
	for _, tag := range o.Tags {
		if tag == "" {
			return fmt.Errorf("crdt: op %s has empty tag", o.ID())
		}
	}
	return nil
}
