// Package store wraps a CRDT state and an append-only operation log. Local
// mutations are timestamped, applied, and fsynced before they are reported as
// successful; remote operations are deduplicated by id and folded into the
// same state.
package store

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"

	"github.com/rushikeshg25/driftdb/internal/crdt"
	"github.com/rushikeshg25/driftdb/internal/hlc"
)

const (
	logName      = "ops.jsonl"
	identityName = "node"
)

var (
	// ErrNoNode is returned when a store is opened without a replica id.
	ErrNoNode = errors.New("store: node id is required")
	// ErrClosed is returned after the store has been closed.
	ErrClosed = errors.New("store: closed")
	// ErrEmptyKey is returned for operations missing a document or field.
	ErrEmptyKey = errors.New("store: document and field are required")
	// ErrNodeMismatch is returned when a data directory already belongs to a
	// different replica id.
	ErrNodeMismatch = errors.New("store: data directory belongs to another node")
)

// Store is a single replica's database: an op log plus its materialized state.
type Store struct {
	mu    sync.RWMutex
	dir   string
	node  string
	clock *hlc.Clock
	state *crdt.State
	ops   map[string]crdt.Op
	seqs  map[string]uint64
	f     *os.File
}

// Open loads (or creates) the store in dir for the given replica id.
func Open(dir, node string) (*Store, error) {
	if node == "" {
		return nil, ErrNoNode
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("store: create directory: %w", err)
	}
	if err := pinIdentity(dir, node); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, logName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("store: open log: %w", err)
	}

	s := &Store{
		dir:   dir,
		node:  node,
		clock: hlc.New(node),
		state: crdt.NewState(),
		ops:   map[string]crdt.Op{},
		seqs:  map[string]uint64{},
		f:     f,
	}
	if err := s.replay(); err != nil {
		f.Close()
		return nil, err
	}
	return s, nil
}

// pinIdentity records the replica id in the data directory and refuses to
// silently reuse operations from another identity.
func pinIdentity(dir, node string) error {
	path := filepath.Join(dir, identityName)
	raw, err := os.ReadFile(path)
	switch {
	case err == nil:
		existing := strings.TrimSpace(string(raw))
		if existing != node {
			return fmt.Errorf("%w: %q (requested %q)", ErrNodeMismatch, existing, node)
		}
		return nil
	case errors.Is(err, os.ErrNotExist):
		if err := os.WriteFile(path, []byte(node+"\n"), 0o644); err != nil {
			return fmt.Errorf("store: write node identity: %w", err)
		}
		return nil
	default:
		return fmt.Errorf("store: read node identity: %w", err)
	}
}

// replay rebuilds state from disk. A trailing line without a newline is
// treated as a torn write from a crash and truncated away.
func (s *Store) replay() error {
	if _, err := s.f.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("store: seek log: %w", err)
	}
	r := bufio.NewReaderSize(s.f, 1<<20)

	var offset int64
	for {
		line, err := r.ReadBytes('\n')
		if len(line) > 0 {
			if line[len(line)-1] != '\n' {
				if terr := s.f.Truncate(offset); terr != nil {
					return fmt.Errorf("store: truncate torn write: %w", terr)
				}
				break
			}
			if len(bytes.TrimSpace(line)) > 0 {
				if err := s.ingest(line[:len(line)-1]); err != nil {
					return fmt.Errorf("store: replay at offset %d: %w", offset, err)
				}
			}
			offset += int64(len(line))
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("store: read log: %w", err)
		}
	}
	_, err := s.f.Seek(0, io.SeekEnd)
	return err
}

func (s *Store) ingest(raw []byte) error {
	var op crdt.Op
	if err := json.Unmarshal(raw, &op); err != nil {
		return fmt.Errorf("decode op: %w", err)
	}
	if err := op.Validate(); err != nil {
		return err
	}
	if existing, ok := s.ops[op.ID()]; ok {
		if !reflect.DeepEqual(existing, op) {
			return fmt.Errorf("op %s appears twice with different contents", op.ID())
		}
		return nil
	}
	if err := s.state.Apply(op); err != nil {
		return err
	}
	s.ops[op.ID()] = op
	if op.Seq > s.seqs[op.Node] {
		s.seqs[op.Node] = op.Seq
	}
	if op.Node != s.node {
		s.clock.Observe(op.HLC)
	}
	return nil
}

// applyLocked persists and applies an operation. It reports whether the op was
// new. The caller must hold the write lock.
func (s *Store) applyLocked(op crdt.Op) (bool, error) {
	if s.f == nil {
		return false, ErrClosed
	}
	if existing, ok := s.ops[op.ID()]; ok {
		if !reflect.DeepEqual(existing, op) {
			return false, fmt.Errorf("op %s already exists with different contents", op.ID())
		}
		return false, nil
	}
	if err := op.Validate(); err != nil {
		return false, err
	}
	if err := s.state.Apply(op); err != nil {
		return false, err
	}
	line, err := json.Marshal(op)
	if err != nil {
		return false, fmt.Errorf("encode op: %w", err)
	}
	line = append(line, '\n')
	if _, err := s.f.Write(line); err != nil {
		return false, fmt.Errorf("append op: %w", err)
	}
	if err := s.f.Sync(); err != nil {
		return false, fmt.Errorf("sync op: %w", err)
	}
	s.ops[op.ID()] = op
	if op.Seq > s.seqs[op.Node] {
		s.seqs[op.Node] = op.Seq
	}
	if op.Node != s.node {
		s.clock.Observe(op.HLC)
	}
	return true, nil
}

// localLocked mints, applies, and persists a local operation.
func (s *Store) localLocked(kind crdt.OpKind, doc, field, value, element string, tags []string) (crdt.Op, error) {
	if s.f == nil {
		return crdt.Op{}, ErrClosed
	}
	if doc == "" || field == "" {
		return crdt.Op{}, ErrEmptyKey
	}
	op := crdt.Op{
		Node:    s.node,
		Seq:     s.seqs[s.node] + 1,
		HLC:     s.clock.Now(),
		Kind:    kind,
		Doc:     doc,
		Field:   field,
		Value:   value,
		Element: element,
		Tags:    tags,
	}
	if _, err := s.applyLocked(op); err != nil {
		return crdt.Op{}, err
	}
	return op, nil
}

// Set writes a value into a register field.
func (s *Store) Set(doc, field, value string) (crdt.Op, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.localLocked(crdt.OpSet, doc, field, value, "", nil)
}

// Delete tombstones a register field.
func (s *Store) Delete(doc, field string) (crdt.Op, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.localLocked(crdt.OpDelete, doc, field, "", "", nil)
}

// SetAdd adds an element to an OR-Set field.
func (s *Store) SetAdd(doc, field, element string) (crdt.Op, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.localLocked(crdt.OpSetAdd, doc, field, "", element, nil)
}

// SetRemove removes an element, recording exactly the tags this replica has
// observed so a concurrent add elsewhere still wins.
func (s *Store) SetRemove(doc, field, element string) (crdt.Op, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if doc == "" || field == "" {
		return crdt.Op{}, ErrEmptyKey
	}
	tags := s.state.ObservedTags(doc, field, element)
	return s.localLocked(crdt.OpSetRemove, doc, field, "", element, tags)
}

// Apply folds remote operations into the store, returning how many were new.
func (s *Store) Apply(ops []crdt.Op) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	applied := 0
	for _, op := range ops {
		ok, err := s.applyLocked(op)
		if err != nil {
			return applied, fmt.Errorf("apply %s: %w", op.ID(), err)
		}
		if ok {
			applied++
		}
	}
	return applied, nil
}

// Doc returns the materialized view of one document.
func (s *Store) Doc(id string) (map[string]crdt.FieldView, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state.Doc(id)
}

// DocIDs lists documents in sorted order.
func (s *Store) DocIDs() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state.DocIDs()
}

// Ops returns every stored operation sorted by node and sequence.
func (s *Store) Ops() []crdt.Op {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]crdt.Op, 0, len(s.ops))
	for _, op := range s.ops {
		out = append(out, op)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Node != out[j].Node {
			return out[i].Node < out[j].Node
		}
		return out[i].Seq < out[j].Seq
	})
	return out
}

// Clock returns a copy of the per-node maximum sequence vector clock.
func (s *Store) Clock() map[string]uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]uint64, len(s.seqs))
	for node, seq := range s.seqs {
		out[node] = seq
	}
	return out
}

// Missing returns operations the holder of remote has not seen yet.
func (s *Store) Missing(remote map[string]uint64) []crdt.Op {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []crdt.Op
	for _, op := range s.ops {
		if op.Seq > remote[op.Node] {
			out = append(out, op)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Node != out[j].Node {
			return out[i].Node < out[j].Node
		}
		return out[i].Seq < out[j].Seq
	})
	return out
}

// CanonicalJSON serializes materialized state for equality checks.
func (s *Store) CanonicalJSON() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state.CanonicalJSON()
}

// Stats is a point-in-time summary used by the CLI.
type Stats struct {
	Node  string            `json:"node"`
	Dir   string            `json:"dir"`
	Docs  int               `json:"docs"`
	Ops   int               `json:"ops"`
	Clock map[string]uint64 `json:"clock"`
}

// Stats returns a summary of the store.
func (s *Store) Stats() Stats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	clock := make(map[string]uint64, len(s.seqs))
	for node, seq := range s.seqs {
		clock[node] = seq
	}
	return Stats{
		Node:  s.node,
		Dir:   s.dir,
		Docs:  len(s.state.Docs),
		Ops:   len(s.ops),
		Clock: clock,
	}
}

// Node returns the replica id.
func (s *Store) Node() string { return s.node }

// Dir returns the data directory.
func (s *Store) Dir() string { return s.dir }

// Close flushes and closes the operation log.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return nil
	}
	err := s.f.Close()
	s.f = nil
	return err
}
