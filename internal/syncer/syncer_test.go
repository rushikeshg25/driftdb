package syncer

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/rushikeshg25/driftdb/internal/store"
)

func openStore(t *testing.T, node string) *store.Store {
	t.Helper()
	s, err := store.Open(t.TempDir(), node)
	if err != nil {
		t.Fatalf("open %s: %v", node, err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func serve(t *testing.T, st *store.Store) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Serve(ctx, ln, st) }()
	t.Cleanup(func() {
		cancel()
		ln.Close()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("serve: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("serve did not stop")
		}
	})
	return ln.Addr().String()
}

func syncPair(t *testing.T, client, server *store.Store, addr string) (int, int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pulled, pushed, err := Exchange(ctx, addr, client)
	if err != nil {
		t.Fatalf("exchange %s -> %s: %v", client.Node(), server.Node(), err)
	}
	return pulled, pushed
}

func requireConverged(t *testing.T, stores ...*store.Store) {
	t.Helper()
	want, err := stores[0].CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range stores[1:] {
		got, err := s.CanonicalJSON()
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != string(want) {
			t.Fatalf("%s diverged:\nwant %s\ngot  %s", s.Node(), want, got)
		}
	}
}

func TestExchangePullsAndPushes(t *testing.T) {
	a := openStore(t, "a")
	b := openStore(t, "b")
	addr := serve(t, b)

	if _, err := a.Set("note", "title", "from-a"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Set("note", "body", "from-b"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.SetAdd("note", "tags", "urgent"); err != nil {
		t.Fatal(err)
	}

	pulled, pushed := syncPair(t, a, b, addr)
	if pulled != 2 || pushed != 1 {
		t.Fatalf("pulled/pushed = %d/%d, want 2/1", pulled, pushed)
	}
	requireConverged(t, a, b)

	// A second exchange moves nothing.
	pulled, pushed = syncPair(t, a, b, addr)
	if pulled != 0 || pushed != 0 {
		t.Fatalf("second exchange moved %d/%d, want 0/0", pulled, pushed)
	}
}

func TestExchangePropagatesDeletesAndSets(t *testing.T) {
	a := openStore(t, "a")
	b := openStore(t, "b")
	c := openStore(t, "c")
	addrB := serve(t, b)

	if _, err := a.Set("doc", "title", "draft"); err != nil {
		t.Fatal(err)
	}
	if _, err := a.SetAdd("doc", "tags", "one"); err != nil {
		t.Fatal(err)
	}
	syncPair(t, a, b, addrB)

	if _, err := b.Delete("doc", "title"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.SetRemove("doc", "tags", "one"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.SetAdd("doc", "tags", "two"); err != nil {
		t.Fatal(err)
	}
	syncPair(t, a, b, addrB)

	fields, ok := a.Doc("doc")
	if !ok {
		t.Fatal("doc missing")
	}
	if !fields["title"].Deleted {
		t.Fatal("delete did not propagate")
	}
	if got := fields["tags"].Elements; len(got) != 1 || got[0] != "two" {
		t.Fatalf("tags = %v, want [two]", got)
	}

	// c catches up via b; a already has b's operations.
	syncPair(t, c, b, addrB)
	requireConverged(t, a, b, c)
}

func TestConcurrentWritesConverge(t *testing.T) {
	a := openStore(t, "a")
	b := openStore(t, "b")
	c := openStore(t, "c")
	addrA := serve(t, a)
	addrB := serve(t, b)
	addrC := serve(t, c)

	// All three write the same field before hearing from each other.
	if _, err := a.Set("shared", "title", "a-value"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Set("shared", "title", "b-value"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Set("shared", "title", "c-value"); err != nil {
		t.Fatal(err)
	}
	// And two of them race on an OR-Set element.
	if _, err := a.SetAdd("shared", "tags", "x"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SetAdd("shared", "tags", "x"); err != nil {
		t.Fatal(err)
	}

	syncPair(t, a, b, addrB)
	syncPair(t, b, c, addrC)
	syncPair(t, c, a, addrA)
	// Second round spreads transitively learned operations.
	syncPair(t, a, b, addrB)
	syncPair(t, b, c, addrC)

	requireConverged(t, a, b, c)

	fields, _ := a.Doc("shared")
	if got := fields["tags"].Elements; len(got) != 1 || got[0] != "x" {
		t.Fatalf("tags = %v, want [x]", got)
	}
}

func TestConcurrentAddSurvivesRemoveAcrossReplicas(t *testing.T) {
	a := openStore(t, "a")
	b := openStore(t, "b")
	addrB := serve(t, b)

	if _, err := a.SetAdd("board", "tags", "x"); err != nil {
		t.Fatal(err)
	}
	syncPair(t, a, b, addrB)

	// Both replicas remove, but b concurrently adds a fresh tag.
	if _, err := a.SetRemove("board", "tags", "x"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.SetAdd("board", "tags", "x"); err != nil {
		t.Fatal(err)
	}
	syncPair(t, a, b, addrB)

	fields, _ := a.Doc("board")
	if got := fields["tags"].Elements; len(got) != 1 || got[0] != "x" {
		t.Fatalf("concurrent add should win, tags = %v", got)
	}
	requireConverged(t, a, b)
}

func TestPushedOpsAreDurable(t *testing.T) {
	dir := t.TempDir()
	b, err := store.Open(dir, "b")
	if err != nil {
		t.Fatal(err)
	}
	addr := serve(t, b)

	a := openStore(t, "a")
	if _, err := a.Set("note", "title", "durable"); err != nil {
		t.Fatal(err)
	}
	syncPair(t, a, b, addr)
	b.Close()

	reopened, err := store.Open(dir, "b")
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	fields, ok := reopened.Doc("note")
	if !ok || fields["title"].Value != "durable" {
		t.Fatalf("pushed op did not survive reopen: %+v", fields)
	}
}

func TestExchangePaginatesLargeLogs(t *testing.T) {
	a := openStore(t, "a")
	b := openStore(t, "b")
	addrA := serve(t, a)

	// More operations than one wire message, in the server->client direction.
	for i := 0; i < batchSize+17; i++ {
		if _, err := a.Set("bulk", "field", fmt.Sprintf("a-%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	pulled, pushed := syncPair(t, b, a, addrA)
	if pulled != batchSize+17 || pushed != 0 {
		t.Fatalf("pulled/pushed = %d/%d, want %d/0", pulled, pushed, batchSize+17)
	}

	// And in the client->server (push) direction.
	for i := 0; i < batchSize+17; i++ {
		if _, err := b.Set("bulk", "field", fmt.Sprintf("b-%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	pulled, pushed = syncPair(t, b, a, addrA)
	if pulled != 0 || pushed != batchSize+17 {
		t.Fatalf("pulled/pushed = %d/%d, want 0/%d", pulled, pushed, batchSize+17)
	}
	requireConverged(t, a, b)
}

func TestExchangeRejectsUnreachablePeer(t *testing.T) {
	a := openStore(t, "a")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	// Reserve a port, then close it so nothing is listening.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()

	if _, _, err := Exchange(ctx, addr, a); err == nil {
		t.Fatal("expected dial error")
	}
}

func TestServeRejectsBadHello(t *testing.T) {
	b := openStore(t, "b")
	addr := serve(t, b)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if _, err := fmt.Fprintln(conn, `{"type":"nope"}`); err != nil {
		t.Fatal(err)
	}
	var msg Message
	if err := json.NewDecoder(conn).Decode(&msg); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if msg.Type != "err" {
		t.Fatalf("type = %q, want err", msg.Type)
	}
}

func TestServeStopsOnContextCancel(t *testing.T) {
	b := openStore(t, "b")
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Serve(ctx, ln, b) }()

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("serve returned %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("serve did not stop")
	}
}
