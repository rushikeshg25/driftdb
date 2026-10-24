// Package syncer implements driftdb's anti-entropy replication protocol.
//
// One exchange is a single TCP connection carrying newline-delimited JSON:
//
//	client -> hello(node, clock)
//	server -> hello(node, clock)
//	server -> ops*            (ops the client is missing)
//	server -> done
//	client -> push*           (ops the server is missing)
//	client -> pushdone
//	server -> ack(count)
//
// The vector clock exchanged in hello makes each direction incremental, and a
// repeated exchange is a no-op once both replicas are caught up.
package syncer

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/rushikeshg25/driftdb/internal/crdt"
	"github.com/rushikeshg25/driftdb/internal/store"
)

const (
	// batchSize bounds how many operations travel in one message.
	batchSize = 500
	// exchangeTimeout bounds a single exchange, not the lifetime of a server.
	exchangeTimeout = 2 * time.Minute
)

// ErrProtocol is returned when a peer sends an unexpected message.
var ErrProtocol = errors.New("syncer: protocol violation")

// Message is one frame of the replication protocol.
type Message struct {
	Type  string            `json:"type"`
	Node  string            `json:"node,omitempty"`
	Clock map[string]uint64 `json:"clock,omitempty"`
	Ops   []crdt.Op         `json:"ops,omitempty"`
	Count int               `json:"count,omitempty"`
	Error string            `json:"error,omitempty"`
}

func send(enc *json.Encoder, m Message) error {
	if err := enc.Encode(m); err != nil {
		return fmt.Errorf("syncer: send %s: %w", m.Type, err)
	}
	return nil
}

func recv(dec *json.Decoder) (Message, error) {
	var m Message
	if err := dec.Decode(&m); err != nil {
		return Message{}, fmt.Errorf("syncer: receive: %w", err)
	}
	if m.Type == "err" {
		return Message{}, fmt.Errorf("syncer: peer error: %s", m.Error)
	}
	return m, nil
}

func batches(ops []crdt.Op) [][]crdt.Op {
	var out [][]crdt.Op
	for start := 0; start < len(ops); start += batchSize {
		end := min(start+batchSize, len(ops))
		out = append(out, ops[start:end])
	}
	return out
}

// Exchange runs one full anti-entropy exchange with the peer at addr.
func Exchange(ctx context.Context, addr string, st *store.Store) (pulled, pushed int, err error) {
	dialer := net.Dialer{Timeout: 10 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return 0, 0, fmt.Errorf("syncer: dial %s: %w", addr, err)
	}
	defer conn.Close()

	// Closing the connection is the only portable way to interrupt a blocked
	// read when the caller cancels.
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()

	dec := json.NewDecoder(bufio.NewReaderSize(conn, 1<<20))
	enc := json.NewEncoder(conn)
	sendMsg := func(m Message) error {
		touch(conn)
		return send(enc, m)
	}
	recvMsg := func() (Message, error) {
		touch(conn)
		return recv(dec)
	}

	if err := sendMsg(Message{Type: "hello", Node: st.Node(), Clock: st.Clock()}); err != nil {
		return 0, 0, err
	}
	hello, err := recvMsg()
	if err != nil {
		return 0, 0, err
	}
	if hello.Type != "hello" {
		return 0, 0, fmt.Errorf("%w: expected hello, got %q", ErrProtocol, hello.Type)
	}

	touch(conn)
	for pulling := true; pulling; {
		m, err := recvMsg()
		if err != nil {
			return pulled, pushed, err
		}
		switch m.Type {
		case "ops":
			n, applyErr := st.Apply(m.Ops)
			if applyErr != nil {
				return pulled, pushed, applyErr
			}
			pulled += n
		case "done":
			pulling = false
		default:
			return pulled, pushed, fmt.Errorf("%w: expected ops or done, got %q", ErrProtocol, m.Type)
		}
	}

	for _, batch := range batches(st.Missing(hello.Clock)) {
		if err := sendMsg(Message{Type: "push", Ops: batch}); err != nil {
			return pulled, pushed, err
		}
	}
	if err := sendMsg(Message{Type: "pushdone"}); err != nil {
		return pulled, pushed, err
	}

	ack, err := recvMsg()
	if err != nil {
		return pulled, pushed, err
	}
	if ack.Type != "ack" {
		return pulled, pushed, fmt.Errorf("%w: expected ack, got %q", ErrProtocol, ack.Type)
	}
	return pulled, ack.Count, nil
}

// Serve accepts replication connections until ctx is cancelled.
func Serve(ctx context.Context, ln net.Listener, st *store.Store) error {
	var wg sync.WaitGroup
	stop := context.AfterFunc(ctx, func() { ln.Close() })
	defer stop()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				wg.Wait()
				return nil
			}
			if errors.Is(err, net.ErrClosed) {
				// The listener was closed by someone else. Do not wait for
				// connections: nothing has told them to stop.
				return nil
			}
			return fmt.Errorf("syncer: accept: %w", err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			handleConn(ctx, conn, st)
		}()
	}
}

// ListenAndServe listens on addr and serves replication forever.
func ListenAndServe(ctx context.Context, addr string, st *store.Store) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("syncer: listen: %w", err)
	}
	return Serve(ctx, ln, st)
}

// touch turns the absolute exchange timeout into an idle timeout so a large but
// healthy sync is not cut off mid-stream.
func touch(conn net.Conn) {
	_ = conn.SetDeadline(time.Now().Add(exchangeTimeout))
}

func handleConn(ctx context.Context, conn net.Conn, st *store.Store) {
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
	touch(conn)

	dec := json.NewDecoder(bufio.NewReaderSize(conn, 1<<20))
	enc := json.NewEncoder(conn)
	sendMsg := func(m Message) error {
		touch(conn)
		return send(enc, m)
	}
	recvMsg := func() (Message, error) {
		touch(conn)
		return recv(dec)
	}

	hello, err := recvMsg()
	if err != nil {
		send(enc, Message{Type: "err", Error: err.Error()})
		return
	}
	if hello.Type != "hello" {
		send(enc, Message{Type: "err", Error: fmt.Sprintf("expected hello, got %q", hello.Type)})
		return
	}
	if hello.Node == "" {
		send(enc, Message{Type: "err", Error: "hello is missing node"})
		return
	}

	if err := sendMsg(Message{Type: "hello", Node: st.Node(), Clock: st.Clock()}); err != nil {
		return
	}
	for _, batch := range batches(st.Missing(hello.Clock)) {
		if err := sendMsg(Message{Type: "ops", Ops: batch}); err != nil {
			return
		}
	}
	if err := sendMsg(Message{Type: "done"}); err != nil {
		return
	}

	applied := 0
	for {
		m, err := recvMsg()
		if err != nil {
			return
		}
		switch m.Type {
		case "push":
			n, applyErr := st.Apply(m.Ops)
			if applyErr != nil {
				send(enc, Message{Type: "err", Error: applyErr.Error()})
				return
			}
			applied += n
		case "pushdone":
			sendMsg(Message{Type: "ack", Count: applied})
			return
		default:
			send(enc, Message{Type: "err", Error: fmt.Sprintf("unexpected message %q", m.Type)})
			return
		}
	}
}
