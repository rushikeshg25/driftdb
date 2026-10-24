# driftdb

[![ci](https://github.com/rushikeshg25/driftdb/actions/workflows/ci.yml/badge.svg)](https://github.com/rushikeshg25/driftdb/actions/workflows/ci.yml)
[![Go 1.24+](https://img.shields.io/badge/go-1.24%2B-00ADD8?logo=go)](go.mod)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

**A local-first, offline-capable document store written in Go.** Each replica
keeps a durable operation log, accepts writes with no network, and converges
deterministically when replicas next exchange operations. No central server, no
third-party dependencies, and no conflict-resolution code in your application.

```
$ driftdb --data ./alice --node alice put note title "buy milk"
ok alice/1

$ driftdb --data ./bob --node bob serve --listen 127.0.0.1:7070 &
driftdb bob listening on 127.0.0.1:7070 (data: ./bob)

$ driftdb --data ./alice --node alice sync 127.0.0.1:7070
pulled 0, pushed 1
```

## Overview

Most applications still treat "offline" as an error path. driftdb makes it the
normal path: every replica owns a durable operation log, edits are timestamped
locally, and replication is a stateless anti-entropy exchange over TCP. Two
replicas that have seen the same operations materialize the same state,
regardless of the order those operations arrived in.

driftdb is intended as a compact, readable reference for CRDT-based sync and as
an embedded store for prototypes, demos, and tests. It is not a production
database; see [Limits](#limits).

The data model is deliberately small:

| Type | Semantics | Conflict rule |
| --- | --- | --- |
| register | one string per field | last writer wins by hybrid logical clock |
| set | unordered string membership | observed-remove (OR-Set), add wins |

Deletes are tombstones with timestamps, so a concurrent newer write can
resurrect a field. Set removal records the exact add-tags it observed, so a
concurrent add of the same element survives the merge.

## How it works

```
            local mutation
                  |
                  v
        +-------------------+        fsync        +----------------+
        |   hybrid clock    |-------------------->|  ops.jsonl     |
        +-------------------+                     +----------------+
                  |                                       ^
                  v                                       |
        +-------------------+                     +----------------+
        | materialized CRDT |<--------------------|  anti-entropy  |
        |      state        |    apply (idempotent)|  over TCP      |
        +-------------------+                     +----------------+
```

**Operations.** Every mutation becomes an `Op` with a stable id (`node/seq`), an
HLC timestamp, and enough data to be applied without context. Operations are
immutable and deduplicated by id.

**Hybrid logical clock.** Timestamps are `(wall, counter, node)`. Wall time
gives intuitive last-writer-wins behaviour; the counter keeps timestamps
strictly monotonic when the clock stalls or jumps backwards; the node id breaks
ties deterministically. Receiving an operation folds its timestamp into the
local clock, so a local write after a sync always wins over what was just
received.

**Replication.** `sync` opens one TCP connection and exchanges newline-delimited
JSON:

```
client -> hello(node, vector clock)
server -> hello(node, vector clock)
server -> ops*              operations the client is missing
server -> done
client -> push*             operations the server is missing
client -> pushdone
server -> ack(applied count)
```

Both sides send only operations above the other's per-node high-water mark, so
repeat syncs are no-ops. Applying an operation twice is harmless.

**Durability.** Local and remote operations are appended to `ops.jsonl` and
`fsync`ed before the command returns. On startup the log is replayed. A torn
final line from a crash is truncated and discarded. The replica id is pinned in
the `node` file so a data directory cannot be accidentally reused under a
different identity.

## Install and run

Requires Go 1.24+ and no third-party dependencies (Linux, macOS, or Windows).

```sh
go install github.com/rushikeshg25/driftdb/cmd/driftdb@latest

# or from a checkout:
go build -o bin/driftdb ./cmd/driftdb
./examples/demo.sh        # two offline replicas, conflict resolution, then sync
```

Or via `make build`, `make test`, `make demo`.

## Commands

```
driftdb [flags] <command> [arguments]

get <doc>                  print a document as JSON
put <doc> <field> <value>  set a register field
del <doc> <field>          delete a register field
sadd <doc> <field> <elem>  add an element to a set field
srem <doc> <field> <elem>  remove an element from a set field
ls                         list documents
log [limit]                show the operation log
status                     show replica statistics
export [path]              write the op log as NDJSON
import <path>              merge an NDJSON op log
sync <address>             exchange operations with a peer once
serve                      serve replication until interrupted
```

Flags may appear before or after the command:

```
--data DIR     data directory (default ./driftdb-data)
--node ID      replica id (default $DRIFTDB_NODE or "local")
--listen ADDR  replication listen address (default 127.0.0.1:7070)
```

`help` remains a valid document, field, or value. Use `--` before literal
arguments that begin with a dash, for example:
`driftdb --data ./alice --node alice put note title -- --help`.
Invalid commands and argument counts exit with code 2 before opening a store
or starting a listener.

Replica ids must be 1-128 characters and cannot contain `/` or whitespace; the
id is part of every operation id (`node/seq`). The id is recorded in the data
directory on first open and cannot change later.

`get` prints live register values and set members. A deleted register is shown
as `"deleted": true` without a value until a newer write resurrects it. A field
is either a register or a set: a local `put`/`del` on a set, or `sadd`/`srem` on
a register, fails with a type-conflict error instead of silently hiding data.
Operations received from peers still merge deterministically by HLC even if they
mix types.

## A conflict, step by step

```sh
# Alice and Bob start from the same state, including tag "urgent".
driftdb --data ./a --node a put note title "draft"
driftdb --data ./a --node a sadd note tags urgent
driftdb --data ./b --node b serve --listen 127.0.0.1:7070 &
driftdb --data ./a --node a sync 127.0.0.1:7070

# Both go offline and edit the same field / race on the same set element.
driftdb --data ./a --node a put note title "alice"
driftdb --data ./b --node b put note title "bob"
driftdb --data ./a --node a srem note tags urgent   # removes the observed add
driftdb --data ./b --node b sadd note tags urgent   # concurrent add, new tag

driftdb --data ./a --node a sync 127.0.0.1:7070

driftdb --data ./a --node a get note
driftdb --data ./b --node b get note   # identical JSON
```

The register resolves to the operation with the greater HLC, and the set keeps
`urgent` because Bob's add was concurrent with Alice's remove (add-wins).

## Limits

This is a compact reference implementation, not a production database:

- Replication is unauthenticated and unencrypted; run it on a trusted network or
  tunnel it.
- Anti-entropy walks the log; there is no compaction or snapshot transfer yet,
  so the log grows forever and sync cost is proportional to unseen operations.
- Tombstones are never garbage collected.
- Only two CRDT shapes exist. There is no text/sequence CRDT.
- Whole operations are retransmitted; there is no delta compression.
- Clock skew can change *which* concurrent write wins, but it cannot cause
  divergence.
- A single data directory must not be opened by two processes at once.

## Status

v0.1 is complete and usable: CRDT state, durable storage, TCP anti-entropy, CLI,
tests, and CI are all in place. CI runs `gofmt`, `go vet`, and `go test -race`.
The roadmap in [plan.md](plan.md) covers authentication, compaction/snapshots,
and transport hardening.

## Layout

```
cmd/driftdb          CLI
internal/hlc         hybrid logical clock
internal/crdt        ops, LWW register, OR-Set, materialized state
internal/store       durable op log, vector clock, local/remote apply
internal/syncer      wire protocol, anti-entropy client and server
examples/demo.sh     end-to-end offline-first demo
```

## Development

```sh
make test    # go test ./...
make race    # go test -race ./...
make vet
make demo
```

See [plan.md](plan.md) for milestones, design decisions, and the roadmap.

MIT licensed.
