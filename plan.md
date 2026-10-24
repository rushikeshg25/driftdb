# driftdb — plan and status

**Status:** v0.1, complete reference implementation. Local-first document store
with offline writes and anti-entropy replication in Go, no third-party
dependencies.

## Goal

Make offline-first editing the normal path rather than an error path. A replica
should accept writes with no network, persist them durably, and converge with
peers later without a central server or custom merge code in the application.

The smallest useful data model is a document of string fields and string sets.
That is enough to demonstrate the hard parts — conflict resolution, causal
metadata, durability, and replication — without hiding them behind a query
engine.

## Delivered in v0.1

- [x] Hybrid logical clock with monotonic `(wall, counter, node)` timestamps.
- [x] Operation model with stable `node/seq` ids and structural validation.
- [x] LWW register fields with timestamped tombstones and resurrection.
- [x] Observed-remove set fields with add-wins concurrent merges.
- [x] Deterministic state folding; any permutation of the same ops converges.
- [x] Append-only `ops.jsonl` with per-write `fsync` and replay on open.
- [x] Torn trailing write detection and truncation on recovery.
- [x] Contiguous per-node vector clocks, so sparse imports cannot hide gaps.
- [x] Replica identity pinned to the data directory, with ids validated for
      use in operation ids.
- [x] Local type-conflict guard: a register cannot silently become a set (or
      the reverse) through a local write.
- [x] One-connection pull+push anti-entropy protocol over NDJSON/TCP.
- [x] Cancellation-safe client sync and graceful shutdown of active peers.
- [x] CLI for writes, reads, logs, export/import, sync, and serve.
- [x] Two- and three-replica convergence tests, batching tests, and benchmarks.
- [x] Offline-first demo script and CI (gofmt, vet, race tests, build).

## Architecture decisions

**Operations are the source of truth.** State is a fold over an immutable,
deduplicated op log. This makes retries idempotent, replication trivial to
reason about, and recovery a replay. A state-based merge would avoid log growth
but makes “what happened” harder to inspect.

**Hybrid logical clocks, not wall clocks.** Wall time gives intuitive
last-writer-wins behaviour; the logical counter protects monotonicity when the
clock stalls or jumps backwards; the node id makes ties deterministic. Clock
skew can change *which* concurrent write wins, but never causes divergence.

**Tombstones instead of physical deletes.** A delete carries its own timestamp,
so a concurrent newer write can resurrect a field and replicas still agree.

**Observed-remove sets instead of last-writer-wins sets.** A removal records the
tags the remover actually saw. A concurrent add mints a fresh tag and survives,
which is the intuitive “add wins” behaviour and avoids silently losing data.

**Pull then push on one connection.** The client sends its vector clock, the
server replies with what the client lacks, then the client pushes what the
server lacks. One connection converges both sides, and repeat syncs are no-ops.

**Durability before acknowledgement.** Each op is appended and `fsync`ed before
the local call returns. The tradeoff is throughput (~3.7 ms/write on APFS in the
included benchmark); batching is a roadmap item, not a hidden default.

**Stdlib only.** The point is to show the mechanics, not to hide them behind a
framework. JSON lines, `net`, and `sync` are sufficient.

## Non-goals for v0.1

- No SQL, secondary indexes, or query language.
- No consensus or cluster membership; replicas are explicitly paired.
- No authentication, TLS, or multi-tenant isolation.
- No log compaction, snapshot transfer, or tombstone GC.
- No text/sequence CRDT or numeric counters.
- No support for two processes opening one data directory.

## Roadmap

### v0.2 — storage and sync scale
- [ ] Snapshot format plus op-log truncation.
- [ ] Snapshot transfer for peers that are behind the truncation point.
- [ ] Group commit / batched `fsync` with an explicit durability mode.
- [ ] Per-record checksums and `fsync` of the data directory on creation.
- [ ] Tombstone garbage collection once all known replicas have observed a delete.
- [ ] `flock` on the data directory to enforce one process per replica.
- [ ] Compressed op batches (gzip/zstd) and delta sync for large logs.

### v0.3 — operations
- [ ] TLS and a shared token for replication.
- [ ] Watchers/subscriptions for local changes and applied remote ops.
- [ ] Metrics: log size, ops/sec, sync duration, last-seen clocks.
- [ ] `driftdb status --json` and machine-readable op inspection.
- [ ] Scheduled background anti-entropy instead of manual `sync`.

### v0.4 — data model
- [ ] Counters (PN-counter) and nested maps.
- [ ] A sequence CRDT (RGA) for collaborative text.
- [ ] Binary values and content hashing.
- [ ] Typed schemas and indexes for local queries.
- [ ] Mutable document deletion (cascading tombstones).

### Testing and tooling
- [ ] Fuzz the parser, op validation, and log replay.
- [ ] Property tests for HLC and op-set convergence at larger scale.
- [ ] A chaos harness that kills and partitions replicas during sync.
- [ ] Multi-node benchmarks (write throughput vs. replica count).
- [ ] A small TUI or web inspector for op logs and vector clocks.

## Verification

```sh
make test      # unit and integration tests
make race      # race detector
make vet
make demo      # two offline replicas converge
```

The convergence property is checked from two directions: shuffled op
application in `internal/crdt`, and real TCP exchanges between two and three
replicas in `internal/syncer`.
