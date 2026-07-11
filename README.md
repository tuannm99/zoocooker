# zoocooker

Mini ZooKeeper-inspired coordination service in Rust.

This repository is intentionally scaffolded around a staged implementation plan:

1. Single-node state machine
2. Session and ephemeral nodes
3. Persistence with WAL and snapshots
4. Cluster replication through Raft
5. Production hardening

The core rule for the design is simple:

- All writes become `Command`s.
- Only the state machine mutates the tree.
- Network, watches, WAL, and consensus stay outside the state machine.

## Workspace Layout

```text
crates/
  client/      SDK and client helpers
  consensus/   command replication abstraction
  protocol/    domain types and gRPC/proto layer
  server/      gRPC server, watch fan-out, session cleaner
  storage/     in-memory tree and apply(Command)
docs/
  implementation-guide.md
  phase-index.md
  testing-strategy.md
  phases/
proto/
  coordination.proto
```

## Suggested Build Order

Read [docs/implementation-guide.md](docs/implementation-guide.md) and implement top to bottom.
For the longer phase-by-phase notes, use [docs/phase-index.md](docs/phase-index.md).

The intended order is:

1. Finish `protocol` domain types if you want richer metadata.
2. Implement path parsing and tree mutation in `storage`.
3. Expose tree operations through `server`.
4. Add watch dispatching in the server layer.
5. Add session tracking and ephemeral cleanup.
6. Add WAL/snapshot support.
7. Swap `SingleNodeConsensus` with a Raft-backed implementation.

## Current Status

The repository currently contains:

- A Rust workspace
- A protobuf definition for the MVP API
- Working single-node storage CRUD
- gRPC service and thin client helpers
- One-shot data and child watches
- Session heartbeat and ephemeral cleanup
- WAL replay and snapshot primitives
- A runnable `zoocooker-server` binary
- An in-memory replicated cluster harness for consensus-boundary tests
- An implementation guide with checkpoints and notes

What it does not yet contain:

- A production OpenRaft-backed networked cluster
- Real auth/ACL enforcement
- External metrics/health endpoint export

## Running Locally

In-memory single-node server:

```sh
cargo run -p zoocooker-server --bin zoocooker-server -- --addr 127.0.0.1:50051
```

Persistent single-node server:

```sh
cargo run -p zoocooker-server --bin zoocooker-server -- \
  --addr 127.0.0.1:50051 \
  --wal ./data/commands.wal \
  --snapshot ./data/snapshot.json
```

Useful options:

- `--session-ttl-ms <ms>`
- `--watch-channel-capacity <n>`
- `--max-watches <n>`
