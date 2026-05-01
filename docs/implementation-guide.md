# Implementation Guide

This guide is meant to be used incrementally. Each section maps to a concrete code area in the workspace.

## 0. Rules To Keep

1. Do not let RPC handlers mutate the tree directly.
2. Do not put `tokio::mpsc::Sender` or transport concerns inside storage.
3. Keep the storage state machine deterministic and mostly synchronous.
4. Represent every write as a `Command`.
5. Emit logical `WatchEvent`s from storage, then fan them out in the server layer.
6. Treat session timeout cleanup as another source of commands, not a special-case mutation path.

## 1. Protocol Crate

Target files:

- `crates/protocol/src/command.rs`
- `crates/protocol/src/path.rs`
- `crates/protocol/src/types.rs`

Implement in this order:

1. Path parsing and validation
2. `SessionId`, `Zxid`, and version newtypes
3. `Command` variants for create/set/delete
4. `WatchEvent` payloads
5. Error codes

Notes:

- Normalize `/` and reject empty segments.
- Decide early whether you allow trailing slashes. For ZooKeeper-like behavior, it is usually cleaner to reject them.
- Keep `Command` serializable because WAL and Raft will use the same type later.

## 2. Storage Crate

Target files:

- `crates/storage/src/tree.rs`
- `crates/storage/src/store.rs`
- `crates/storage/src/stat.rs`

Implement in this order:

1. Root node and path lookup
2. `create`
3. `get`
4. `set`
5. `delete`
6. `exists`
7. Version checks
8. Child version updates
9. `apply(Command)`

Notes:

- Start with `HashMap<String, ZNode>` keyed by normalized absolute path.
- Maintain child names, not child full paths, inside each node.
- `apply(Command)` should return both operation result and generated watch events.
- Sequential node naming can stay TODO until the rest is stable.

## 3. Server Crate

Target files:

- `crates/server/src/service.rs`
- `crates/server/src/watch.rs`
- `crates/server/src/session.rs`

Implement in this order:

1. Map gRPC requests to `Command`
2. Call `Consensus::submit`
3. Convert storage results to RPC responses
4. Add a watch registry and one-shot delivery
5. Add session manager and heartbeat flow

Notes:

- The server owns async fan-out.
- Storage only describes which watches should fire.
- For MVP, keep reads local and direct.

## 4. Consensus Crate

Target files:

- `crates/consensus/src/lib.rs`

Implement in this order:

1. `SingleNodeConsensus`
2. Trait boundary for submit/read access
3. Log replication abstraction
4. Later: replace internals with OpenRaft

Notes:

- The first implementation should just lock storage and call `apply`.
- Do not leak Raft types into `server` or `storage`.

## 5. Client Crate

Target files:

- `crates/client/src/lib.rs`

Implement in this order:

1. gRPC client wrapper
2. High-level helpers for `create/get/set/delete/exists`
3. Watch stream wrapper
4. Heartbeat helper

Notes:

- Keep this crate thin.
- Retry and leader redirect logic can wait until cluster mode exists.

## 6. Persistence

No code is scaffolded yet beyond placeholders, but the direction should be:

1. Serialize `Command` records into a WAL
2. Replay WAL at startup
3. Snapshot tree plus session metadata
4. Truncate or rotate WAL after snapshot

Notes:

- Persist commands, not internal mutation diffs.
- Use the same `Command` type as consensus.

## 7. Tests You Should Add Early

Add unit tests before cluster work:

1. Path normalization and invalid paths
2. Create under missing parent
3. Duplicate create
4. Delete non-empty node
5. Version mismatch on set/delete
6. Ephemeral cleanup deletes expected paths
7. Watch fires exactly once

## 8. Good Next Milestone

A realistic first milestone is:

1. `cargo test` passes for storage path and CRUD behavior
2. A single-node gRPC server starts
3. A tiny client can create/get/delete a node

Do not start Raft before that milestone is clean.
