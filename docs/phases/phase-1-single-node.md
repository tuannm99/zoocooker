# Phase 1: Single-Node Core

Goal: build a correct single-node coordination service before adding sessions, persistence, or consensus.

## Scope

Implement:

- normalized path handling
- in-memory znode tree
- `create`, `get`, `set`, `delete`, `exists`
- `version` and `cversion`
- basic stat metadata
- one-shot watch registration
- gRPC server and client skeleton that can execute CRUD flows

Do not implement yet:

- real session expiration
- WAL or snapshot
- Raft
- ACL/auth
- compaction

## Core Design Requirements

1. All writes must go through `Command`.
2. Storage must generate logical watch events, not send them.
3. Reads may bypass consensus in this phase.
4. Tree mutation logic must stay synchronous and deterministic.

## Suggested Implementation Order

### Rule 1. Path normalization first

Finish path helpers before touching storage logic.

Implement:

- root handling for `/`
- reject empty segment
- reject trailing slash
- derive parent path
- derive leaf name

Why:

- every later bug becomes harder if path rules are fuzzy

### Rule 2. Tree CRUD second

Implement storage operations in this order:

1. `exists`
2. `get`
3. `create`
4. `set`
5. `delete`

Rules:

- parent must exist before create
- node must not already exist
- delete must fail on non-empty node
- root cannot be deleted

### Rule 3. Versioning before watch fan-out

Implement:

- `version` increment on data change
- `cversion` increment on child add/remove
- expected-version compare on `set`
- expected-version compare on `delete`

### Rule 4. Watch semantics last in this phase

Support only:

- data watch
- children watch
- one-shot trigger

Do not attempt:

- persistent watch
- watch replay
- disconnected client recovery

## Files To Focus On

- `crates/protocol/src/path.rs`
- `crates/protocol/src/command.rs`
- `crates/storage/src/tree.rs`
- `crates/storage/src/store.rs`
- `crates/server/src/service.rs`
- `crates/server/src/watch.rs`

## Unit Test Requirements

Must add tests for:

1. normalize `/`
2. reject `/a/`
3. reject `//a`
4. create under missing parent
5. create duplicate node
6. set with version mismatch
7. delete with version mismatch
8. delete non-empty node
9. root exists on startup
10. `cversion` increments on child mutation

## Integration Test Requirements

Must add at least these flows:

1. client creates, gets, sets, deletes a node through gRPC
2. `exists` returns false before create and true after create
3. one watch fires once when node data changes
4. child watch fires when a child is created below watched parent

## Exit Criteria

- `cargo test` is stable with no flaky timing-based failures
- storage APIs are covered by unit tests
- a real server can serve basic client CRUD flows

## Reference Material

For semantics:

- ZooKeeper 3.9 documentation index: https://zookeeper.apache.org/doc/r3.9.2/index.html
- ZooKeeper programmer guide overview and data model semantics: https://zookeeper.apache.org/doc/r3.4.9/zookeeperProgrammers.html

For Rust transport and codegen:

- tonic overview: https://docs.rs/crate/tonic/=0.14.2
- tonic-prost-build usage: https://docs.rs/crate/tonic-prost-build/0.14.5

Notes:

- The ZooKeeper links are used as semantic references, not as a protocol-compatibility requirement.
- The programmer guide URL above is older, but the referenced znode/watch/session semantics are stable enough for this project.
