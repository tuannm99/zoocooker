# Phase 2: Sessions And Ephemeral Nodes

Goal: add session lifecycle, heartbeat handling, and deterministic ephemeral cleanup.

## Scope

Implement:

- session allocation
- heartbeat refresh
- session TTL tracking
- ephemeral node ownership
- background expiration scan
- cleanup path that removes expired session ephemeral nodes

Do not implement yet:

- persistent storage of sessions
- multi-node session coordination
- reconnect semantics identical to ZooKeeper client protocol

## Core Design Requirements

1. Session expiration must not mutate storage directly.
2. Expiration should create normal delete commands.
3. All cleanup should produce the same watch/stat effects as user-driven delete.
4. Time-driven logic must be testable without real sleeps.

## Suggested Implementation Order

### Rule 1. Session ID and heartbeat first

Implement:

- `Heartbeat(session_id: Option<...>)`
- create new session when none is provided
- refresh existing session on heartbeat
- reject or recreate invalid session according to the policy you choose

Document the chosen policy clearly.

### Rule 2. Ephemeral ownership second

Implement:

- `ephemeral_owner` on node
- per-session owned path tracking
- validation that ephemeral nodes cannot be parents if you choose ZooKeeper-like semantics

### Rule 3. Timeout cleanup third

Implement:

- scanner task
- expired session collection
- generated delete commands for all ephemeral paths

Constraints:

- order cleanup from deepest path upward
- idempotent behavior if the node is already gone

## Files To Focus On

- `crates/protocol/src/types.rs`
- `crates/server/src/session.rs`
- `crates/server/src/service.rs`
- `crates/storage/src/tree.rs`
- `crates/storage/src/store.rs`

## Unit Test Requirements

Must add tests for:

1. creating a new session from empty heartbeat
2. heartbeat refresh updates last-seen time
3. expired session is collected after TTL
4. non-expired session is retained
5. ephemeral node records owner
6. expired session cleanup chooses deepest child-first delete order

## Integration Test Requirements

Must add at least these flows:

1. client receives session id from heartbeat
2. client creates ephemeral node tied to that session
3. after timeout, ephemeral node disappears
4. data/child watches fire from ephemeral cleanup exactly once

## Time-Test Requirements

Use paused Tokio time for session expiry tests.

Required:

- no real multi-second sleeps
- no retry loops based on wall-clock waiting

Reference:

- Tokio testing guide: https://tokio.rs/tokio/topics/testing

## Exit Criteria

- session expiration is deterministic
- ephemeral cleanup reuses the same mutation path as normal deletes
- tests are stable under paused time

## Reference Material

For semantics:

- ZooKeeper current docs index: https://zookeeper.apache.org/doc/r3.9.2/index.html
- ZooKeeper programmer guide sections on sessions, ephemeral nodes, and watches: https://zookeeper.apache.org/doc/r3.4.9/zookeeperProgrammers.html

For async runtime behavior:

- Tokio testing guide: https://tokio.rs/tokio/topics/testing
