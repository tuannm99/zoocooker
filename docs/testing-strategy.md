# Testing Strategy

This project should treat tests as part of the design, not as cleanup after implementation.

## Test Layers

### 1. Unit tests

Use for:

- path normalization
- version checks
- stat updates
- tree mutation rules
- watch event generation
- session bookkeeping
- WAL record encode/decode

Requirements:

- fast
- deterministic
- no network
- no wall-clock sleeps
- no filesystem unless the code under test is persistence-specific

### 2. Integration tests

Use for:

- gRPC request/response behavior
- watch registration and delivery
- heartbeat/session flows
- restart recovery using real files
- leader redirect behavior

Requirements:

- exercise public APIs
- start real service objects
- use temp directories for state
- avoid relying on timing races

### 3. End-to-end tests

Use for:

- client talks to server through transport
- multi-node cluster flows
- restart and recovery drills
- follower lag and leader failover scenarios

Requirements:

- run less frequently than unit tests
- isolate ports and temp data
- prefer explicit orchestration over ad hoc sleeps

## Rust Test Organization

Use Rust conventions:

- unit tests near implementation under `#[cfg(test)]`
- integration tests in top-level `tests/`
- shared integration helpers under `tests/common/mod.rs`

References:

- Rust Book, writing tests: https://doc.rust-lang.org/book/ch11-01-writing-tests.html
- Rust Book, test organization: https://doc.rust-lang.org/book/ch11-03-test-organization.html

## Tokio Test Rules

Time-sensitive code must be tested with paused Tokio time whenever possible.

Use:

- `tokio::time::pause()`
- `tokio::time::advance()`
- `#[tokio::test(start_paused = true)]` when suitable

Do not:

- use `sleep(Duration::from_secs(...))` in tests unless there is no alternative
- rely on scheduler luck
- assert exact ordering across unrelated tasks unless ordering is part of the contract

Reference:

- Tokio testing guide: https://tokio.rs/tokio/topics/testing

## What Every Phase Must Add

For every phase:

1. unit tests for core invariants
2. integration tests for public behavior
3. at least one regression test for each bug found during development

## Minimum Coverage Expectations By Area

Storage:

- invalid path
- missing parent
- duplicate create
- delete non-empty
- version mismatch
- stat counters update
- root edge cases

Watch:

- one-shot delivery
- correct event kind
- multiple watchers on same path
- watcher removal after fire
- no duplicate fire on one registration

Session:

- heartbeat refresh
- timeout expiration
- session reuse rules
- ephemeral ownership tracking
- cleanup after expiration

Persistence:

- WAL append and replay
- partial replay rules
- snapshot round-trip
- startup recovery ordering

Consensus:

- leader-only write path
- committed entries applied once
- stale follower catches up
- snapshot installation behavior

## Test Discipline

1. Prefer deterministic fixtures over random data.
2. If you use randomization, seed it explicitly and log the seed.
3. Avoid hidden global state.
4. If a test depends on timing, document why.
5. If a failure mode is important, write the failing test before the fix.

## CI Intent

Eventually the project should separate test runs into:

1. `cargo test --workspace` for fast unit and integration coverage
2. selected heavier tests behind feature flags or ignored tests
3. cluster and restart drills in a slower suite
