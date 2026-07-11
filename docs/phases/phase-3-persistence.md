# Phase 3: Persistence

Goal: survive restart by replaying committed commands and restoring snapshots.

## Scope

Implement:

- WAL append for committed commands
- WAL replay on startup
- snapshot format for tree plus session metadata
- restore path from snapshot then replay remaining WAL

Do not implement yet:

- distributed snapshot install
- full storage-engine abstraction for many backends
- aggressive compaction policies

## Core Design Requirements

1. Persist commands, not ad hoc internal diffs.
2. Startup restore order must be deterministic.
3. Snapshot data format must be versioned.
4. WAL append success semantics must be documented before coding.

## Suggested Implementation Order

### Rule 1. WAL record model first

Define:

- record header or length framing
- serialized `Command`
- checksum or corruption detection strategy

Decide and document:

- whether partial trailing record is ignored or treated as fatal
- when fsync happens

### Rule 2. Replay second

Implement:

- open WAL
- iterate records in order
- re-apply into empty state machine
- stop or repair on corruption according to the chosen policy

### Rule 3. Snapshot third

Implement snapshot contents:

- all nodes
- node stats needed for correctness
- session metadata required for ephemeral cleanup
- zxid or logical sequence metadata

### Rule 4. Recovery path fourth

Startup algorithm:

1. load latest snapshot if present
2. open WAL segments newer than snapshot
3. replay in order
4. publish ready state only after restore finishes

## Files To Add Or Extend

Likely areas:

- `crates/storage/`
- `crates/consensus/` if commit boundaries are involved
- a new persistence module or crate if the code grows enough

## Unit Test Requirements

Must add tests for:

1. command serialize/deserialize round-trip
2. WAL append and sequential replay
3. replay on empty store
4. replay after snapshot base state
5. snapshot encode/decode round-trip
6. handling trailing partial WAL record

## Integration Test Requirements

Must add at least these flows:

1. create data, restart process, verify data survives
2. create ephemeral node and restore session metadata as designed
3. snapshot then restart then continue writing
4. corruption test for clearly defined failure behavior

## Failure-Mode Requirements

Document behavior for:

- crash after append before response
- crash after response but before snapshot
- truncated WAL
- unreadable snapshot

If behavior is not ideal yet, document the limitation explicitly.

## Implementation Plan

Current next steps:

1. Add a small length-prefixed JSON WAL that stores committed `Command` records.
2. Append and fsync a command before applying it in the persistent single-node consensus path.
3. Replay complete WAL records into an empty `TreeStore`; ignore a trailing partial record.
4. Add a versioned JSON snapshot for `TreeStore` state, including nodes, stats, zxid, and sequence counters.
5. Restore from snapshot first, then replay WAL records for the first persistence milestone.

Initial failure behavior:

- Crash after WAL append before response may replay an already committed command on restart.
- Crash after response before snapshot is recovered from WAL.
- Truncated trailing WAL records are ignored.
- Corrupt complete WAL records are treated as fatal replay errors.
- Unreadable snapshots are treated as fatal restore errors.

## Exit Criteria

- restart recovery passes repeatedly
- WAL replay order is verified
- snapshot restore is covered by tests

## Reference Material

For proto/build and serialization context:

- tonic-prost-build: https://docs.rs/crate/tonic-prost-build/0.14.5
- prost-build notes on `protoc`: https://docs.rs/crate/prost-build/0.12.3

For storage engine ideas if using `redb`:

- redb durability: https://docs.rs/redb/latest/redb/enum.Durability.html
- redb write transaction: https://docs.rs/redb/latest/redb/struct.WriteTransaction.html

Notes:

- You do not need `redb` for the first WAL implementation.
- A plain append-only file is often better for learning and for validating recovery semantics.
