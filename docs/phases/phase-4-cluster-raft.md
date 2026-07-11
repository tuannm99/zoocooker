# Phase 4: Cluster And Raft

Goal: replicate commands across nodes and make leader-mediated writes the only legal write path.

## Scope

Implement:

- leader/follower roles through Raft
- log replication for commands
- leader-only write handling
- follower catch-up
- snapshot integration with the consensus layer

Start simple:

- write through leader only
- reads from leader only until semantics are clear

Defer if needed:

- follower local reads
- advanced membership operations
- protocol compatibility with real ZooKeeper peers

## Core Design Requirements

1. Replicate `Command`, not tree diffs.
2. Apply only committed log entries.
3. Server layer must stay isolated from Raft internals.
4. State machine behavior must be identical whether command came from local single-node mode or replicated mode.

## Suggested Implementation Order

### Rule 1. Stabilize the consensus boundary first

Before integrating OpenRaft, make sure `Consensus` trait cleanly represents:

- submit write command
- possibly read access
- leadership or redirect metadata

### Rule 2. Integrate leader-only writes second

Behavior:

- followers reject or redirect writes
- leader appends command to raft log
- apply only after commit

### Rule 3. Add networked replication third

Implement:

- node identity
- transport for raft RPCs
- persistent raft log storage
- state machine apply path

### Rule 4. Snapshot and catch-up fourth

Implement:

- follower replay from log
- snapshot install for lagging nodes
- restart of a node from local persisted state

## Files To Focus On

- `crates/consensus/src/lib.rs`
- `crates/server/src/service.rs`
- persistence code from Phase 3

You may eventually split `consensus` into multiple modules once OpenRaft integration grows.

## Unit Test Requirements

Must add tests for:

1. command is not applied before commit
2. committed command is applied exactly once
3. follower write path returns redirect or not-leader error
4. leadership metadata is surfaced to server layer correctly

## Integration Test Requirements

Must add at least these flows:

1. three-node cluster elects a leader
2. write to leader replicates to followers
3. write to follower is rejected or redirected
4. follower restart catches up
5. lagging follower restores from snapshot when needed

## Failure-Test Requirements

Must exercise:

- leader crash
- follower crash during replication
- network partition of one follower
- leader change during client retries

Be careful:

- do not try to test every failure mode in one giant flaky test
- prefer small scenario-specific tests

## Exit Criteria

- cluster reaches steady-state repeatedly
- committed writes survive leader failover
- lagging nodes catch up through log or snapshot

## Implementation Status

Current implemented slice:

1. `Consensus` now exposes leadership metadata independently from server internals.
2. Single-node and persistent single-node modes report themselves as leader.
3. `ReplicatedClusterConsensus` provides an in-memory replicated cluster harness.
4. Followers reject write/read paths with a `NotLeader` error carrying leader metadata.
5. Leader writes are appended to the cluster log and applied to all node stores as committed commands.

Known remaining work before this is a real Raft phase:

- Replace the in-memory cluster harness with OpenRaft or another real Raft implementation.
- Add raft RPC transport, durable raft log storage, and membership management.
- Add failure tests for leader crash, follower crash, and partitions.
- Wire redirect metadata into the external proto API instead of only gRPC status text.

## Reference Material

For Raft theory:

- Raft home page: https://raft.github.io/
- Raft paper at USENIX: https://www.usenix.org/node/184041.

For implementation library:

- OpenRaft crate docs: https://docs.rs/crate/openraft/latest
- OpenRaft API docs: https://docs.rs/openraft/latest/openraft/

Notes:

- As of May 1, 2026, `openraft` latest docs show the 0.9.x line as the stable released line and also list 0.10.0 alpha releases. Prefer the stable line unless you have a concrete reason to chase alpha APIs.
