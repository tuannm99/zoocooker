# Phase Guides

This document is the entrypoint for the longer implementation notes.

Keep [implementation-guide.md](./implementation-guide.md) as the compact execution checklist.
Use the files below when you need more detail about scope, references, and testing expectations.

## Recommended Reading Order

1. [implementation-guide.md](./implementation-guide.md)
2. [testing-strategy.md](./testing-strategy.md)
3. [phases/phase-1-single-node.md](./phases/phase-1-single-node.md)
4. [phases/phase-2-session-ephemeral.md](./phases/phase-2-session-ephemeral.md)
5. [phases/phase-3-persistence.md](./phases/phase-3-persistence.md)
6. [phases/phase-4-cluster-raft.md](./phases/phase-4-cluster-raft.md)
7. [phases/phase-5-production-hardening.md](./phases/phase-5-production-hardening.md)

## Ground Rules

1. Do not start a later phase before the earlier phase has passing tests.
2. Each phase should end with a small runnable milestone, not just internal code changes.
3. Every bug fix should first become a regression test.
4. Every public API addition should have both unit coverage and at least one end-to-end flow test.
5. Avoid mixing persistence and consensus work into Phase 1 or 2.

## Suggested Exit Gates

Phase 1 exit gate:

- Storage CRUD semantics are stable.
- Watch one-shot semantics work for the supported subset.
- A single-node server can handle basic client flows.

Phase 2 exit gate:

- Sessions expire correctly.
- Ephemeral cleanup is deterministic.
- Time-based tests are stable under paused Tokio time.

Phase 3 exit gate:

- Restart recovery works from WAL.
- Snapshot restore is verified.
- Crash-style tests cover interrupted writes and replay.

Phase 4 exit gate:

- Leader-only writes are enforced.
- Log replication is correct under follower lag.
- Membership and snapshot behavior are tested.

Phase 5 exit gate:

- Basic observability is in place.
- Failure-mode behavior is bounded and documented.
- Integration and resilience tests are good enough to support repeated local runs.
