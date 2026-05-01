# Phase 5: Production Hardening

Goal: make the system observable, bounded, and less fragile under real operational pressure.

## Scope

Implement in this phase:

- auth or ACL model
- metrics
- health endpoints
- log compaction policy
- backpressure limits
- retry and redirect behavior in client
- operational docs

This phase is about engineering quality, not changing the core storage semantics.

## Core Design Requirements

1. Do not add production knobs without a testable reason.
2. Every limit must have a documented failure behavior.
3. Metrics names and meanings must be stable enough to monitor.
4. Client retry policy must avoid duplicating non-idempotent writes silently.

## Suggested Implementation Order

### Rule 1. Observability first

Add:

- structured tracing
- request ids or correlation ids
- leader and term exposure
- basic counters and latency histograms

### Rule 2. Backpressure second

Define limits for:

- max message size
- max watch registrations
- max outstanding streams
- queue depth before rejection

### Rule 3. Client behavior third

Implement:

- redirect handling
- retry policy for safe operations
- timeout configuration

### Rule 4. Security and ACL fourth

Only after behavior is stable:

- define principal model
- define ACL inheritance or lack thereof
- test every permission boundary

## Unit Test Requirements

Must add tests for:

1. ACL allow/deny rules
2. redirect parsing and retry policy decisions
3. backpressure rejection path
4. metrics update logic where practical

## Integration Test Requirements

Must add at least these flows:

1. client receives follower redirect and succeeds against leader
2. watch-heavy workload hits configured limit and fails predictably
3. auth failure and success cases
4. snapshot/log compaction does not break restart recovery

## Non-Functional Test Requirements

Add repeatable local drills for:

- restart many times under load
- burst watch registrations
- slow client streams
- follower catch-up after compaction

Even if you do not fully automate load tests, keep a documented manual drill procedure.

## Exit Criteria

- failure behavior is documented
- major limits are enforced
- metrics and logs are sufficient to debug basic incidents

## Reference Material

For ZooKeeper operational semantics and documentation index:

- ZooKeeper docs index: https://zookeeper.apache.org/doc/r3.9.2/index.html
- ZooKeeper releases page: https://zookeeper.apache.org/releases.html

For gRPC and transport primitives:

- tonic overview: https://docs.rs/crate/tonic/=0.14.2
- tonic client/server docs: https://docs.rs/tonic/latest/tonic/client/ and https://docs.rs/tonic/latest/tonic/server/
