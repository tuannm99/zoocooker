# Operational Notes

## Current Modes

- `SingleNodeConsensus`: in-memory single-node mode.
- `PersistentSingleNodeConsensus`: single-node mode with WAL replay and optional snapshot restore.
- `ReplicatedClusterConsensus`: in-memory cluster harness for validating the consensus boundary. This is not a production Raft implementation.

## Failure Behavior

- Follower writes return a failed-precondition gRPC status with leader hint text.
- Watch registration above `ServerConfig::max_watches` returns resource-exhausted.
- WAL replay ignores trailing partial records.
- Corrupt complete WAL records fail startup replay.
- Unreadable or unsupported snapshots fail restore.

## Local Drills

Run the full deterministic suite:

```sh
cargo test
```

Run an in-memory local server:

```sh
cargo run -p zoocooker-server --bin zoocooker-server -- --addr 127.0.0.1:50051
```

Run a persistent local server:

```sh
cargo run -p zoocooker-server --bin zoocooker-server -- \
  --addr 127.0.0.1:50051 \
  --wal ./data/commands.wal \
  --snapshot ./data/snapshot.json
```

Persistence restart drill:

1. Create a `PersistentSingleNodeConsensus` with a WAL path.
2. Submit create/set commands.
3. Drop the instance.
4. Recreate it with the same WAL path.
5. Verify data and versions are restored.

Watch burst drill:

1. Start a service with a low `max_watches`.
2. Register watches until the configured limit is reached.
3. Verify the next registration returns resource-exhausted.

Session cleanup drill:

1. Create a session through heartbeat.
2. Create an ephemeral node using that session id.
3. Expire the session through `cleanup_expired_sessions`.
4. Verify the node is deleted and watches fire once.
