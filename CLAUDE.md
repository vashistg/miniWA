# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Start infrastructure (ScyllaDB + Kafka) — must be running before the server
docker compose up -d

# Create / migrate ScyllaDB schema (run after first docker compose up, and after schema changes)
mix mini_wa.db.setup

# Start the dev server (hot-reloads Elixir, JS, and CSS automatically)
mix phx.server

# Pre-commit gate: compile with warnings-as-errors, remove unused deps, format, test
mix precommit

# Run tests
mix test
mix test test/path/to/file_test.exs          # single file
mix test --failed                             # re-run only previously failed tests

# Inspect ScyllaDB
docker exec -it mini_wa_scylla cqlsh

# Reset Kafka consumer group offset (forces full replay — useful for testing dedup)
docker exec -it mini_wa_kafka \
  kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group mini_wa_consumer_group --topic messages \
  --reset-offsets --to-earliest --execute
```

## Architecture

This is a WhatsApp-scale messaging prototype, not a standard Phoenix CRUD app. There is no Ecto, no database ORM, and no LiveView — the UI is a single static HTML page with vanilla JS over a Phoenix Channel WebSocket.

### Message flow (happy path)

```
Browser → Channel.handle_in("send_msg") → Session.send_message/4
  → Producer.publish/1 (brod.produce_sync) → Kafka broker ACK
  → Channel ← {:tick1, msg} ← Session ← (broker confirmed)
  ↓ async
Consumer.handle_message → DB.persist_message (ScyllaDB)
  → Registry.lookup(recipient) → Session.deliver → Channel → Browser "msg" event
  → Browser → Channel.handle_in("delivered") → Session.notify_delivered
  → Channel ← {:tick2} → sender's browser
```

Group messages follow the same path but `type: "group"` in the Kafka payload routes the consumer to `process_group/1`, which writes once to ScyllaDB then fans out to every member.

### Process model (`lib/mini_wa/`)

- **`session.ex`** — one `GenServer` per connected user, `restart: :temporary`. Registered in `MiniWa.Presence.Registry` (ETS) under their `user_id`. Owns: Kafka publish, tick propagation, offline drain on reconnect. Stopped when the channel process dies (monitored via `Process.monitor`).
- **`db.ex`** — thin Xandra wrapper. All CQL parameters must be typed tuples: `{"text", val}`, `{"bigint", val}`. Two separate tables serve two separate query patterns: `messages` (durable log, partition by `conversation_id`) and `undelivered_messages` (offline queue, partition by `recipient_id`).
- **`streaming/producer.ex`** — synchronous Kafka publish via `:brod.produce_sync/5`. Partition key = `conversation_id` so all messages in a conversation land on the same partition (ordering guarantee).
- **`streaming/consumer.ex`** — `brod_group_subscriber_v2`. Routes by `type` field in the Kafka JSON payload: `"group"` → `process_group/1`, anything else → `process_1to1/1`. Starts with exponential backoff after ensuring the topic exists.

### Key invariants

- **`conversation_id` for 1:1**: sorted alphabetical join of both user IDs, e.g. `"alice:bob"`. Computed by `DB.conversation_id/2`.
- **`conversation_id` for groups**: the group's UUID, stored as-is.
- **`message_id` format**: `<12-char zero-padded hex ms timestamp><8-char random hex>`. Lexicographic order = chronological order, which means `undelivered_messages` drains in send order (clustered by `message_id`) without extra sorting.
- **Tick-1** fires after Kafka ACK (not after ScyllaDB write). ScyllaDB write happens asynchronously in the consumer.
- **The channel never sends `conversation_id`** for 1:1 messages. The JS client uses `from` as the conversation key for 1:1. Only group messages include `conversation_id` in the WebSocket `msg` event.
- **daisyUI ships a `.modal` component** that conflicts with any custom `.modal` class. Custom modal elements use `.dialog` / `.dialog-header` / `.dialog-body` / `.dialog-footer` instead.

### ScyllaDB schema (`lib/mix/tasks/mini_wa.db.setup.ex`)

| Table | Partition key | Clustering key | Purpose |
|---|---|---|---|
| `messages` | `conversation_id` | `message_id` | Durable log for all messages |
| `undelivered_messages` | `recipient_id` | `message_id` | Offline queue, drained on reconnect |
| `users` | `user_id` | — | Registered users |
| `groups` | `group_id` | — | Group metadata |
| `group_members` | `group_id` | `user_id` | Fan-out: who's in a group |
| `user_groups` | `user_id` | `group_id` | Sidebar: which groups a user belongs to |

### Frontend (`assets/js/app.js`)

Single-page app with no framework. Key globals exposed on `window.__mwa` for console testing:
- `__mwa.burst("bob", 30)` — sends 30 numbered messages through the normal send path for ordering tests.

The JS conversation model uses `activeConv` (a user ID for 1:1, a group UUID for groups) and `activeConvType` (`"user"` | `"group"`). `localStorage` keys are `miniwa_msgs_<myUserId>_<convId>` and serve as the device-side message store (messages survive reconnects without hitting the server).

## Xandra quirk

Every query parameter must carry its CQL type explicitly — bare values will raise `FunctionClauseError` in `Xandra.Protocol.V4.encode_query_value/1`:

```elixir
# correct
Xandra.execute(conn, "INSERT INTO t (id, ts) VALUES (?, ?)", [{"text", id}, {"bigint", ts}])

# wrong — will crash at runtime
Xandra.execute(conn, "INSERT INTO t (id, ts) VALUES (?, ?)", [id, ts])
```
