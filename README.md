# kvstore

A lightweight, Redis-protocol-compatible, in-memory key-value server written in Go. It speaks [RESP](https://redis.io/docs/reference/protocol-spec/) (REdis Serialization Protocol), so any standard Redis client (`redis-cli`, `redis-py`, `go-redis`, etc.) can connect and issue commands out of the box.

## Features

- **RESP wire protocol** — full reader/writer for Simple Strings, Errors, Integers, Bulk Strings, and Arrays.
- **Sharded storage** — the keyspace is split across 32 independent shards (each with its own `sync.RWMutex`), so operations on unrelated keys run fully in parallel instead of contending on a single global lock.
- **Per-key TTL** — `SET key value EX seconds` stores an expiration timestamp; expired keys are reclaimed both lazily on read and actively by a background sweeper.
- **LRU eviction** — each shard maintains an intrusive doubly-linked list. When a shard exceeds `MaxShardSize` (10 000 keys), the least-recently-used entry is evicted on the next `SET`.
- **Graceful shutdown** — `SIGINT` / `SIGTERM` stops the sweeper, drains connections, and exits cleanly.

## Architecture

```
┌──────────────────────────────────────────────────────┐
│  main.go                                             │
│  ├─ starts background TTL sweeper (100 ms interval)  │
│  ├─ launches TCP server on :6379                     │
│  └─ handles OS signals for graceful shutdown          │
├──────────────────────────────────────────────────────┤
│  server/server.go                                    │
│  ├─ accepts TCP connections (one goroutine each)     │
│  ├─ decodes RESP frames via resp.Reader              │
│  ├─ dispatches commands → store                      │
│  └─ encodes RESP replies via resp.Writer             │
├──────────────────────────────────────────────────────┤
│  store/store.go                                      │
│  ├─ 32 shards, FNV-1a hash for shard selection       │
│  ├─ intrusive doubly-linked LRU list per shard       │
│  └─ per-key nanosecond TTL expiration                │
├──────────────────────────────────────────────────────┤
│  resp/resp.go                                        │
│  └─ RESP v2 reader / writer (encode & decode)        │
└──────────────────────────────────────────────────────┘
```

## Supported Commands

| Command | Syntax | Description |
|---------|--------|-------------|
| **PING** | `PING [message]` | Returns `PONG`, or echoes `message` as a bulk string. |
| **SET** | `SET key value [EX seconds]` | Stores a key-value pair with an optional TTL. |
| **GET** | `GET key` | Retrieves the value for a key (returns nil bulk for missing/expired keys). |
| **DEL** | `DEL key [key ...]` | Deletes one or more keys; returns the count of keys removed. |
| **TTL** | `TTL key` | Returns remaining TTL in seconds (`-2` = not found, `-1` = no expiry). |

## Getting Started

### Prerequisites

- **Go 1.21+**

### Build & Run

```bash
# Clone the repository
git clone https://github.com/DHSY-ishere/LightWeightKVStore.git
cd LightWeightKVStore

# Run directly
go run .

# — or build a binary —
go build -o kvstore .
./kvstore
```

The server starts listening on **`localhost:6379`**.

### Connect with redis-cli

```bash
redis-cli -p 6379

127.0.0.1:6379> PING
PONG

127.0.0.1:6379> SET greeting "hello world" EX 60
OK

127.0.0.1:6379> GET greeting
"hello world"

127.0.0.1:6379> TTL greeting
(integer) 59

127.0.0.1:6379> DEL greeting
(integer) 1

127.0.0.1:6379> GET greeting
(nil)
```

### Run Tests

```bash
go test ./store/...
```

Tests cover basic CRUD, TTL expiry, LRU eviction, background sweep, and concurrent access (50 goroutines × 200 ops with race detector).

## Design Decisions

| Decision | Rationale |
|----------|-----------|
| **32 shards (power of two)** | Shard selection uses a bitmask (`hash & 31`) instead of modulo — cheaper on the CPU and spreads keys evenly enough to avoid hot spots. |
| **FNV-1a hash** | Non-cryptographic, zero-allocation, good distribution. No need for collision-resistance here — just fast bucketing. |
| **Intrusive linked list** | Avoids the extra allocation and `interface{}` boxing of `container/list`; each map entry *is* its own list node. |
| **Write lock on `Get`** | A cache hit moves the node to the front of the LRU list (a mutation), so a read lock alone isn't safe. Cross-shard independence keeps this from becoming a bottleneck. |
| **Lazy + active expiration** | Lazy deletion on `Get` gives instant reclaim for hot keys; the background sweeper (100 ms interval) catches cold expired keys that are never read again. |

## Project Structure

```
.
├── main.go              # Entry point, sweeper, signal handling
├── go.mod               # Go module (kvstore, Go 1.21)
├── resp/
│   └── resp.go          # RESP v2 protocol reader & writer
├── server/
│   └── server.go        # TCP server, command dispatch
└── store/
    ├── store.go          # Sharded KV store with LRU & TTL
    └── store_test.go     # Unit & concurrency tests
```

## License

This project is provided as-is for educational and personal use.
