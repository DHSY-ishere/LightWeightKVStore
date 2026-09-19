// Package store implements the in-memory key-value storage engine.
//
// A naive implementation would wrap a single map[string]string in one
// sync.RWMutex. Under concurrent load every goroutine handling a client
// connection would contend on that single lock, even when operating on
// completely unrelated keys — the lock becomes a global bottleneck that
// serializes all writes (and blocks readers during writes) regardless
// of how many CPU cores are available.
//
// Instead, the keyspace is partitioned into a fixed number of
// independent shards, each owning its own map and its own RWMutex. A
// key is deterministically routed to exactly one shard by hashing it,
// so operations on keys in different shards can proceed fully in
// parallel — lock contention is isolated to whichever shard(s) happen
// to collide, rather than the whole store.
//
// Each shard additionally maintains its own intrusive doubly-linked
// list for LRU eviction and stores a per-key expiration timestamp for
// TTL support. Both features are local to the shard and fall under
// the shard's existing lock — no global mutex is introduced.
package store

import (
	"hash/fnv"
	"sync"
	"time"
)

// shardCount is the number of independent partitions the keyspace is
// split into. A power of two is used so shard selection can be a fast
// bitmask (hash & (shardCount-1)) instead of a modulo. 32 is a common
// default: enough to spread lock contention thinly across cores
// without spending memory on excessive per-shard bookkeeping.
const shardCount = 32

// MaxShardSize is the maximum number of live keys a single shard will
// hold. Once a SET would push a shard over this limit, the least
// recently used entry in that shard is evicted. The effective total
// capacity of the store is therefore roughly shardCount * MaxShardSize.
const MaxShardSize = 10000

// node is one entry in a shard's key space. It doubles as the map
// value and as a node in that shard's intrusive LRU linked list, so
// there is a single allocation per key rather than a separate
// "CacheItem" plus a wrapping list node.
type node struct {
	key       string
	value     string
	expiresAt int64 // UnixNano; 0 means "no expiration"
	prev, next *node
}

// expired reports whether the node's TTL has passed as of now.
func (n *node) expired(now int64) bool {
	return n.expiresAt != 0 && n.expiresAt <= now
}

// shard is one partition of the keyspace: an isolated map plus an
// intrusive LRU list, guarded by a single lock. Contention on this
// lock only affects goroutines operating on keys hashed into this
// specific shard.
//
// head is the most recently used node, tail is the least recently
// used node — the next candidate for eviction.
type shard struct {
	mu   sync.RWMutex
	data map[string]*node
	head *node
	tail *node
}

// Store is a sharded, concurrency-safe key-value store with per-key
// TTL expiration and per-shard LRU eviction.
type Store struct {
	shards [shardCount]*shard
}

// New creates a Store with all shards initialized and ready to use.
func New() *Store {
	s := &Store{}
	for i := range s.shards {
		s.shards[i] = &shard{data: make(map[string]*node)}
	}
	return s
}

// getShard picks which shard a key belongs to.
//
// FNV-1a is used because it's a lightweight, non-cryptographic hash:
// it needs no setup/seeding, runs in a tight byte-at-a-time loop, and
// distributes keys across shards uniformly enough to avoid hot spots.
// We don't need collision-resistance or security properties here —
// just fast, well-spread bucketing.
func (s *Store) getShard(key string) *shard {
	h := fnv.New32a()
	h.Write([]byte(key))
	// shardCount is a power of two, so `& (shardCount-1)` is equivalent
	// to `% shardCount` but avoids the (slower) division/modulo op.
	idx := h.Sum32() & (shardCount - 1)
	return s.shards[idx]
}

// --- intrusive doubly-linked list helpers ---
//
// These mutate sh.head/sh.tail and node pointers directly. Every
// caller already holds sh.mu for writing, so no further locking is
// needed here. A custom list is used instead of container/list to
// avoid its interface{}-boxing and separate Element allocation — here
// the node itself is both the map value and the list element.

// unlink removes n from the list without touching the map. Safe to
// call whether or not n is currently head/tail.
func (sh *shard) unlink(n *node) {
	if n.prev != nil {
		n.prev.next = n.next
	} else {
		sh.head = n.next
	}
	if n.next != nil {
		n.next.prev = n.prev
	} else {
		sh.tail = n.prev
	}
	n.prev, n.next = nil, nil
}

// pushFront inserts n as the most recently used node.
func (sh *shard) pushFront(n *node) {
	n.prev = nil
	n.next = sh.head
	if sh.head != nil {
		sh.head.prev = n
	}
	sh.head = n
	if sh.tail == nil {
		sh.tail = n
	}
}

// touch marks n as the most recently used node.
func (sh *shard) touch(n *node) {
	if sh.head == n {
		return
	}
	sh.unlink(n)
	sh.pushFront(n)
}

// evict removes n from both the list and the map.
func (sh *shard) evict(n *node) {
	sh.unlink(n)
	delete(sh.data, n.key)
}

// --- public API ---

// Set stores value under key with an optional TTL. A ttl of 0 (or
// less) means the key never expires. If the key already exists its
// value and expiration are overwritten and it becomes most recently
// used. Otherwise a new node is inserted at the front of the shard's
// LRU list; if that pushes the shard over MaxShardSize, the least
// recently used node in the shard is evicted.
func (s *Store) Set(key, value string, ttl time.Duration) {
	var expiresAt int64
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl).UnixNano()
	}

	sh := s.getShard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	if n, ok := sh.data[key]; ok {
		n.value = value
		n.expiresAt = expiresAt
		sh.touch(n)
		return
	}

	n := &node{key: key, value: value, expiresAt: expiresAt}
	sh.data[key] = n
	sh.pushFront(n)

	if len(sh.data) > MaxShardSize {
		if tail := sh.tail; tail != nil {
			sh.evict(tail)
		}
	}
}

// Get retrieves the value stored under key. ok is false if the key
// does not exist or has expired.
//
// Note this takes the write lock rather than a read lock: a cache hit
// must move the node to the front of the LRU list, which mutates
// shard state. This trades away true concurrent reads (the classic
// upside of RWMutex) in exchange for O(1) LRU bookkeeping — reads
// within a shard now serialize against each other, but shards remain
// fully independent, so contention is still confined to whichever
// shard a hot key happens to land in.
func (s *Store) Get(key string) (value string, ok bool) {
	sh := s.getShard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	n, ok := sh.data[key]
	if !ok {
		return "", false
	}
	// Lazy deletion: a key past its TTL is treated as absent and
	// reclaimed immediately, rather than waiting for the background
	// sweeper to get to it.
	if n.expired(time.Now().UnixNano()) {
		sh.evict(n)
		return "", false
	}
	sh.touch(n)
	return n.value, true
}

// Del removes key from the store. It returns the number of keys that
// were actually deleted (0 or 1), matching Redis DEL semantics for a
// single key. An already-expired key counts as not present.
func (s *Store) Del(key string) int {
	sh := s.getShard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	n, ok := sh.data[key]
	if !ok {
		return 0
	}
	sh.evict(n)
	return 1
}

// TTL returns the remaining time to live of a key in seconds:
//   -2 if the key does not exist or has already expired
//   -1 if the key exists but has no associated expire
//   >= 0 remaining TTL in seconds
func (s *Store) TTL(key string) int64 {
	sh := s.getShard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	n, ok := sh.data[key]
	if !ok {
		return -2
	}
	now := time.Now().UnixNano()
	if n.expired(now) {
		sh.evict(n)
		return -2
	}
	if n.expiresAt == 0 {
		return -1
	}
	return (n.expiresAt - now) / int64(time.Second)
}


// Sweep performs one active-expiration pass over every shard, deleting
// any key whose TTL has passed. It is meant to be called periodically
// from a background goroutine (see main.go) so that expired keys are
// reclaimed even if nothing ever GETs them again.
//
// Each shard is locked, scanned, and unlocked independently rather
// than locking the whole store at once, so a sweep never blocks more
// than one shard's traffic at a time. Per-shard size is bounded by
// MaxShardSize, so each shard's scan is O(MaxShardSize) — bounded and
// cheap — rather than growing unboundedly with total store size.
func (s *Store) Sweep() {
	now := time.Now().UnixNano()
	for _, sh := range s.shards {
		sh.mu.Lock()
		for _, n := range sh.data {
			if n.expired(now) {
				sh.evict(n)
			}
		}
		sh.mu.Unlock()
	}
}
