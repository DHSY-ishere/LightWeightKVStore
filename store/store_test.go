package store

import (
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestSetGet(t *testing.T) {
	s := New()
	s.Set("a", "1", 0)

	v, ok := s.Get("a")
	if !ok || v != "1" {
		t.Fatalf("Get(a) = %q, %v; want 1, true", v, ok)
	}

	if _, ok := s.Get("missing"); ok {
		t.Fatalf("Get(missing) ok = true; want false")
	}
}

func TestSetOverwrite(t *testing.T) {
	s := New()
	s.Set("a", "1", 0)
	s.Set("a", "2", 0)

	v, ok := s.Get("a")
	if !ok || v != "2" {
		t.Fatalf("Get(a) = %q, %v; want 2, true", v, ok)
	}
}

func TestDel(t *testing.T) {
	s := New()
	s.Set("a", "1", 0)

	if n := s.Del("a"); n != 1 {
		t.Fatalf("Del(a) = %d; want 1", n)
	}
	if n := s.Del("a"); n != 0 {
		t.Fatalf("Del(a) second time = %d; want 0", n)
	}
	if _, ok := s.Get("a"); ok {
		t.Fatalf("Get(a) after Del ok = true; want false")
	}
}

func TestTTLExpiry(t *testing.T) {
	s := New()
	s.Set("a", "1", 20*time.Millisecond)

	if _, ok := s.Get("a"); !ok {
		t.Fatalf("Get(a) immediately after Set ok = false; want true")
	}

	time.Sleep(40 * time.Millisecond)

	if _, ok := s.Get("a"); ok {
		t.Fatalf("Get(a) after TTL expiry ok = true; want false")
	}
}

func TestTTLZeroMeansNoExpiry(t *testing.T) {
	s := New()
	s.Set("a", "1", 0)
	time.Sleep(10 * time.Millisecond)

	if _, ok := s.Get("a"); !ok {
		t.Fatalf("Get(a) with no TTL ok = false; want true")
	}
}

func TestSweepReclaimsExpiredKeys(t *testing.T) {
	s := New()
	s.Set("a", "1", 10*time.Millisecond)
	time.Sleep(20 * time.Millisecond)

	s.Sweep()

	sh := s.getShard("a")
	sh.mu.RLock()
	_, ok := sh.data["a"]
	sh.mu.RUnlock()
	if ok {
		t.Fatalf("key still present in shard map after Sweep")
	}
}

func TestLRUEviction(t *testing.T) {
	s := New()

	// Fill a single shard past MaxShardSize by writing directly to it so
	// hashing across shardCount doesn't dilute the test.
	sh := s.shards[0]
	sh.mu.Lock()
	for i := 0; i < MaxShardSize; i++ {
		k := "k" + strconv.Itoa(i)
		n := &node{key: k, value: "v"}
		sh.data[k] = n
		sh.pushFront(n)
	}
	sh.mu.Unlock()

	if len(sh.data) != MaxShardSize {
		t.Fatalf("shard size = %d; want %d", len(sh.data), MaxShardSize)
	}

	// Touch the oldest key so it becomes most-recently-used and survives.
	sh.mu.Lock()
	sh.touch(sh.data["k0"])
	sh.mu.Unlock()

	// One more insert should evict the new least-recently-used key
	// (k1, since k0 was just touched to the front), not k0.
	sh.mu.Lock()
	overflowNode := &node{key: "overflow", value: "v"}
	sh.data["overflow"] = overflowNode
	sh.pushFront(overflowNode)
	if len(sh.data) > MaxShardSize {
		sh.evict(sh.tail)
	}
	sh.mu.Unlock()

	if len(sh.data) != MaxShardSize {
		t.Fatalf("shard size after eviction = %d; want %d", len(sh.data), MaxShardSize)
	}
	if _, ok := sh.data["k0"]; !ok {
		t.Fatalf("recently-touched key k0 was evicted; want it retained")
	}
	if _, ok := sh.data["k1"]; ok {
		t.Fatalf("least-recently-used key k1 was retained; want it evicted")
	}
}

func TestConcurrentAccess(t *testing.T) {
	s := New()
	var wg sync.WaitGroup
	for g := 0; g < 50; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				k := strconv.Itoa((g*200 + i) % 100)
				s.Set(k, "v", time.Millisecond*time.Duration(i%5))
				s.Get(k)
				s.Del(k)
				s.Sweep()
			}
		}(g)
	}
	wg.Wait()
}
