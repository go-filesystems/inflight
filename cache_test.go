// Copyright (c) 2026, go-filesystems
// SPDX-License-Identifier: BSD-3-Clause

package inflight

import (
	"sync"
	"testing"
)

func bytesOf(b []byte) int64 { return int64(len(b)) }

func blk(n int) []byte { return make([]byte, n) }

func TestCacheFIFOAndBound(t *testing.T) {
	c := NewCache[int, []byte](30, 30, bytesOf)
	for i := 0; i < 5; i++ {
		c.Put(i, blk(10))
	}
	if c.Len() != 3 {
		t.Fatalf("Len = %d, want 3 (30 bytes of 10)", c.Len())
	}
	for _, k := range []int{0, 1} {
		if _, ok := c.Get(k); ok {
			t.Errorf("key %d survived, the oldest go first", k)
		}
	}
	for _, k := range []int{2, 3, 4} {
		if _, ok := c.Get(k); !ok {
			t.Errorf("key %d evicted, it is among the newest", k)
		}
	}
}

func TestCacheOversizedEntryIsNotStored(t *testing.T) {
	c := NewCache[int, []byte](30, 30, bytesOf)
	c.Put(1, blk(10))
	c.Put(2, blk(31))
	if _, ok := c.Get(2); ok {
		t.Error("an entry larger than the bound was stored")
	}
	if _, ok := c.Get(1); !ok {
		t.Error("an oversized Put emptied the cache")
	}
	c.Put(3, blk(30)) // exactly the bound: stored, evicts 1
	if _, ok := c.Get(3); !ok || c.Len() != 1 {
		t.Errorf("an entry of exactly the bound: len %d", c.Len())
	}
}

func TestCacheGrowsOnlyWhenAnEvictedKeyReturns(t *testing.T) {
	// One sequential pass over more than the cache holds never grows it.
	c := NewCache[int, []byte](40, 400, bytesOf)
	for i := 0; i < 50; i++ {
		c.Put(i, blk(10))
	}
	if c.Cap() != 40 {
		t.Fatalf("a single pass grew the cache to %d", c.Cap())
	}
	// A second pass asks for evicted keys: it grows, never past the maximum.
	for pass := 0; pass < 5; pass++ {
		for i := 0; i < 50; i++ {
			if _, ok := c.Get(i); !ok {
				c.Put(i, blk(10))
			}
		}
	}
	if c.Cap() <= 40 || c.Cap() > 400 {
		t.Fatalf("Cap = %d, want grown above 40 and at most 400", c.Cap())
	}
	if c.Len() > 40 && int64(c.Len())*10 > c.Cap() {
		t.Errorf("holds %d entries over a %d byte bound", c.Len(), c.Cap())
	}
	// At the maximum it stays.
	c = NewCache[int, []byte](10, 20, bytesOf)
	for pass := 0; pass < 20; pass++ {
		for i := 0; i < 10; i++ {
			if _, ok := c.Get(i); !ok {
				c.Put(i, blk(10))
			}
		}
	}
	if c.Cap() != 20 {
		t.Errorf("Cap = %d, want the maximum 20", c.Cap())
	}
}

func TestCacheSetMaxShrinksButNotBelowStart(t *testing.T) {
	c := NewCache[int, []byte](20, 200, bytesOf)
	for pass := 0; pass < 10; pass++ {
		for i := 0; i < 15; i++ {
			if _, ok := c.Get(i); !ok {
				c.Put(i, blk(10))
			}
		}
	}
	if c.Cap() <= 20 {
		t.Fatalf("did not grow: %d", c.Cap())
	}
	c.SetMax(5) // below the start: the start
	if c.Cap() != 20 || int64(c.Len())*10 > 20 {
		t.Errorf("after SetMax(5): cap %d len %d", c.Cap(), c.Len())
	}
}

func TestCacheReplaceKeepsAgeAndAccounting(t *testing.T) {
	c := NewCache[int, []byte](30, 30, bytesOf)
	c.Put(1, blk(10))
	c.Put(2, blk(10))
	c.Put(1, blk(20)) // replaced, now 20 + 10 = 30
	if c.Len() != 2 {
		t.Fatalf("Len = %d", c.Len())
	}
	c.Put(3, blk(10)) // 40 > 30: key 1 is the oldest and goes
	if _, ok := c.Get(1); ok {
		t.Error("the replaced key kept a younger age")
	}
	if _, ok := c.Get(2); !ok {
		t.Error("key 2 lost")
	}
	c.Put(2, blk(30)) // replaced by the whole bound: everything else goes, itself stays
	if _, ok := c.Get(2); !ok || c.Len() != 1 {
		t.Errorf("a replacement that fills the bound: len %d", c.Len())
	}
}

func TestNilCacheIsOff(t *testing.T) {
	var c *Cache[int, []byte]
	c.Put(1, blk(1))
	c.SetMax(10)
	if _, ok := c.Get(1); ok || c.Len() != 0 || c.Cap() != 0 {
		t.Error("a nil cache held something")
	}
}

func TestCacheConcurrent(t *testing.T) {
	c := NewCache[int, []byte](1<<10, 1<<14, bytesOf)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				k := (i*7 + g) % 300
				if _, ok := c.Get(k); !ok {
					c.Put(k, blk(16))
				}
			}
		}()
	}
	wg.Wait()
	if int64(c.Len())*16 > c.Cap() {
		t.Errorf("%d entries over a %d byte bound", c.Len(), c.Cap())
	}
}

func TestCacheMaxBelowStartAndReplaceOldest(t *testing.T) {
	c := NewCache[int, []byte](30, 10, bytesOf) // max below start: start
	if c.capMax != 30 {
		t.Fatalf("capMax = %d, want the start 30", c.capMax)
	}
	c.Put(1, blk(10))
	c.Put(2, blk(10))
	c.Put(1, blk(25)) // the OLDEST key replaced by a larger value: 35 > 30, key 2 goes, key 1 stays
	if _, ok := c.Get(1); !ok {
		t.Error("the replaced key was evicted by its own growth")
	}
	if _, ok := c.Get(2); ok || c.Len() != 1 {
		t.Errorf("key 2 should have made room: len %d", c.Len())
	}
	c.Put(3, blk(5)) // 30: fits with key 1
	if c.Len() != 2 {
		t.Errorf("len %d", c.Len())
	}
}

func TestCacheGhostSetIsBounded(t *testing.T) {
	c := NewCache[int, []byte](10, 10, bytesOf)
	c.ghostCap = 3
	for i := 0; i < 20; i++ {
		c.Put(i, blk(10))
	}
	if len(c.ghost) != 3 || len(c.ghostQ) != 3 {
		t.Errorf("ghost %d/%d, want 3", len(c.ghost), len(c.ghostQ))
	}
	if _, ok := c.ghost[18]; !ok {
		t.Error("the most recent evicted key is not remembered")
	}
	if _, ok := c.ghost[0]; ok {
		t.Error("the oldest evicted key was not forgotten")
	}
}

func FuzzCache(f *testing.F) {
	f.Add([]byte{1, 2, 3, 200, 1, 2, 3, 250, 9, 9, 9, 9})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, ops []byte) {
		c := NewCache[byte, []byte](64, 512, bytesOf)
		c.ghostCap = 4
		for i := 0; i+1 < len(ops); i += 2 {
			k, n := ops[i]%16, int(ops[i+1])
			switch {
			case n%5 == 0:
				c.SetMax(int64(n))
			default:
				if _, ok := c.Get(k); !ok {
					c.Put(k, blk(n%97))
				}
			}
			var sum int64
			for _, v := range c.m {
				sum += int64(len(v))
			}
			if sum != c.used || len(c.m) != len(c.keys) {
				t.Fatalf("accounting: used %d, held %d, %d entries, %d keys", c.used, sum, len(c.m), len(c.keys))
			}
			if c.Len() > 1 && c.used > c.cap {
				t.Fatalf("%d bytes over a %d bound with %d entries", c.used, c.cap, c.Len())
			}
			if c.cap > c.capMax || c.cap < c.start {
				t.Fatalf("cap %d outside [%d,%d]", c.cap, c.start, c.capMax)
			}
		}
	})
}

func TestCacheSoleEntryReplacedByOneOverTheBound(t *testing.T) {
	c := NewCache[int, []byte](30, 30, bytesOf)
	c.Put(1, blk(10))
	c.Put(1, blk(40)) // a replacement is kept, alone, even over the bound
	if v, ok := c.Get(1); !ok || len(v) != 40 || c.Len() != 1 {
		t.Errorf("sole entry: ok %v len %d", ok, c.Len())
	}
	c.Put(2, blk(5)) // the next Put evicts it
	if _, ok := c.Get(1); ok {
		t.Error("the over-bound entry survived a Put")
	}
}
