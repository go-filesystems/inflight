package inflight

import (
	"sync"
	"testing"
)

func seenCache() *Cache[int, []byte] {
	return NewCache[int, []byte](1<<20, 1<<20, func(b []byte) int64 { return int64(len(b)) })
}

// The first Seen of a key is false, the second true, and the key is then
// forgotten: the third is false again.
func TestSeenSecondTouch(t *testing.T) {
	c := seenCache()
	for i, want := range []bool{false, true, false, true} {
		if got := c.Seen(7); got != want {
			t.Fatalf("call %d: Seen = %v, want %v", i, got, want)
		}
	}
	if c.Seen(8) {
		t.Fatal("a different key was seen")
	}
}

// Seen does not touch the entries, their count or the bound.
func TestSeenLeavesTheCacheAlone(t *testing.T) {
	c := seenCache()
	c.Put(1, make([]byte, 100))
	capBefore := c.Cap()
	c.Seen(1)
	c.Seen(1)
	if _, ok := c.Get(1); !ok || c.Len() != 1 || c.Cap() != capBefore {
		t.Fatalf("Seen changed the cache: len %d cap %d", c.Len(), c.Cap())
	}
	c.Seen(2)
	if _, ok := c.Get(2); ok || c.Len() != 1 {
		t.Fatal("Seen stored an entry")
	}
}

// Only seenCap keys are remembered, the oldest forgotten first, and the memory
// stays within the cap whatever is passed.
func TestSeenIsBounded(t *testing.T) {
	c := seenCache()
	c.seenCap = 3
	for k := 0; k < 3; k++ {
		c.Seen(k)
	}
	c.Seen(3) // forgets 0
	if c.Seen(0) {
		t.Fatal("key 0 outlived the cap")
	}
	// 0 was just added again, which forgot 1.
	if c.Seen(1) {
		t.Fatal("key 1 outlived the cap")
	}
	if !c.Seen(3) {
		t.Fatal("key 3 forgotten too early")
	}
	for k := 100; k < 1000; k++ {
		c.Seen(k)
		if len(c.seen) > c.seenCap || len(c.seenQ) > c.seenCap {
			t.Fatalf("seen %d / queue %d over the cap %d", len(c.seen), len(c.seenQ), c.seenCap)
		}
	}
}

// A key seen, forgotten by the second touch and seen again is not dropped by
// the stale queue entry of its first touch.
func TestSeenStaleQueueEntry(t *testing.T) {
	c := seenCache()
	c.seenCap = 2
	c.Seen(1)
	c.Seen(1) // true: forgotten, its queue entry is stale
	c.Seen(1) // false: remembered again
	c.Seen(2) // queue is full: the stale entry of the first touch is dropped
	if !c.Seen(1) {
		t.Fatal("a stale queue entry forgot a key that was seen again")
	}
}

func TestSeenNilCache(t *testing.T) {
	var c *Cache[int, []byte]
	if c.Seen(1) || c.Seen(1) {
		t.Fatal("a nil cache reported a key as seen")
	}
}

func TestSeenConcurrent(t *testing.T) {
	c := seenCache()
	c.seenCap = 64
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				c.Seen(i % 200)
			}
		}()
	}
	wg.Wait()
	if len(c.seen) > c.seenCap {
		t.Fatalf("seen %d over the cap", len(c.seen))
	}
}
