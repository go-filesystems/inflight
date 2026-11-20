// Copyright (c) 2026, go-filesystems
// SPDX-License-Identifier: BSD-3-Clause

package inflight

import "sync"

// Cache is a bounded FIFO cache of decoded blocks that grows when it is too
// small and not otherwise. It was extracted from go-filesystems/squashfs
// v0.9.0 (its PR 61), where it holds decompressed data blocks, so that the
// read-only filesystems that decode blocks in parallel (see [Ordered]) share
// one implementation of the part that is easy to get subtly wrong: the bound,
// and the rule for when to exceed it.
//
// The bound is in bytes, as reported by the size function given to
// [NewCache], called once per Put and never under the cache's lock. It starts
// at the start argument (at least 1) and grows, by a sixteenth at a time (at
// least one entry), only when a key that was evicted is asked for again: a
// random read of a file larger than the cache does at once; one sequential
// pass never does, a second pass over a file larger than the cache does. It
// never passes max. A value larger than the current bound is not stored, so
// one huge value cannot empty the cache or exceed max; putting such a value
// under a key that is cached removes the old one.
//
// The values are shared, not copied: callers must not modify what Get returns
// or what they Put. A Cache is safe for concurrent use. The zero value is not
// usable; a nil *Cache reports a miss and stores nothing, which is how a
// caller turns the cache off.
type Cache[K comparable, V any] struct {
	size func(V) int64

	mu       sync.Mutex
	m        map[K]cacheEntry[V]
	keys     []K // insertion order, for FIFO eviction
	used     int64
	start    int64
	cap      int64
	capMax   int64
	ghost    map[K]uint64 // evicted keys -> the sequence of their latest eviction
	ghostQ   []ghostEnt[K]
	ghostSeq uint64
	ghostCap int

	seen    map[K]uint64 // keys passed to Seen once -> the sequence of that call
	seenQ   []ghostEnt[K]
	seenSeq uint64
	seenCap int
}

type cacheEntry[V any] struct {
	v    V
	cost int64
}

type ghostEnt[K comparable] struct {
	k   K
	seq uint64
}

// DefaultGhostCap is how many evicted keys a Cache remembers (keys only).
const DefaultGhostCap = 16384

// NewCache returns a Cache that starts at start bytes (at least 1) and may
// grow to max (at least start). size reports the cost of a value in bytes (a
// negative result counts as 0); it must not touch the cache.
func NewCache[K comparable, V any](start, max int64, size func(V) int64) *Cache[K, V] {
	start = maxInt(start, 1)
	max = maxInt(max, start)
	return &Cache[K, V]{size: size, m: make(map[K]cacheEntry[V]), start: start, cap: start, capMax: max, ghostCap: DefaultGhostCap, seenCap: DefaultSeenCap}
}

func maxInt(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// Get returns the cached value of k.
func (c *Cache[K, V]) Get(k K) (V, bool) {
	if c == nil {
		var z V
		return z, false
	}
	c.mu.Lock()
	e, ok := c.m[k]
	c.mu.Unlock()
	return e.v, ok
}

// Put stores v under k as the newest entry, evicting the oldest while the
// cache is over its bound. A value already cached under k is dropped first.
func (c *Cache[K, V]) Put(k K, v V) {
	if c == nil {
		return
	}
	cost := maxInt(c.size(v), 0)
	c.mu.Lock()
	defer c.mu.Unlock()
	if old, exists := c.m[k]; exists {
		c.used -= old.cost
		delete(c.m, k)
		c.dropKeyLocked(k)
	} else if _, evicted := c.ghost[k]; evicted {
		// An entry we evicted is wanted again: the cache was too small for this
		// workload, not merely cold. Grow it, up to its bound.
		delete(c.ghost, k)
		if c.cap < c.capMax {
			c.cap = min(c.capMax, c.cap+maxInt(cost, c.cap/16))
		}
	}
	if cost > c.cap {
		return
	}
	c.m[k] = cacheEntry[V]{v, cost}
	c.keys = append(c.keys, k)
	c.used += cost
	c.evictLocked()
}

// dropKeyLocked removes k from the insertion order.
func (c *Cache[K, V]) dropKeyLocked(k K) {
	for i, x := range c.keys {
		if x == k {
			c.keys = append(c.keys[:i], c.keys[i+1:]...)
			return
		}
	}
}

// evictLocked drops the oldest entries until the cache fits.
func (c *Cache[K, V]) evictLocked() {
	for c.used > c.cap && len(c.keys) > 0 {
		old := c.keys[0]
		c.keys = c.keys[1:]
		c.used -= c.m[old].cost
		delete(c.m, old)
		c.remember(old)
	}
}

func (c *Cache[K, V]) remember(k K) {
	if c.ghost == nil {
		c.ghost = make(map[K]uint64)
	}
	if len(c.ghostQ) >= c.ghostCap {
		g := c.ghostQ[0]
		c.ghostQ = c.ghostQ[1:]
		if c.ghost[g.k] == g.seq { // not re-remembered since: forget it
			delete(c.ghost, g.k)
		}
	}
	c.ghostSeq++
	c.ghost[k] = c.ghostSeq
	c.ghostQ = append(c.ghostQ, ghostEnt[K]{k, c.ghostSeq})
}

// SetMax sets the most bytes the cache may grow to, never below its starting
// size. A cache already above it shrinks to it at once.
func (c *Cache[K, V]) SetMax(max int64) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	max = maxInt(max, c.start)
	c.capMax = max
	if c.cap > max {
		c.cap = max
		c.evictLocked()
	}
}

// Len is the number of entries held.
func (c *Cache[K, V]) Len() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.m)
}

// Cap is the current bound in bytes.
func (c *Cache[K, V]) Cap() int64 {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cap
}

// DefaultSeenCap is how many keys [Cache.Seen] remembers (keys only).
const DefaultSeenCap = 16384

// Seen is admission on second touch. It reports whether k was passed to Seen
// before (and not since forgotten), and remembers it if not: the first call
// for a key returns false, the next returns true and forgets the key, so a
// caller that Puts only when Seen is true caches what is read twice and never
// what is read once. A stream read once, however long, then costs no cache
// memory and no copy. Only keys are kept, at most DefaultSeenCap of them, the
// oldest forgotten first; a nil *Cache reports false and remembers nothing.
//
// It is independent of the entries and of the keys of evicted entries that
// grow the bound: Seen neither reads nor changes them.
func (c *Cache[K, V]) Seen(k K) bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.seen[k]; ok {
		delete(c.seen, k)
		return true
	}
	if c.seen == nil {
		c.seen = make(map[K]uint64)
	}
	for len(c.seenQ) >= c.seenCap {
		g := c.seenQ[0]
		c.seenQ = c.seenQ[1:]
		if c.seen[g.k] == g.seq { // not re-added since: forget it
			delete(c.seen, g.k)
		}
	}
	c.seenSeq++
	c.seen[k] = c.seenSeq
	c.seenQ = append(c.seenQ, ghostEnt[K]{k, c.seenSeq})
	return false
}
