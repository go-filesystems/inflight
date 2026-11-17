// Copyright (c) 2026, go-filesystems
// SPDX-License-Identifier: BSD-3-Clause

package inflight

import "sync"

// Cache is a bounded FIFO cache of decoded blocks that grows when it is too
// small and not otherwise. It was extracted from go-filesystems/squashfs
// v0.9.0 (PR #61), where it holds decompressed data blocks, so that the
// read-only filesystems that decode blocks in parallel (see [Ordered]) share
// one implementation of the part that is easy to get subtly wrong: the bound,
// and the rule for when to exceed it.
//
// The bound is in bytes, as reported by the size function given to
// [NewCache]. It starts at the start argument and grows, by a sixteenth at a
// time (at least one entry), only when a key that was evicted is asked for
// again: a random read of a file larger than the cache does at once; one
// sequential pass never does, a second pass over a file larger than the cache
// does. It never passes max. An entry larger than the current bound is not
// stored at all, so one huge value cannot empty the cache or exceed max.
//
// The values are shared, not copied: callers must not modify what Get returns
// or what they Put. A Cache is safe for concurrent use. The zero value is not
// usable; a nil *Cache reports a miss and stores nothing, which is how a
// caller turns the cache off.
type Cache[K comparable, V any] struct {
	mu       sync.Mutex
	size     func(V) int64
	m        map[K]V
	keys     []K // insertion order, for FIFO eviction
	used     int64
	start    int64
	cap      int64
	capMax   int64
	ghost    map[K]struct{} // keys of evicted entries, at most ghostCap
	ghostQ   []K
	ghostCap int
}

// DefaultGhostCap is how many evicted keys a Cache remembers (keys only).
const DefaultGhostCap = 16384

// NewCache returns a Cache that starts at start bytes and may grow to max.
// max below start means start. size reports the cost of a value in bytes
// (a negative result counts as 0).
func NewCache[K comparable, V any](start, max int64, size func(V) int64) *Cache[K, V] {
	if max < start {
		max = start
	}
	return &Cache[K, V]{size: size, m: make(map[K]V), start: start, cap: start, capMax: max, ghostCap: DefaultGhostCap}
}

// Get returns the cached value of k.
func (c *Cache[K, V]) Get(k K) (V, bool) {
	if c == nil {
		var z V
		return z, false
	}
	c.mu.Lock()
	v, ok := c.m[k]
	c.mu.Unlock()
	return v, ok
}

// Put stores v under k, evicting the oldest entries while the cache is over
// its bound. Putting a key again replaces its value without changing its age.
func (c *Cache[K, V]) Put(k K, v V) {
	if c == nil {
		return
	}
	cost := max(c.size(v), 0)
	c.mu.Lock()
	defer c.mu.Unlock()
	if old, exists := c.m[k]; exists {
		c.used += cost - max(c.size(old), 0)
		c.m[k] = v
		c.evictLocked(&k)
		return
	}
	if _, evicted := c.ghost[k]; evicted {
		// An entry we evicted is wanted again: the cache was too small for this
		// workload, not merely cold. Grow it, up to its bound.
		delete(c.ghost, k)
		if c.cap < c.capMax {
			c.cap = min(c.capMax, c.cap+max(cost, c.cap/16))
		}
	}
	if cost > c.cap {
		return
	}
	c.m[k] = v
	c.keys = append(c.keys, k)
	c.used += cost
	c.evictLocked(&k)
}

// evictLocked drops the oldest entries (never *keep, when given) until the
// cache fits.
func (c *Cache[K, V]) evictLocked(keep *K) {
	for c.used > c.cap && len(c.keys) > 0 {
		old := c.keys[0]
		if keep != nil && old == *keep {
			if len(c.keys) == 1 {
				return
			}
			// The entry just stored is the oldest only when it is alone; with
			// others present, rotate it out of the way.
			c.keys = append(c.keys[1:], old)
			continue
		}
		c.keys = c.keys[1:]
		c.used -= max(c.size(c.m[old]), 0)
		delete(c.m, old)
		c.remember(old)
	}
}

func (c *Cache[K, V]) remember(k K) {
	if c.ghost == nil {
		c.ghost = make(map[K]struct{})
	}
	if len(c.ghostQ) >= c.ghostCap {
		delete(c.ghost, c.ghostQ[0])
		c.ghostQ = c.ghostQ[1:]
	}
	c.ghost[k] = struct{}{}
	c.ghostQ = append(c.ghostQ, k)
}

// SetMax sets the most bytes the cache may grow to, never below its starting
// size. A cache already above it shrinks to it at once.
func (c *Cache[K, V]) SetMax(max int64) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if max < c.start {
		max = c.start
	}
	c.capMax = max
	if c.cap > max {
		c.cap = max
		c.evictLocked(nil)
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
