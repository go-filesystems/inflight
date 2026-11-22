// Copyright (c) 2026, go-filesystems
// SPDX-License-Identifier: BSD-3-Clause

package inflight

import (
	"container/list"
	"context"
	"errors"
	"net"
	"sync"
	"time"
)

// ErrExceedsBudget is returned by [Budget.Acquire] and [Budget.TryAcquire]
// callers that ask for more than the whole budget: such a request could never
// be satisfied, so it is refused instead of waiting forever.
var ErrExceedsBudget = errors.New("inflight: request exceeds the whole budget")

// ErrBadDeadline is returned by [WithDeadline] for a non-positive duration.
var ErrBadDeadline = errors.New("inflight: deadline must be positive")

// Budget is a weighted semaphore over bytes: a server-wide cap on memory that
// requests hold while they are being served. Waiters are served in arrival
// order, so a large request is not starved by a stream of small ones.
type Budget struct {
	mu      sync.Mutex
	total   int64
	used    int64
	waiters list.List // of *waiter, FIFO
}

type waiter struct {
	n     int64
	ready chan struct{}
}

// NewBudget makes a budget of total bytes. total must be positive; a
// non-positive total is raised to 1 so that the zero value is never usable
// by accident.
func NewBudget(total int64) *Budget {
	if total < 1 {
		total = 1
	}
	return &Budget{total: total}
}

// Total returns the size of the budget.
func (b *Budget) Total() int64 { return b.total }

// InUse returns the bytes currently held.
func (b *Budget) InUse() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.used
}

// Acquire takes n bytes, blocking until they are available or ctx is done.
// It returns [ErrExceedsBudget] when n is larger than the whole budget, a
// negative n as an error too, and ctx's error when the wait is abandoned; in
// every error case nothing is held.
func (b *Budget) Acquire(ctx context.Context, n int64) error {
	if n < 0 || n > b.total {
		return ErrExceedsBudget
	}
	b.mu.Lock()
	if b.waiters.Len() == 0 && b.used+n <= b.total {
		b.used += n
		b.mu.Unlock()
		return nil
	}
	w := &waiter{n: n, ready: make(chan struct{})}
	el := b.waiters.PushBack(w)
	b.mu.Unlock()

	select {
	case <-w.ready:
		return nil
	case <-ctx.Done():
		b.mu.Lock()
		select {
		case <-w.ready:
			// Granted just as the context ended: keep the grant.
			b.mu.Unlock()
			return nil
		default:
		}
		first := b.waiters.Front() == el
		b.waiters.Remove(el)
		if first {
			b.wake()
		}
		b.mu.Unlock()
		return ctx.Err()
	}
}

// TryAcquire takes n bytes if they are available now and nobody is waiting
// ahead, and reports whether it did. It never blocks; n out of range is false.
func (b *Budget) TryAcquire(n int64) bool {
	if n < 0 || n > b.total {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.waiters.Len() > 0 || b.used+n > b.total {
		return false
	}
	b.used += n
	return true
}

// Release gives n bytes back. Releasing more than is held is a programming
// error and panics, rather than letting the accounting drift.
func (b *Budget) Release(n int64) {
	if n < 0 {
		panic("inflight: Budget.Release of a negative size")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if n > b.used {
		panic("inflight: Budget.Release of more than is held")
	}
	b.used -= n
	b.wake()
}

// wake grants, in order, every waiter at the head that now fits. b.mu held.
func (b *Budget) wake() {
	for el := b.waiters.Front(); el != nil; el = b.waiters.Front() {
		w := el.Value.(*waiter)
		if b.used+w.n > b.total {
			return
		}
		b.used += w.n
		b.waiters.Remove(el)
		close(w.ready)
	}
}

// Gate caps the number of slots held at once, for example connections that
// have not authenticated yet. It never blocks: a caller that cannot enter
// refuses the connection. A Gate made with [NewKeyGate] also caps the slots
// held per key (a source address).
type Gate struct {
	mu     sync.Mutex
	max    int
	held   int
	perKey int
	keys   map[string]int
}

// NewGate makes a gate of max slots; max below 1 is raised to 1.
func NewGate(max int) *Gate {
	if max < 1 {
		max = 1
	}
	return &Gate{max: max}
}

// NewKeyGate makes a gate of max slots in all and at most perKey per key; both
// are raised to 1 when smaller.
func NewKeyGate(max, perKey int) *Gate {
	g := NewGate(max)
	if perKey < 1 {
		perKey = 1
	}
	g.perKey = perKey
	g.keys = make(map[string]int)
	return g
}

// TryEnter takes a slot if one is free and reports whether it did. Pair every
// true with one [Gate.Leave].
func (g *Gate) TryEnter() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.held >= g.max {
		return false
	}
	g.held++
	return true
}

// Leave gives a slot taken by TryEnter back. Leaving a gate that holds nothing
// panics.
func (g *Gate) Leave() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.held == 0 {
		panic("inflight: Gate.Leave without TryEnter")
	}
	g.held--
}

// TryEnterKey takes a slot for key if both the gate and the key have room. On
// a gate made by [NewGate] there is no per-key cap and only the total applies.
// Pair every true with one [Gate.LeaveKey] of the same key.
func (g *Gate) TryEnterKey(key string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.held >= g.max {
		return false
	}
	if g.keys != nil {
		if g.keys[key] >= g.perKey {
			return false
		}
		g.keys[key]++
	}
	g.held++
	return true
}

// LeaveKey gives back a slot taken by TryEnterKey. The key's entry is removed
// at zero, so the map is bounded by the slots held, not by the keys seen.
func (g *Gate) LeaveKey(key string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.held == 0 {
		panic("inflight: Gate.LeaveKey without TryEnterKey")
	}
	if g.keys != nil {
		if g.keys[key] == 0 {
			panic("inflight: Gate.LeaveKey of a key that holds nothing")
		}
		if g.keys[key]--; g.keys[key] == 0 {
			delete(g.keys, key)
		}
	}
	g.held--
}

// Held returns the slots currently held.
func (g *Gate) Held() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.held
}

// WithDeadline sets conn's read and write deadline to d from now, runs f, and
// clears the deadline afterwards, so a handshake (for example ssh.NewServerConn)
// cannot be held open by a silent peer yet the established connection has no
// deadline left behind. It returns f's error, or the error of clearing the
// deadline when f succeeded. d must be positive.
func WithDeadline(conn net.Conn, d time.Duration, f func() error) error {
	if d <= 0 {
		return ErrBadDeadline
	}
	if err := conn.SetDeadline(time.Now().Add(d)); err != nil {
		return err
	}
	err := f()
	if cerr := conn.SetDeadline(time.Time{}); err == nil {
		err = cerr
	}
	return err
}
