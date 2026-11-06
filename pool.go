// Copyright (c) 2026, go-filesystems
// SPDX-License-Identifier: BSD-3-Clause

package inflight

import (
	"errors"
	"io"
	"math"
	"sync"
)

// Errors returned by [NewPool] and [Pool.NewConn]. A limit below one would
// make every request wait for ever, and a missing writer or an unnamed order
// would fail at the first reply; each is refused where it is made.
var (
	ErrLimit  = errors.New("inflight: a limit must be at least 1")
	ErrOrder  = errors.New("inflight: reply order must be InOrder or AnyOrder")
	ErrWriter = errors.New("inflight: nil reply writer")
)

// Order is the order in which a connection's replies leave. The zero value is
// not an order, so a connection cannot be made without choosing one.
type Order uint8

const (
	// InOrder writes replies in the order the requests arrived, whatever order
	// the work finishes in. Work is concurrent, replies are sequential.
	InOrder Order = iota + 1
	// AnyOrder writes each reply as soon as it is ready.
	AnyOrder
)

// Pool is what every connection of one server shares: the cap on requests in
// flight across all of them, and the byte ranges being worked on, which is
// what makes two connections writing one file order exactly as two requests of
// one connection do.
//
// A Pool is safe for concurrent use. Make one per server with [NewPool].
type Pool struct {
	tokens chan struct{}
	rs     rangeSet
}

// NewPool returns a pool that allows total requests in flight across all its
// connections. total must be at least 1.
func NewPool(total int) (*Pool, error) {
	if total < 1 {
		return nil, ErrLimit
	}
	return &Pool{tokens: make(chan struct{}, total)}, nil
}

// Span is the part of a file a request touches: a key naming the file (a path,
// a file handle: the caller decides what identifies "the same file" across
// handles), a half-open byte range, and whether the request writes it. Make
// one with [ReadRange], [WriteRange] or [WriteAll]; the zero Span is the empty
// range of the empty key, which conflicts with nothing.
type Span struct {
	key    string
	lo, hi uint64 // [lo, hi)
	write  bool
}

// ReadRange is a request that reads n bytes of key from off. The end
// saturates, so an offset straight off the wire cannot wrap.
func ReadRange(key string, off, n uint64) Span {
	return Span{key: key, lo: off, hi: satAdd(off, n)}
}

// WriteRange is a request that writes n bytes of key from off.
func WriteRange(key string, off, n uint64) Span {
	return Span{key: key, lo: off, hi: satAdd(off, n), write: true}
}

// WriteAll is a request that may rewrite the whole of key (a driver without
// positional writes): it orders against every other request on the key.
func WriteAll(key string) Span {
	return Span{key: key, hi: math.MaxUint64, write: true}
}

func (a *Span) conflicts(b *Span) bool {
	// An empty range touches nothing, so it conflicts with nothing.
	return a.key == b.key && (a.write || b.write) &&
		a.lo < a.hi && b.lo < b.hi && a.lo < b.hi && b.lo < a.hi
}

// satAdd is a+b, saturating.
func satAdd(a, b uint64) uint64 {
	if a > math.MaxUint64-b {
		return math.MaxUint64
	}
	return a + b
}

// rangeSet is the set of spans being processed, and what makes overlapping
// work wait its turn.
type rangeSet struct {
	mu     sync.Mutex
	cv     *sync.Cond
	active []*Span
}

// acquire blocks until sp conflicts with nothing in progress, then registers
// it. It is called from connections' readers, one request at a time, which is
// what keeps a connection's own requests in arrival order.
func (r *rangeSet) acquire(sp *Span) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cv == nil {
		r.cv = sync.NewCond(&r.mu)
	}
	for r.blocked(sp) {
		r.cv.Wait()
	}
	r.active = append(r.active, sp)
}

func (r *rangeSet) blocked(sp *Span) bool {
	for _, a := range r.active {
		if sp.conflicts(a) {
			return true
		}
	}
	return false
}

func (r *rangeSet) release(sp *Span) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, a := range r.active {
		if a == sp {
			r.active = append(r.active[:i], r.active[i+1:]...)
			break
		}
	}
	r.cv.Broadcast()
}

// replyWriter serialises replies onto one stream: each reply is one Write, so
// holding the mutex around it keeps replies whole. A failed Write is
// remembered, and the connection ends at its next request.
type replyWriter struct {
	wmu sync.Mutex // held across a Write, which may block on a slow peer
	w   io.Writer

	emu sync.Mutex // guards err only, so failure() never waits on a Write
	err error
}

func (r *replyWriter) Write(p []byte) {
	r.wmu.Lock()
	defer r.wmu.Unlock()
	if r.failure() != nil {
		return
	}
	if _, err := r.w.Write(p); err != nil {
		r.fail(err)
	}
}

// fail records why the connection cannot go on, keeping the first.
func (r *replyWriter) fail(err error) {
	r.emu.Lock()
	defer r.emu.Unlock()
	if r.err == nil {
		r.err = err
	}
}

func (r *replyWriter) failure() error {
	r.emu.Lock()
	defer r.emu.Unlock()
	return r.err
}
