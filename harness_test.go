// Copyright (c) 2026, go-filesystems
// SPDX-License-Identifier: BSD-3-Clause

package inflight

import (
	"encoding/binary"
	"fmt"
	"sync"
	"testing"
	"time"
)

// The harness drives a Conn with a script of requests and checks, from the
// outside and with its own bookkeeping, what the package promises: no two
// conflicting spans run together (across connections), a barrier runs alone
// (within its connection), no more than the bounds run at once, and replies
// leave in the promised order.

type opKind uint8

const (
	kRead opKind = iota
	kWrite
	kWriteAll
	kBarrier
	kSilent // a barrier with no reply
)

type op struct {
	kind   opKind
	key    string
	lo, n  uint64
	jitter time.Duration
}

func (o op) span() Span {
	switch o.kind {
	case kRead:
		return ReadRange(o.key, o.lo, o.n)
	case kWrite:
		return WriteRange(o.key, o.lo, o.n)
	}
	return WriteAll(o.key)
}

type lockedBuf struct {
	mu sync.Mutex
	b  []byte
}

func (l *lockedBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.b = append(l.b, p...)
	return len(p), nil
}

func (l *lockedBuf) replies() []uint32 {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []uint32
	for i := 0; i+4 <= len(l.b); i += 4 {
		out = append(out, binary.BigEndian.Uint32(l.b[i:]))
	}
	return out
}

type entry struct {
	conn    int
	sp      Span
	barrier bool
}

// monitor is the independent witness shared by every connection of a test.
type monitor struct {
	mu         sync.Mutex
	active     map[int]entry // by global id
	next       int
	maxActive  int
	violations []string
}

func newMonitor() *monitor { return &monitor{active: map[int]entry{}} }

func (m *monitor) enter(conn int, sp Span, barrier bool) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	id := m.next
	m.next++
	for _, a := range m.active {
		if a.conn == conn && (a.barrier || barrier) {
			m.violations = append(m.violations, "a barrier ran beside another request of its connection")
		}
		if sp.conflicts(&a.sp) {
			m.violations = append(m.violations, "conflicting spans ran together")
		}
	}
	m.active[id] = entry{conn, sp, barrier}
	m.maxActive = max(m.maxActive, len(m.active))
	return id
}

func (m *monitor) exit(id int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.active, id)
}

func (m *monitor) check(t testing.TB) {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, v := range m.violations {
		t.Error(v)
	}
}

// runScript sends ops through c, one request per op, from the calling
// goroutine as the reader, and waits for all to be answered.
func runScript(t testing.TB, c *Conn, connID int, m *monitor, ops []op) {
	t.Helper()
	for i, o := range ops {
		sl, err := c.Begin()
		if err != nil {
			t.Errorf("Begin: %v", err)
			return
		}
		seq := uint32(i)
		o := o
		run := func(*Slot) ([]byte, error) {
			barrier := o.kind == kBarrier || o.kind == kSilent
			id := m.enter(connID, o.span(), barrier)
			time.Sleep(o.jitter)
			m.exit(id)
			if o.kind == kSilent {
				return nil, nil
			}
			return binary.BigEndian.AppendUint32(nil, seq), nil
		}
		if o.kind == kBarrier || o.kind == kSilent {
			c.Barrier(sl, run)
		} else {
			c.Go(sl, o.span(), run)
		}
	}
	c.Wait()
}

// expectReplies checks the replies of a finished script: all of them, once
// each, and in request order when the connection promised it.
func expectReplies(t testing.TB, got []uint32, ops []op, order Order) {
	t.Helper()
	var want []uint32
	for i, o := range ops {
		if o.kind != kSilent {
			want = append(want, uint32(i))
		}
	}
	if len(got) != len(want) {
		t.Errorf("%d replies, want %d", len(got), len(want))
		return
	}
	if order == InOrder {
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("reply %d is request %d, want %d (replies %v)", i, got[i], want[i], got)
				return
			}
		}
		return
	}
	seen := map[uint32]bool{}
	for _, g := range got {
		seen[g] = true
	}
	for _, w := range want {
		if !seen[w] {
			t.Errorf("request %d was never answered", w)
		}
	}
}

func mustPool(t testing.TB, n int) *Pool {
	t.Helper()
	p, err := NewPool(n)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func mustConn(t testing.TB, p *Pool, w *lockedBuf, order Order, n int) *Conn {
	t.Helper()
	c, err := p.NewConn(w, order, n)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func (o Order) String() string {
	switch o {
	case InOrder:
		return "InOrder"
	case AnyOrder:
		return "AnyOrder"
	}
	return fmt.Sprintf("Order(%d)", uint8(o))
}
