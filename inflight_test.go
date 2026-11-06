// Copyright (c) 2026, go-filesystems
// SPDX-License-Identifier: BSD-3-Clause

package inflight

import (
	"errors"
	"io"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestConstructorsRefuseIllegalStates(t *testing.T) {
	if _, err := NewPool(0); !errors.Is(err, ErrLimit) {
		t.Fatalf("NewPool(0) = %v", err)
	}
	p := mustPool(t, 1)
	var b lockedBuf
	for _, tc := range []struct {
		name  string
		w     io.Writer
		order Order
		max   int
		want  error
	}{
		{"nil writer", nil, InOrder, 1, ErrWriter},
		{"zero order", &b, 0, 1, ErrOrder},
		{"unknown order", &b, 3, 1, ErrOrder},
		{"no slots", &b, InOrder, 0, ErrLimit},
	} {
		if _, err := p.NewConn(tc.w, tc.order, tc.max); !errors.Is(err, tc.want) {
			t.Errorf("%s: %v, want %v", tc.name, err, tc.want)
		}
	}
	if InOrder.String() != "InOrder" || AnyOrder.String() != "AnyOrder" || Order(0).String() != "Order(0)" {
		t.Error("Order.String")
	}
}

func TestSpanConflicts(t *testing.T) {
	r := func(k string, lo, n uint64) Span { return ReadRange(k, lo, n) }
	w := func(k string, lo, n uint64) Span { return WriteRange(k, lo, n) }
	for _, tc := range []struct {
		name string
		a, b Span
		want bool
	}{
		{"two reads overlap", r("f", 0, 10), r("f", 5, 10), false},
		{"read and write overlap", r("f", 0, 10), w("f", 9, 4), true},
		{"adjacent ranges", w("f", 0, 10), w("f", 10, 10), false},
		{"disjoint writes", w("f", 0, 10), w("f", 100, 10), false},
		{"other key", w("f", 0, 10), w("g", 0, 10), false},
		{"write all", WriteAll("f"), r("f", 1<<60, 1), true},
		{"write all other key", WriteAll("f"), r("g", 0, 1), false},
		{"empty write", w("f", 5, 0), w("f", 0, 100), false},
		{"zero span", Span{}, WriteAll(""), false},
		{"offset past the end saturates", w("f", math.MaxUint64-1, 100), r("f", math.MaxUint64-2, 100), true},
	} {
		if got := tc.a.conflicts(&tc.b); got != tc.want {
			t.Errorf("%s: %v, want %v", tc.name, got, tc.want)
		}
		if got := tc.b.conflicts(&tc.a); got != tc.want {
			t.Errorf("%s (reversed): %v, want %v", tc.name, got, tc.want)
		}
	}
	if s := ReadRange("f", math.MaxUint64, math.MaxUint64); s.hi != math.MaxUint64 {
		t.Errorf("end wrapped: %d", s.hi)
	}
}

func scriptOps() []op {
	j := 200 * time.Microsecond
	return []op{
		{kRead, "a", 0, 10, j}, {kRead, "a", 5, 10, j}, {kWrite, "a", 0, 8, j},
		{kWrite, "a", 100, 8, 0}, {kWrite, "b", 0, 8, j}, {kWriteAll, "a", 0, 0, j},
		{kBarrier, "", 0, 0, j}, {kRead, "a", 0, 4, 0}, {kRead, "a", 4, 4, j},
		{kSilent, "", 0, 0, 0}, {kWrite, "a", 0, 4, j}, {kRead, "a", 2, 4, j},
		{kBarrier, "", 0, 0, 0}, {kWrite, "b", 0, 1, j},
	}
}

func TestScriptsEveryPolicyAndWidth(t *testing.T) {
	for _, order := range []Order{InOrder, AnyOrder} {
		for _, width := range []int{1, 2, 8} {
			t.Run(order.String(), func(t *testing.T) {
				m := newMonitor()
				var b lockedBuf
				c := mustConn(t, mustPool(t, 64), &b, order, width)
				ops := scriptOps()
				runScript(t, c, 0, m, ops)
				m.check(t)
				expectReplies(t, b.replies(), ops, order)
				if m.maxActive > width {
					t.Errorf("%d ran at once, bound %d", m.maxActive, width)
				}
				if err := c.Err(); err != nil {
					t.Error(err)
				}
			})
		}
	}
}

func TestRequestsRunTogetherWhenTheyMay(t *testing.T) {
	m := newMonitor()
	var b lockedBuf
	c := mustConn(t, mustPool(t, 64), &b, InOrder, 4)
	ops := make([]op, 4)
	for i := range ops {
		ops[i] = op{kRead, "a", uint64(i) * 10, 10, 50 * time.Millisecond}
	}
	runScript(t, c, 0, m, ops)
	if m.maxActive < 2 {
		t.Errorf("never more than %d at once: no concurrency", m.maxActive)
	}
}

func TestTwoConnectionsOrderConflictsAcrossThePool(t *testing.T) {
	m := newMonitor()
	p := mustPool(t, 64)
	var b0, b1 lockedBuf
	c0 := mustConn(t, p, &b0, AnyOrder, 4)
	c1 := mustConn(t, p, &b1, InOrder, 4)
	var ops []op
	for i := 0; i < 30; i++ {
		k := kRead
		if i%3 == 0 {
			k = kWrite
		}
		ops = append(ops, op{k, "a", uint64(i%4) * 4, 8, 100 * time.Microsecond})
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); runScript(t, c0, 0, m, ops) }()
	go func() { defer wg.Done(); runScript(t, c1, 1, m, ops) }()
	wg.Wait()
	m.check(t)
	expectReplies(t, b0.replies(), ops, AnyOrder)
	expectReplies(t, b1.replies(), ops, InOrder)
}

func TestPoolBudgetBoundsAllConnections(t *testing.T) {
	m := newMonitor()
	p := mustPool(t, 3)
	var bs [4]lockedBuf
	var wg sync.WaitGroup
	for i := range bs {
		c := mustConn(t, p, &bs[i], AnyOrder, 8)
		ops := make([]op, 12)
		for j := range ops {
			ops[j] = op{kRead, "a", 0, 1, 300 * time.Microsecond}
		}
		wg.Add(1)
		go func() { defer wg.Done(); runScript(t, c, i, m, ops) }()
	}
	wg.Wait()
	m.check(t)
	if m.maxActive > 3 {
		t.Errorf("%d ran at once across the pool, budget 3", m.maxActive)
	}
	for i := range bs {
		if n := len(bs[i].replies()); n != 12 {
			t.Errorf("conn %d: %d replies", i, n)
		}
	}
}

// blocks reports whether f is still blocked after a short wait.
func blocks(f func()) (blocked bool, unblock func()) {
	done := make(chan struct{})
	go func() { f(); close(done) }()
	select {
	case <-done:
		return false, func() {}
	case <-time.After(50 * time.Millisecond):
		return true, func() { <-done }
	}
}

func TestBeginBlocksAtTheBoundsBeforeAnythingIsRead(t *testing.T) {
	p := mustPool(t, 64)
	var b lockedBuf
	c := mustConn(t, p, &b, InOrder, 2)
	gate := make(chan struct{})
	hold := func(*Slot) ([]byte, error) { <-gate; return []byte{1, 2, 3, 4}, nil }
	for i := 0; i < 2; i++ {
		sl, err := c.Begin()
		if err != nil {
			t.Fatal(err)
		}
		c.Go(sl, ReadRange("a", 0, 1), hold)
	}
	var third *Slot
	blocked, unblock := blocks(func() { third, _ = c.Begin() })
	if !blocked {
		t.Fatal("a third Begin did not wait with 2 slots in use")
	}
	close(gate)
	unblock()
	c.Abandon(third)
	c.Wait()
	if n := len(b.replies()); n != 2 {
		t.Errorf("%d replies", n)
	}

	// The pool's budget is a bound of its own, shared with another connection.
	p1 := mustPool(t, 1)
	c1 := mustConn(t, p1, &lockedBuf{}, AnyOrder, 8)
	c2 := mustConn(t, p1, &lockedBuf{}, AnyOrder, 8)
	s1, _ := c1.Begin()
	blocked, unblock = blocks(func() { s2, _ := c2.Begin(); c2.Abandon(s2) })
	if !blocked {
		t.Fatal("the pool budget did not hold a second connection back")
	}
	c1.Abandon(s1)
	unblock()
}

func TestAbandonFreesTheSlotAndTheToken(t *testing.T) {
	c := mustConn(t, mustPool(t, 1), &lockedBuf{}, InOrder, 1)
	for i := 0; i < 3; i++ {
		sl, err := c.Begin()
		if err != nil {
			t.Fatal(err)
		}
		c.Abandon(sl)
	}
}

func TestSlotsAreReused(t *testing.T) {
	var b lockedBuf
	c := mustConn(t, mustPool(t, 8), &b, InOrder, 1)
	var first *Slot
	for i := 0; i < 3; i++ {
		sl, _ := c.Begin()
		if first == nil {
			first = sl
		} else if sl != first {
			t.Fatal("a new slot was made while one was free")
		}
		sl.Out = append(sl.Out[:0], 0, 0, 0, byte(i))
		c.Barrier(sl, func(sl *Slot) ([]byte, error) { return sl.Out, nil })
	}
	c.Wait()
	if got := b.replies(); len(got) != 3 || got[2] != 2 {
		t.Errorf("replies %v", got)
	}
}

func TestRunErrorEndsTheConnection(t *testing.T) {
	for _, order := range []Order{InOrder, AnyOrder} {
		var b lockedBuf
		c := mustConn(t, mustPool(t, 4), &b, order, 2)
		boom := errors.New("boom")
		sl, _ := c.Begin()
		c.Go(sl, ReadRange("a", 0, 1), func(*Slot) ([]byte, error) { return []byte{0, 0, 0, 9}, boom })
		c.Wait()
		if !errors.Is(c.Err(), boom) {
			t.Fatalf("%v: Err = %v", order, c.Err())
		}
		if len(b.replies()) != 0 {
			t.Errorf("%v: a reply was written for a failed request", order)
		}
		if sl, err := c.Begin(); !errors.Is(err, boom) || sl != nil {
			t.Errorf("%v: Begin after failure = %v, %v", order, sl, err)
		}
	}
}

type failingWriter struct {
	n    atomic.Int32
	fail error
}

func (f *failingWriter) Write(p []byte) (int, error) {
	f.n.Add(1)
	return 0, f.fail
}

func TestWriteErrorIsRememberedAndStopsLaterWrites(t *testing.T) {
	w := &failingWriter{fail: errors.New("broken pipe")}
	c, err := mustPool(t, 4).NewConn(w, InOrder, 1)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		sl, err := c.Begin()
		if err != nil {
			break
		}
		c.Barrier(sl, func(*Slot) ([]byte, error) { return []byte{0, 0, 0, 1}, nil })
	}
	c.Wait()
	if !errors.Is(c.Err(), w.fail) {
		t.Fatalf("Err = %v", c.Err())
	}
	if n := w.n.Load(); n != 1 {
		t.Errorf("%d writes reached a failed stream, want 1", n)
	}
	// A write attempted after the failure never reaches the stream.
	c.w.Write([]byte{1})
	if n := w.n.Load(); n != 1 {
		t.Errorf("%d writes", n)
	}
}

// slowFirstWriter blocks its first Write until released.
type slowFirstWriter struct {
	lockedBuf
	first   sync.Once
	entered chan struct{}
	release chan struct{}
}

func (s *slowFirstWriter) Write(p []byte) (int, error) {
	s.first.Do(func() { close(s.entered); <-s.release })
	return s.lockedBuf.Write(p)
}

func TestInOrderRepliesWaitForTheHeadAndAreFlushedByWhoeverIsWriting(t *testing.T) {
	w := &slowFirstWriter{entered: make(chan struct{}), release: make(chan struct{})}
	c, _ := mustPool(t, 8).NewConn(w, InOrder, 8)
	gate := make(chan struct{})
	reply := func(n byte) Run {
		return func(*Slot) ([]byte, error) { return []byte{0, 0, 0, n}, nil }
	}
	// 0 is slow to finish; 1, 2 and 3 finish first and must wait behind it.
	sl, _ := c.Begin()
	c.Go(sl, ReadRange("a", 0, 1), func(*Slot) ([]byte, error) { <-gate; return []byte{0, 0, 0, 0}, nil })
	for i := byte(1); i < 4; i++ {
		sl, _ := c.Begin()
		c.Go(sl, ReadRange("a", 0, 1), reply(i))
	}
	time.Sleep(20 * time.Millisecond)
	if n := len(w.replies()); n != 0 {
		t.Fatalf("%d replies written ahead of the head", n)
	}
	close(gate)
	<-w.entered // the head's Write is now blocked on a slow peer
	// The flusher is mid-write with 1, 2, 3 done and queued behind it; the
	// reader can still enqueue, and a later request completes while it writes.
	sl, _ = c.Begin()
	c.Go(sl, ReadRange("a", 0, 1), reply(4))
	time.Sleep(20 * time.Millisecond)
	close(w.release)
	c.Wait()
	got := w.replies()
	if len(got) != 5 {
		t.Fatalf("replies %v", got)
	}
	for i, g := range got {
		if g != uint32(i) {
			t.Fatalf("replies %v are not in request order", got)
		}
	}
}

func TestAnyOrderRepliesLeaveWhenReady(t *testing.T) {
	var b lockedBuf
	c := mustConn(t, mustPool(t, 8), &b, AnyOrder, 4)
	gate := make(chan struct{})
	sl, _ := c.Begin()
	c.Go(sl, ReadRange("a", 0, 1), func(*Slot) ([]byte, error) { <-gate; return []byte{0, 0, 0, 0}, nil })
	sl, _ = c.Begin()
	c.Go(sl, ReadRange("a", 0, 1), func(*Slot) ([]byte, error) { return []byte{0, 0, 0, 1}, nil })
	deadline := time.Now().Add(5 * time.Second)
	for len(b.replies()) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := b.replies(); len(got) != 1 || got[0] != 1 {
		t.Fatalf("replies %v: the ready one should not wait for the slow one", got)
	}
	close(gate)
	c.Wait()
	if got := b.replies(); len(got) != 2 || got[1] != 0 {
		t.Fatalf("replies %v", got)
	}
}

func TestWriteBehindAReadWaitsAndKeepsArrivalOrder(t *testing.T) {
	var b lockedBuf
	c := mustConn(t, mustPool(t, 8), &b, InOrder, 4)
	var order []int
	var mu sync.Mutex
	rec := func(i int, d time.Duration) Run {
		return func(*Slot) ([]byte, error) {
			mu.Lock()
			order = append(order, i)
			mu.Unlock()
			time.Sleep(d)
			return []byte{0, 0, 0, byte(i)}, nil
		}
	}
	for i, sp := range []Span{ReadRange("a", 0, 10), WriteRange("a", 5, 10), ReadRange("a", 12, 4)} {
		sl, _ := c.Begin()
		c.Go(sl, sp, rec(i, 20*time.Millisecond))
	}
	c.Wait()
	mu.Lock()
	defer mu.Unlock()
	if len(order) != 3 || order[0] != 0 || order[1] != 1 || order[2] != 2 {
		t.Errorf("started in order %v, want 0 1 2", order)
	}
}

func TestSequentialConnRunsOnTheReadersGoroutine(t *testing.T) {
	var b lockedBuf
	c := mustConn(t, mustPool(t, 8), &b, InOrder, 1)
	ran := false
	sl, _ := c.Begin()
	c.Go(sl, WriteAll("a"), func(*Slot) ([]byte, error) { ran = true; return nil, nil })
	if !ran {
		t.Fatal("Go returned before a sequential request ran")
	}
	c.Wait()
}

func TestFSLock(t *testing.T) {
	for _, shared := range []bool{false, true} {
		l := NewFSLock(shared)
		l.LockIO()
		l.LockIO() // File I/O is always shared with itself
		// The path call goes first: a pending exclusive Lock would hold back
		// every later shared one, and hide what is being tested.
		blocked, unblock2 := blocks(func() { l.LockPath(); l.UnlockPath() })
		if blocked == shared {
			t.Errorf("shared=%v: a path call blocked=%v beside File I/O", shared, blocked)
		}
		blocked, unblock := blocks(func() { l.Lock(); l.Unlock() })
		if !blocked {
			t.Errorf("shared=%v: an exclusive call ran beside File I/O", shared)
		}
		l.UnlockIO()
		l.UnlockIO()
		unblock()
		unblock2()
	}
}
