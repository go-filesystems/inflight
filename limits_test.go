// Copyright (c) 2026, go-filesystems
// SPDX-License-Identifier: BSD-3-Clause

package inflight

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBudgetBasics(t *testing.T) {
	b := NewBudget(10)
	if b.Total() != 10 {
		t.Fatal("total")
	}
	if !b.TryAcquire(6) || b.InUse() != 6 {
		t.Fatal("try 6")
	}
	if b.TryAcquire(5) {
		t.Fatal("5 must not fit")
	}
	if !b.TryAcquire(4) || b.InUse() != 10 {
		t.Fatal("try 4")
	}
	b.Release(10)
	if b.InUse() != 0 {
		t.Fatal("release")
	}
	if NewBudget(0).Total() != 1 || NewBudget(-5).Total() != 1 {
		t.Fatal("floor")
	}
}

func TestBudgetErrors(t *testing.T) {
	b := NewBudget(10)
	if err := b.Acquire(context.Background(), 11); !errors.Is(err, ErrExceedsBudget) {
		t.Fatalf("n>total: %v", err)
	}
	if err := b.Acquire(context.Background(), -1); !errors.Is(err, ErrExceedsBudget) {
		t.Fatalf("n<0: %v", err)
	}
	if b.TryAcquire(11) || b.TryAcquire(-1) {
		t.Fatal("try out of range")
	}
	if b.InUse() != 0 {
		t.Fatal("nothing held after errors")
	}
	for name, f := range map[string]func(){
		"negative": func() { b.Release(-1) },
		"over":     func() { b.Release(1) },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("%s release did not panic", name)
				}
			}()
			f()
		}()
	}
}

func TestBudgetAcquireWaitsAndFIFO(t *testing.T) {
	b := NewBudget(10)
	if err := b.Acquire(context.Background(), 10); err != nil {
		t.Fatal(err)
	}
	var order []int
	var mu sync.Mutex
	var wg sync.WaitGroup
	start := func(id int, n int64) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := b.Acquire(context.Background(), n); err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			order = append(order, id)
			mu.Unlock()
			b.Release(n) // 8+5 > 10: waiter 2 is granted only after this
		}()
		// wait until it is queued
		for {
			b.mu.Lock()
			l := b.waiters.Len()
			b.mu.Unlock()
			if l == id {
				return
			}
			time.Sleep(time.Millisecond)
		}
	}
	start(1, 8) // large first
	start(2, 5)
	if b.TryAcquire(1) {
		t.Fatal("TryAcquire must not overtake waiters")
	}
	b.Release(10)
	wg.Wait()
	if len(order) != 2 || order[0] != 1 || order[1] != 2 {
		t.Fatalf("order %v", order)
	}
	if b.InUse() != 0 {
		t.Fatalf("in use %d", b.InUse())
	}
}

func TestBudgetCancel(t *testing.T) {
	b := NewBudget(10)
	_ = b.Acquire(context.Background(), 10)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- b.Acquire(ctx, 5) }()
	for {
		b.mu.Lock()
		l := b.waiters.Len()
		b.mu.Unlock()
		if l == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("%v", err)
	}
	if b.InUse() != 10 {
		t.Fatal("accounting after cancel")
	}
	b.Release(10)
	if !b.TryAcquire(10) {
		t.Fatal("budget leaked by cancelled waiter")
	}
}

// A cancelled head waiter must unblock the one behind it that now fits.
func TestBudgetCancelHeadWakesNext(t *testing.T) {
	b := NewBudget(10)
	_ = b.Acquire(context.Background(), 6)
	ctx, cancel := context.WithCancel(context.Background())
	head := make(chan error, 1)
	go func() { head <- b.Acquire(ctx, 8) }() // does not fit, heads the queue
	for {
		b.mu.Lock()
		l := b.waiters.Len()
		b.mu.Unlock()
		if l == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	next := make(chan error, 1)
	go func() { next <- b.Acquire(context.Background(), 2) }() // fits, but behind
	for {
		b.mu.Lock()
		l := b.waiters.Len()
		b.mu.Unlock()
		if l == 2 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-head; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := <-next; err != nil {
		t.Fatal(err)
	}
	if b.InUse() != 8 {
		t.Fatalf("in use %d", b.InUse())
	}
}

// Context already done and budget free: Acquire still succeeds (fast path);
// context done with no room returns the error with nothing held.
func TestBudgetCancelledContextNoRoom(t *testing.T) {
	b := NewBudget(1)
	_ = b.Acquire(context.Background(), 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := b.Acquire(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if b.InUse() != 1 {
		t.Fatal("held")
	}
}

// Grant racing with cancel: the grant is kept and accounted exactly.
func TestBudgetGrantCancelRace(t *testing.T) {
	for i := 0; i < 300; i++ {
		b := NewBudget(1)
		_ = b.Acquire(context.Background(), 1)
		ctx, cancel := context.WithCancel(context.Background())
		res := make(chan error, 1)
		go func() { res <- b.Acquire(ctx, 1) }()
		for {
			b.mu.Lock()
			l := b.waiters.Len()
			b.mu.Unlock()
			if l == 1 {
				break
			}
			time.Sleep(time.Microsecond)
		}
		go cancel()
		b.Release(1)
		err := <-res
		want := int64(1)
		if err != nil {
			want = 0
		}
		if b.InUse() != want {
			t.Fatalf("err=%v in use %d", err, b.InUse())
		}
	}
}

func TestBudgetConcurrentExact(t *testing.T) {
	const total = 100
	b := NewBudget(total)
	var cur, peak atomic.Int64
	var wg sync.WaitGroup
	for g := 0; g < 32; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				n := int64((g*7+i)%total + 1)
				if i%3 == 0 {
					if !b.TryAcquire(n) {
						continue
					}
				} else if err := b.Acquire(context.Background(), n); err != nil {
					t.Error(err)
					return
				}
				c := cur.Add(n)
				for {
					p := peak.Load()
					if c <= p || peak.CompareAndSwap(p, c) {
						break
					}
				}
				if c > total {
					t.Errorf("over budget: %d", c)
				}
				cur.Add(-n)
				b.Release(n)
			}
		}(g)
	}
	wg.Wait()
	if b.InUse() != 0 {
		t.Fatalf("leak %d", b.InUse())
	}
}

func TestGate(t *testing.T) {
	g := NewGate(2)
	if !g.TryEnter() || !g.TryEnter() || g.TryEnter() {
		t.Fatal("cap 2")
	}
	if g.Held() != 2 {
		t.Fatal("held")
	}
	g.Leave()
	if !g.TryEnter() {
		t.Fatal("slot freed")
	}
	g.Leave()
	g.Leave()
	if NewGate(0).max != 1 {
		t.Fatal("floor")
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("Leave on empty must panic")
			}
		}()
		g.Leave()
	}()
	// Without per-key cap, TryEnterKey applies the total only.
	if !g.TryEnterKey("a") || !g.TryEnterKey("a") || g.TryEnterKey("a") {
		t.Fatal("plain gate by key")
	}
	g.LeaveKey("a")
	g.LeaveKey("a")
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("LeaveKey on empty must panic")
			}
		}()
		g.LeaveKey("a")
	}()
}

func TestKeyGate(t *testing.T) {
	g := NewKeyGate(3, 2)
	if !g.TryEnterKey("a") || !g.TryEnterKey("a") {
		t.Fatal("a x2")
	}
	if g.TryEnterKey("a") {
		t.Fatal("per-key cap")
	}
	if !g.TryEnterKey("b") {
		t.Fatal("b")
	}
	if g.TryEnterKey("c") {
		t.Fatal("total cap")
	}
	g.LeaveKey("a")
	if !g.TryEnterKey("c") {
		t.Fatal("room after leave")
	}
	g.LeaveKey("a")
	g.LeaveKey("b")
	g.LeaveKey("c")
	if g.Held() != 0 || len(g.keys) != 0 {
		t.Fatalf("leak: held %d keys %d", g.Held(), len(g.keys))
	}
	if NewKeyGate(0, 0).perKey != 1 {
		t.Fatal("floor")
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("LeaveKey of unheld key must panic")
			}
		}()
		g.TryEnterKey("x")
		defer g.LeaveKey("x")
		g.LeaveKey("y")
	}()
}

func TestGateConcurrent(t *testing.T) {
	g := NewKeyGate(10, 3)
	var cur atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := string(rune('a' + i%5))
			for j := 0; j < 500; j++ {
				if g.TryEnterKey(key) {
					if cur.Add(1) > 10 {
						t.Error("over gate")
					}
					cur.Add(-1)
					g.LeaveKey(key)
				}
			}
		}(i)
	}
	wg.Wait()
	if g.Held() != 0 || len(g.keys) != 0 {
		t.Fatal("leak")
	}
}

func TestWithDeadline(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	if err := WithDeadline(c1, 0, func() error { return nil }); !errors.Is(err, ErrBadDeadline) {
		t.Fatal(err)
	}

	// A silent peer: the read inside f times out.
	err := WithDeadline(c1, 30*time.Millisecond, func() error {
		_, err := c1.Read(make([]byte, 1))
		return err
	})
	var ne net.Error
	if !errors.As(err, &ne) || !ne.Timeout() {
		t.Fatalf("want timeout, got %v", err)
	}

	// Deadline cleared afterwards: a later read is not already expired.
	go func() {
		time.Sleep(60 * time.Millisecond)
		c2.Write([]byte("x"))
	}()
	if _, err := c1.Read(make([]byte, 1)); err != nil {
		t.Fatalf("deadline left behind: %v", err)
	}

	// f's error is returned as is.
	want := errors.New("boom")
	if err := WithDeadline(c1, time.Second, func() error { return want }); err != want {
		t.Fatal(err)
	}
}

type badConn struct {
	net.Conn
	setErr   error
	setCalls int
	failAt   int
}

func (b *badConn) SetDeadline(time.Time) error {
	b.setCalls++
	if b.setCalls == b.failAt {
		return b.setErr
	}
	return nil
}

func TestWithDeadlineSetErrors(t *testing.T) {
	boom := errors.New("set")
	// setting fails: f never runs
	ran := false
	bc := &badConn{setErr: boom, failAt: 1}
	if err := WithDeadline(bc, time.Second, func() error { ran = true; return nil }); err != boom || ran {
		t.Fatalf("err=%v ran=%v", err, ran)
	}
	// clearing fails after success: reported
	bc = &badConn{setErr: boom, failAt: 2}
	if err := WithDeadline(bc, time.Second, func() error { return nil }); err != boom {
		t.Fatal(err)
	}
	// clearing fails after f failed: f's error wins
	want := errors.New("f")
	bc = &badConn{setErr: boom, failAt: 2}
	if err := WithDeadline(bc, time.Second, func() error { return want }); err != want {
		t.Fatal(err)
	}
}

// grantingCtx is already cancelled and, when its Done is first read (inside
// Acquire, after the waiter is queued), releases the budget so that the grant
// and the cancellation are both ready: select picks either, and the grant
// branch must keep the grant.
type grantingCtx struct {
	context.Context
	b    *Budget
	once sync.Once
}

func (g *grantingCtx) Done() <-chan struct{} {
	g.once.Do(func() { g.b.Release(1) })
	c := make(chan struct{})
	close(c)
	return c
}

func (g *grantingCtx) Err() error { return context.Canceled }

func TestBudgetGrantedWhileCancelled(t *testing.T) {
	sawGrant, sawCancel := false, false
	for i := 0; i < 200 && !(sawGrant && sawCancel); i++ {
		b := NewBudget(1)
		_ = b.Acquire(context.Background(), 1)
		err := b.Acquire(&grantingCtx{Context: context.Background(), b: b}, 1)
		if err == nil {
			sawGrant = true
			if b.InUse() != 1 {
				t.Fatal("grant not kept")
			}
		} else {
			sawCancel = true
			if b.InUse() != 0 {
				t.Fatal("cancel held budget")
			}
		}
	}
	if !sawGrant {
		t.Fatal("grant branch never taken")
	}
}
