// Copyright (c) 2026, go-filesystems
// SPDX-License-Identifier: BSD-3-Clause

package inflight

import (
	"context"
	"errors"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestOrderedBounds(t *testing.T) {
	for _, tc := range []struct{ n, workers int }{{-1, 1}, {1, 0}, {1, -3}} {
		err := Ordered(t.Context(), tc.n, tc.workers,
			func(context.Context, int) (int, error) { return 0, nil },
			func(int, int) error { return nil })
		if !errors.Is(err, ErrBounds) {
			t.Errorf("n=%d workers=%d: err = %v, want ErrBounds", tc.n, tc.workers, err)
		}
	}
	called := false
	err := Ordered(t.Context(), 0, 4, func(context.Context, int) (int, error) { called = true; return 0, nil },
		func(int, int) error { called = true; return nil })
	if err != nil || called {
		t.Errorf("n=0: err = %v, called = %v", err, called)
	}
}

// Slow early items must not reorder the results, and no more than workers
// results may exist at once (started and not yet emitted).
func TestOrderedOrderAndBound(t *testing.T) {
	const n = 200
	for _, workers := range []int{1, 2, 3, 8, n, 10 * n} {
		var live, peak atomic.Int64
		next := 0
		err := Ordered(t.Context(), n, workers,
			func(_ context.Context, i int) (int, error) {
				v := live.Add(1)
				for {
					p := peak.Load()
					if v <= p || peak.CompareAndSwap(p, v) {
						break
					}
				}
				if i%7 == 0 {
					time.Sleep(2 * time.Millisecond) // later items overtake this one
				}
				return i * i, nil
			},
			func(i, v int) error {
				if i != next || v != i*i {
					t.Fatalf("workers=%d: emit(%d, %d), want index %d", workers, i, v, next)
				}
				next++
				live.Add(-1)
				return nil
			})
		if err != nil || next != n {
			t.Fatalf("workers=%d: err = %v, emitted %d of %d", workers, err, next, n)
		}
		if p := peak.Load(); p > int64(min(workers, n)) {
			t.Errorf("workers=%d: %d results existed at once", workers, p)
		}
	}
}

// The error returned is the first in index order, not the first in time.
func TestOrderedFirstErrorByIndex(t *testing.T) {
	early, late := errors.New("item 3"), errors.New("item 5")
	for range 20 {
		var emitted []int
		err := Ordered(t.Context(), 10, 10,
			func(_ context.Context, i int) (int, error) {
				switch i {
				case 3:
					time.Sleep(5 * time.Millisecond)
					return 0, early
				case 5:
					return 0, late
				}
				return i, nil
			},
			func(i, _ int) error { emitted = append(emitted, i); return nil })
		if err != early {
			t.Fatalf("err = %v, want %v", err, early)
		}
		if len(emitted) != 3 {
			t.Fatalf("emitted %v, want items 0-2 only", emitted)
		}
	}
}

// An error from emit stops the run; the producer, blocked on a token that is
// never given back, must wake and no goroutine may outlive the call.
func TestOrderedEmitErrorStops(t *testing.T) {
	before := runtime.NumGoroutine()
	stop := errors.New("disk full")
	var started atomic.Int64
	err := Ordered(t.Context(), 1000, 1,
		func(context.Context, int) (int, error) { started.Add(1); return 0, nil },
		func(i, _ int) error {
			if i == 1 {
				return stop
			}
			return nil
		})
	if err != stop {
		t.Fatalf("err = %v, want %v", err, stop)
	}
	if s := started.Load(); s > 3 {
		t.Errorf("%d items started after the failure of item 1 with one worker", s)
	}
	settle(t, before)
}

// A failure cancels the context of the items still running.
func TestOrderedErrorCancelsWork(t *testing.T) {
	boom := errors.New("boom")
	err := Ordered(t.Context(), 2, 2,
		func(ctx context.Context, i int) (int, error) {
			if i == 1 {
				<-ctx.Done() // only the failure of item 0 can end this
				return 0, ctx.Err()
			}
			return 0, boom
		},
		func(int, int) error { return nil })
	if err != boom {
		t.Fatalf("err = %v, want %v", err, boom)
	}
}

// The caller's context ends the run even while a work is stuck: Ordered
// returns its error once the stuck item is released.
func TestOrderedContextDone(t *testing.T) {
	before := runtime.NumGoroutine()
	ctx, cancel := context.WithCancel(t.Context())
	release := make(chan struct{})
	started := make(chan struct{})
	var once sync.Once
	done := make(chan error, 1)
	go func() {
		done <- Ordered(ctx, 4, 2,
			func(_ context.Context, i int) (int, error) {
				once.Do(func() { close(started) })
				<-release // deaf to ctx: the consumer must wake on its own
				return i, nil
			},
			func(int, int) error { return nil })
	}()
	<-started
	cancel()
	time.Sleep(20 * time.Millisecond)
	close(release)
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	settle(t, before)
}

// settle waits for the goroutine count to fall back: none may outlive Ordered.
func settle(t *testing.T, before int) {
	t.Helper()
	for range 200 {
		if runtime.NumGoroutine() <= before {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Errorf("goroutines: %d before, %d after", before, runtime.NumGoroutine())
}

func FuzzOrdered(f *testing.F) {
	f.Add(uint8(10), uint8(3), uint8(255), uint8(255))
	f.Add(uint8(0), uint8(1), uint8(0), uint8(0))
	f.Add(uint8(50), uint8(200), uint8(7), uint8(9))
	f.Fuzz(func(t *testing.T, n, workers, failWork, failEmit uint8) {
		bad := errors.New("bad")
		next := 0
		err := Ordered(t.Context(), int(n), int(workers)%64+1,
			func(_ context.Context, i int) (int, error) {
				if i == int(failWork) {
					return 0, bad
				}
				return i, nil
			},
			func(i, v int) error {
				if i != next || v != i {
					t.Fatalf("emit(%d, %d), want index %d", i, v, next)
				}
				next++
				if i == int(failEmit) {
					return bad
				}
				return nil
			})
		// work(failWork) fails before its emit; emit(failEmit) fails after it
		// has been given the item. The earlier index decides.
		want := int(n)
		switch {
		case int(failWork) < int(n) && int(failWork) <= int(failEmit):
			want = int(failWork)
		case int(failEmit) < int(n):
			want = int(failEmit) + 1
		}
		if failing := int(failWork) < int(n) || int(failEmit) < int(n); failing != (err == bad) {
			t.Fatalf("err = %v, failing = %v", err, failing)
		}
		if next != want {
			t.Fatalf("emitted %d items, want %d (err %v)", next, want, err)
		}
	})
}

// Allocations do not grow with n: the workers and the ring are allocated once
// per call, nothing per item.
func TestOrderedAllocsIndependentOfN(t *testing.T) {
	run := func(n int) float64 {
		return testing.AllocsPerRun(20, func() {
			_ = Ordered(context.Background(), n, 4,
				func(_ context.Context, i int) (int, error) { return i, nil },
				func(int, int) error { return nil })
		})
	}
	small, large := run(8), run(4096)
	if large > small+4 {
		t.Errorf("allocs/call: %v for n=8, %v for n=4096: grows with n", small, large)
	}
}

func BenchmarkOrderedSpin(b *testing.B) {
	for _, items := range []int{32, 128, 1024} {
		b.Run(itoa(items), func(b *testing.B) {
			w := runtime.GOMAXPROCS(0)
			b.ReportAllocs()
			for b.Loop() {
				_ = Ordered(context.Background(), items, w,
					func(_ context.Context, i int) (int, error) {
						x := i
						for range 200000 {
							x = x*1664525 + 1013904223
						}
						return x, nil
					}, func(int, int) error { return nil })
			}
		})
	}
}

func itoa(n int) string { return strconv.Itoa(n) }
