// Copyright (c) 2026, go-filesystems
// SPDX-License-Identifier: BSD-3-Clause

package inflight

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestEachBounds(t *testing.T) {
	for _, tc := range []struct{ n, workers int }{{-1, 1}, {1, 0}, {1, -3}} {
		err := Each(t.Context(), tc.n, tc.workers, func(context.Context, int) error { return nil })
		if !errors.Is(err, ErrBounds) {
			t.Errorf("n=%d workers=%d: err = %v, want ErrBounds", tc.n, tc.workers, err)
		}
	}
	if err := Each(t.Context(), 0, 4, func(context.Context, int) error { t.Error("called"); return nil }); err != nil {
		t.Errorf("n=0: %v", err)
	}
}

func TestEachRunsEveryItemWithinBound(t *testing.T) {
	const n, workers = 500, 5
	var running, peak atomic.Int32
	seen := make([]atomic.Int32, n)
	err := Each(t.Context(), n, workers, func(_ context.Context, i int) error {
		c := running.Add(1)
		for p := peak.Load(); c > p && !peak.CompareAndSwap(p, c); p = peak.Load() {
		}
		seen[i].Add(1)
		time.Sleep(50 * time.Microsecond)
		running.Add(-1)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := range seen {
		if seen[i].Load() != 1 {
			t.Fatalf("item %d ran %d times", i, seen[i].Load())
		}
	}
	if p := peak.Load(); p > workers {
		t.Errorf("peak concurrency %d > %d", p, workers)
	}
}

func TestEachLowestIndexErrorWins(t *testing.T) {
	low, high := errors.New("low"), errors.New("high")
	for range 50 {
		err := Each(t.Context(), 64, 8, func(ctx context.Context, i int) error {
			switch i {
			case 3:
				time.Sleep(2 * time.Millisecond) // fails later than 40
				return low
			case 40:
				return high
			}
			return nil
		})
		if !errors.Is(err, low) {
			t.Fatalf("err = %v, want the lowest-index failure", err)
		}
	}
}

func TestEachErrorCancelsAndStops(t *testing.T) {
	boom := errors.New("boom")
	var started atomic.Int32
	err := Each(t.Context(), 100000, 4, func(ctx context.Context, i int) error {
		started.Add(1)
		if i == 0 {
			for started.Load() < 4 { // the other workers are inside work
				time.Sleep(time.Microsecond)
			}
			return boom
		}
		<-ctx.Done()
		return ctx.Err() // our own stop: must not mask boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	if s := started.Load(); s > 100 {
		t.Errorf("%d items started after the failure", s)
	}
}

func TestEachContextDone(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	var started atomic.Int32
	err := Each(ctx, 100000, 4, func(ctx context.Context, i int) error {
		if started.Add(1) == 10 {
			cancel()
		}
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestEachAllocsIndependentOfN(t *testing.T) {
	run := func(n int) float64 {
		return testing.AllocsPerRun(20, func() {
			_ = Each(context.Background(), n, 4, func(context.Context, int) error { return nil })
		})
	}
	small, large := run(8), run(4096)
	if large > small+2 {
		t.Errorf("allocs/call: %v for n=8, %v for n=4096: grows with n", small, large)
	}
}
