// Copyright (c) 2026, go-filesystems
// SPDX-License-Identifier: BSD-3-Clause

package inflight

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

// Each runs work(ctx, 0), …, work(ctx, n-1) on at most workers goroutines at
// once and returns when all have returned, or once the run has stopped on an
// error.
//
// It is [Ordered] without the hand-off: for items that write their own output
// (a slot of a slice, a file), nothing is emitted, so no worker waits for the
// caller or for an earlier item, and there is no per-item synchronisation but
// one atomic increment. That is the difference between a bound on goroutines
// and a bound on results: Ordered pays one wake-up per item to keep results
// in order and bounded, Each pays none.
//
// # Errors
//
// Items start in index order, so when item j fails every item below j has
// already started, and the error returned is the lowest-index one among the
// items that failed: the same on every run when the failures do not depend on
// timing. The context given to work is then cancelled and no further item
// starts; an item that fails with [context.Canceled] after that cancellation is
// the run's own stop, not a failure, and is ignored. If ctx is done first, Each
// returns ctx's error. No goroutine outlives the call. n < 0 or workers < 1
// returns [ErrBounds].
func Each(ctx context.Context, n, workers int, work func(ctx context.Context, i int) error) error {
	if n < 0 || workers < 1 {
		return ErrBounds
	}
	if n == 0 {
		return nil
	}
	workers = min(workers, n)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		next    atomic.Int64
		stopped atomic.Bool
		mu      sync.Mutex
		first   = -1
		firstEr error
		wg      sync.WaitGroup
	)
	fail := func(i int, err error) {
		mu.Lock()
		if first < 0 || i < first {
			first, firstEr = i, err
		}
		mu.Unlock()
		stopped.Store(true)
		cancel()
	}
	body := func() {
		for !stopped.Load() && ctx.Err() == nil {
			i := int(next.Add(1) - 1)
			if i >= n {
				return
			}
			if err := work(ctx, i); err != nil {
				if stopped.Load() && errors.Is(err, context.Canceled) {
					continue // our own stop reaching an item below the failure
				}
				fail(i, err)
			}
		}
	}
	for range workers - 1 {
		wg.Go(body)
	}
	body() // the caller's goroutine is the last worker
	wg.Wait()
	mu.Lock()
	defer mu.Unlock()
	if first >= 0 {
		return firstEr
	}
	return ctx.Err() // nil unless the parent was cancelled (cancel is deferred)
}
