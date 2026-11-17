// Copyright (c) 2026, go-filesystems
// SPDX-License-Identifier: BSD-3-Clause

package inflight

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

// ErrBounds is returned by [Ordered] when n is negative or workers is below 1:
// a bound of zero would wait forever and a negative count has no meaning.
var ErrBounds = errors.New("inflight: Ordered needs n >= 0 and workers >= 1")

// Ordered runs work(ctx, 0), …, work(ctx, n-1) on at most workers goroutines at
// once and hands each result to emit in index order, from the calling
// goroutine, so a caller that writes what emit receives writes it in the same
// order whatever the workers' speeds.
//
// It is the batch counterpart of [Conn]: the same two properties, bounded and
// ordered, for n independent items known up front (the blocks of a file, the
// slides of a deck, the files of a build) instead of requests arriving on a
// connection.
//
// # Bounds
//
// A token is taken before item i starts and given back once emit has returned
// for it, so at most workers results exist at any time, being computed or
// waiting for their turn behind a slower earlier item. Memory is therefore
// bounded by workers results, not by n, and so is the bookkeeping: nothing is
// allocated per item beyond the item's own result. No goroutine outlives the
// call, whichever way it returns.
//
// # Errors
//
// The first error in INDEX order, from work or from emit, stops the run and is
// returned: the context given to work is cancelled, no item after it starts,
// and Ordered returns once the items already running have returned. An error
// is thus the same on every run, not whichever worker failed first. A work
// whose items must not stop one another returns its failure inside T and nil.
// If ctx is done first, Ordered returns ctx's error. emit is never called
// concurrently and never after Ordered has returned.
func Ordered[T any](ctx context.Context, n, workers int, work func(ctx context.Context, i int) (T, error), emit func(i int, v T) error) error {
	if n < 0 || workers < 1 {
		return ErrBounds
	}
	if n == 0 {
		return nil
	}
	workers = min(workers, n) // a bound above n holds nothing more; nor does the ring
	type result struct {
		v   T
		err error
	}
	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	defer wg.Wait()
	defer cancel() // runs first: the producer and the workers stop, then Wait
	tokens := make(chan struct{}, workers)
	// ring[i%workers] carries item i's result: the token of item i+workers is
	// taken only after item i has been emitted, so a slot is never shared.
	ring := make([]chan result, workers)
	for k := range ring {
		ring[k] = make(chan result, 1) // a worker never blocks on its send
	}
	// workers goroutines, started once: each takes a token, then claims the
	// next unclaimed index. Tokens first, index second, so the claimed indices
	// are always the lowest unclaimed ones and the item the caller waits for is
	// never starved of a token by a later one. No goroutine and no allocation
	// per item, and no producer to wake: a worker that finishes an item goes
	// straight to the next one.
	var next atomic.Int64
	for range workers {
		wg.Go(func() {
			for {
				select {
				case tokens <- struct{}{}:
				case <-ctx.Done():
					return
				}
				c := next.Add(1) - 1
				if c >= int64(n) || ctx.Err() != nil {
					return
				}
				i := int(c)
				v, err := work(ctx, i)
				ring[i%workers] <- result{v, err}
			}
		})
	}
	for i := range n {
		var r result
		select {
		case r = <-ring[i%workers]:
		case <-ctx.Done():
			return ctx.Err()
		}
		if r.err != nil {
			return r.err
		}
		if err := emit(i, r.v); err != nil {
			return err
		}
		<-tokens
	}
	return nil
}
