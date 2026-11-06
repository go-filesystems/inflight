// Copyright (c) 2026, go-filesystems
// SPDX-License-Identifier: BSD-3-Clause

package inflight

import (
	"io"
	"sync"
)

// Slot is one request's working memory. A connection owns at most Max of them,
// made on demand and reused for its life, so steady-state traffic allocates
// nothing per request. The three buffers are the caller's to use as it likes
// (the request as read, the reply as framed, a scratch buffer); the package
// never looks inside them.
//
// A Slot belongs to the caller from [Conn.Begin] until it is handed to
// [Conn.Go], [Conn.Barrier] or [Conn.Abandon], and to the Conn after that.
type Slot struct {
	In, Out, Data []byte

	sp    Span // the range the request holds in the pool, if held
	held  bool
	done  bool   // the reply is ready and waits for its turn to be written
	reply []byte // what run returned
}

// Run is the work of one request. It returns the reply, framed and ready to
// write (it may alias the slot's buffers), or nil for a request that has none.
// A non-nil error ends the connection: the reply is not written and nothing
// after it is, the way a failed Write does.
type Run func(*Slot) ([]byte, error)

// Conn is the in-flight state of one connection. Make it with [Pool.NewConn].
//
// Begin, Go, Barrier, Abandon and Wait are called by ONE goroutine, the
// connection's reader.
type Conn struct {
	pool  *Pool
	order Order
	w     *replyWriter
	max   int

	smu  sync.Mutex // guards free and made
	scv  *sync.Cond
	free []*Slot
	made int

	wg sync.WaitGroup // requests handed to Go or Barrier, not yet written

	// queue holds the requests in flight in order of arrival, for InOrder;
	// replies are written from its head. qmu guards it.
	qmu      sync.Mutex
	queue    []*Slot
	flushing bool // a goroutine is writing replies from the head of queue
}

// NewConn returns a connection that writes its replies to w in the given
// order and has at most max requests in flight, within the pool's budget. max
// must be at least 1; 1 is the sequential connection. w is never closed.
func (p *Pool) NewConn(w io.Writer, order Order, max int) (*Conn, error) {
	switch {
	case w == nil:
		return nil, ErrWriter
	case order != InOrder && order != AnyOrder:
		return nil, ErrOrder
	case max < 1:
		return nil, ErrLimit
	}
	c := &Conn{pool: p, order: order, w: &replyWriter{w: w}, max: max}
	c.scv = sync.NewCond(&c.smu)
	return c, nil
}

// Err is why the connection can go no further, or nil: the first write error,
// or the first error a [Run] returned.
func (c *Conn) Err() error { return c.w.failure() }

// Begin takes a slot of this connection and then a token of the pool, blocking
// while either is unavailable, and returns the slot to read the request into.
// Call it BEFORE reading the request: that is what makes a bound a bound (see
// the package documentation). If the connection has failed it returns the
// error instead, holding nothing.
//
// Call it only once the next request has begun to arrive (a Peek of one byte
// is enough): an idle connection must hold nothing of the pool's budget.
func (c *Conn) Begin() (*Slot, error) {
	if err := c.Err(); err != nil {
		return nil, err
	}
	sl := c.takeSlot()
	c.pool.tokens <- struct{}{}
	return sl, nil
}

// takeSlot returns a free slot, making one while fewer than max exist.
func (c *Conn) takeSlot() *Slot {
	c.smu.Lock()
	defer c.smu.Unlock()
	for {
		if n := len(c.free); n > 0 {
			sl := c.free[n-1]
			c.free = c.free[:n-1]
			return sl
		}
		if c.made < c.max {
			c.made++
			return &Slot{}
		}
		c.scv.Wait()
	}
}

// release gives back the token and the slot of a request that is over.
func (c *Conn) release(sl *Slot) {
	<-c.pool.tokens
	c.smu.Lock()
	c.free = append(c.free, sl)
	c.smu.Unlock()
	c.scv.Signal()
}

// Abandon gives back a slot from [Conn.Begin] whose request could not be read.
func (c *Conn) Abandon(sl *Slot) { c.release(sl) }

// Go runs the request on a goroutine of its own, beside the others in flight,
// and returns as soon as the request may start: it blocks only while another
// request, of any connection of the pool, holds a conflicting span (see
// [Span]). With max 1 it runs on the caller's goroutine instead.
func (c *Conn) Go(sl *Slot, sp Span, run Run) {
	sl.sp, sl.held = sp, true
	c.pool.rs.acquire(&sl.sp)
	c.enqueue(sl)
	if c.max == 1 {
		c.finish(sl, run)
		return
	}
	go c.finish(sl, run)
}

// Barrier waits until every request before it has been answered, runs this one
// alone, and returns once it has run (its reply is written by the time the
// next Barrier or Wait returns).
func (c *Conn) Barrier(sl *Slot, run Run) {
	c.wg.Wait()
	sl.held = false
	c.enqueue(sl)
	c.finish(sl, run)
}

// Wait blocks until every request handed to Go or Barrier has been answered
// (or has failed on a dead stream). Call it when the reader loop ends, before
// freeing anything a request might still use.
func (c *Conn) Wait() { c.wg.Wait() }

// enqueue puts a request in the connection's order of arrival.
func (c *Conn) enqueue(sl *Slot) {
	sl.done, sl.reply = false, nil
	c.wg.Add(1)
	if c.order == InOrder {
		c.qmu.Lock()
		c.queue = append(c.queue, sl)
		c.qmu.Unlock()
	}
}

// finish runs the request and completes it: its byte range is let go at once
// (a later overlapping request need not wait for the reply to be written), and
// the reply is written, immediately for AnyOrder, or together with every reply
// now at the head of the queue for InOrder. A slot, and the pool's token with
// it, is freed when its reply is written.
func (c *Conn) finish(sl *Slot, run Run) {
	reply, err := run(sl)
	if err != nil {
		c.w.fail(err)
		reply = nil
	}
	if sl.held {
		c.pool.rs.release(&sl.sp)
		sl.held = false
	}
	if c.order == AnyOrder {
		c.write(sl, reply)
		return
	}
	c.qmu.Lock()
	sl.done, sl.reply = true, reply
	if c.flushing {
		// Another goroutine is writing replies and will reach this one.
		c.qmu.Unlock()
		return
	}
	c.flushing = true
	for len(c.queue) > 0 && c.queue[0].done {
		h := c.queue[0]
		c.queue = c.queue[1:]
		// The write may block on a peer that is slow to read; the lock is not
		// held across it, so the reader still enqueues.
		c.qmu.Unlock()
		c.write(h, h.reply)
		c.qmu.Lock()
	}
	c.flushing = false
	c.qmu.Unlock()
}

// write sends one reply and retires its request.
func (c *Conn) write(sl *Slot, reply []byte) {
	if reply != nil {
		c.w.Write(reply)
	}
	sl.reply = nil
	c.release(sl)
	c.wg.Done()
}
