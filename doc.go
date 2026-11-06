// Copyright (c) 2026, go-filesystems
// SPDX-License-Identifier: BSD-3-Clause

// Package inflight answers several requests of one connection at once, within
// bounds, and knows nothing of the protocol it serves.
//
// It was extracted from go-filesystems/sftp v0.9.0 (PRs #32 and #34) so that
// the NFS, SMB and SFTP servers share one implementation of the part that is
// easy to get subtly wrong: the bounds, the ordering and the barriers.
//
// # Shape
//
// A server makes one [Pool], which holds what is shared by every connection:
// the server-wide cap on requests in flight and the set of byte ranges being
// worked on. Each connection makes a [Conn] from it, saying where replies go
// and in which [Order] they leave.
//
// The connection's reader loop is
//
//	for {
//		wait until the next request begins (a Peek, or a blocking read of its
//		first byte): an idle connection holds nothing
//		sl, err := c.Begin()        // bounds taken here, before any read
//		read the request into sl.In
//		c.Go(sl, span, run)         // runs beside others; or
//		c.Barrier(sl, run)          // runs alone
//	}
//	c.Wait()
//
// # Bounds
//
// [Conn.Begin] takes one of the connection's slots and then one token of the
// pool's budget, and blocks while either is unavailable. It is called before
// the request is read, so at a bound the reader stops reading and the peer
// waits in the transport's window: nothing has been allocated for a request
// that is refused. A slot and its token are given back when the request's
// reply has been written, so a peer that does not read its replies keeps its
// requests counted. That is the same property as OpenSSH's sftp-server, which
// stops reading input while its output queue cannot take a maximum-size
// message (sftp-server.c process_requests, lines 2064-2066 and 2121), and as
// knfsd's threads and Samba's aio threads, which a stalled peer also holds
// until the connection is closed.
//
// # Ordering
//
// Requests are decoded one at a time by the reader, so a connection's
// requests are always STARTED in arrival order. [Conn.Go] then lets a request
// overlap others, except that two requests on the same key whose ranges
// overlap, with a write among them, run in arrival order (across every
// connection of the pool). [Conn.Barrier] waits until every earlier request
// is answered, runs alone and only then returns, so the reader does not look
// at the next one before it.
//
// Replies leave in the order the requests arrived ([InOrder]: SFTP, whose
// OpenSSH client refuses to resume a transfer whose replies came back
// reordered) or in the order they finish ([AnyOrder]: ONC RPC matches a reply
// to its call by xid, SMB2 by MessageId).
package inflight
