# inflight

Bounded, optionally ordered concurrent request dispatch shared by the
go-filesystems NFS, SMB and SFTP servers. It carries no protocol knowledge.

```go
pool, _ := inflight.NewPool(256)                       // server-wide cap
conn, _ := pool.NewConn(w, inflight.InOrder, 64)       // per connection
for {
    br.Peek(1)                                          // idle holds nothing
    sl, err := conn.Begin()                             // bounds taken BEFORE the read
    // read the request into sl.In
    conn.Go(sl, inflight.ReadRange(key, off, n), run)   // runs beside others
    // or conn.Barrier(sl, run)                         // runs alone
}
conn.Wait()
```

- **Bounds** (`Pool`, `Conn.Begin`): a per-connection slot and a server-wide
  token are taken before a request is read; a peer that does not read replies
  keeps them, as with OpenSSH sftp-server, knfsd and Samba.
- **Reply order**: `InOrder` (SFTP, whose OpenSSH client refuses to resume on
  reordered replies) or `AnyOrder` (ONC RPC xid, SMB2 MessageId).
- **Batching** (`Conn.SetBatch`, `InOrder` only, off by default): replies ready
  together at the head of the queue leave in one `Write` of at most the given
  bytes (`DefaultBatch` = 256 KiB). Slots are released only after the write, so
  the pool's budget is unchanged; the one extra buffer is at most the bound.
- **Ordering**: two requests on one key with overlapping ranges, a write among
  them, run in arrival order, across every connection of the pool.
- **Barriers**: wait for everything in flight, run alone.
- **Ordered** (`inflight.Ordered(ctx, n, workers, work, emit)`): the batch
  counterpart for n independent items known up front (blocks, slides, files):
  `work` runs on at most `workers` goroutines, `emit` receives the results in
  index order from the caller's goroutine, at most `workers` results exist at
  once whatever n is, the first error in index order stops the run, and no
  goroutine outlives the call. Memory is bounded by the workers, not by n.
  `workers` goroutines are started once and nothing is allocated per item.
- **Each** (`inflight.Each(ctx, n, workers, work)`): Ordered without the
  hand-off, for items that write their own output. No result is emitted, so no
  worker waits for the caller or for an earlier item: one atomic increment per
  item. Measured on go-compressions/edonr's SumNParallel (16 cores), Ordered's
  per-item wake-ups cost 14 % of throughput; Each is at parity with a private
  pool. The lowest-index error stops the run.
- **FSLock**: the go-filesystems/interface concurrency contract as a lock.

Extracted from go-filesystems/sftp v0.9.0. 100% statement coverage, race
detector on, every public entry point fuzzed with its corpus committed.
