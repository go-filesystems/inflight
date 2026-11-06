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
- **Ordering**: two requests on one key with overlapping ranges, a write among
  them, run in arrival order, across every connection of the pool.
- **Barriers**: wait for everything in flight, run alone.
- **FSLock**: the go-filesystems/interface concurrency contract as a lock.

Extracted from go-filesystems/sftp v0.9.0. 100% statement coverage, race
detector on, every public entry point fuzzed with its corpus committed.
