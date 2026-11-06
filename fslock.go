// Copyright (c) 2026, go-filesystems
// SPDX-License-Identifier: BSD-3-Clause

package inflight

import "sync"

// FSLock is the driver contract made executable, for a server whose backing
// store is a go-filesystems/interface Filesystem.
//
// That module documents exactly two things as safe for concurrent use:
// File.ReadAt, and WritableFile.WriteAt on non-overlapping ranges (interface
// v0.5.0). It documents nothing about concurrent path-based calls, Truncate
// or Sync. So:
//
//   - File.ReadAt and WritableFile.WriteAt hold the lock SHARED (LockIO): any
//     number run together; keeping their ranges apart is [Conn.Go]'s job.
//   - Every other driver call holds it EXCLUSIVELY (Lock), and so never
//     overlaps any File I/O or any other call.
//   - Read-only path calls (Stat, ListDir, ReadFile, ReadLink) hold it shared
//     too (LockPath), but only if the embedder asserted, with
//     NewFSLock(true), that the driver allows it: right for a host directory,
//     wrong for a driver with a shared cursor or cache.
type FSLock struct {
	mu         sync.RWMutex
	pathShared bool
}

// NewFSLock returns the lock; pathOpsConcurrent is the embedder's assertion
// that read-only path calls may run beside each other and beside File I/O.
func NewFSLock(pathOpsConcurrent bool) *FSLock { return &FSLock{pathShared: pathOpsConcurrent} }

// LockIO and UnlockIO bracket File.ReadAt and WritableFile.WriteAt.
func (l *FSLock) LockIO()   { l.mu.RLock() }
func (l *FSLock) UnlockIO() { l.mu.RUnlock() }

// Lock and Unlock bracket every driver call that is not File I/O or a
// read-only path call.
func (l *FSLock) Lock()   { l.mu.Lock() }
func (l *FSLock) Unlock() { l.mu.Unlock() }

// LockPath and UnlockPath bracket a read-only path call: shared if the
// embedder asserted it, exclusive otherwise.
func (l *FSLock) LockPath() {
	if l.pathShared {
		l.mu.RLock()
	} else {
		l.mu.Lock()
	}
}

func (l *FSLock) UnlockPath() {
	if l.pathShared {
		l.mu.RUnlock()
	} else {
		l.mu.Unlock()
	}
}
