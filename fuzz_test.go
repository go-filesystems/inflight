// Copyright (c) 2026, go-filesystems
// SPDX-License-Identifier: BSD-3-Clause

package inflight

import (
	"io"
	"math"
	"testing"
	"time"
)

// FuzzSpan feeds the span constructors the offsets and lengths a peer chooses.
// Whatever they are: the range is well formed (no wrap), conflict is symmetric
// and agrees with brute-force interval arithmetic on small values.
func FuzzSpan(f *testing.F) {
	f.Add("a", uint64(0), uint64(10), uint8(0), "a", uint64(5), uint64(10), uint8(1))
	f.Add("a", uint64(math.MaxUint64), uint64(math.MaxUint64), uint8(1), "a", uint64(0), uint64(0), uint8(2))
	f.Add("", uint64(1), uint64(0), uint8(2), "b", uint64(1), uint64(1), uint8(0))
	mk := func(kind uint8, key string, off, n uint64) Span {
		switch kind % 3 {
		case 0:
			return ReadRange(key, off, n)
		case 1:
			return WriteRange(key, off, n)
		}
		return WriteAll(key)
	}
	f.Fuzz(func(t *testing.T, k1 string, o1, n1 uint64, kind1 uint8, k2 string, o2, n2 uint64, kind2 uint8) {
		a, b := mk(kind1, k1, o1, n1), mk(kind2, k2, o2, n2)
		if a.hi < a.lo || b.hi < b.lo {
			t.Fatalf("malformed span %+v %+v", a, b)
		}
		if a.conflicts(&b) != b.conflicts(&a) {
			t.Fatalf("conflict is not symmetric: %+v %+v", a, b)
		}
		if !a.write && !b.write && a.conflicts(&b) {
			t.Fatalf("two reads conflict: %+v %+v", a, b)
		}
		if k1 != k2 && a.conflicts(&b) {
			t.Fatalf("different keys conflict: %+v %+v", a, b)
		}
		// Brute force where the arithmetic cannot saturate.
		if o1 < 1<<20 && n1 < 1<<20 && o2 < 1<<20 && n2 < 1<<20 && kind1%3 != 2 && kind2%3 != 2 {
			want := k1 == k2 && (a.write || b.write) && n1 > 0 && n2 > 0 && o1 < o2+n2 && o2 < o1+n1
			if a.conflicts(&b) != want {
				t.Fatalf("conflicts = %v, want %v: %+v %+v", a.conflicts(&b), want, a, b)
			}
		}
	})
}

// FuzzConn decodes a script of requests from the input, runs it through a
// connection whose policy and width the input also picks, and holds the
// package to its promises with the harness's independent witness.
func FuzzConn(f *testing.F) {
	f.Add([]byte{0, 0, 1, 2, 3})
	f.Add([]byte{7, 1, 0, 0, 4, 1, 0, 0, 4, 3, 0, 0, 0, 0, 1, 2, 3, 4, 2, 0, 0, 0})
	f.Add([]byte{4, 0, 0, 0, 8, 1, 0, 0, 8, 4, 0, 0, 0, 1, 1, 4, 8, 2, 1, 0, 0, 0, 1, 4, 8})
	f.Add([]byte{255, 1, 1, 3, 3, 3, 1, 1, 3, 3, 4, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 1 {
			return
		}
		order := InOrder
		if data[0]&1 == 1 {
			order = AnyOrder
		}
		width := int(data[0]>>1)%6 + 1
		var ops []op
		for rest := data[1:]; len(rest) >= 4 && len(ops) < 40; rest = rest[4:] {
			ops = append(ops, op{
				kind:   opKind(rest[0] % 5),
				key:    string('a' + rest[1]%2),
				lo:     uint64(rest[2] % 16),
				n:      uint64(rest[3]%8) + 1,
				jitter: time.Duration(rest[3]%4) * 20 * time.Microsecond,
			})
		}
		m := newMonitor()
		var b lockedBuf
		p, _ := NewPool(1 + int(data[0])%7)
		c, err := p.NewConn(&b, order, width)
		if err != nil {
			t.Fatal(err)
		}
		runScript(t, c, 0, m, ops)
		m.check(t)
		expectReplies(t, b.replies(), ops, order)
		if m.maxActive > width {
			t.Fatalf("%d ran at once, bound %d", m.maxActive, width)
		}
	})
}

// FuzzConnWriteFailure fails the stream after a chosen number of writes: the
// connection must still answer every Begin and Wait without hanging, and
// report the failure.
func FuzzConnWriteFailure(f *testing.F) {
	f.Add(uint8(0), uint8(5), uint8(1))
	f.Add(uint8(3), uint8(9), uint8(4))
	f.Fuzz(func(t *testing.T, after, reqs, width uint8) {
		w := &afterWriter{left: int(after % 8)}
		p, _ := NewPool(8)
		c, err := p.NewConn(w, Order(after%2)+1, int(width%5)+1)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < int(reqs%32); i++ {
			sl, err := c.Begin()
			if err != nil {
				break
			}
			c.Go(sl, WriteRange("a", uint64(i), 1), func(*Slot) ([]byte, error) { return []byte{0}, nil })
		}
		c.Wait()
		if w.failed && c.Err() == nil {
			t.Fatal("a failed write was not reported")
		}
	})
}

type afterWriter struct {
	left   int
	failed bool
}

func (a *afterWriter) Write(p []byte) (int, error) {
	if a.left == 0 {
		a.failed = true
		return 0, io.ErrClosedPipe
	}
	a.left--
	return len(p), nil
}
