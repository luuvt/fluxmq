// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package amqp

import (
	"net"
	"testing"
	"time"
)

// deadlineSpyConn records every SetReadDeadline/SetWriteDeadline call it
// receives, so the tests can assert deadlineConn touches exactly the calls
// it is supposed to (Write) and none of the ones it must leave alone (Read).
type deadlineSpyConn struct {
	net.Conn
	readDeadlines  []time.Time
	writeDeadlines []time.Time
}

func (s *deadlineSpyConn) SetReadDeadline(t time.Time) error {
	s.readDeadlines = append(s.readDeadlines, t)
	return nil
}

func (s *deadlineSpyConn) SetWriteDeadline(t time.Time) error {
	s.writeDeadlines = append(s.writeDeadlines, t)
	return nil
}

func (s *deadlineSpyConn) Read(b []byte) (int, error)  { return 0, nil }
func (s *deadlineSpyConn) Write(b []byte) (int, error) { return len(b), nil }

func TestDeadlineConnWriteSetsRollingDeadline(t *testing.T) {
	spy := &deadlineSpyConn{}
	dc := &deadlineConn{Conn: spy, writeTimeout: 5 * time.Second}

	before := time.Now()
	if _, err := dc.Write([]byte("payload")); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	after := time.Now()

	if len(spy.writeDeadlines) != 1 {
		t.Fatalf("expected exactly 1 SetWriteDeadline call, got %d", len(spy.writeDeadlines))
	}
	deadline := spy.writeDeadlines[0]
	// The deadline must land within [before+timeout, after+timeout] — a
	// fixed clock, not a session deadline computed once at construction.
	if deadline.Before(before.Add(5*time.Second)) || deadline.After(after.Add(5*time.Second)) {
		t.Fatalf("deadline %v not in expected window around now+5s (before=%v after=%v)", deadline, before, after)
	}

	// A second Write must push the deadline forward again (rolling, not fixed).
	if _, err := dc.Write([]byte("payload2")); err != nil {
		t.Fatalf("second Write failed: %v", err)
	}
	if len(spy.writeDeadlines) != 2 {
		t.Fatalf("expected 2 SetWriteDeadline calls after 2 writes, got %d", len(spy.writeDeadlines))
	}
	if !spy.writeDeadlines[1].After(spy.writeDeadlines[0]) {
		t.Fatalf("second deadline %v should be later than first %v", spy.writeDeadlines[1], spy.writeDeadlines[0])
	}
}

func TestDeadlineConnReadLeavesDeadlineAlone(t *testing.T) {
	spy := &deadlineSpyConn{}
	dc := &deadlineConn{Conn: spy, writeTimeout: 5 * time.Second}

	if _, err := dc.Read(make([]byte, 16)); err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	// deadlineConn must not implement/override Read at all — this call goes
	// straight through to the embedded net.Conn, so SetReadDeadline is never
	// touched. amqp091-go's own heartbeater owns the read deadline; fighting
	// it here is exactly the mistake this fix avoids (see the WriteTimeout
	// field doc on Options).
	if len(spy.readDeadlines) != 0 {
		t.Fatalf("expected SetReadDeadline to never be called, got %d calls", len(spy.readDeadlines))
	}
}
