// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package amqp

import (
	"net"
	"time"
)

// deadlineConn wraps a net.Conn and pushes a rolling write deadline forward
// before every Write, so a stalled write fails after timeout instead of
// blocking forever. See the WriteTimeout field doc on Options for why this
// exists.
//
// Read is intentionally left untouched (promoted straight from the embedded
// net.Conn): amqp091-go already manages the read deadline itself —
// Connection.heartbeater resets it to 3x the negotiated heartbeat interval
// on every frame received (connection.go) — and overriding that here would
// just fight a mechanism that already works correctly. It is specifically
// the write side that amqp091-go leaves permanently unbounded on purpose:
// DefaultDial sets a deadline only for the initial handshake, then
// openComplete clears it for good with the comment "RabbitMQ uses TCP flow
// control at this point for pushback so Writes can intentionally block."
// That is a reasonable design for ordinary backpressure (which resolves in
// seconds), but it means a write that is not merely slow but genuinely stuck
// — broker/cluster hiccup, a half-open connection, a silent network
// partition — blocks forever with no bound at all. That single stuck write
// (inside Publish, inside Ack/Nack/Reject — see queue.go/pubsub.go
// withChannelLock — or inside amqp091-go's own heartbeat frame writer, they
// all end up here) then holds whatever higher-level lock called it
// (pubChMu/subChMu) for good, and every later call on the same Client blocks
// on that lock too — the connection looks "open" the entire time, since
// nothing ever surfaces an error to trigger OnConnectionLost/AutoReconnect.
// WriteTimeout turns that indefinite hang into a bounded I/O error instead.
type deadlineConn struct {
	net.Conn
	writeTimeout time.Duration
}

func (d *deadlineConn) Write(b []byte) (int, error) {
	if err := d.Conn.SetWriteDeadline(time.Now().Add(d.writeTimeout)); err != nil {
		return 0, err
	}
	return d.Conn.Write(b)
}
