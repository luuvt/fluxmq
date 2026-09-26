// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	corebroker "github.com/absmach/fluxmq/broker"
	"github.com/absmach/fluxmq/message"
)

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("broken pipe") }

func newDeliveryConn(t *testing.T, w io.Writer, consumerFilter string) (*Broker, *Connection, *Channel) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	b := New(nil, logger)
	c := &Connection{
		broker: b, writer: bufio.NewWriterSize(w, 16), frameMax: defaultFrameMax,
		logger: logger, connID: testConn1, channels: make(map[uint16]*Channel),
	}
	ch := newChannel(c, 1)
	ch.consumers[testCtag] = &consumer{tag: testCtag, queue: consumerFilter, mqttFilter: consumerFilter, noAck: true}
	c.channels[1] = ch
	b.connections.Store(c.connID, c)
	return b, c, ch
}

// A queue sender commits a delivery the receiving node reports taken. A message
// no consumer took (closing connection or channel, cancelled consumer, broken
// socket) must be reported, or it is lost with the cursor already past it.
func TestQueueDeliveryNotTakenIsReported(t *testing.T) {
	cases := map[string]func(t *testing.T) *Broker{
		"connection closing": func(t *testing.T) *Broker {
			b, c, _ := newDeliveryConn(t, &bytes.Buffer{}, testTelemetryWild)
			c.closed.Store(true)
			return b
		},
		"channel closed": func(t *testing.T) *Broker {
			b, _, ch := newDeliveryConn(t, &bytes.Buffer{}, testTelemetryWild)
			ch.closed.Store(true)
			return b
		},
		"no matching consumer": func(t *testing.T) *Broker {
			b, _, _ := newDeliveryConn(t, &bytes.Buffer{}, testSensorWild)
			return b
		},
		"socket write fails": func(t *testing.T) *Broker {
			b, _, _ := newDeliveryConn(t, failingWriter{}, testTelemetryWild)
			return b
		},
	}
	for name, setup := range cases {
		t.Run(name, func(t *testing.T) {
			b := setup(t)
			for _, deliver := range []func(context.Context, string, *message.Envelope) error{b.DeliverToClient, b.DeliverToClusterMessage} {
				err := deliver(context.Background(), PrefixedClientID(testConn1), message.New(testTelemetryRoom1, make([]byte, 64)))
				if !corebroker.IsErrClientNotConnected(err) {
					t.Fatalf("want ErrClientNotConnected, got %v", err)
				}
			}
		})
	}
}

func TestQueueDeliveryTakenReportsNil(t *testing.T) {
	b, _, _ := newDeliveryConn(t, &bytes.Buffer{}, testTelemetryWild)
	if err := b.DeliverToClient(context.Background(), PrefixedClientID(testConn1), message.New(testTelemetryRoom1, []byte("x"))); err != nil {
		t.Fatalf("a delivery a consumer took must succeed, got %v", err)
	}
}
