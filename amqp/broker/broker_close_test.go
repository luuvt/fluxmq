// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"bufio"
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestBrokerCloseUnblocksIdleConnectionForCleanup is the regression test for
// a production bug: on a k8s rolling restart, every AMQP 0.9.1 connection
// that is idle (blocked in processFrames' readFrame, the normal steady state
// for a consumer that is only waiting on deliveries -- exactly the shape of
// device-control-client/domains-es-sub/journal-es-sub/etc. connections
// proxied through nginx) never runs its deferred cleanup(), so its
// subscriptions are never removed from the cluster's etcd store.
//
// Root cause: Broker.Close() calls Connection.close() for every registered
// connection, and close() only cancels the connection's context and closes
// closeCh -- it never closes the underlying net.Conn. processFrames' loop
// only polls closeCh via a non-blocking select at the top of each iteration
// before immediately blocking again on a real network read
// (readFrame -> c.reader.Read). A connection idled inside that blocking read
// never observes the closed channel, so run()'s `defer c.cleanup()` never
// fires, and Connection.cleanup() -- the only place that calls
// cluster.RemoveAllSubscriptions/ReleaseSession -- never runs.
//
// ReleaseSession's ownership key is lease-based and self-expires once the
// process is gone, but AddSubscription's entries carry no lease (see
// cluster/etcd.go) -- they are cleaned up lazily, only the next time the
// exact same client ID reconnects (removeOrphanedClusterSubscriptions,
// mqtt/broker/session.go). AMQP 0.9.1 client IDs are generated from
// "<remote-addr>@<sequence>" (Broker.nextConnectionID, monotonic
// connectionSeq) and are never reused across reconnects, so that lazy path
// can never trigger for them -- every graceful broker restart permanently
// orphans the subscriptions of every AMQP client that was idle at shutdown
// time. This reproduces as a stable "skipped subscribers with unknown
// session owner during cross-node routing" count in cluster/etcd.go's
// RoutePublish that never decreases (confirmed live against the aiot-cloud
// k3s cluster: skipped=22 on every publish to an unrelated device's state
// topic, unchanged across 5+ minutes of observation).
func TestBrokerCloseUnblocksIdleConnectionForCleanup(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping blocking-read test in short mode")
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	b := New(nil, logger)
	cl := newTestCluster(false)
	b.SetCluster(cl)

	_, server := newTestConn(t)
	conn := &Connection{
		broker:   b,
		conn:     server,
		reader:   bufio.NewReaderSize(server, 4096),
		writer:   bufio.NewWriter(io.Discard),
		ctx:      context.Background(),
		closeCh:  make(chan struct{}),
		logger:   logger,
		connID:   testConnectionID,
		channels: make(map[uint16]*Channel),
	}
	// registered + connID set is what makes cleanup() call into the cluster
	// -- exactly the state a real connection is in for the rest of its life
	// once the AMQP handshake completes (registerAndValidate).
	conn.registered = true
	b.registerConnection(conn.connID, conn)

	done := make(chan struct{})
	go func() {
		defer close(done)
		defer conn.cleanup()
		conn.processFrames() //nolint:errcheck // idle connection: readFrame blocks until conn.Close(), no frame to inspect
	}()

	// Let the goroutine actually reach the blocking read before shutting
	// the broker down, so this exercises the same idle steady-state a real
	// consumer connection sits in almost all the time.
	time.Sleep(50 * time.Millisecond)

	require.NoError(t, b.Close())

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("BUG: idle connection's processFrames goroutine never returned after Broker.Close() -- " +
			"its subscriptions were never removed from the cluster store")
	}

	require.True(t, cl.hasDeadline("RemoveAllSubscriptions"),
		"Broker.Close() must flush every connection's subscriptions from the cluster store, not just cancel its context")
	require.True(t, cl.hasDeadline("ReleaseSession"),
		"Broker.Close() must release every connection's session ownership")
}
