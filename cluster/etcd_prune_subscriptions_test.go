// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/absmach/fluxmq/storage"
	"github.com/stretchr/testify/require"
)

// TestPruneOrphanedSubscriptions_RemovesEntryWithNoLiveOwner is the
// regression test for the production incident tracked in aiot_cloud's
// TODO-fluxmq-upgrade.md (2026-09-08): RoutePublish's cross-node routing
// permanently drops messages for a topic once a subscription entry
// outlives its session. AddSubscription's entries carry no lease (unlike
// AcquireSession's owner key), and the only existing cleanup --
// removeOrphanedClusterSubscriptions (mqtt/broker/session.go),
// claimOrphanedSessionState's helper -- only fires the next time the EXACT
// SAME client ID reconnects. AMQP 0.9.1 client IDs are
// "<remote-addr>@<sequence>" (a monotonic per-process counter, see
// amqp/broker/broker.go's nextConnectionID) and are never reused, so that
// lazy path can never reach them: once orphaned (an ungraceful disconnect,
// or -- before this fork's Broker.Close() fix -- any graceful broker
// restart), the entry is permanently stuck, and RoutePublish logs "skipped
// subscribers with unknown session owner" for it on every single matching
// publish, forever. Confirmed live: skipped=22, stable and climbing, on a
// production cluster whose broker had already been restarted with the
// Close() fix -- the fix stops NEW leaks, it does not clean up the ones
// already sitting in etcd from before it existed.
func TestPruneOrphanedSubscriptions_RemovesEntryWithNoLiveOwner(t *testing.T) {
	c := newSingleNodeEtcdCluster(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// orphaned-client has a subscription but NO session owner key -- exactly
	// the state left behind by a dead AMQP connection whose ephemeral ID
	// will never reconnect to trigger the lazy claim-on-reconnect cleanup.
	require.NoError(t, c.AddSubscription(ctx, "orphaned-client", "m/#", 1, storage.SubscribeOptions{}))

	// live-client has both a subscription AND a live (leased) session owner
	// -- must survive the prune untouched.
	require.NoError(t, c.AcquireSession(ctx, "live-client", c.nodeID))
	require.NoError(t, c.AddSubscription(ctx, "live-client", "m/#", 1, storage.SubscribeOptions{}))

	c.pruneOrphanedSubscriptions()

	orphanedSubs, err := c.GetSubscriptionsForClient(ctx, "orphaned-client")
	require.NoError(t, err)
	require.Empty(t, orphanedSubs, "BUG: orphaned-client's subscription (no live owner) must be pruned")

	liveSubs, err := c.GetSubscriptionsForClient(ctx, "live-client")
	require.NoError(t, err)
	require.Len(t, liveSubs, 1, "live-client's subscription (live owner) must survive the prune")
}

// TestPruneOrphanedSubscriptions_NoSubscriptionsIsNoop guards against the
// prune pass erroring or panicking when there is nothing to prune -- it
// runs unconditionally on every reconcile tick (see reconcileSessionOwnerCache),
// so a clean cluster with zero subscriptions must be a true no-op.
func TestPruneOrphanedSubscriptions_NoSubscriptionsIsNoop(t *testing.T) {
	c := newSingleNodeEtcdCluster(t)
	c.pruneOrphanedSubscriptions()
}
