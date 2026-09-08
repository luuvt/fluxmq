// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/absmach/fluxmq/message"
	memlog "github.com/absmach/fluxmq/queue/storage/memory/log"
	"github.com/absmach/fluxmq/queue/types"
)

// TestCleanupStaleConsumers_FollowerMustNotEvictFromLocalStaleRead is the
// regression test for the "cancelled by server" bug tracked in aiot_cloud's
// TODO-fluxmq-upgrade.md (07-Sep-2026 entry: consumers on the `events` queue
// getting evicted and reconnecting every ~2-4 minutes, reproducing
// identically on a freshly restarted, clean 3-node cluster).
//
// Root cause: cleanupStaleConsumers (queue/manager.go) runs on every node's
// own ConsumerTimeout ticker unconditionally -- there is no leader check.
// It decides staleness via consumerManager.CleanupStaleConsumers, which
// reads the consumer's LastHeartbeat through raftGroupStore.GetConsumerGroup
// -- and that method reads straight from this node's LOCAL base store (see
// its doc comment), unlike every *mutating* group-store method
// (RegisterConsumer, UpdateConsumerGroup, ...), which all go through
// applyOrForward (Raft consensus, forwarded to the leader from a follower).
//
// A follower's local view can legitimately lag behind the leader's for a
// currently-alive consumer's LastHeartbeat -- ordinary Raft log-apply delay,
// not corruption -- which is exactly what this test simulates: the LOCAL
// store already holds a consumer whose LastHeartbeat looks stale, on a node
// that is NOT the leader for this replicated queue. The true, fresh
// heartbeat is presumed already committed on the leader; this follower just
// hasn't applied it yet.
//
// Correct behaviour: only the leader's local view is authoritative, so only
// the leader may decide a consumer is stale (see runRetentionLoop's
// analogous truncation guard, queue/manager.go: "queue %q truncation must
// run on its raft leader" -- cleanupStaleConsumers has no equivalent guard
// today). A follower must leave the decision alone and let the leader's own
// cleanup pass (replicated to followers the normal way) handle it.
//
// This test currently FAILS against unfixed code -- that is the point: it
// documents the bug before the fix lands. It must start passing once
// cleanupStaleConsumers gains a per-queue leader check mirroring
// runRetentionLoop's.
func TestCleanupStaleConsumers_FollowerMustNotEvictFromLocalStaleRead(t *testing.T) {
	const queueName = "events"
	const groupID = "workers"
	const consumerID = "consumer-1"
	const leaderNodeID = "node-2"

	logStore := memlog.New()
	groupStore := newMockGroupStore()

	config := DefaultConfig()
	// Replicated queues require a non-local write policy (see
	// ErrReplicationWritePolicy) -- forward matches this test's follower
	// setup, where writes get forwarded to the leader rather than rejected.
	config.WritePolicy = WritePolicyForward
	manager := NewManager(
		logStore,
		groupStore,
		DeliveryTargetFunc(func(context.Context, string, *message.Envelope) error { return nil }),
		config,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		nil,
	)
	ctx := context.Background()

	// Wire this node up as a FOLLOWER (not leader) for the replicated queue
	// -- exactly the raftGroupStore setup TestRaftGroupStoreRequeueForwardsFromFollower
	// (groupstore_raft_requeue_test.go) uses for the same node role. Set up
	// before CreateQueue: a replicated queue's creation itself requires an
	// enabled coordinator.
	coordinator := &mockQueueCoordinator{
		enabled:           true,
		replicatedByQueue: map[string]bool{queueName: true},
		leaderByQueue:     map[string]bool{queueName: false},
		leaderIDByQueue:   map[string]string{queueName: leaderNodeID},
	}
	forwarder := new(recordingGroupForwarder)
	manager.SetRaftCoordinator(coordinator)
	manager.raftGroupStore.SetForwarder(forwarder)

	queueCfg := types.DefaultQueueConfig(queueName, "$queue/"+queueName+"/#")
	queueCfg.Replication.Enabled = true
	if err := manager.CreateQueue(ctx, queueCfg); err != nil {
		t.Fatalf("CreateQueue failed: %v", err)
	}

	group := types.NewConsumerGroupState(queueName, groupID, "")
	if err := groupStore.CreateConsumerGroup(ctx, group); err != nil {
		t.Fatalf("CreateConsumerGroup failed: %v", err)
	}

	// Simulate this node's LOCAL replica lagging behind the leader: the
	// consumer is actually alive (its true heartbeat, just committed on the
	// leader, is fresh) but THIS node's local base store still only has the
	// older heartbeat it last applied from the Raft log.
	staleLocalHeartbeat := time.Now().Add(-config.ConsumerTimeout - time.Second)
	info := &types.ConsumerInfo{
		ID:            consumerID,
		ClientID:      consumerID,
		RegisteredAt:  staleLocalHeartbeat,
		LastHeartbeat: staleLocalHeartbeat,
	}
	if err := groupStore.RegisterConsumer(ctx, queueName, groupID, info); err != nil {
		t.Fatalf("RegisterConsumer failed: %v", err)
	}

	manager.cleanupStaleConsumers()

	// Proof #1: a follower with only a locally-stale view has no business
	// deciding this consumer is dead -- that call belongs to the leader
	// alone. Forwarding a removal to the leader here proves this follower
	// unilaterally made that call from stale local data.
	if forwarder.op != nil {
		t.Fatalf("BUG: follower node forwarded a stale-consumer removal (op=%+v) for a replicated queue "+
			"it is not the leader of -- only the raft leader's local view is authoritative for staleness; "+
			"a follower's local read can lag behind a consumer's true (fresh) heartbeat on the leader",
			forwarder.op)
	}

	// Proof #2: the consumer must still be registered in this node's own
	// store too -- consumerManager.CleanupStaleConsumers mutates the group
	// object returned by GetConsumerGroup in place (DeleteConsumer) before
	// ever reaching the forward call, so a follower that ran the cleanup at
	// all has already corrupted its local view regardless of whether the
	// forward above succeeds.
	stored, err := groupStore.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		t.Fatalf("GetConsumerGroup failed: %v", err)
	}
	found := false
	stored.ForEachConsumer(func(id string, _ *types.ConsumerInfo) bool {
		if id == consumerID {
			found = true
		}
		return true
	})
	if !found {
		t.Fatal("BUG: consumer was removed from this node's local store -- " +
			"a non-leader node must never evict a consumer based on its own possibly-stale local heartbeat view")
	}
}
