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

// TestCleanupStaleConsumers_FollowerMustNotEvictFromLocalStaleRead simulates
// a follower whose local consumer-group view lags the leader's (ordinary
// Raft log-apply delay, not corruption): LastHeartbeat looks stale locally
// even though the true, fresh heartbeat is already committed on the leader.
// Only the leader's view is authoritative, so a follower must never decide
// staleness on its own -- reproduces the "cancelled by server" bug
// (TODO-fluxmq-upgrade.md, 07-Sep-2026).
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

	// Wire this node up as a FOLLOWER for the replicated queue, before
	// CreateQueue since it requires an enabled coordinator.
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

	// This node's local replica lags the leader: the consumer is really
	// alive, but the local heartbeat it last applied looks stale.
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

	// A follower must never decide this consumer is dead on its own.
	if forwarder.op != nil {
		t.Fatalf("BUG: follower forwarded a stale-consumer removal (op=%+v) for a queue it is not the leader of", forwarder.op)
	}

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
		t.Fatal("BUG: a non-leader node must never evict a consumer based on its own possibly-stale local view")
	}
}
