// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package raft

import (
	"context"
	"testing"
	"time"

	"github.com/absmach/fluxmq/queue/types"
	"github.com/stretchr/testify/require"
)

// A nack requeues an entry by backdating it past the visibility timeout, which
// is what makes it due at once. The time has to survive the trip through the
// log rather than be replaced by the moment the leader proposed it.
func TestManagerRequeueKeepsAttemptTime(t *testing.T) {
	ctx := context.Background()
	node := newReplayNode(t, 1<<20)
	node.start()
	t.Cleanup(node.stop)

	require.NoError(t, node.store.CreateQueue(ctx, replicatedQueueConfig()))
	group := types.NewConsumerGroupState(replayQueue, "workers", "#")
	group.Mode = types.GroupModeStream
	group.AutoCommit = false
	require.NoError(t, node.manager.ApplyCreateGroup(ctx, replayQueue, group))
	node.append(1)
	require.NoError(t, node.manager.ApplyAddPending(ctx, replayQueue, "workers", &types.PendingEntry{
		Offset: 0, ConsumerID: "consumer-1", ClaimedAt: time.Now(), DeliveryCount: 1,
	}))

	attemptedAt := time.Now().Add(-time.Hour)
	require.NoError(t, node.manager.ApplyRequeuePending(ctx, replayQueue, "workers", "consumer-1", 0, attemptedAt))

	stored, err := node.store.GetConsumerGroup(ctx, replayQueue, "workers")
	require.NoError(t, err)
	entry, owner := stored.FindPending(0)
	require.Equal(t, "consumer-1", owner)
	require.WithinDuration(t, attemptedAt, entry.ClaimedAt, time.Millisecond,
		"the requeue must make the entry due when the nack asked, not when the leader proposed it")
}
