// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package consumer

import (
	"context"
	"testing"
	"time"

	memlog "github.com/absmach/fluxmq/queue/storage/memory/log"
	"github.com/absmach/fluxmq/queue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeCountingStore counts heartbeat writes (raft entries when replicated).
type writeCountingStore struct {
	groupStore
	writes int
}

func (s *writeCountingStore) RegisterConsumer(ctx context.Context, queueName, groupID string, consumer *types.ConsumerInfo) error {
	s.writes++
	return s.groupStore.RegisterConsumer(ctx, queueName, groupID, consumer)
}

func newHeartbeatFixture(t *testing.T, refresh time.Duration, lastHeartbeat time.Time) (*Manager, *writeCountingStore) {
	t.Helper()
	ctx := context.Background()
	store := &writeCountingStore{groupStore: groupStore{Store: memlog.New()}}
	require.NoError(t, store.CreateQueue(ctx, types.DefaultQueueConfig(testStreamQueue, testStreamQueue+"/#")))
	require.NoError(t, store.CreateConsumerGroup(ctx, types.NewConsumerGroupState(testStreamQueue, testStreamGroup, "")))
	require.NoError(t, store.groupStore.RegisterConsumer(ctx, testStreamQueue, testStreamGroup, &types.ConsumerInfo{
		ID: testStreamConsumer, ClientID: testStreamConsumer, RegisteredAt: lastHeartbeat, LastHeartbeat: lastHeartbeat,
	}))
	return NewManager(store, store, Config{VisibilityTimeout: time.Minute, HeartbeatRefresh: refresh}), store
}

func recordedHeartbeat(t *testing.T, store *writeCountingStore) time.Time {
	t.Helper()
	group, err := store.GetConsumerGroup(context.Background(), testStreamQueue, testStreamGroup)
	require.NoError(t, err)
	info, ok := group.GetConsumer(testStreamConsumer)
	require.True(t, ok)
	return info.LastHeartbeat
}

// Delivery passes touch consumers constantly; a fresh heartbeat is not rewritten.
func TestUpdateHeartbeatSkipsFreshHeartbeat(t *testing.T) {
	fresh := time.Now()
	manager, store := newHeartbeatFixture(t, 5*time.Second, fresh)

	for range 10 {
		require.NoError(t, manager.UpdateHeartbeat(context.Background(), testStreamQueue, testStreamGroup, testStreamConsumer))
	}

	assert.Zero(t, store.writes, "a fresh heartbeat must not be rewritten")
	assert.True(t, fresh.Equal(recordedHeartbeat(t, store)), "a skipped touch must not change the recorded value either")
}

func TestUpdateHeartbeatRewritesStaleHeartbeat(t *testing.T) {
	stale := time.Now().Add(-10 * time.Second)
	manager, store := newHeartbeatFixture(t, 5*time.Second, stale)

	require.NoError(t, manager.UpdateHeartbeat(context.Background(), testStreamQueue, testStreamGroup, testStreamConsumer))
	require.NoError(t, manager.UpdateHeartbeat(context.Background(), testStreamQueue, testStreamGroup, testStreamConsumer))

	assert.Equal(t, 1, store.writes, "once rewritten the heartbeat is fresh again")
	assert.True(t, recordedHeartbeat(t, store).After(stale))
}

func TestUpdateHeartbeatWithoutRefreshWritesEveryTime(t *testing.T) {
	manager, store := newHeartbeatFixture(t, 0, time.Now())

	for range 3 {
		require.NoError(t, manager.UpdateHeartbeat(context.Background(), testStreamQueue, testStreamGroup, testStreamConsumer))
	}

	assert.Equal(t, 3, store.writes)
}

func TestUpdateHeartbeatOfUnknownConsumer(t *testing.T) {
	manager, store := newHeartbeatFixture(t, 5*time.Second, time.Now())

	err := manager.UpdateHeartbeat(context.Background(), testStreamQueue, testStreamGroup, "gone")

	require.ErrorIs(t, err, ErrConsumerNotFound)
	assert.Zero(t, store.writes)
}
