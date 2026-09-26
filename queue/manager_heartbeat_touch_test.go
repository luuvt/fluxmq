// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	memlog "github.com/absmach/fluxmq/queue/storage/memory/log"
	"github.com/absmach/fluxmq/queue/types"
)

// TestHeartbeatTouchLoop_KeepsConnectedConsumerAliveWithoutDeliveryActivity
// seeds a consumer with an already-stale LastHeartbeat on a client the
// Deliverer reports as connected, runs the heartbeat-touch pass directly
// (not via its ticker), and asserts cleanupStaleConsumers no longer
// considers it stale -- reproduces the "cancelled by server" incidents on a
// low-traffic queue (TODO-fluxmq-upgrade.md).
func TestHeartbeatTouchLoop_KeepsConnectedConsumerAliveWithoutDeliveryActivity(t *testing.T) {
	logStore := memlog.New()
	groupStore := newMockGroupStore()

	deliverer := &targetCheckingDeliverer{targets: map[string]bool{
		"client-connected":    true,
		"client-disconnected": false,
	}}

	config := DefaultConfig()
	config.ConsumerTimeout = 100 * time.Millisecond
	manager := NewManager(logStore, groupStore, deliverer, config,
		slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	ctx := context.Background()

	const queueName = "events"
	queueCfg := types.DefaultQueueConfig(queueName, "$queue/"+queueName+"/#")
	if err := manager.CreateQueue(ctx, queueCfg); err != nil {
		t.Fatalf("CreateQueue failed: %v", err)
	}

	group := types.NewConsumerGroupState(queueName, "workers", "")
	if err := groupStore.CreateConsumerGroup(ctx, group); err != nil {
		t.Fatalf("CreateConsumerGroup failed: %v", err)
	}

	// Both consumers start with a LastHeartbeat already older than
	// ConsumerTimeout -- as if their delivery loop hasn't run since long
	// before this test began, exactly the low-traffic-group scenario.
	staleHeartbeat := time.Now().Add(-time.Hour)
	for _, info := range []*types.ConsumerInfo{
		{ID: "connected-consumer", ClientID: "client-connected", RegisteredAt: staleHeartbeat, LastHeartbeat: staleHeartbeat},
		{ID: "disconnected-consumer", ClientID: "client-disconnected", RegisteredAt: staleHeartbeat, LastHeartbeat: staleHeartbeat},
	} {
		if err := groupStore.RegisterConsumer(ctx, queueName, "workers", info); err != nil {
			t.Fatalf("RegisterConsumer(%s) failed: %v", info.ID, err)
		}
	}

	// The heartbeat-touch pass must only refresh the consumer whose client
	// is confirmed connected -- it must not blindly touch every consumer,
	// or a genuinely dead one would never age out.
	manager.touchLiveConsumerHeartbeats()

	removed, err := manager.consumerManager.CleanupStaleConsumers(ctx, queueName, "workers", config.ConsumerTimeout)
	if err != nil {
		t.Fatalf("CleanupStaleConsumers failed: %v", err)
	}

	if len(removed) != 1 || removed[0] != "disconnected-consumer" {
		t.Fatalf("expected exactly [disconnected-consumer] to be reaped, got %v", removed)
	}

	stored, err := groupStore.GetConsumerGroup(ctx, queueName, "workers")
	if err != nil {
		t.Fatalf("GetConsumerGroup failed: %v", err)
	}
	if _, ok := stored.GetConsumer("connected-consumer"); !ok {
		t.Fatal("connected-consumer must still be registered -- the heartbeat touch loop should have kept it alive despite no delivery activity")
	}
	if _, ok := stored.GetConsumer("disconnected-consumer"); ok {
		t.Fatal("disconnected-consumer must have been reaped -- it was never confirmed connected, so the touch loop must not have kept it alive")
	}
}

// TestTouchLiveLocalConsumers_SkipsRemoteConsumer guards against the touch
// loop blindly asserting liveness for a consumer proxied to another node --
// only that node's own local connection registry can confirm it, so this
// node touching it would be an unverified assumption, not a confirmed one.
func TestTouchLiveLocalConsumers_SkipsRemoteConsumer(t *testing.T) {
	deliverer := &checkingDeliverer{connected: map[string]bool{"client-remote": true}}
	// remote != nil gives the engine a non-empty localNodeID ("node-1", see
	// newTestEngine) -- setting the consumer's ProxyNodeID to a DIFFERENT
	// node is what makes isRemoteConsumer true below.
	engine, _, groupStore := newTestEngine(t, deliverer, &mockRemoteRouter{})
	ctx := context.Background()

	const queueName = "events"
	group := types.NewConsumerGroupState(queueName, "workers", "")
	if err := groupStore.CreateConsumerGroup(ctx, group); err != nil {
		t.Fatalf("CreateConsumerGroup failed: %v", err)
	}

	staleHeartbeat := time.Now().Add(-time.Hour)
	info := &types.ConsumerInfo{
		ID: "remote-consumer", ClientID: "client-remote", ProxyNodeID: "other-node",
		RegisteredAt: staleHeartbeat, LastHeartbeat: staleHeartbeat,
	}
	if err := groupStore.RegisterConsumer(ctx, queueName, "workers", info); err != nil {
		t.Fatalf("RegisterConsumer failed: %v", err)
	}

	engine.touchLiveLocalConsumers(ctx, queueName, group)

	stored, err := groupStore.GetConsumerGroup(ctx, queueName, "workers")
	if err != nil {
		t.Fatalf("GetConsumerGroup failed: %v", err)
	}
	consumerInfo, ok := stored.GetConsumer("remote-consumer")
	if !ok {
		t.Fatal("remote-consumer should still be registered")
	}
	if !consumerInfo.LastHeartbeat.Equal(staleHeartbeat) {
		t.Fatalf("remote-consumer's heartbeat must be untouched by this node (owned by %q), got %v, want unchanged %v",
			info.ProxyNodeID, consumerInfo.LastHeartbeat, staleHeartbeat)
	}
}

func TestHeartbeatRefreshStaysWellInsideConsumerTimeout(t *testing.T) {
	for _, tc := range []struct {
		interval, timeout, want time.Duration
	}{
		{10 * time.Second, 2 * time.Minute, 5 * time.Second},
		{10 * time.Second, 100 * time.Millisecond, 25 * time.Millisecond},
		{0, 2 * time.Minute, 30 * time.Second},
		{10 * time.Second, 0, 5 * time.Second},
	} {
		if got := heartbeatRefresh(tc.interval, tc.timeout); got != tc.want {
			t.Fatalf("heartbeatRefresh(%v, %v) = %v, want %v", tc.interval, tc.timeout, got, tc.want)
		}
	}
}
