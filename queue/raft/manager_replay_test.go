// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package raft

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/absmach/fluxmq/logstorage"
	"github.com/absmach/fluxmq/message"
	"github.com/absmach/fluxmq/queue/types"
	"github.com/stretchr/testify/require"
)

const replayQueue = "replay-events"

// replayNode is one broker process: a durable queue store and a single-member
// raft group over the same directories, so stopping and starting it again is a
// restart that keeps both the raft log and the records.
type replayNode struct {
	t        *testing.T
	raftDir  string
	storeDir string
	bindAddr string
	config   ManagerConfig

	store   *logstorage.Adapter
	manager *Manager
}

func newReplayNode(t *testing.T, snapshotThreshold uint64) *replayNode {
	t.Helper()

	cfg := DefaultManagerConfig()
	cfg.Enabled = true
	cfg.ReplicationFactor = 1
	cfg.MinInSyncReplicas = 1
	cfg.HeartbeatTimeout = 500 * time.Millisecond
	cfg.ElectionTimeout = 500 * time.Millisecond
	cfg.SnapshotThreshold = snapshotThreshold

	return &replayNode{
		t:        t,
		raftDir:  t.TempDir(),
		storeDir: t.TempDir(),
		bindAddr: freeLocalAddr(t),
		config:   cfg,
	}
}

func freeLocalAddr(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := listener.Addr().String()
	require.NoError(t, listener.Close())
	return addr
}

func (n *replayNode) start() {
	n.t.Helper()

	store, err := logstorage.NewAdapter(n.storeDir, logstorage.DefaultAdapterConfig())
	require.NoError(n.t, err)
	n.store = store

	n.manager = NewManager("node-1", n.bindAddr, n.raftDir, store, store, nil, n.config, nil, discardLogger())
	require.NoError(n.t, n.manager.Start(context.Background()))
	require.NoError(n.t, n.manager.WaitForLeader(context.Background(), 10*time.Second))
	// The barrier returns once every entry before it has been applied, which
	// on a restart is the whole replay.
	require.NoError(n.t, n.manager.raft.Barrier(10*time.Second).Error())
}

func (n *replayNode) stop() {
	n.t.Helper()

	require.NoError(n.t, n.manager.Stop())
	require.NoError(n.t, n.store.Close())
}

func (n *replayNode) append(count int) {
	n.t.Helper()

	for i := range count {
		envelope := newQueuedEnvelope(fmt.Sprintf("msg-%d", i), "$queue/"+replayQueue, []byte(fmt.Sprintf("payload-%d", i)))
		_, err := n.manager.ApplyAppend(context.Background(), replayQueue, envelope)
		message.Release(envelope)
		require.NoError(n.t, err)
	}
}

func (n *replayNode) tail() uint64 {
	n.t.Helper()

	tail, err := n.store.Tail(context.Background(), replayQueue)
	require.NoError(n.t, err)
	return tail
}

// replicatedQueueConfig is a queue declared in the broker config: every node
// creates it locally, so no entry in the raft log creates it.
func replicatedQueueConfig() types.QueueConfig {
	cfg := types.DefaultQueueConfig(replayQueue, "$queue/"+replayQueue+"/#")
	cfg.MaxDepth = 4242
	cfg.Replication = types.ReplicationConfig{
		Enabled:           true,
		ReplicationFactor: 1,
		MinInSyncReplicas: 1,
		Mode:              types.ReplicationSync,
		AckTimeout:        5 * time.Second,
	}
	return cfg
}

// A restart with no snapshot replays the log from its first entry. The queue
// store is durable, so without a reset every replay appended each record again
// and the log grew by its whole history on every restart.
func TestManagerRestartWithoutSnapshotDoesNotDuplicateRecords(t *testing.T) {
	ctx := context.Background()
	node := newReplayNode(t, 1<<20)

	node.start()
	require.NoError(t, node.store.CreateQueue(ctx, replicatedQueueConfig()))
	group := types.NewConsumerGroupState(replayQueue, "workers", "#")
	require.NoError(t, node.manager.ApplyCreateGroup(ctx, replayQueue, group))
	node.append(25)
	require.NoError(t, node.manager.ApplyUpdateCursor(ctx, replayQueue, "workers", 10))
	require.Equal(t, uint64(25), node.tail())
	node.stop()

	for restart := 1; restart <= 2; restart++ {
		node.start()
		require.Equal(t, uint64(25), node.tail(), "restart %d replayed records onto the existing log", restart)

		count, err := node.store.Count(ctx, replayQueue)
		require.NoError(t, err)
		require.Equal(t, uint64(25), count)

		for offset := range uint64(25) {
			stored, err := node.store.Read(ctx, replayQueue, offset)
			require.NoError(t, err)
			require.Equal(t, fmt.Sprintf("payload-%d", offset), string(stored.PayloadBytes()))
			message.Release(stored)
		}

		cfg, err := node.store.GetQueue(ctx, replayQueue)
		require.NoError(t, err)
		require.Equal(t, int64(4242), cfg.MaxDepth, "the declared config must survive the reset")

		restored, err := node.store.GetConsumerGroup(ctx, replayQueue, "workers")
		require.NoError(t, err)
		require.Equal(t, uint64(10), restored.CursorView().Cursor)
		node.stop()
	}
}

// With a snapshot the start-up restore already rebuilds the state, and the
// reset must stay out of its way: records written after the snapshot are
// replayed on top of it exactly once.
func TestManagerRestartWithSnapshotDoesNotDuplicateRecords(t *testing.T) {
	ctx := context.Background()
	node := newReplayNode(t, 1<<20)

	node.start()
	require.NoError(t, node.store.CreateQueue(ctx, replicatedQueueConfig()))
	node.append(10)
	require.NoError(t, node.manager.raft.Snapshot().Error())
	node.append(5)
	require.Equal(t, uint64(15), node.tail())
	node.stop()

	node.start()
	require.Equal(t, uint64(15), node.tail())
	count, err := node.store.Count(ctx, replayQueue)
	require.NoError(t, err)
	require.Equal(t, uint64(15), count)
	node.stop()
}

// Queues no raft group replicates are not the log's to rebuild, and a reset
// that touched them would lose records nothing will bring back.
func TestManagerRestartKeepsUnreplicatedQueues(t *testing.T) {
	ctx := context.Background()
	node := newReplayNode(t, 1<<20)
	const local = "local-only"

	node.start()
	require.NoError(t, node.store.CreateQueue(ctx, types.DefaultQueueConfig(local, local+"/#")))
	envelope := newQueuedEnvelope("local-1", local, []byte("kept"))
	_, err := node.store.Append(ctx, local, envelope)
	require.NoError(t, err)
	node.stop()

	node.start()
	count, err := node.store.Count(ctx, local)
	require.NoError(t, err)
	require.Equal(t, uint64(1), count)
	node.stop()
}
