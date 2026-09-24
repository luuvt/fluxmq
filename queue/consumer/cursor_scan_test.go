// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package consumer

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/absmach/fluxmq/message"
	memlog "github.com/absmach/fluxmq/queue/storage/memory/log"
	"github.com/absmach/fluxmq/queue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A group whose filter matches (almost) nothing in a busy log is the shape
// that stalled production: a group/+ consumer over a stream carrying mostly
// client/update records. These tests pin that a cursor scan is bounded, moves
// the cursor past what it examined, and still delivers every match.

const (
	scanQueue    = "scan-queue"
	scanGroup    = "channels"
	scanConsumer = "consumer-1"
	scanNoise    = "$queue/" + scanQueue + "/client/update"
	scanMatch    = "$queue/" + scanQueue + "/group/create"
	scanFilter   = "group/+"
)

// countingStore counts log reads, which is the cost that stalled the
// single-goroutine delivery engine.
type countingStore struct {
	*groupStore
	reads atomic.Int64
}

func (s *countingStore) Read(ctx context.Context, queueName string, offset uint64) (*message.Envelope, error) {
	s.reads.Add(1)
	return s.groupStore.Read(ctx, queueName, offset)
}

type scanFixture struct {
	manager *Manager
	store   *countingStore
}

func newScanFixture(t *testing.T, mode types.ConsumerGroupMode, autoCommit bool) scanFixture {
	t.Helper()

	ctx := context.Background()
	store := &countingStore{groupStore: &groupStore{Store: memlog.New()}}
	require.NoError(t, store.CreateQueue(ctx, types.DefaultQueueConfig(scanQueue, "$queue/"+scanQueue+"/#")))

	group := types.NewConsumerGroupState(scanQueue, scanGroup, scanFilter)
	group.Mode = mode
	group.AutoCommit = autoCommit
	require.NoError(t, store.CreateConsumerGroup(ctx, group))

	manager := NewManager(store, store.groupStore, Config{
		VisibilityTimeout: time.Minute,
		MaxDeliveryCount:  5,
		ClaimBatchSize:    10,
		MaxPELSize:        1000,
	})
	require.NoError(t, manager.RegisterConsumer(ctx, scanQueue, scanGroup, scanConsumer, scanConsumer, ""))

	return scanFixture{manager: manager, store: store}
}

func (f scanFixture) append(t *testing.T, topic string, n int) {
	t.Helper()
	for range n {
		_, err := f.store.Append(context.Background(), scanQueue, message.New(topic, []byte("payload")))
		require.NoError(t, err)
	}
}

func (f scanFixture) cursor(t *testing.T) uint64 {
	t.Helper()
	group, err := f.store.GetConsumerGroup(context.Background(), scanQueue, scanGroup)
	require.NoError(t, err)
	return group.CursorView().Cursor
}

func (f scanFixture) peek(t *testing.T) ([]*message.Envelope, uint64, error) {
	t.Helper()
	return f.manager.PeekBatchStream(context.Background(), scanQueue, scanGroup, scanConsumer, NewFilter(scanFilter), 10)
}

func offsets(msgs []*message.Envelope) []uint64 {
	out := make([]uint64, 0, len(msgs))
	for _, msg := range msgs {
		out = append(out, msg.BrokerMeta.Queue.Offset)
	}
	return out
}

// Before the fix the cursor stayed put and the second peek re-read all 1000
// records; every delivery pass paid for the whole log again.
func TestPeekBatchStreamStepsCursorPastNonMatchingRecords(t *testing.T) {
	f := newScanFixture(t, types.GroupModeStream, true)
	f.append(t, scanNoise, 1000)

	msgs, next, err := f.peek(t)
	assert.ErrorIs(t, err, ErrNoMessages)
	assert.Empty(t, msgs)
	assert.Equal(t, uint64(1000), next)
	assert.Equal(t, uint64(1000), f.cursor(t), "cursor must be persisted past records the filter can never match")

	f.store.reads.Store(0)
	_, _, err = f.peek(t)
	assert.ErrorIs(t, err, ErrNoMessages)
	assert.Zero(t, f.store.reads.Load(), "a caught-up group must not rescan the log")
}

func TestPeekBatchStreamBoundsRecordsExaminedPerCall(t *testing.T) {
	f := newScanFixture(t, types.GroupModeStream, true)
	f.append(t, scanNoise, 2*maxCursorScan+10)
	f.append(t, scanMatch, 1)
	tail := uint64(2*maxCursorScan + 11)

	var got []uint64
	calls := 0
	for f.cursor(t) < tail {
		calls++
		require.LessOrEqual(t, calls, 10, "scan never reached the tail")
		f.store.reads.Store(0)
		msgs, next, err := f.peek(t)
		assert.LessOrEqual(t, f.store.reads.Load(), int64(maxCursorScan))
		if err != nil {
			require.ErrorIs(t, err, ErrNoMessages)
			continue
		}
		got = append(got, offsets(msgs)...)
		releaseMessages(msgs)
		// The delivery engine commits after a successful delivery.
		require.NoError(t, f.manager.CommitStreamCursor(context.Background(), scanQueue, scanGroup, next))
	}

	assert.Equal(t, 3, calls)
	assert.Equal(t, []uint64{tail - 1}, got, "the match past the scanned noise must still be delivered")
}

// Skipping must never swallow a match: the cursor only moves past records that
// were examined and rejected.
func TestPeekBatchStreamDeliversEveryMatchAroundSkippedRecords(t *testing.T) {
	ctx := context.Background()
	f := newScanFixture(t, types.GroupModeStream, true)
	f.append(t, scanNoise, 50)
	f.append(t, scanMatch, 1) // 50
	f.append(t, scanNoise, 10)
	f.append(t, scanMatch, 1) // 61

	msgs, next, err := f.peek(t)
	require.NoError(t, err)
	assert.Equal(t, []uint64{50, 61}, offsets(msgs))
	releaseMessages(msgs)
	require.NoError(t, f.manager.CommitStreamCursor(ctx, scanQueue, scanGroup, next))

	f.append(t, scanNoise, 30)
	_, _, err = f.peek(t)
	require.ErrorIs(t, err, ErrNoMessages)
	assert.Equal(t, uint64(92), f.cursor(t))

	f.append(t, scanMatch, 1) // 92
	msgs, _, err = f.peek(t)
	require.NoError(t, err)
	assert.Equal(t, []uint64{92}, offsets(msgs))
	releaseMessages(msgs)
}

// A peek that found matches but whose delivery then failed must leave the
// cursor where it was, so the same records are offered again.
func TestPeekBatchStreamDoesNotCommitUndeliveredMatches(t *testing.T) {
	f := newScanFixture(t, types.GroupModeStream, true)
	f.append(t, scanNoise, 20)
	f.append(t, scanMatch, 1)

	msgs, _, err := f.peek(t)
	require.NoError(t, err)
	releaseMessages(msgs)
	assert.Zero(t, f.cursor(t))

	msgs, _, err = f.peek(t)
	require.NoError(t, err)
	assert.Equal(t, []uint64{20}, offsets(msgs))
	releaseMessages(msgs)
}

// Retention can truncate past a group's cursor (the cursor-0 groups seen in
// production). The scan starts at the head instead of probing each truncated
// offset.
func TestPeekBatchStreamStartsAtHeadAfterTruncation(t *testing.T) {
	ctx := context.Background()
	f := newScanFixture(t, types.GroupModeStream, true)
	f.append(t, scanNoise, 900)
	f.append(t, scanMatch, 1) // 900
	require.NoError(t, f.store.Truncate(ctx, scanQueue, 895))

	f.store.reads.Store(0)
	msgs, next, err := f.peek(t)
	require.NoError(t, err)
	assert.Equal(t, []uint64{900}, offsets(msgs))
	assert.Equal(t, uint64(901), next)
	assert.Equal(t, int64(6), f.store.reads.Load(), "only retained records are read")
	releaseMessages(msgs)
}

func TestClaimBatchStepsCursorPastNonMatchingRecords(t *testing.T) {
	ctx := context.Background()
	f := newScanFixture(t, types.GroupModeQueue, true)
	f.append(t, scanNoise, 2*maxCursorScan+10)

	for range 3 {
		f.store.reads.Store(0)
		_, err := f.manager.ClaimBatch(ctx, scanQueue, scanGroup, scanConsumer, NewFilter(scanFilter), 10)
		assert.ErrorIs(t, err, ErrNoMessages)
		assert.LessOrEqual(t, f.store.reads.Load(), int64(maxCursorScan))
	}
	assert.Equal(t, uint64(2*maxCursorScan+10), f.cursor(t))

	f.append(t, scanMatch, 1)
	msgs, err := f.manager.ClaimBatch(ctx, scanQueue, scanGroup, scanConsumer, NewFilter(scanFilter), 10)
	require.NoError(t, err)
	assert.Equal(t, []uint64{2*maxCursorScan + 10}, offsets(msgs))
	releaseMessages(msgs)
}

// ClaimBatch reuses one group snapshot across claimFromCursor calls. After a
// claim advanced the cursor, a following no-match scan must not write a
// position computed from an older view.
func TestClaimBatchNoMatchScanNeverMovesCursorBackwards(t *testing.T) {
	ctx := context.Background()
	f := newScanFixture(t, types.GroupModeQueue, true)
	f.append(t, scanMatch, 3)
	f.append(t, scanNoise, 5)

	msgs, err := f.manager.ClaimBatch(ctx, scanQueue, scanGroup, scanConsumer, NewFilter(scanFilter), 10)
	require.NoError(t, err)
	assert.Equal(t, []uint64{0, 1, 2}, offsets(msgs))
	releaseMessages(msgs)
	assert.Equal(t, uint64(8), f.cursor(t))
}

func TestClaimManualStreamStepsCursorPastNonMatchingRecords(t *testing.T) {
	ctx := context.Background()
	f := newScanFixture(t, types.GroupModeStream, false)
	f.append(t, scanNoise, 100)

	_, err := f.manager.ClaimManualStream(ctx, scanQueue, scanGroup, scanConsumer, NewFilter(scanFilter))
	assert.ErrorIs(t, err, ErrNoMessages)
	assert.Equal(t, uint64(100), f.cursor(t))

	f.append(t, scanMatch, 1)
	msg, err := f.manager.ClaimManualStream(ctx, scanQueue, scanGroup, scanConsumer, NewFilter(scanFilter))
	require.NoError(t, err)
	assert.Equal(t, uint64(100), msg.BrokerMeta.Queue.Offset)
	message.Release(msg)
}

// A scan cut short by the budget says so, so the delivery engine reschedules
// the queue instead of waiting a full tick per maxCursorScan records; one that
// reached the tail is a plain ErrNoMessages. Both stay ErrNoMessages for
// callers that only care that nothing was returned.
func TestCursorScanReportsIncompleteScans(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name  string
		mode  types.ConsumerGroupMode
		auto  bool
		claim func(f scanFixture) error
	}{
		{"peek", types.GroupModeStream, true, func(f scanFixture) error { _, _, err := f.peek(t); return err }},
		{"claim-batch", types.GroupModeQueue, true, func(f scanFixture) error {
			_, err := f.manager.ClaimBatch(ctx, scanQueue, scanGroup, scanConsumer, NewFilter(scanFilter), 10)
			return err
		}},
		{"manual-stream", types.GroupModeStream, false, func(f scanFixture) error {
			_, err := f.manager.ClaimManualStream(ctx, scanQueue, scanGroup, scanConsumer, NewFilter(scanFilter))
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newScanFixture(t, tc.mode, tc.auto)
			f.append(t, scanNoise, maxCursorScan+5)

			err := tc.claim(f)
			assert.ErrorIs(t, err, ErrScanIncomplete)
			assert.ErrorIs(t, err, ErrNoMessages)

			err = tc.claim(f)
			assert.ErrorIs(t, err, ErrNoMessages)
			assert.NotErrorIs(t, err, ErrScanIncomplete)
			assert.Equal(t, uint64(maxCursorScan+5), f.cursor(t))
		})
	}
}
