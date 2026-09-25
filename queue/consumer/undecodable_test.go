// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package consumer

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/absmach/fluxmq/message"
	"github.com/absmach/fluxmq/queue/storage"
	"github.com/absmach/fluxmq/queue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Envelope v0 records left at the head of the events log after the v1 upgrade
// stopped every group whose cursor was behind them: the read error was
// returned without moving the cursor, so each pass failed at the same offset.

// undecodableStore fails reads of chosen offsets the way the log adapter
// fails a record it cannot decode.
type undecodableStore struct {
	*countingStore
	bad     map[uint64]bool
	failing error
}

func (s *undecodableStore) Read(ctx context.Context, queueName string, offset uint64) (*message.Envelope, error) {
	if s.failing != nil {
		return nil, s.failing
	}
	if s.bad[offset] {
		return nil, fmt.Errorf("%w: decode queue envelope metadata at offset %d: %w",
			storage.ErrUndecodableRecord, offset, message.ErrUnsupportedVersion)
	}
	return s.countingStore.Read(ctx, queueName, offset)
}

func newUndecodableFixture(t *testing.T, mode types.ConsumerGroupMode, bad ...uint64) (scanFixture, *undecodableStore) {
	t.Helper()
	f := newScanFixture(t, mode, true)
	store := &undecodableStore{countingStore: f.store, bad: map[uint64]bool{}}
	for _, offset := range bad {
		store.bad[offset] = true
	}
	f.manager = NewManager(store, f.store.groupStore, Config{
		VisibilityTimeout: time.Minute,
		MaxDeliveryCount:  5,
		ClaimBatchSize:    10,
		MaxPELSize:        1000,
	})
	require.NoError(t, f.manager.RegisterConsumer(context.Background(), scanQueue, scanGroup, scanConsumer, scanConsumer, ""))
	return f, store
}

func TestPeekBatchStreamStepsPastUndecodableRecords(t *testing.T) {
	f, _ := newUndecodableFixture(t, types.GroupModeStream, 0, 1, 2, 3)
	f.append(t, scanMatch, 6)

	msgs, next, err := f.peek(t)
	require.NoError(t, err)
	assert.Equal(t, []uint64{4, 5}, offsets(msgs))
	assert.Equal(t, uint64(6), next)
	releaseMessages(msgs)
}

// Only undecodable records at the cursor: the cursor must still be persisted
// past them, or the next pass reads them again.
func TestPeekBatchStreamPersistsCursorPastOnlyUndecodableRecords(t *testing.T) {
	f, _ := newUndecodableFixture(t, types.GroupModeStream, 0, 1, 2, 3)
	f.append(t, scanMatch, 4)

	_, _, err := f.peek(t)
	assert.ErrorIs(t, err, ErrNoMessages)
	assert.Equal(t, uint64(4), f.cursor(t))

	f.append(t, scanMatch, 1)
	msgs, _, err := f.peek(t)
	require.NoError(t, err)
	assert.Equal(t, []uint64{4}, offsets(msgs))
	releaseMessages(msgs)
}

// Any other read failure may be transient and must keep the cursor where it
// is, so nothing is skipped because a disk hiccupped.
func TestPeekBatchStreamKeepsCursorOnOtherReadErrors(t *testing.T) {
	f, store := newUndecodableFixture(t, types.GroupModeStream)
	f.append(t, scanMatch, 3)
	store.failing = errors.New("read segment: input/output error")

	_, _, err := f.peek(t)
	require.Error(t, err)
	assert.Zero(t, f.cursor(t))
}

func TestClaimStepsPastUndecodableRecords(t *testing.T) {
	f, _ := newUndecodableFixture(t, types.GroupModeQueue, 0, 1, 2)
	f.append(t, scanMatch, 4)

	msg, err := f.manager.Claim(context.Background(), scanQueue, scanGroup, scanConsumer, NewFilter(scanFilter))
	require.NoError(t, err)
	assert.Equal(t, uint64(3), msg.BrokerMeta.Queue.Offset)
	message.Release(msg)
	assert.Equal(t, uint64(4), f.cursor(t))
}

func TestClaimKeepsCursorOnOtherReadErrors(t *testing.T) {
	f, store := newUndecodableFixture(t, types.GroupModeQueue)
	f.append(t, scanMatch, 3)
	store.failing = errors.New("read segment: input/output error")

	_, err := f.manager.Claim(context.Background(), scanQueue, scanGroup, scanConsumer, NewFilter(scanFilter))
	require.Error(t, err)
	assert.Zero(t, f.cursor(t))
}

// A pending entry claimed before its record became unreadable (a v0 record
// claimed by the old broker) can never be delivered again; it must leave the
// PEL instead of failing every claim of lapsed entries.
func TestClaimPendingBatchDropsUndecodableEntries(t *testing.T) {
	f, store := newUndecodableFixture(t, types.GroupModeQueue)
	f.append(t, scanMatch, 2)
	ctx := context.Background()
	for range 2 {
		msg, err := f.manager.Claim(ctx, scanQueue, scanGroup, scanConsumer, NewFilter(scanFilter))
		require.NoError(t, err)
		message.Release(msg)
	}
	store.bad[0] = true

	msgs, err := f.manager.ClaimPendingBatch(ctx, scanQueue, scanGroup, "consumer-2", 0, 10)
	require.NoError(t, err)
	assert.Equal(t, []uint64{1}, offsets(msgs))
	releaseMessages(msgs)

	pending, err := f.manager.GetPendingCount(ctx, scanQueue, scanGroup)
	require.NoError(t, err)
	assert.Equal(t, 1, pending, "the undecodable entry must leave the PEL")
}

func TestStealWorkDropsUndecodableEntries(t *testing.T) {
	f, store := newUndecodableFixture(t, types.GroupModeQueue)
	f.append(t, scanMatch, 1)
	ctx := context.Background()
	msg, err := f.manager.Claim(ctx, scanQueue, scanGroup, scanConsumer, NewFilter(scanFilter))
	require.NoError(t, err)
	message.Release(msg)
	store.bad[0] = true

	group, err := f.store.GetConsumerGroup(ctx, scanQueue, scanGroup)
	require.NoError(t, err)
	f.manager.config.VisibilityTimeout = 0
	_, err = f.manager.stealWork(ctx, group, "consumer-2", NewFilter(scanFilter))
	assert.ErrorIs(t, err, ErrNoMessages)

	pending, err := f.manager.GetPendingCount(ctx, scanQueue, scanGroup)
	require.NoError(t, err)
	assert.Zero(t, pending)
}
