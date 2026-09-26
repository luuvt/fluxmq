// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package raft

import (
	"context"
	"testing"
	"time"

	"github.com/absmach/fluxmq/logstorage"
	"github.com/absmach/fluxmq/queue/storage"
	"github.com/absmach/fluxmq/queue/types"
	"github.com/stretchr/testify/require"
)

const (
	raceOffset   = uint64(7)
	raceNewOwner = "consumer-new"
)

// newTransferredEntryFSM holds raceOffset for raceNewOwner, taken over from
// testOperationConsumerA: the state a replica is in once it has applied a
// redelivery's transfer. A follower that had not applied the transfer yet
// proposes settlements naming testOperationConsumerA.
func newTransferredEntryFSM(t *testing.T) (*LogFSM, *logstorage.Adapter) {
	t.Helper()
	ctx := context.Background()
	fsm, adapter := newAdapterFSM(t)

	require.NoError(t, adapter.CreateQueue(ctx, conformanceQueueConfig()))
	// A manual-commit stream group, the kind that stalled: the adapter serves
	// its pending list from the group state alone, where a queue group's is
	// rebuilt from the offset-keyed store on every read.
	group := types.NewConsumerGroupState(testOperationQueue, testOperationGroup, "jobs/#")
	group.Mode = types.GroupModeStream
	group.AutoCommit = false
	require.NoError(t, adapter.CreateConsumerGroup(ctx, group))
	require.NoError(t, adapter.AddPendingEntry(ctx, testOperationQueue, testOperationGroup, &types.PendingEntry{
		Offset: raceOffset, ConsumerID: testOperationConsumerA, ClaimedAt: conformanceTime, DeliveryCount: 1,
	}))
	require.NoError(t, adapter.TransferPendingEntry(ctx, testOperationQueue, testOperationGroup, raceOffset, testOperationConsumerA, raceNewOwner))

	return fsm, adapter
}

func pendingOwnerOf(t *testing.T, adapter *logstorage.Adapter, offset uint64) (types.PendingEntry, string) {
	t.Helper()
	group, err := adapter.GetConsumerGroup(context.Background(), testOperationQueue, testOperationGroup)
	require.NoError(t, err)
	return group.FindPending(offset)
}

func TestLogFSMRemovePendingNamingPreviousOwnerSettlesRecord(t *testing.T) {
	fsm, adapter := newTransferredEntryFSM(t)

	result := fsm.applyRemovePending(context.Background(), &Operation{
		Type: OpRemovePending, QueueName: testOperationQueue, GroupID: testOperationGroup,
		ConsumerID: testOperationConsumerA, Offset: raceOffset,
	})

	require.NoError(t, result.Error)
	_, owner := pendingOwnerOf(t, adapter, raceOffset)
	require.Empty(t, owner, "an ack naming the owner a lagging follower saw must still settle the record")
	entries, err := adapter.GetPendingEntries(context.Background(), testOperationQueue, testOperationGroup, raceNewOwner)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestLogFSMRequeuePendingNamingPreviousOwnerRequeuesRecord(t *testing.T) {
	fsm, adapter := newTransferredEntryFSM(t)
	attemptedAt := conformanceTime.Add(-time.Hour)

	var result *ApplyResult
	require.NotPanics(t, func() {
		result = fsm.applyRequeuePending(context.Background(), &Operation{
			Type: OpRequeuePending, Timestamp: attemptedAt, QueueName: testOperationQueue, GroupID: testOperationGroup,
			ConsumerID: testOperationConsumerA, Offset: raceOffset,
		})
	})

	require.NoError(t, result.Error)
	entry, owner := pendingOwnerOf(t, adapter, raceOffset)
	require.Equal(t, raceNewOwner, owner, "a nack changes when the record is retried, not who holds it")
	require.True(t, attemptedAt.Equal(entry.ClaimedAt))
}

// Every replica applies the same entry against the same state, so a settlement
// that finds nothing to act on is refused alike everywhere. Stopping the node
// instead stopped all of them, and again on every replay of the entry.
func TestLogFSMSettlementOfMissingEntryIsRefusedNotFatal(t *testing.T) {
	fsm, _ := newTransferredEntryFSM(t)
	ctx := context.Background()
	const missing = raceOffset + 100

	ops := map[string]func() *ApplyResult{
		"requeue": func() *ApplyResult {
			return fsm.applyRequeuePending(ctx, &Operation{
				Type: OpRequeuePending, Timestamp: conformanceTime, QueueName: testOperationQueue, GroupID: testOperationGroup,
				ConsumerID: testOperationConsumerA, Offset: missing,
			})
		},
		"transfer": func() *ApplyResult {
			return fsm.applyTransferPending(ctx, &Operation{
				Type: OpTransferPending, QueueName: testOperationQueue, GroupID: testOperationGroup,
				Offset: missing, FromConsumer: testOperationConsumerA, ToConsumer: testOperationConsumerB,
			})
		},
		"remove": func() *ApplyResult {
			return fsm.applyRemovePending(ctx, &Operation{
				Type: OpRemovePending, QueueName: testOperationQueue, GroupID: testOperationGroup,
				ConsumerID: testOperationConsumerA, Offset: missing,
			})
		},
	}
	for name, apply := range ops {
		var result *ApplyResult
		require.NotPanics(t, func() { result = apply() }, name)
		switch name {
		case "remove":
			require.NoError(t, result.Error, "an already settled record stays settled")
		case "transfer":
			// The adapter's offset-keyed claim tolerates a missing offset; the
			// memory store refuses it. Either way nothing may stop.
			if result.Error != nil {
				require.ErrorIs(t, result.Error, storage.ErrPendingEntryNotFound, name)
			}
		default:
			require.ErrorIs(t, result.Error, storage.ErrPendingEntryNotFound, name)
		}
	}

	_, owner := pendingOwnerOf(t, fsm.groupStore.(*logstorage.Adapter), raceOffset)
	require.Equal(t, raceNewOwner, owner, "a refused settlement leaves other entries alone")
}
