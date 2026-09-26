// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"connectrpc.com/connect"
	"github.com/absmach/fluxmq/message"
	clusterv1 "github.com/absmach/fluxmq/pkg/proto/cluster/v1"
	queueTypes "github.com/absmach/fluxmq/queue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fencingQueueHandler is a receiving node that has seen raft term seen for
// every queue.
type fencingQueueHandler struct {
	seen      uint64
	delivered []uint64
}

func (h *fencingQueueHandler) EnqueueLocal(context.Context, string, *message.Envelope) error {
	return nil
}

func (h *fencingQueueHandler) DeliverQueueMessage(_ context.Context, _ string, msg *message.Envelope) error {
	h.delivered = append(h.delivered, msg.BrokerMeta.Queue.Offset)
	message.Release(msg)
	return nil
}

func (h *fencingQueueHandler) HandleQueuePublish(context.Context, *message.Envelope, queueTypes.PublishMode, []string) error {
	return nil
}

func (h *fencingQueueHandler) HandleForwardedGroupOp(context.Context, string, *clusterv1.GroupOperation) error {
	return nil
}

func (h *fencingQueueHandler) CheckQueueDeliveryTerm(queueName string, leaderTerm uint64) error {
	if leaderTerm < h.seen {
		return fmt.Errorf("%w: %s term %d < %d", ErrStaleLeaderDelivery, queueName, leaderTerm, h.seen)
	}
	return nil
}

func stampedQueueWire(t *testing.T, offset, leaderTerm uint64) *clusterv1.RouteQueueMessageRequest {
	t.Helper()
	envelope := queueTestEnvelope("$queue/m", "")
	defer message.Release(envelope)
	envelope.BrokerMeta.Queue.Offset = offset
	wire, err := encodeRouteQueueMessage("consumer", envelope, leaderTerm)
	require.NoError(t, err)
	return wire
}

// A leader that was paused keeps delivering from its own view when it resumes,
// and deliveries it had in flight arrive late; the new leader delivers the same
// records. The node the consumer is on voted in the newer term, so it refuses
// what the deposed leader stamped and accepts the rest.
func TestRouteQueueBatchRefusesDeliveriesFromDeposedLeader(t *testing.T) {
	handler := &fencingQueueHandler{seen: 6}
	tr := newTestTransport(testNodeA, nil)
	tr.SetQueueHandler(handler)

	resp, err := tr.RouteQueueBatch(context.Background(), connect.NewRequest(&clusterv1.RouteQueueBatchRequest{
		Messages: []*clusterv1.RouteQueueMessageRequest{
			stampedQueueWire(t, 10, 0), // unreplicated queue: not fenced
			stampedQueueWire(t, 11, 5), // deposed leader
			stampedQueueWire(t, 12, 6), // current leader
		},
	}))

	require.NoError(t, err)
	assert.Equal(t, []uint64{10, 12}, handler.delivered)
	require.Len(t, resp.Msg.Failures, 1)
	assert.Equal(t, uint32(1), resp.Msg.Failures[0].Index)
	assert.True(t, resp.Msg.Failures[0].StaleLeader)
	assert.False(t, resp.Msg.Failures[0].ClientNotConnected, "the consumer is there; it must not be evicted")
}

func TestRouteQueueMessageRefusesDeliveryFromDeposedLeader(t *testing.T) {
	handler := &fencingQueueHandler{seen: 6}
	tr := newTestTransport(testNodeA, nil)
	tr.SetQueueHandler(handler)

	resp, err := tr.RouteQueueMessage(context.Background(), connect.NewRequest(stampedQueueWire(t, 11, 5)))

	require.NoError(t, err)
	assert.False(t, resp.Msg.Success)
	assert.Empty(t, handler.delivered)
}

// The sender stamps each delivery with its term, and stops at once when every
// delivery is refused as stale: it was deposed, and no retry can change that.
func TestSendRouteQueueBatchStampsTermAndStopsWhenDeposed(t *testing.T) {
	calls := 0
	var stamped []uint64
	mock := &mockBrokerClient{
		routeQueueBatchFn: func(_ context.Context, req *connect.Request[clusterv1.RouteQueueBatchRequest]) (*connect.Response[clusterv1.RouteQueueBatchResponse], error) {
			calls++
			for _, wire := range req.Msg.Messages {
				stamped = append(stamped, wire.LeaderTerm)
			}
			return connect.NewResponse(&clusterv1.RouteQueueBatchResponse{
				Success: false,
				Error:   testAlwaysFails,
				Failures: []*clusterv1.RouteQueueBatchError{
					{Index: 0, ClientId: testWorkerA, QueueName: testOrders, Error: "stale", StaleLeader: true},
				},
			}), nil
		},
	}
	tr := newTestTransport(testNodeA, mock)

	delivery := newQueueDelivery(testWorkerA, testOrders, "m1", "1")
	delivery.LeaderTerm = 5
	err := tr.SendRouteQueueBatch(context.Background(), testNodeA, []QueueDelivery{delivery})

	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrStaleLeaderDelivery), "got %v", err)
	assert.Equal(t, 1, calls, "one round, not the whole partial retry budget")
	assert.Equal(t, []uint64{5}, stamped)
}
