// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"

	"github.com/absmach/fluxmq/message"
	clusterv1 "github.com/absmach/fluxmq/pkg/proto/cluster/v1"
	"github.com/absmach/fluxmq/queue/raft"
	memlog "github.com/absmach/fluxmq/queue/storage/memory/log"
	"github.com/absmach/fluxmq/queue/types"
)

// allOpsForwarder records every group mutation a follower forwards to the
// leader (recordingGroupForwarder keeps only the last one).
type allOpsForwarder struct {
	mu  sync.Mutex
	ops []string
}

func (f *allOpsForwarder) ForwardGroupOp(_ context.Context, _, _ string, op *clusterv1.GroupOperation) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	// Heartbeat touch (RegisterConsumer) is expected from any node; only
	// delivery-state mutations matter here.
	if _, heartbeat := op.GetOperation().(*clusterv1.GroupOperation_RegisterConsumer); heartbeat {
		return nil
	}
	f.ops = append(f.ops, fmt.Sprintf("%T", op.GetOperation()))
	return nil
}

func (f *allOpsForwarder) recorded() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.ops...)
}

// newReplicatedManualStreamNode builds one broker node's queue manager for a
// replicated stream queue with a manual-commit group and one locally
// connected consumer. isLeader selects whether this node leads the queue.
// Two records (offsets 0 and 1) are already in the (replicated) log.
//
// The group state seeded into this node's local store is deliberately the
// state the LEADER held two records ago: cursor 0, nothing pending. On a
// follower that is exactly what ordinary Raft apply lag looks like after the
// leader has delivered offsets 0 and 1 and the consumer has acked both.
func newReplicatedManualStreamNode(t *testing.T, isLeader bool, consumerNode string) (*Manager, *allOpsForwarder, func() []uint64) {
	t.Helper()
	const queueName = "replication"
	const groupID = "channels@aiot_cloud/group/+"
	const consumerID = "consumer-1"

	logStore := memlog.New()
	groupStore := newMockGroupStore()

	var mu sync.Mutex
	var delivered []uint64
	deliverer := DeliveryTargetFunc(func(_ context.Context, _ string, msg *message.Envelope) error {
		mu.Lock()
		defer mu.Unlock()
		delivered = append(delivered, msg.BrokerMeta.Queue.Offset)
		return nil
	})

	config := DefaultConfig()
	config.WritePolicy = WritePolicyForward
	manager := NewManager(logStore, groupStore, deliverer, config,
		slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	ctx := context.Background()

	coordinator := &mockQueueCoordinator{
		enabled:           true,
		replicatedByQueue: map[string]bool{queueName: true},
		leaderByQueue:     map[string]bool{queueName: isLeader},
		leaderIDByQueue:   map[string]string{queueName: "node-leader"},
	}
	forwarder := new(allOpsForwarder)
	manager.SetRaftCoordinator(coordinator)
	manager.raftGroupStore.SetForwarder(forwarder)

	queueCfg := types.DefaultQueueConfig(queueName, "$queue/"+queueName+"/#")
	queueCfg.Type = types.QueueTypeStream
	queueCfg.Replication.Enabled = true
	if err := logStore.CreateQueue(ctx, queueCfg); err != nil {
		t.Fatalf("CreateQueue failed: %v", err)
	}
	for _, id := range []string{"update-name-A", "update-name-B"} {
		env := newQueueEnvelope(id, "$queue/"+queueName+"/aiot_cloud/group/update", []byte(id))
		if _, err := logStore.Append(ctx, queueName, env); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}

	group := types.NewConsumerGroupState(queueName, groupID, "aiot_cloud/group/+")
	group.Mode = types.GroupModeStream
	group.SetAutoCommit(false)
	group.SetConsumer(consumerID, &types.ConsumerInfo{ID: consumerID, ClientID: consumerID, ProxyNodeID: consumerNode})
	if err := groupStore.CreateConsumerGroup(ctx, group); err != nil {
		t.Fatalf("CreateConsumerGroup failed: %v", err)
	}

	return manager, forwarder, func() []uint64 {
		mu.Lock()
		defer mu.Unlock()
		return append([]uint64(nil), delivered...)
	}
}

// Control: the leader's own view is authoritative, so delivering from it is
// correct.
func TestReplicatedManualStream_LeaderDeliversFromItsView(t *testing.T) {
	manager, _, delivered := newReplicatedManualStreamNode(t, true, "")
	manager.delivery.DeliverQueue(context.Background(), "replication")

	got := delivered()
	if len(got) != 1 || got[0] != 0 {
		t.Fatalf("leader: expected exactly offset 0 delivered, got %v", got)
	}
}

// A follower's local group view lags the leader (reads are local, writes are
// forwarded -- see raftGroupStore). If the follower's delivery engine claims
// from that view, it hands the consumer a record the leader has already
// delivered and the consumer has already acked -- a stale redelivery that
// arrives AFTER newer records (offset 1 here), i.e. out of order.
func TestReplicatedManualStream_FollowerMustNotDeliverFromLaggingView(t *testing.T) {
	manager, forwarder, delivered := newReplicatedManualStreamNode(t, false, "")
	manager.delivery.DeliverQueue(context.Background(), "replication")

	if got := delivered(); len(got) > 0 {
		t.Errorf("BUG: follower delivered offsets %v from its lagging local view of a replicated queue "+
			"(leader already delivered+acked 0 and 1 in this scenario) -> duplicate and out-of-order redelivery", got)
	}
	if ops := forwarder.recorded(); len(ops) > 0 {
		t.Errorf("BUG: follower forwarded group mutations to the leader based on its stale view: %v", ops)
	}
}

// The leader-only guard must not starve consumers connected to a follower:
// the leader routes their records to that node like any remote consumer.
func TestReplicatedManualStream_LeaderRoutesToConsumerOnFollower(t *testing.T) {
	manager, _, delivered := newReplicatedManualStreamNode(t, true, "node-follower")
	remote := &mockRemoteRouter{}
	manager.delivery.remote = remote
	manager.delivery.localNodeID = "node-leader"

	manager.delivery.DeliverQueue(context.Background(), "replication")

	if got := delivered(); len(got) != 0 {
		t.Fatalf("leader delivered locally %v to a consumer that lives on node-follower", got)
	}
	remote.mu.Lock()
	defer remote.mu.Unlock()
	if len(remote.routed) != 1 || remote.routed[0].nodeID != "node-follower" || remote.routed[0].msg.BrokerMeta.Queue.Offset != 0 {
		t.Fatalf("expected offset 0 routed to node-follower, got %+v", remote.routed)
	}
}

// settleOnlyReplicator accepts every forwarded group op; the test is about
// what the leader does after applying one, not the apply itself.
type settleOnlyReplicator struct {
	raft.GroupStateReplicator
	removed int
}

func (r *settleOnlyReplicator) ApplyRemovePending(context.Context, string, string, string, uint64) error {
	r.removed++
	return nil
}

// Only the leader delivers a replicated queue, so an ack a follower forwards is
// the leader's cue that a manual group's in-flight slot is free. Without a
// schedule here the next record waited for the leader's periodic sweep.
func TestForwardedSettlementSchedulesLeaderDelivery(t *testing.T) {
	mgr := NewManager(memlog.New(), newMockGroupStore(), nil, DefaultConfig(), slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	replicator := &settleOnlyReplicator{}
	mgr.groupReplicator = replicator

	wire, err := encodeGroupOperation(&raft.Operation{
		Type:       raft.OpRemovePending,
		QueueName:  "replicated",
		GroupID:    "workers",
		ConsumerID: "consumer-1",
		Offset:     7,
	})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if err := mgr.HandleForwardedGroupOp(context.Background(), "replicated", wire); err != nil {
		t.Fatalf("HandleForwardedGroupOp: %v", err)
	}
	if replicator.removed != 1 {
		t.Fatalf("expected the settlement to be applied once, got %d", replicator.removed)
	}

	select {
	case queueName := <-mgr.delivery.schedule.pending():
		if queueName != "replicated" {
			t.Fatalf("scheduled %q, want replicated", queueName)
		}
	default:
		t.Fatal("a forwarded settlement must schedule delivery on the leader")
	}
}

// The pull API reaches the state machine without passing the delivery engine's
// leader check. On a follower it would claim from the same lagging view, so it
// must refuse, retryably and naming the leader as not local; on the leader it
// serves the claim.
func TestReplicatedPullClaimRequiresLeader(t *testing.T) {
	for _, isLeader := range []bool{false, true} {
		manager, forwarder, _ := newReplicatedManualStreamNode(t, isLeader, "")
		ctx := context.Background()

		consumeOutcome, consumeErr := manager.StateMachine().Consume(ctx, ConsumeCommand{
			QueueName: "replication", GroupID: "channels@aiot_cloud/group/+", ConsumerID: "consumer-1", Limit: 10,
		})
		releaseEnvelopes(consumeOutcome.Messages)
		_, claimErr := manager.StateMachine().Claim(ctx, ClaimCommand{
			QueueName: "replication", GroupID: "channels@aiot_cloud/group/+", ConsumerID: "consumer-1", Limit: 10,
		})

		if isLeader {
			if consumeErr != nil {
				t.Fatalf("leader: Consume failed: %v", consumeErr)
			}
			// Claim takes over idle pending entries of a queue-mode group, so
			// on this stream group it fails for its own reason; what matters is
			// that the leader is not the reason.
			if claimErr != nil && ClassifyError(claimErr).Leader == LeaderNotLocal {
				t.Fatalf("leader: Claim refused as not leader: %v", claimErr)
			}
			continue
		}
		for name, err := range map[string]error{"Consume": consumeErr, "Claim": claimErr} {
			failure := ClassifyError(err)
			if err == nil || failure.Leader != LeaderNotLocal || !failure.Retryable {
				t.Fatalf("follower: %s must refuse with a retryable not-local failure, got %v", name, err)
			}
		}
		if ops := forwarder.recorded(); len(ops) > 0 {
			t.Fatalf("follower: a refused claim forwarded group mutations: %v", ops)
		}
	}
}
