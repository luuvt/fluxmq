// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/absmach/fluxmq/cluster"
)

// fencingCoordinator is a raft coordinator that reports a term and can be told
// whether a quorum still confirms this node's leadership.
type fencingCoordinator struct {
	*mockQueueCoordinator
	term      uint64
	verifyErr error
	calls     []string
}

func (c *fencingCoordinator) IsLeaderForQueue(queueName string) bool {
	c.calls = append(c.calls, "leader")
	return c.mockQueueCoordinator.IsLeaderForQueue(queueName)
}

func (c *fencingCoordinator) LeaderTermForQueue(string) (uint64, bool) {
	c.calls = append(c.calls, "term")
	return c.term, true
}

func (c *fencingCoordinator) VerifyLeaderForQueue(context.Context, string) error {
	c.calls = append(c.calls, "verify")
	return c.verifyErr
}

// termRecordingRouter records the term each batched delivery carries, and
// whether anything fell back to the unstamped single-message path.
type termRecordingRouter struct {
	mockRemoteRouter
	batchErrs []error
	terms     []uint64
	batches   int
}

func (r *termRecordingRouter) RouteQueueBatch(_ context.Context, _ string, deliveries []cluster.QueueDelivery) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, delivery := range deliveries {
		r.terms = append(r.terms, delivery.LeaderTerm)
	}
	r.batches++
	if len(r.batchErrs) >= r.batches {
		return r.batchErrs[r.batches-1]
	}
	return nil
}

func newFencedLeader(t *testing.T, consumerNode string, term uint64) (*Manager, *fencingCoordinator, func() []uint64) {
	t.Helper()
	manager, _, delivered := newReplicatedManualStreamNode(t, true, consumerNode)
	coordinator := &fencingCoordinator{
		mockQueueCoordinator: &mockQueueCoordinator{
			enabled:           true,
			replicatedByQueue: map[string]bool{"replication": true},
			leaderByQueue:     map[string]bool{"replication": true},
			leaderIDByQueue:   map[string]string{"replication": "node-leader"},
		},
		term: term,
	}
	manager.SetRaftCoordinator(coordinator)
	manager.delivery.localNodeID = "node-leader"
	return manager, coordinator, delivered
}

// Consumers on other nodes refuse a delivery stamped with a term older than the
// one they have seen, so every delivery of a replicated queue carries the term
// under which this node led it.
func TestReplicatedDeliveryCarriesLeaderTerm(t *testing.T) {
	manager, _, _ := newFencedLeader(t, "node-follower", 7)
	router := &termRecordingRouter{}
	manager.delivery.remote = router

	manager.delivery.DeliverQueue(context.Background(), "replication")

	if len(router.terms) != 1 || router.terms[0] != 7 {
		t.Fatalf("expected one delivery stamped with term 7, got %v", router.terms)
	}
}

// A node that has already heard of a newer term may still report itself leader
// for an instant. Reading the term first means such a pass carries the term it
// actually led, which its receivers refuse, never the newer one.
func TestReplicatedDeliveryReadsTermBeforeLeadership(t *testing.T) {
	manager, coordinator, _ := newFencedLeader(t, "node-follower", 7)
	manager.delivery.remote = &termRecordingRouter{}

	manager.delivery.DeliverQueue(context.Background(), "replication")

	if len(coordinator.calls) < 2 || coordinator.calls[0] != "term" || coordinator.calls[1] != "leader" {
		t.Fatalf("the term must be read before the leadership check, got %v", coordinator.calls)
	}
}

// Deliveries to this node's own consumers pass no receiver that could refuse a
// deposed leader, so a quorum has to confirm leadership before the pass.
func TestReplicatedDeliveryWaitsForConfirmedLeadership(t *testing.T) {
	manager, coordinator, delivered := newFencedLeader(t, "", 7)
	coordinator.verifyErr = errors.New("leadership lost")

	manager.delivery.DeliverQueue(context.Background(), "replication")

	if got := delivered(); len(got) != 0 {
		t.Fatalf("an unconfirmed leader delivered %v", got)
	}

	coordinator.verifyErr = nil
	manager.delivery.DeliverQueue(context.Background(), "replication")
	if got := delivered(); len(got) != 1 {
		t.Fatalf("a confirmed leader must deliver, got %v", got)
	}
}

// A refusal as stale means this node was deposed: the new leader delivers the
// record, so it must not be retried by any path, least of all the unstamped
// single-message RPC that no receiver can fence.
func TestStaleLeaderRefusalIsNotRetriedUnfenced(t *testing.T) {
	manager, _, _ := newFencedLeader(t, "node-follower", 7)
	router := &termRecordingRouter{batchErrs: []error{fmt.Errorf("%w: refused", cluster.ErrStaleLeaderDelivery)}}
	manager.delivery.remote = router

	manager.delivery.DeliverQueue(context.Background(), "replication")

	if router.batches != 1 {
		t.Fatalf("expected a single batch attempt, got %d", router.batches)
	}
	if len(router.routed) != 0 {
		t.Fatalf("a stale refusal fell back to the unfenced single-message path: %+v", router.routed)
	}
}

// Other batch failures still fall back per delivery, but a stamped delivery
// goes alone through the batch path so it keeps its term.
func TestStampedDeliveryFallbackKeepsItsTerm(t *testing.T) {
	manager, _, _ := newFencedLeader(t, "node-follower", 7)
	router := &termRecordingRouter{batchErrs: []error{errors.New("coalesced batch failed")}}
	manager.delivery.remote = router

	manager.delivery.DeliverQueue(context.Background(), "replication")

	if len(router.routed) != 0 {
		t.Fatalf("a stamped delivery fell back to the unfenced single-message path: %+v", router.routed)
	}
	if router.batches != 2 || len(router.terms) != 2 || router.terms[1] != 7 {
		t.Fatalf("expected the delivery resent alone with term 7, got batches=%d terms=%v", router.batches, router.terms)
	}
}

func TestCheckQueueDeliveryTermRefusesOlderTerm(t *testing.T) {
	manager, _, _ := newFencedLeader(t, "", 6)

	if err := manager.CheckQueueDeliveryTerm("replication", 5); !errors.Is(err, cluster.ErrStaleLeaderDelivery) {
		t.Fatalf("term 5 after seeing 6 must be refused as stale, got %v", err)
	}
	for _, term := range []uint64{6, 7} {
		if err := manager.CheckQueueDeliveryTerm("replication", term); err != nil {
			t.Fatalf("term %d after seeing 6 must be accepted, got %v", term, err)
		}
	}

	unfenced, _, _ := newReplicatedManualStreamNode(t, false, "")
	if err := unfenced.CheckQueueDeliveryTerm("replication", 1); err != nil {
		t.Fatalf("a node that cannot report a term must not refuse, got %v", err)
	}
}
