// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package raft

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// The delivery fence stamps deliveries with the leader's term and asks a quorum
// to confirm leadership before a pass. A running leader reports a term and
// confirms; the confirmation is then reused within the same term.
func TestManagerReportsTermAndVerifiesLeadership(t *testing.T) {
	node := newReplayNode(t, 1<<20)
	node.start()
	t.Cleanup(node.stop)

	term := node.manager.CurrentTerm()
	require.NotZero(t, term)
	require.NoError(t, node.manager.VerifyLeader(context.Background()))

	node.manager.verifyMu.Lock()
	verifiedAt, verifiedTerm := node.manager.verifiedAt, node.manager.verifiedTerm
	node.manager.verifyMu.Unlock()
	require.Equal(t, term, verifiedTerm)

	require.NoError(t, node.manager.VerifyLeader(context.Background()))
	node.manager.verifyMu.Lock()
	reused := node.manager.verifiedAt.Equal(verifiedAt)
	node.manager.verifyMu.Unlock()
	require.True(t, reused, "a fresh confirmation in the same term must be reused, not repeated")
}

func TestManagerWithoutRaftCannotVerify(t *testing.T) {
	m := &Manager{}
	require.Zero(t, m.CurrentTerm())
	require.ErrorIs(t, m.VerifyLeader(context.Background()), ErrRaftDisabled)
}
