// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/absmach/fluxmq/message"
	"github.com/absmach/fluxmq/queue/storage/memory/log"
	"github.com/absmach/fluxmq/queue/types"
)

// StreamGroupAutoCommit phản ánh chế độ thật của group, kể cả pattern và group chưa có.
func TestStreamGroupAutoCommit(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	noop := DeliveryTargetFunc(func(context.Context, string, *message.Envelope) error { return nil })
	mgr := NewManager(log.New(), newMockGroupStore(), noop, DefaultConfig(), logger, nil)

	queueCfg := types.DefaultQueueConfig(testQueueEvents, "$queue/events/#")
	queueCfg.Type = types.QueueTypeStream
	if err := mgr.CreateQueue(ctx, queueCfg); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}

	off := false
	subs := []struct {
		group, pattern string
		auto           *bool
		want           bool
	}{
		{"auto", "client/update", nil, true},
		{"manual", "client/update", &off, false},
		{"plain", "", nil, true},
	}
	for _, s := range subs {
		cursor := &types.CursorOption{Position: types.CursorDefault, Mode: types.GroupModeStream, AutoCommit: s.auto}
		if err := mgr.SubscribeWithCursor(ctx, testQueueEvents, s.pattern, testClientOneID, s.group, "", cursor); err != nil {
			t.Fatalf("subscribe %s: %v", s.group, err)
		}
		if got := mgr.StreamGroupAutoCommit(ctx, testQueueEvents, s.group, s.pattern); got != s.want {
			t.Errorf("%s: auto-commit = %v, want %v", s.group, got, s.want)
		}
	}
	if mgr.StreamGroupAutoCommit(ctx, testQueueEvents, "missing", "") {
		t.Error("group chưa có phải trả false")
	}
}
