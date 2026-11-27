// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/absmach/fluxmq/amqp/codec"
	"github.com/absmach/fluxmq/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func autoCommitOn() *atomic.Bool {
	b := new(atomic.Bool)
	b.Store(true)
	return b
}

func streamProps(offset int) map[string]string {
	return map[string]string{
		message.PropertyQueueName:    testEvents,
		message.PropertyGroupID:      "g@client/update",
		message.PropertyOffset:       strconv.Itoa(offset),
		message.PropertyStreamOffset: strconv.Itoa(offset),
	}
}

// Client không ack trên group auto-commit không prefetch: unacked phải bị chặn trần.
func TestAutoCommitStreamUnackedBounded(t *testing.T) {
	ch, _ := newTestChannel(t)
	cons := &consumer{tag: "c1", queueName: testEvents}
	cons.autoCommitStream = autoCommitOn()

	n := maxAutoCommitUnacked + 500
	for i := 0; i < n; i++ {
		require.NoError(t, ch.sendDelivery(cons, "client/update", []byte("x"), streamProps(i)))
	}

	ch.unackedMu.Lock()
	defer ch.unackedMu.Unlock()
	assert.Len(t, ch.unacked, maxAutoCommitUnacked)
	assert.Len(t, ch.autoCommitTags, maxAutoCommitUnacked)
	_, newest := ch.unacked[ch.autoCommitTags[len(ch.autoCommitTags)-1]]
	assert.True(t, newest, "tag mới nhất vẫn ack được")
}

// Có prefetch hoặc group manual-commit: giữ nguyên hành vi cũ (không cắt).
func TestUnackedNotTrimmedWithPrefetchOrManual(t *testing.T) {
	n := maxAutoCommitUnacked + 10

	ch, _ := newTestChannel(t)
	ch.prefetchCount = 50000
	ac := &consumer{tag: "c1", queueName: testEvents}
	ac.autoCommitStream = autoCommitOn()
	for i := 0; i < n; i++ {
		require.NoError(t, ch.sendDelivery(ac, "client/update", []byte("x"), streamProps(i)))
	}
	assert.Len(t, ch.unacked, n)

	ch2, _ := newTestChannel(t)
	manual := &consumer{tag: "c2", queueName: testEvents}
	for i := 0; i < n; i++ {
		require.NoError(t, ch2.sendDelivery(manual, "client/update", []byte("x"), streamProps(i)))
	}
	assert.Len(t, ch2.unacked, n)
}

// Ack tag đã bị cắt thì bỏ qua êm, ack tag còn giữ thì xoá khỏi map.
func TestAutoCommitAckAfterTrim(t *testing.T) {
	ch, _ := newTestChannel(t)
	cons := &consumer{tag: "c1", queueName: testEvents}
	cons.autoCommitStream = autoCommitOn()
	for i := 0; i < maxAutoCommitUnacked+1; i++ {
		require.NoError(t, ch.sendDelivery(cons, "client/update", []byte("x"), streamProps(i)))
	}
	ch.unackedMu.Lock()
	newest := ch.autoCommitTags[len(ch.autoCommitTags)-1]
	oldest := ch.autoCommitTags[0]
	ch.unackedMu.Unlock()

	require.NoError(t, ch.handleBasicAck(&codec.BasicAck{DeliveryTag: oldest - 1}))
	assert.Len(t, ch.unacked, maxAutoCommitUnacked)
	require.NoError(t, ch.handleBasicAck(&codec.BasicAck{DeliveryTag: newest}))
	assert.Len(t, ch.unacked, maxAutoCommitUnacked-1)
}
