// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package logstorage

import (
	"context"
	"testing"

	"github.com/absmach/fluxmq/message"
	"github.com/absmach/fluxmq/queue/storage"
	"github.com/absmach/fluxmq/queue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
)

// A record written before the v1 envelope has version 0. Reading it must be
// reported as undecodable, the one read error a consumer may step past.
func TestReadReportsLegacyEnvelopeAsUndecodable(t *testing.T) {
	ctx := context.Background()
	adapter := newDedupeAdapter(t, t.TempDir())
	require.NoError(t, adapter.CreateQueue(ctx, types.DefaultQueueConfig(testDedupeQueue, testDedupeQueue+"/#")))

	legacy := protowire.AppendTag(nil, 1, protowire.VarintType)
	legacy = protowire.AppendVarint(legacy, 0)
	legacy = protowire.AppendTag(legacy, 2, protowire.BytesType)
	legacy = protowire.AppendString(legacy, testDedupeQueue+"/client/update")
	offset, err := adapter.store.Append(testDedupeQueue, []byte("payload"), nil, map[string][]byte{headerEnvelope: legacy})
	require.NoError(t, err)

	_, err = adapter.Read(ctx, testDedupeQueue, offset)
	assert.ErrorIs(t, err, storage.ErrUndecodableRecord)
	assert.ErrorIs(t, err, message.ErrUnsupportedVersion, "the cause must stay visible")

	_, err = adapter.Read(ctx, testDedupeQueue, offset+1)
	assert.ErrorIs(t, err, storage.ErrOffsetOutOfRange)
	assert.NotErrorIs(t, err, storage.ErrUndecodableRecord)
}
