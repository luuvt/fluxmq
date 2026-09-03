// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package amqp

import (
	"errors"
	"testing"
	"time"
)

func TestOptionsSetURL(t *testing.T) {
	opts := NewOptions().SetURL("amqp://user:pass@localhost:5672/vhost")
	if opts.URL != "amqp://user:pass@localhost:5672/vhost" {
		t.Fatalf("expected URL to be set, got %q", opts.URL)
	}
}

func TestOptionsWriteTimeout(t *testing.T) {
	cases := []struct {
		name      string
		configure func(*Options)
		want      time.Duration
	}{
		{
			name:      "explicit WriteTimeout wins over Heartbeat",
			configure: func(o *Options) { o.Heartbeat = 5 * time.Second; o.WriteTimeout = 3 * time.Second },
			want:      3 * time.Second,
		},
		{
			name:      "derives 2x Heartbeat when WriteTimeout unset",
			configure: func(o *Options) { o.Heartbeat = 45 * time.Second },
			want:      90 * time.Second,
		},
		{
			name:      "falls back to DefaultWriteTimeout when both unset",
			configure: func(o *Options) { o.Heartbeat = 0 },
			want:      DefaultWriteTimeout,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := NewOptions()
			opts.Heartbeat = 0 // NewOptions sets DefaultHeartbeat; start from a clean slate
			tc.configure(opts)
			if got := opts.writeTimeout(); got != tc.want {
				t.Fatalf("writeTimeout() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestPublishWithOptionsMandatoryRequiresReturnHandler(t *testing.T) {
	c, err := New(NewOptions())
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	c.connected.Store(true)

	err = c.PublishWithOptions(&PublishOptions{
		Topic:     "events/test",
		Payload:   []byte("hello"),
		Mandatory: true,
	})
	if !errors.Is(err, ErrNoReturnHandler) {
		t.Fatalf("expected ErrNoReturnHandler, got %v", err)
	}
}

func TestPublishWithConfirmNilOptions(t *testing.T) {
	c, err := New(NewOptions())
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	if err := c.PublishWithConfirm(nil, 0); !errors.Is(err, ErrNilOptions) {
		t.Fatalf("expected ErrNilOptions, got %v", err)
	}
}

func TestGetValidation(t *testing.T) {
	c, err := New(NewOptions())
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	// Not connected
	_, _, err = c.Get("test-queue", true)
	if !errors.Is(err, ErrNotConnected) {
		t.Fatalf("expected ErrNotConnected, got %v", err)
	}

	c.connected.Store(true)

	// Empty queue name
	_, _, err = c.Get("", true)
	if !errors.Is(err, ErrInvalidQueueName) {
		t.Fatalf("expected ErrInvalidQueueName, got %v", err)
	}

	_, _, err = c.GetFromQueue("", true)
	if !errors.Is(err, ErrInvalidQueueName) {
		t.Fatalf("expected ErrInvalidQueueName, got %v", err)
	}
}

func TestGetFromQueueNormalizesName(t *testing.T) {
	c, err := New(NewOptions())
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	// Not connected — should still propagate ErrNotConnected
	_, _, err = c.GetFromQueue("my-queue", true)
	if !errors.Is(err, ErrNotConnected) {
		t.Fatalf("expected ErrNotConnected, got %v", err)
	}
}
