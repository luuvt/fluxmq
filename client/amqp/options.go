// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package amqp

import (
	"crypto/tls"
	"net/url"
	"strings"
	"time"
)

// Default values.
const (
	DefaultAddress          = "localhost:5682"
	DefaultDialTimeout      = 10 * time.Second
	DefaultHeartbeat        = 60 * time.Second
	DefaultReconnectBackoff = 1 * time.Second
	DefaultMaxReconnectWait = 2 * time.Minute

	// DefaultWriteTimeout is used when WriteTimeout is left at its zero value
	// and Heartbeat is also zero (heartbeats disabled), so writes still get a
	// bounded deadline. See the WriteTimeout field doc.
	DefaultWriteTimeout = 60 * time.Second
)

// Options configures the AMQP 0.9.1 client.
type Options struct {
	// Connection
	URL            string      // Full AMQP URL (overrides Address/Username/Password/Vhost)
	Address        string      // Broker address (host:port)
	Username       string      // Username for PLAIN auth
	Password       string      // Password for PLAIN auth
	Vhost          string      // Virtual host (default "/")
	ConnectionName string      // Human-readable connection name (sent as "connection_name" in ClientProperties)
	TLSConfig      *tls.Config // TLS configuration (nil for plain TCP)
	DialTimeout    time.Duration
	Heartbeat      time.Duration

	// WriteTimeout bounds every individual Write on the connection's
	// underlying net.Conn with a rolling deadline, reset before each call
	// (see deadlineConn in deadline_conn.go). amqp091-go deliberately leaves
	// writes unbounded after the initial handshake — see openComplete's
	// comment "RabbitMQ uses TCP flow control at this point for pushback so
	// Writes can intentionally block" — which is reasonable for ordinary
	// backpressure (resolves in seconds) but means a write that is not
	// merely slow but genuinely stuck (broker/cluster hiccup, a half-open
	// connection, a silent network partition) blocks forever with no bound
	// at all. That single stuck write — inside Publish, inside
	// Ack/Nack/Reject, or inside amqp091-go's own heartbeat frame writer,
	// they all end up here — then holds whatever higher-level lock called it
	// (pubChMu/subChMu) for good, and every later call on the same Client
	// blocks on that lock too, all while the connection looks "open": no
	// error, no OnConnectionLost, no reconnect, until whatever OS-level TCP
	// timeout eventually fires (observed: multiple hours in production).
	// WriteTimeout turns that indefinite hang into a bounded I/O error,
	// which surfaces through amqp091-go's NotifyClose and lets the existing
	// OnConnectionLost/AutoReconnect path (see handleDisconnect,
	// startReconnect) do its job promptly instead.
	//
	// This intentionally does NOT touch the read deadline: amqp091-go
	// already manages that itself (Connection.heartbeater resets it to 3x
	// the negotiated heartbeat interval on every frame received), and
	// overriding it here would just fight a mechanism that already works.
	//
	// Zero means "derive from Heartbeat": 2*Heartbeat, generous enough to
	// never trip during ordinary backpressure while still being far shorter
	// than any reasonable OS-level TCP timeout, or DefaultWriteTimeout if
	// Heartbeat is also 0 (heartbeats disabled).
	WriteTimeout time.Duration

	// Channel QoS
	PrefetchCount int // Maximum unacked deliveries
	PrefetchSize  int // Maximum bytes in-flight

	// Reconnection
	AutoReconnect    bool
	ReconnectBackoff time.Duration
	MaxReconnectWait time.Duration

	// Callbacks
	OnConnect           func()
	OnConnectionLost    func(error)
	OnReconnecting      func(attempt int)
	OnConsumerCancelled func(consumerTag string)
}

// NewOptions creates Options with sensible defaults.
func NewOptions() *Options {
	return &Options{
		Address:          DefaultAddress,
		Username:         "guest",
		Password:         "guest",
		Vhost:            "/",
		DialTimeout:      DefaultDialTimeout,
		Heartbeat:        DefaultHeartbeat,
		AutoReconnect:    true,
		ReconnectBackoff: DefaultReconnectBackoff,
		MaxReconnectWait: DefaultMaxReconnectWait,
	}
}

// SetURL sets a full AMQP URL and overrides Address/Credentials/Vhost.
func (o *Options) SetURL(rawURL string) *Options {
	o.URL = rawURL
	return o
}

// SetAddress sets the broker address (host:port).
func (o *Options) SetAddress(addr string) *Options {
	o.Address = addr
	return o
}

// SetCredentials sets username and password.
func (o *Options) SetCredentials(username, password string) *Options {
	o.Username = username
	o.Password = password
	return o
}

// SetVhost sets the virtual host.
func (o *Options) SetVhost(vhost string) *Options {
	o.Vhost = vhost
	return o
}

// SetConnectionName sets a human-readable name for the connection.
// This is sent as "connection_name" in AMQP ClientProperties and
// appears in the broker's admin UI for identifying connections.
func (o *Options) SetConnectionName(name string) *Options {
	o.ConnectionName = name
	return o
}

// SetTLSConfig sets TLS configuration.
func (o *Options) SetTLSConfig(cfg *tls.Config) *Options {
	o.TLSConfig = cfg
	return o
}

// SetDialTimeout sets the dial timeout.
func (o *Options) SetDialTimeout(d time.Duration) *Options {
	o.DialTimeout = d
	return o
}

// SetHeartbeat sets the heartbeat interval.
func (o *Options) SetHeartbeat(d time.Duration) *Options {
	o.Heartbeat = d
	return o
}

// SetWriteTimeout sets the per-Write deadline on the connection. See the
// WriteTimeout field doc for what this protects against and how the zero
// value is derived from Heartbeat.
func (o *Options) SetWriteTimeout(d time.Duration) *Options {
	o.WriteTimeout = d
	return o
}

// writeTimeout returns the effective per-Write deadline: WriteTimeout if
// set, otherwise 2*Heartbeat, otherwise DefaultWriteTimeout.
func (o *Options) writeTimeout() time.Duration {
	if o.WriteTimeout > 0 {
		return o.WriteTimeout
	}
	if o.Heartbeat > 0 {
		return 2 * o.Heartbeat
	}
	return DefaultWriteTimeout
}

// SetPrefetch sets channel prefetch limits.
func (o *Options) SetPrefetch(count, size int) *Options {
	o.PrefetchCount = count
	o.PrefetchSize = size
	return o
}

// SetAutoReconnect enables or disables automatic reconnection.
func (o *Options) SetAutoReconnect(enable bool) *Options {
	o.AutoReconnect = enable
	return o
}

// SetReconnectBackoff sets the initial reconnect delay.
func (o *Options) SetReconnectBackoff(d time.Duration) *Options {
	o.ReconnectBackoff = d
	return o
}

// SetMaxReconnectWait sets the maximum reconnect delay.
func (o *Options) SetMaxReconnectWait(d time.Duration) *Options {
	o.MaxReconnectWait = d
	return o
}

// SetOnConnect sets the connection callback.
func (o *Options) SetOnConnect(fn func()) *Options {
	o.OnConnect = fn
	return o
}

// SetOnConnectionLost sets the connection lost callback.
func (o *Options) SetOnConnectionLost(fn func(error)) *Options {
	o.OnConnectionLost = fn
	return o
}

// SetOnReconnecting sets the reconnecting callback.
func (o *Options) SetOnReconnecting(fn func(attempt int)) *Options {
	o.OnReconnecting = fn
	return o
}

// SetOnConsumerCancelled sets the callback for server-initiated consumer cancellation.
func (o *Options) SetOnConsumerCancelled(fn func(consumerTag string)) *Options {
	o.OnConsumerCancelled = fn
	return o
}

// Validate checks the options for errors.
func (o *Options) Validate() error {
	if o.URL == "" && o.Address == "" {
		return ErrNoAddress
	}
	return nil
}

func (o *Options) dialURL() (string, error) {
	if o.URL != "" {
		return o.URL, nil
	}

	scheme := "amqp"
	if o.TLSConfig != nil {
		scheme = "amqps"
	}

	vhost := strings.TrimPrefix(o.Vhost, "/")
	u := &url.URL{
		Scheme: scheme,
		Host:   o.Address,
		Path:   "/" + vhost,
	}

	if o.Username != "" {
		u.User = url.UserPassword(o.Username, o.Password)
	}

	return u.String(), nil
}
