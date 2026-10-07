package nats

import (
	"context"
	"fmt"

	nc "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type JetStream struct {
	js  nc.JetStreamContext
	api jetstream.JetStream
}

func (c *Client) CreateJetStream(ctx context.Context) (*JetStream, error) {
	conn, err := c.connection()
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Use ctx for JS operations where supported by the client.
	js, err := conn.JetStream()
	if err != nil {
		return nil, fmt.Errorf("create jetstream context: %w", err)
	}

	api, err := jetstream.New(conn)
	if err != nil {
		return nil, err
	}
	return &JetStream{js: js, api: api}, nil
}

// Context exposes the legacy client for compatibility. New callers should use
// the context-aware Go kit stream and KV methods.
func (j *JetStream) Context() nc.JetStreamContext {
	if j == nil {
		return nil
	}
	return j.js
}
