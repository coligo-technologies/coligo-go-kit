package nats

import (
	"context"
	"fmt"

	nc "github.com/nats-io/nats.go"
)

type JetStream struct {
	js nc.JetStreamContext
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
	js, err := conn.JetStream(nc.Context(ctx))
	if err != nil {
		return nil, fmt.Errorf("create jetstream context: %w", err)
	}

	return &JetStream{js: js}, nil
}

func (j *JetStream) Context() nc.JetStreamContext {
	if j == nil {
		return nil
	}
	return j.js
}
