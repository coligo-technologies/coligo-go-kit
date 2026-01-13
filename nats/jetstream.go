package nats

import (
	"context"
	"errors"
	"fmt"

	nc "github.com/nats-io/nats.go"
)

type JetStream struct {
	js nc.JetStreamContext
}

func (c *Client) CreateJetStream(ctx context.Context) (*JetStream, error) {
	if c == nil || c.conn == nil {
		return nil, errors.New("nats client is nil or closed")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Use ctx for JS operations where supported by the client.
	js, err := c.conn.JetStream(nc.Context(ctx))
	if err != nil {
		return nil, fmt.Errorf("create jetstream context: %w", err)
	}

	return &JetStream{js: js}, nil
}
