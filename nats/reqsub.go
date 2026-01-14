package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"runtime/debug"
	"time"

	"github.com/nats-io/nats.go"
	nc "github.com/nats-io/nats.go"
)

func (c *Client) Request(subject string, v any) (*nats.Msg, error) {
	if c == nil || c.conn == nil {
		return nil, errors.New("nats client is nil or closed")
	}
	if subject == "" {
		return nil, errors.New("subject must not be empty")
	}

	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("marshal json for request on %q: %w", subject, err)
	}

	msg, err := c.conn.Request(subject, b, 2*time.Second)
	if err != nil {
		return nil, fmt.Errorf("request on %q: %w", subject, err)
	}

	return msg, nil
}

func (c *Client) Subscribe(
	subject string,
	handler func(context.Context, *nc.Msg) error,
) (Subscription, error) {
	if c == nil || c.conn == nil {
		return nil, errors.New("nats client is nil or closed")
	}
	if subject == "" {
		return nil, errors.New("subject must not be empty")
	}
	if handler == nil {
		return nil, errors.New("handler must not be nil")
	}

	sub, err := c.conn.Subscribe(subject, func(m *nc.Msg) {
		// Keep the callback non-blocking to NATS internals:
		// run user code in a goroutine.
		go func(msg *nc.Msg) {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("nats: panic in handler for %q: %v\n%s", subject, r, string(debug.Stack()))
				}
			}()

			// Context: currently background; later we can thread cancellation on Close if needed.
			ctx := context.Background()

			if err := handler(ctx, msg); err != nil {
				log.Printf("nats: handler error for %q: %v", subject, err)
			}
		}(m)
	})
	if err != nil {
		return nil, fmt.Errorf("subscribe to %q: %w", subject, err)
	}

	// Track subscription for auto-unsubscribe on Close.
	c.mu.Lock()
	c.subs = append(c.subs, sub)
	c.mu.Unlock()

	return subscription{s: sub}, nil
}
