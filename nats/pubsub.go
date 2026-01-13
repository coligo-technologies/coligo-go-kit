package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"runtime/debug"

	nc "github.com/nats-io/nats.go"
)

func (c *Client) Publish(subject string, v any) error {
	if c == nil || c.conn == nil {
		return errors.New("nats client is nil or closed")
	}
	if subject == "" {
		return errors.New("subject must not be empty")
	}

	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal json for publish on %q: %w", subject, err)
	}

	if err := c.conn.Publish(subject, b); err != nil {
		return fmt.Errorf("publish on %q: %w", subject, err)
	}
	return nil
}

func (c *Client) Subscribe(subject string, handler func(context.Context, []byte) error) (Subscription, error) {
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
		go func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("nats: panic in handler for %q: %v\n%s", subject, r, string(debug.Stack()))
				}
			}()

			// Context: currently background; later we can thread cancellation on Close if needed.
			ctx := context.Background()

			if err := handler(ctx, m.Data); err != nil {
				log.Printf("nats: handler error for %q: %v", subject, err)
			}
		}()
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
