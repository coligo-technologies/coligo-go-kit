package nats

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	nc "github.com/nats-io/nats.go"

	"github.com/coligo-technologies/coligo-go-kit/internal/natskit"
)

type Client struct {
	conn *nc.Conn

	mu   sync.Mutex
	subs []*nc.Subscription
}

func NewClient(ctx context.Context, url string) (*Client, error) {
	if url == "" {
		return nil, errors.New("nats url must not be empty")
	}

	bo := natskit.DefaultBackoff()

	// Use a bounded connect timeout derived from ctx if it has no deadline.
	connectCtx := ctx
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		connectCtx, cancel = context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
	}

	// We retry connect attempts until ctx is done.
	var lastErr error
	for attempt := 0; ; attempt++ {
		select {
		case <-connectCtx.Done():
			if lastErr != nil {
				return nil, fmt.Errorf("connect to nats: %w (last error)", lastErr)
			}
			return nil, fmt.Errorf("connect to nats: %w", connectCtx.Err())
		default:
		}

		// Dial with a per-attempt timeout so a single try doesn't hang too long.
		// Keep it <= 5s to allow multiple tries within a short ctx.
		perAttemptTimeout := 5 * time.Second
		if dl, ok := connectCtx.Deadline(); ok {
			remaining := time.Until(dl)
			if remaining < perAttemptTimeout {
				if remaining <= 0 {
					continue
				}
				perAttemptTimeout = remaining
			}
		}

		// IMPORTANT: allow reconnect handling; we still do initial retry ourselves.
		opts := []nc.Option{
			nc.Timeout(perAttemptTimeout),
			nc.RetryOnFailedConnect(true),
			nc.MaxReconnects(-1), // infinite reconnects
			nc.ReconnectWait(250 * time.Millisecond),
			nc.DisconnectErrHandler(func(_ *nc.Conn, err error) {
				if err != nil {
					log.Printf("nats: disconnected: %v", err)
				} else {
					log.Printf("nats: disconnected")
				}
			}),
			nc.ReconnectHandler(func(c *nc.Conn) {
				log.Printf("nats: reconnected to %s", c.ConnectedUrl())
			}),
			nc.ClosedHandler(func(_ *nc.Conn) {
				log.Printf("nats: connection closed")
			}),
		}

		conn, err := nc.Connect(url, opts...)
		if err == nil {
			return &Client{conn: conn}, nil
		}

		lastErr = err

		// backoff before next attempt
		sleep := bo.Duration(attempt)
		if err := natskit.Sleep(connectCtx, sleep); err != nil {
			return nil, fmt.Errorf("connect to nats: %w (last error: %v)", err, lastErr)
		}
	}
}

func (c *Client) Close() {
	if c == nil {
		return
	}

	c.mu.Lock()
	subs := c.subs
	c.subs = nil
	conn := c.conn
	c.conn = nil
	c.mu.Unlock()

	// Best-effort drain subscriptions (lets in-flight callbacks finish).
	for _, s := range subs {
		if s != nil {
			_ = s.Drain()
		}
	}

	if conn == nil {
		return
	}

	// Best-effort flush pending publishes.
	_ = conn.FlushTimeout(2 * time.Second)

	// Drain the connection gracefully, but don't risk hanging forever.
	done := make(chan struct{})
	go func() {
		_ = conn.Drain() // Drain will close the connection when done.
		close(done)
	}()

	select {
	case <-done:
		// drained successfully
	case <-time.After(3 * time.Second):
		// fallback: force close
		conn.Close()
	}
}
