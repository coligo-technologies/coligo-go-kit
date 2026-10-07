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

	mu       sync.Mutex
	subs     []*nc.Subscription
	handlers sync.WaitGroup
}

var errClientClosed = errors.New("nats client is nil or closed")

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
				return nil, fmt.Errorf("connect to nats: %w (last error: %v)", connectCtx.Err(), lastErr)
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
			nc.RetryOnFailedConnect(false),
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
			nc.ErrorHandler(func(_ *nc.Conn, sub *nc.Subscription, err error) {
				subject := ""
				if sub != nil {
					subject = sub.Subject
				}
				log.Printf("nats: asynchronous error on %q: %v", subject, err)
			}),
			nc.ClosedHandler(func(_ *nc.Conn) {
				log.Printf("nats: connection closed")
			}),
		}

		// Connect has no context parameter. Dispatch one bounded attempt so
		// cancellation can return promptly even during the server handshake.
		type connectionResult struct {
			conn *nc.Conn
			err  error
		}
		results := make(chan connectionResult)
		go func() {
			conn, err := nc.Connect(url, opts...)
			select {
			case results <- connectionResult{conn, err}:
			case <-connectCtx.Done():
				if conn != nil {
					conn.Close()
				}
			}
		}()
		var conn *nc.Conn
		var err error
		select {
		case result := <-results:
			conn, err = result.conn, result.err
		case <-connectCtx.Done():
			return nil, fmt.Errorf("connect to nats: %w", connectCtx.Err())
		}
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

// Connected reports whether the client currently has an active NATS connection.
func (c *Client) Connected() bool {
	conn, err := c.connection()
	return err == nil && conn.IsConnected()
}

// Flush sends any buffered writes to the NATS server.
func (c *Client) Flush() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return c.FlushContext(ctx)
}

// FlushContext waits for the server to process buffered writes.
func (c *Client) FlushContext(ctx context.Context) error {
	conn, err := c.connection()
	if err != nil {
		return err
	}
	// NATS requires a deadline for FlushWithContext.
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
	}
	return transportError(conn.FlushWithContext(ctx))
}

func (c *Client) connection() (*nc.Conn, error) {
	if c == nil {
		return nil, errClientClosed
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return nil, errClientClosed
	}
	return c.conn, nil
}

// Close drains subscriptions and waits for concurrent handlers and connection
// closure. After three seconds it force-closes the connection; handler work is
// not cancelled. Call Close from outside subscription handlers.
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
	if conn == nil {
		return
	}

	timeout := time.NewTimer(3 * time.Second)
	defer timeout.Stop()
	drained := make([]<-chan nc.SubStatus, 0, len(subs))
	for _, s := range subs {
		drained = append(drained, s.StatusChanged(nc.SubscriptionClosed))
		_ = s.Drain()
	}
	// Once callbacks have drained, no more concurrent handlers can be added.
	for _, done := range drained {
		select {
		case <-done:
		case <-timeout.C:
			conn.Close()
			return
		}
	}
	handlersDone := make(chan struct{})
	go func() { c.handlers.Wait(); close(handlersDone) }()
	select {
	case <-handlersDone:
	case <-timeout.C:
		conn.Close()
		return
	}

	closed := conn.StatusChanged(nc.CLOSED)
	defer conn.RemoveStatusListener(closed)
	if conn.IsClosed() {
		return
	}
	if err := conn.Drain(); err != nil {
		conn.Close()
		return
	}
	select {
	case <-closed:
	case <-timeout.C:
		conn.Close()
	}
}
