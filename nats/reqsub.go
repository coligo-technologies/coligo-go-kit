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
)

func (c *Client) Request(subject string, jsonBody any) (*Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	response, err := c.RequestContext(ctx, subject, jsonBody)
	if errors.Is(err, context.DeadlineExceeded) {
		err = errors.Join(err, nats.ErrTimeout)
	}
	return response, err
}

// RequestContext uses the caller's deadline and cancellation. Cancelling the
// request stops waiting for a reply; it does not cancel work in the responder.
func (c *Client) RequestContext(ctx context.Context, subject string, jsonBody any) (*Response, error) {
	conn, err := c.connection()
	if err != nil {
		return BadRequest(err.Error()), err
	}
	if subject == "" {
		err := errors.New("subject must not be empty")
		return BadRequest(err.Error()), err
	}

	b, err := json.Marshal(jsonBody)
	if err != nil {
		return InternalServerError(
				fmt.Sprintf("marshal json for request on %q failed", subject),
			),
			fmt.Errorf("marshal request for %q: %w", subject, err)
	}

	msg, err := conn.RequestWithContext(ctx, subject, b)
	if err != nil {
		if errors.Is(err, nats.ErrTimeout) || errors.Is(err, context.DeadlineExceeded) {
			return GatewayTimeout(
					fmt.Sprintf("request on %q timed out", subject),
				),
				fmt.Errorf("request %q: %w", subject, err)
		}
		return BadGateway(
				fmt.Sprintf("request on %q failed", subject),
			),
			fmt.Errorf("request %q: %w", subject, err)
	}

	var resp Response
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		return InternalServerError(
				fmt.Sprintf("invalid response envelope from %q", subject),
			),
			fmt.Errorf("unmarshal response from %q: %w", subject, err)
	}

	return &resp, nil
}

func (c *Client) Subscribe(
	subject string,
	handler func([]byte) (*Response, error),
) (Subscription, error) {
	return c.subscribeRequest(subject, handler, false)
}

// SubscribeConcurrent runs each request handler in its own goroutine. Handlers
// must synchronize shared state; requests may complete out of order.
func (c *Client) SubscribeConcurrent(subject string, handler func([]byte) (*Response, error)) (Subscription, error) {
	return c.subscribeRequest(subject, handler, true)
}

func (c *Client) subscribeRequest(subject string, handler func([]byte) (*Response, error), concurrent bool) (Subscription, error) {
	if subject == "" {
		return nil, errors.New("subject must not be empty")
	}
	if handler == nil {
		return nil, errors.New("handler must not be nil")
	}

	handle := func(m *nats.Msg) {
		// Keep reply logic local to the handler to avoid extra helpers on Client.
		respond := func(resp *Response) {
			b, err := json.Marshal(resp)
			if err != nil {
				// Best-effort fallback (matches Response json tags)
				_ = m.Respond([]byte(`{"statusCode":500,"message":"failed to marshal response","data":null}`))
				return
			}
			_ = m.Respond(b)
		}

		defer func() {
			if r := recover(); r != nil {
				log.Printf("nats: panic in handler for %q: %v\n%s", subject, r, string(debug.Stack()))
				respond(InternalServerError("panic in handler"))
			}
		}()

		resp, err := handler(m.Data)
		if err != nil {
			log.Printf("nats: handler error for %q: %v", subject, err)
			if resp == nil {
				resp = InternalServerError("handler error")
			}
		}
		if resp == nil {
			resp = InternalServerError("handler returned nil response")
		}

		respond(resp)
	}

	return c.subscribe(subject, func(m *nats.Msg) {
		if concurrent {
			c.handlers.Add(1)
			go func() {
				defer c.handlers.Done()
				handle(m)
			}()
		} else {
			handle(m)
		}
	})
}
