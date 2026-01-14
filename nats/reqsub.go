package nats

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"runtime/debug"
	"time"

	"github.com/nats-io/nats.go"
)

func (c *Client) Request(subject string, jsonBody any) (*Response, error) {
	if c == nil || c.conn == nil {
		err := errors.New("nats client is nil or closed")
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

	msg, err := c.conn.Request(subject, b, 2*time.Second)
	if err != nil {
		if errors.Is(err, nats.ErrTimeout) {
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
	if c == nil || c.conn == nil {
		return nil, errors.New("nats client is nil or closed")
	}
	if subject == "" {
		return nil, errors.New("subject must not be empty")
	}
	if handler == nil {
		return nil, errors.New("handler must not be nil")
	}

	sub, err := c.conn.Subscribe(subject, func(m *nats.Msg) {
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
	})
	if err != nil {
		return nil, fmt.Errorf("subscribe to %q: %w", subject, err)
	}

	c.mu.Lock()
	c.subs = append(c.subs, sub)
	c.mu.Unlock()

	return subscription{s: sub}, nil
}
