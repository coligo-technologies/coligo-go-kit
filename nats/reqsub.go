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

var errRequestEncoding = errors.New("invalid JSON request")

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
	raw, err := c.request(ctx, subject, jsonBody)
	if err != nil {
		if errors.Is(err, errClientClosed) || subject == "" {
			return BadRequest(err.Error()), err
		}
		if errors.Is(err, errRequestEncoding) {
			return InternalServerError(fmt.Sprintf("marshal json for request on %q failed", subject)), err
		}
		if errors.Is(err, ErrTimeout) {
			return GatewayTimeout(fmt.Sprintf("request on %q timed out", subject)), err
		}
		return BadGateway(fmt.Sprintf("request on %q failed", subject)), err
	}

	var resp Response
	if err := json.Unmarshal(raw, &resp); err != nil {
		return InternalServerError(
				fmt.Sprintf("invalid response envelope from %q", subject),
			),
			fmt.Errorf("unmarshal response from %q: %w", subject, err)
	}

	return &resp, nil
}

func (c *Client) request(ctx context.Context, subject string, jsonBody any) ([]byte, error) {
	conn, err := c.connection()
	if err != nil {
		return nil, err
	}
	if subject == "" {
		return nil, errors.New("subject must not be empty")
	}
	b, err := json.Marshal(jsonBody)
	if err != nil {
		return nil, fmt.Errorf("marshal request for %q: %w", subject, errors.Join(errRequestEncoding, err))
	}
	msg, err := conn.RequestWithContext(ctx, subject, b)
	if err != nil {
		return nil, fmt.Errorf("request %q: %w", subject, transportError(err))
	}
	return msg.Data, nil
}

// RequestInto checks the response status and decodes data directly into result.
// A nil result ignores data; a non-nil result requires non-null response data.
func (c *Client) RequestInto(ctx context.Context, subject string, request, result any) error {
	raw, err := c.request(ctx, subject, request)
	if err != nil {
		return err
	}
	var response struct {
		StatusCode int             `json:"statusCode"`
		Message    string          `json:"message"`
		Data       json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(raw, &response); err != nil {
		return fmt.Errorf("decode response on %q: %w", subject, err)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return &StatusError{StatusCode: response.StatusCode, Message: response.Message}
	}
	if result == nil {
		return nil
	}
	if len(response.Data) == 0 || string(response.Data) == "null" {
		return ErrNoData
	}
	if err := json.Unmarshal(response.Data, result); err != nil {
		return fmt.Errorf("decode response data on %q: %w", subject, err)
	}
	return nil
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
				if err := m.Respond([]byte(`{"statusCode":500,"message":"failed to marshal response","data":null}`)); err != nil {
					log.Printf("nats: fallback reply on %q failed: %v", subject, err)
				}
				return
			}
			if err := m.Respond(b); err != nil {
				log.Printf("nats: reply on %q failed: %v", subject, err)
			}
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
