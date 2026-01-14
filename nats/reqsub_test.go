package nats_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/coligo-technologies/coligo-go-kit/internal/testutil"
	kitnats "github.com/coligo-technologies/coligo-go-kit/nats"
)

func TestRequestSubscribe_JSON(t *testing.T) {
	s, url := testutil.StartServer(t)
	defer s.Shutdown()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c, err := kitnats.NewClient(ctx, url)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	_, err = c.Subscribe("demo.events", func(raw []byte) (*kitnats.Response, error) {
		var body any
		if err := json.Unmarshal(raw, &body); err != nil {
			return kitnats.BadRequest("invalid JSON"), err
		}

		return kitnats.NewResponse(
			200,
			"echo",
			map[string]any{"echo": body},
		), nil
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	payload := map[string]any{"hello": "world"}
	resp, err := c.Request("demo.events", payload)
	if err != nil {
		t.Fatalf("Request: %v", err)
	}

	if resp.StatusCode != 200 {
		t.Fatalf("unexpected status: got %d, want %d", resp.StatusCode, 200)
	}

	// Validate echo envelope shape
	data, ok := resp.Data.(map[string]any)
	if !ok {
		t.Fatalf("unexpected data type: got %T, want map[string]any", resp.Data)
	}

	echo, ok := data["echo"]
	if !ok {
		t.Fatalf("missing data.echo")
	}

	echoMap, ok := echo.(map[string]any)
	if !ok {
		t.Fatalf("unexpected echo type: got %T, want map[string]any", echo)
	}

	if got := echoMap["hello"]; got != "world" {
		t.Fatalf("unexpected echo.hello: got %#v, want %q", got, "world")
	}
}
