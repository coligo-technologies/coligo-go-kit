package nats_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/coligo-technologies/coligo-go-kit/internal/testutil"
	kitnats "github.com/coligo-technologies/coligo-go-kit/nats"
	nats "github.com/nats-io/nats.go"
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

	_, err = c.Subscribe("demo.events", func(ctx context.Context, msg *nats.Msg) error {
		response := map[string]any{"status": "ok", "echo": string(msg.Data)}
		b, _ := json.Marshal(response)
		return msg.Respond(b)
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	payload := map[string]any{"hello": "world"}
	msg, err := c.Request("demo.events", payload)
	if err != nil {
		t.Fatalf("Request: %v", err)
	}

	var resp struct {
		Status string `json:"status"`
		Echo   string `json:"echo"`
	}
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp.Status != "ok" {
		t.Errorf("unexpected status: got %q, want %q", resp.Status, "ok")
	}
	if resp.Echo == "" {
		t.Errorf("unexpected empty echo")
	}
}
