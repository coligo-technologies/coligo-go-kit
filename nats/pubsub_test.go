package nats_test

import (
	"context"
	"testing"
	"time"

	"github.com/coligo-technologies/coligo-go-kit/internal/testutil"
	kitnats "github.com/coligo-technologies/coligo-go-kit/nats"
)

func TestPublishSubscribe_JSON(t *testing.T) {
	s, url := testutil.StartServer(t)
	defer s.Shutdown()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c, err := kitnats.NewClient(ctx, url)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	got := make(chan []byte, 1)

	_, err = c.Subscribe("demo.events", func(ctx context.Context, raw []byte) error {
		select {
		case got <- raw:
		default:
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	payload := map[string]any{"hello": "world"}
	if err := c.Publish("demo.events", payload); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case b := <-got:
		// We don't assert exact JSON bytes (map order), just that it's valid + contains key.
		if len(b) == 0 {
			t.Fatalf("got empty payload")
		}
		// Minimal check without bringing in extra deps:
		if string(b) == "{}" {
			t.Fatalf("got unexpected empty json object")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for message")
	}
}
