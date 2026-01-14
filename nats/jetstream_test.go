package nats_test

import (
	"context"
	"testing"
	"time"

	"github.com/coligo-technologies/coligo-go-kit/internal/testutil"
	kitnats "github.com/coligo-technologies/coligo-go-kit/nats"
)

func TestJetStream_Context(t *testing.T) {
	s, url := testutil.StartServer(t)
	defer s.Shutdown()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c, err := kitnats.NewClient(ctx, url)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	js, err := c.CreateJetStream(ctx)
	if err != nil {
		t.Fatalf("CreateJetStream: %v", err)
	}

	if js.Context() == nil {
		t.Fatalf("expected non-nil underlying JetStreamContext, got nil")
	}
}
