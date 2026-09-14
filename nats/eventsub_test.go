package nats_test

import (
	"context"
	"testing"
	"time"

	"github.com/coligo-technologies/coligo-go-kit/internal/testutil"
	kitnats "github.com/coligo-technologies/coligo-go-kit/nats"
	natsgo "github.com/nats-io/nats.go"
)

func TestSubscribeEventProvidesMatchedSubjectAndPayload(t *testing.T) {
	server, url := testutil.StartServer(t)
	defer server.Shutdown()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := kitnats.NewClient(ctx, url)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	received := make(chan kitnats.Message, 1)
	_, err = client.SubscribeEvent("doc.>", func(message kitnats.Message) error {
		received <- message
		return nil
	})
	if err != nil {
		t.Fatalf("SubscribeEvent: %v", err)
	}
	if err := client.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	publisher, err := natsgo.Connect(url)
	if err != nil {
		t.Fatalf("Connect publisher: %v", err)
	}
	defer publisher.Close()
	if err := publisher.Publish("doc.opcua.temperature", []byte(`{"value":21.5}`)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if err := publisher.Flush(); err != nil {
		t.Fatalf("Flush publisher: %v", err)
	}

	select {
	case message := <-received:
		if message.Subject != "doc.opcua.temperature" {
			t.Fatalf("unexpected subject: got %q", message.Subject)
		}
		if string(message.Data) != `{"value":21.5}` {
			t.Fatalf("unexpected payload: got %q", message.Data)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for event")
	}
}

func TestClientConnected(t *testing.T) {
	server, url := testutil.StartServer(t)
	defer server.Shutdown()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := kitnats.NewClient(ctx, url)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if !client.Connected() {
		t.Fatal("client should be connected")
	}

	client.Close()
	if client.Connected() {
		t.Fatal("closed client should not be connected")
	}
	if err := client.Flush(); err == nil {
		t.Fatal("Flush should fail after Close")
	}
}
