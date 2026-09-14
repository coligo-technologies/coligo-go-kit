package nats_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/coligo-technologies/coligo-go-kit/internal/testutil"
	kitnats "github.com/coligo-technologies/coligo-go-kit/nats"
)

func TestClientOperationsAreSafeDuringClose(t *testing.T) {
	server, url := testutil.StartServer(t)
	defer server.Shutdown()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := kitnats.NewClient(ctx, url)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	_, err = client.Subscribe("concurrent.request", func([]byte) (*kitnats.Response, error) {
		return kitnats.NewResponse(200, "ok", nil), nil
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := client.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	notification, err := kitnats.NewNotification(
		"test",
		kitnats.NotificationLevel.Info,
		nil,
		kitnats.NotificationOptions{Group: "test", Service: "test"},
	)
	if err != nil {
		t.Fatalf("NewNotification: %v", err)
	}

	started := make(chan struct{}, 5)
	var operations sync.WaitGroup
	operations.Add(5)
	go func() {
		defer operations.Done()
		started <- struct{}{}
		for range 1_000 {
			_ = client.Connected()
			_ = client.Flush()
		}
	}()
	go func() {
		defer operations.Done()
		started <- struct{}{}
		for range 1_000 {
			_ = client.PublishNotification("notifications.test", notification)
		}
	}()
	go func() {
		defer operations.Done()
		started <- struct{}{}
		for range 100 {
			_, _ = client.CreateJetStream(ctx)
		}
	}()
	go func() {
		defer operations.Done()
		started <- struct{}{}
		for index := range 100 {
			subscription, err := client.SubscribeEvent(fmt.Sprintf("event.%d", index), func(kitnats.Message) error {
				return nil
			})
			if err == nil {
				_ = subscription.Unsubscribe()
			}
		}
	}()
	go func() {
		defer operations.Done()
		started <- struct{}{}
		for range 20 {
			_, _ = client.Request("concurrent.request", map[string]any{"test": true})
		}
	}()

	for range 5 {
		<-started
	}
	client.Close()
	operations.Wait()

	if client.Connected() {
		t.Fatal("closed client should not be connected")
	}
	if err := client.Flush(); err == nil {
		t.Fatal("Flush should fail after Close")
	}
}
