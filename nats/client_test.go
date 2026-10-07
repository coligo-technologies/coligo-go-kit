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

func TestCloseWaitsForConcurrentHandlerReply(t *testing.T) {
	s, url := testutil.StartServer(t)
	defer s.Shutdown()
	client, err := kitnats.NewClient(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	requester, err := kitnats.NewClient(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	defer requester.Close()
	started, finish := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-finish:
		default:
			close(finish)
		}
	}()
	_, err = client.SubscribeConcurrent("shutdown", func([]byte) (*kitnats.Response, error) {
		close(started)
		<-finish
		return kitnats.NewResponse(200, "complete", nil), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Flush(); err != nil {
		t.Fatal(err)
	}
	reply := make(chan error, 1)
	go func() {
		response, err := requester.Request("shutdown", nil)
		if err == nil && response.StatusCode != 200 {
			err = fmt.Errorf("unexpected response: %+v", response)
		}
		reply <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}
	closed := make(chan struct{})
	go func() { client.Close(); close(closed) }()
	select {
	case <-closed:
		t.Fatal("Close returned while handler was running")
	case <-time.After(100 * time.Millisecond):
	}
	close(finish)
	select {
	case err := <-reply:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("reply lost during shutdown")
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close did not finish")
	}
}

func TestCloseForcesShutdownForStuckHandler(t *testing.T) {
	s, url := testutil.StartServer(t)
	defer s.Shutdown()
	client, err := kitnats.NewClient(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	requester, err := kitnats.NewClient(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	defer requester.Close()
	started, finish, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	defer close(finish)
	_, err = client.SubscribeConcurrent("stuck", func([]byte) (*kitnats.Response, error) {
		defer close(finished)
		close(started)
		<-finish
		return kitnats.NewResponse(200, "complete", nil), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Flush(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() { _, _ = requester.RequestContext(ctx, "stuck", nil) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}
	closed := make(chan struct{})
	begin := time.Now()
	go func() { client.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not enforce its timeout")
	}
	if elapsed := time.Since(begin); elapsed < 2500*time.Millisecond {
		t.Fatalf("Close returned too early: %v", elapsed)
	}
	select {
	case <-finished:
		t.Fatal("Close cancelled handler work")
	default:
	}
}
