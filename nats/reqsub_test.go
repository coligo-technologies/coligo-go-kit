package nats_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/coligo-technologies/coligo-go-kit/internal/testutil"
	kitnats "github.com/coligo-technologies/coligo-go-kit/nats"
	"github.com/nats-io/nats.go"
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

func TestSubscribeConcurrentDoesNotQueueRequests(t *testing.T) {
	s, url := testutil.StartServer(t)
	defer s.Shutdown()
	c, err := kitnats.NewClient(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	started, finish := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-finish:
		default:
			close(finish)
		}
	}()
	_, err = c.SubscribeConcurrent("reset", func(raw []byte) (*kitnats.Response, error) {
		if string(raw) == `"first"` {
			close(started)
			<-finish
			return kitnats.NewResponse(200, "finished", nil), nil
		}
		return kitnats.NewResponse(409, "in progress", nil), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		response, err := c.Request("reset", "first")
		if err == nil && response.StatusCode != 200 {
			err = fmt.Errorf("got status %d, want 200", response.StatusCode)
		}
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first request did not start")
	}
	response, err := c.Request("reset", "second")
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != 409 {
		t.Fatalf("got status %d, want 409", response.StatusCode)
	}
	select {
	case <-done:
		t.Fatal("first request finished before cleanup was released")
	default:
	}
	close(finish)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("first request did not finish after cleanup")
	}
}

func TestSubscribeConcurrentRecoversPanics(t *testing.T) {
	s, url := testutil.StartServer(t)
	defer s.Shutdown()
	c, err := kitnats.NewClient(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_, err = c.SubscribeConcurrent("panic", func([]byte) (*kitnats.Response, error) { panic("boom") })
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}
	response, err := c.Request("panic", nil)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != 500 {
		t.Fatalf("got status %d, want 500", response.StatusCode)
	}
}

func TestRequestContextCancellation(t *testing.T) {
	s, url := testutil.StartServer(t)
	defer s.Shutdown()
	c, err := kitnats.NewClient(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	started, finish := make(chan struct{}), make(chan struct{})
	defer close(finish)
	_, err = c.Subscribe("cancel", func([]byte) (*kitnats.Response, error) {
		close(started)
		<-finish
		return kitnats.NewResponse(200, "complete", nil), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := c.RequestContext(ctx, "cancel", nil); done <- err }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("request ignored cancellation")
	}
}

func TestRequestContextDeadline(t *testing.T) {
	s, url := testutil.StartServer(t)
	defer s.Shutdown()
	c, err := kitnats.NewClient(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	finish := make(chan struct{})
	defer close(finish)
	_, err = c.Subscribe("deadline", func([]byte) (*kitnats.Response, error) {
		<-finish
		return kitnats.NewResponse(200, "complete", nil), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	response, err := c.RequestContext(ctx, "deadline", nil)
	if !errors.Is(err, context.DeadlineExceeded) || response.StatusCode != 504 {
		t.Fatalf("got %+v, %v; want 504 and context.DeadlineExceeded", response, err)
	}
}

func TestRequestContextAllowsLongerDeadline(t *testing.T) {
	s, url := testutil.StartServer(t)
	defer s.Shutdown()
	c, err := kitnats.NewClient(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_, err = c.Subscribe("long", func([]byte) (*kitnats.Response, error) {
		time.Sleep(2200 * time.Millisecond)
		return kitnats.NewResponse(200, "complete", nil), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	response, err := c.RequestContext(ctx, "long", nil)
	if err != nil || response.StatusCode != 200 {
		t.Fatalf("got %+v, %v", response, err)
	}
}

func TestRequestKeepsDefaultTimeout(t *testing.T) {
	s, url := testutil.StartServer(t)
	defer s.Shutdown()
	c, err := kitnats.NewClient(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	finish := make(chan struct{})
	defer close(finish)
	_, err = c.Subscribe("default-timeout", func([]byte) (*kitnats.Response, error) {
		<-finish
		return kitnats.NewResponse(200, "complete", nil), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}
	begin := time.Now()
	response, err := c.Request("default-timeout", nil)
	if !errors.Is(err, nats.ErrTimeout) || response.StatusCode != 504 {
		t.Fatalf("got %+v, %v; want 504 and nats.ErrTimeout", response, err)
	}
	if elapsed := time.Since(begin); elapsed < 1800*time.Millisecond || elapsed > 3*time.Second {
		t.Fatalf("default timeout was %v, want approximately two seconds", elapsed)
	}
}
