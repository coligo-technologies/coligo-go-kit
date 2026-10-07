package nats_test

import (
	"context"
	"errors"
	"github.com/coligo-technologies/coligo-go-kit/internal/testutil"
	kitnats "github.com/coligo-technologies/coligo-go-kit/nats"
	"testing"
	"time"
)

func TestRequestIntoPreservesIntegerPrecisionAndServiceErrors(t *testing.T) {
	s, url := testutil.StartServer(t)
	defer s.Shutdown()
	client, err := kitnats.NewClient(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	_, err = client.Subscribe("typed", func([]byte) (*kitnats.Response, error) {
		return kitnats.NewResponse(200, "ok", struct {
			ID uint64 `json:"id"`
		}{ID: 9007199254740993}), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Subscribe("conflict", func([]byte) (*kitnats.Response, error) { return kitnats.NewResponse(409, "stale revision", nil), nil })
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Subscribe("empty", func([]byte) (*kitnats.Response, error) { return kitnats.NewResponse(200, "ok", nil), nil })
	if err != nil {
		t.Fatal(err)
	}
	if err = client.Flush(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var result struct {
		ID uint64 `json:"id"`
	}
	if err = client.RequestInto(ctx, "typed", struct{}{}, &result); err != nil {
		t.Fatal(err)
	}
	if result.ID != 9007199254740993 {
		t.Fatalf("integer rounded: %d", result.ID)
	}
	err = client.RequestInto(ctx, "conflict", nil, nil)
	var status *kitnats.StatusError
	if !errors.As(err, &status) || status.StatusCode != 409 || status.Message != "stale revision" {
		t.Fatalf("lost service error: %v", err)
	}
	if err = client.RequestInto(ctx, "empty", nil, nil); err != nil {
		t.Fatal(err)
	}
	if err = client.RequestInto(ctx, "empty", nil, &result); !errors.Is(err, kitnats.ErrNoData) {
		t.Fatalf("got %v, want ErrNoData", err)
	}
	if err = client.RequestInto(ctx, "absent", nil, nil); !errors.Is(err, kitnats.ErrNoResponders) {
		t.Fatalf("got %v, want ErrNoResponders", err)
	}
}
