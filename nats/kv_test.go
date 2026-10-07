package nats_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/coligo-technologies/coligo-go-kit/internal/testutil"
	kitnats "github.com/coligo-technologies/coligo-go-kit/nats"
	"github.com/nats-io/nats.go"
)

func TestKV_SaveLoadUpdateDelete(t *testing.T) {
	s, url := testutil.StartServer(t)
	defer s.Shutdown()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
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

	kv, err := js.KV(ctx, "test_bucket")
	if err != nil {
		t.Fatalf("KV: %v", err)
	}

	// Save
	v1 := []byte("one")
	if err := kv.Save(ctx, "k", v1); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// Load
	got, err := kv.Load(ctx, "k")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !bytes.Equal(got, v1) {
		t.Fatalf("Load mismatch: got %q want %q", got, v1)
	}

	// ---- LoadAll (single key) ----
	all, err := kv.LoadAll(ctx)
	if err != nil {
		t.Fatalf("LoadAll: %v", err)
	}
	if got := all["k"]; !bytes.Equal(got, v1) {
		t.Fatalf("LoadAll mismatch: got %q want %q", got, v1)
	}

	// Update
	v2 := []byte("two")
	if err := kv.Update(ctx, "k", v2); err != nil {
		t.Fatalf("Update: %v", err)
	}
	got, err = kv.Load(ctx, "k")
	if err != nil {
		t.Fatalf("Load after Update: %v", err)
	}
	if !bytes.Equal(got, v2) {
		t.Fatalf("Load after Update mismatch: got %q want %q", got, v2)
	}

	// ---- LoadAll after update ----
	all, err = kv.LoadAll(ctx)
	if err != nil {
		t.Fatalf("LoadAll after update: %v", err)
	}
	if got := all["k"]; !bytes.Equal(got, v2) {
		t.Fatalf("LoadAll after update mismatch: got %q want %q", got, v2)
	}

	// Delete
	if err := kv.Delete(ctx, "k"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, err = kv.Load(ctx, "k")
	if err == nil {
		t.Fatalf("expected error after delete, got nil")
	}

	// ---- LoadAll after delete ----
	all, err = kv.LoadAll(ctx)
	if err != nil {
		t.Fatalf("LoadAll after delete: %v", err)
	}
	if _, ok := all["k"]; ok {
		t.Fatalf("LoadAll should not include deleted key")
	}
}

func TestKVRevisionAwareOperations(t *testing.T) {
	s, url := testutil.StartServer(t)
	defer s.Shutdown()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
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
	kv, err := js.KV(ctx, "revision_bucket")
	if err != nil {
		t.Fatalf("KV: %v", err)
	}

	if _, err := kv.LoadEntry(ctx, "missing"); !errors.Is(err, kitnats.ErrKVKeyNotFound) {
		t.Fatalf("LoadEntry missing error: got %v", err)
	}

	revision, err := kv.Create(ctx, "k", []byte("one"))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if revision == 0 {
		t.Fatal("Create returned zero revision")
	}
	if _, err := kv.Create(ctx, "k", []byte("duplicate")); !errors.Is(err, kitnats.ErrKVRevisionConflict) {
		t.Fatalf("duplicate Create error: got %v", err)
	}

	entry, err := kv.LoadEntry(ctx, "k")
	if err != nil {
		t.Fatalf("LoadEntry: %v", err)
	}
	if entry.Revision != revision || !bytes.Equal(entry.Value, []byte("one")) {
		t.Fatalf("unexpected entry: %+v", entry)
	}

	if _, err := kv.UpdateRevision(ctx, "k", []byte("stale"), revision+1); !errors.Is(err, kitnats.ErrKVRevisionConflict) {
		t.Fatalf("stale UpdateRevision error: got %v", err)
	}
	updatedRevision, err := kv.UpdateRevision(ctx, "k", []byte("two"), revision)
	if err != nil {
		t.Fatalf("UpdateRevision: %v", err)
	}
	if updatedRevision <= revision {
		t.Fatalf("revision did not advance: got %d after %d", updatedRevision, revision)
	}

	status, err := kv.Status(ctx)
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if status.Bucket != "revision_bucket" || status.History != 1 || status.TTL != 0 {
		t.Fatalf("unexpected status: %+v", status)
	}
}

func TestKV_UpdateWithoutPriorLoadStillWorks(t *testing.T) {
	s, url := testutil.StartServer(t)
	defer s.Shutdown()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
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

	kv, err := js.KV(ctx, "test_bucket2")
	if err != nil {
		t.Fatalf("KV: %v", err)
	}

	// Create value without loading into revision cache.
	if err := kv.Save(ctx, "k", []byte("v1")); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// New KV instance (simulates "revision cache empty").
	kv2, err := js.KV(ctx, "test_bucket2")
	if err != nil {
		t.Fatalf("KV2: %v", err)
	}

	if err := kv2.Update(ctx, "k", []byte("v2")); err != nil {
		t.Fatalf("Update with empty cache: %v", err)
	}

	got, err := kv2.Load(ctx, "k")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !bytes.Equal(got, []byte("v2")) {
		t.Fatalf("Load mismatch: got %q want %q", got, "v2")
	}
}

func TestKVClearPreservesBucketAndAllowsReuse(t *testing.T) {
	s, url := testutil.StartServer(t)
	defer s.Shutdown()
	ctx := context.Background()
	c, err := kitnats.NewClient(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	js, err := c.CreateJetStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	kv, err := js.KV(ctx, "clear_test")
	if err != nil {
		t.Fatal(err)
	}
	if err := kv.Save(ctx, "provider", []byte("old")); err != nil {
		t.Fatal(err)
	}
	before, err := kv.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := kv.Clear(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
	if _, err := kv.Load(ctx, "provider"); err != nil {
		t.Fatalf("cancelled clear removed data: %v", err)
	}
	if err := kv.Clear(ctx); err != nil {
		t.Fatal(err)
	}
	stream, err := js.Context().StreamInfo("KV_clear_test")
	if err != nil {
		t.Fatal(err)
	}
	if stream.State.Msgs != 0 {
		t.Fatalf("clear left %d messages", stream.State.Msgs)
	}
	after, err := kv.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Fatalf("bucket config changed: before=%+v after=%+v", before, after)
	}
	if err := kv.Update(ctx, "provider", []byte("new")); err != nil {
		t.Fatalf("reuse after clear: %v", err)
	}
	value, err := kv.Load(ctx, "provider")
	if err != nil || string(value) != "new" {
		t.Fatalf("got %q, %v", value, err)
	}
}

func TestKVOperationsHonorInFlightDeadlines(t *testing.T) {
	s, url := testutil.StartServer(t)
	defer s.Shutdown()
	client, err := kitnats.NewClient(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	js, err := client.CreateJetStream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	kv, err := js.KV(context.Background(), "deadline_test")
	if err != nil {
		t.Fatal(err)
	}
	if err = kv.Save(context.Background(), "key", []byte("value")); err != nil {
		t.Fatal(err)
	}
	// Keep Core NATS connected but stall the JetStream request subjects. Each
	// operation must obey its own deadline rather than the client's default wait.
	if err = s.DisableJetStream(); err != nil {
		t.Fatal(err)
	}
	stalled, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer stalled.Close()
	for _, subject := range []string{"$JS.API.>", "$KV.>"} {
		if _, err = stalled.Subscribe(subject, func(*nats.Msg) {}); err != nil {
			t.Fatal(err)
		}
	}
	if err = stalled.Flush(); err != nil {
		t.Fatal(err)
	}
	operations := map[string]func(context.Context) error{
		"open":           func(ctx context.Context) error { _, err := js.KV(ctx, "other"); return err },
		"save":           func(ctx context.Context) error { return kv.Save(ctx, "key", nil) },
		"update":         func(ctx context.Context) error { return kv.Update(ctx, "key", nil) },
		"delete":         func(ctx context.Context) error { return kv.Delete(ctx, "key") },
		"clear":          func(ctx context.Context) error { return kv.Clear(ctx) },
		"load":           func(ctx context.Context) error { _, err := kv.Load(ctx, "key"); return err },
		"loadEntry":      func(ctx context.Context) error { _, err := kv.LoadEntry(ctx, "key"); return err },
		"create":         func(ctx context.Context) error { _, err := kv.Create(ctx, "new", nil); return err },
		"updateRevision": func(ctx context.Context) error { _, err := kv.UpdateRevision(ctx, "key", nil, 1); return err },
		"status":         func(ctx context.Context) error { _, err := kv.Status(ctx); return err },
		"loadAll":        func(ctx context.Context) error { _, err := kv.LoadAll(ctx); return err },
	}
	for name, operation := range operations {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- operation(ctx) }()
			select {
			case err := <-done:
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("got %v, want context.DeadlineExceeded", err)
				}
			case <-time.After(time.Second):
				t.Fatal("operation ignored its deadline")
			}
		})
	}
}
