package nats_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/coligo-technologies/coligo-go-kit/internal/testutil"
	kitnats "github.com/coligo-technologies/coligo-go-kit/nats"
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
