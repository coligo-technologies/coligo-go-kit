package nats_test

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/coligo-technologies/coligo-go-kit/internal/testutil"
	kitnats "github.com/coligo-technologies/coligo-go-kit/nats"
	"testing"
	"time"
)

func TestStreamHistoryFilteringRetentionAndPurge(t *testing.T) {
	s, url := testutil.StartServer(t)
	defer s.Shutdown()
	c, err := kitnats.NewClient(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	js, err := c.CreateJetStream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	config := kitnats.StreamConfig{Name: "Events", Subjects: []string{"events.>"}, MaxMsgs: 100}
	if err = js.EnsureStream(ctx, config); err != nil {
		t.Fatal(err)
	}
	config.MaxMsgs = 1
	if err = js.EnsureStream(ctx, config); err != nil {
		t.Fatal(err)
	}
	info, err := js.Context().StreamInfo("Events")
	if err != nil {
		t.Fatal(err)
	}
	if info.Config.MaxMsgs != 100 {
		t.Fatal("EnsureStream changed existing retention")
	}
	for _, payload := range []string{`not-json`, `{"id":1}`, `{"id":2}`} {
		if _, err = js.Context().Publish("events.test", []byte(payload)); err != nil {
			t.Fatal(err)
		}
	}
	var values, seqs []uint64
	err = js.ReadHistory(ctx, "Events", "events.test", kitnats.HistoryQuery{Limit: 2, IdleTimeout: time.Millisecond * 100}, func(message kitnats.StoredMessage) bool {
		var value struct {
			ID uint64 `json:"id"`
		}
		if json.Unmarshal(message.Data, &value) != nil {
			return false
		}
		values = append(values, value.ID)
		seqs = append(seqs, message.Sequence)
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 2 || values[0] != 1 || values[1] != 2 || seqs[0] != 2 || seqs[1] != 3 {
		t.Fatalf("unexpected history: %v, %v", values, seqs)
	}
	var filtered []uint64
	err = js.ReadHistory(ctx, "Events", "events.test", kitnats.HistoryQuery{StartSequence: 3, Limit: 1}, func(message kitnats.StoredMessage) bool { filtered = append(filtered, message.Sequence); return true })
	if err != nil || len(filtered) != 1 || filtered[0] != 3 {
		t.Fatalf("sequence filtering: %v, %v", filtered, err)
	}
	if err = js.PurgeStream(ctx, "Events"); err != nil {
		t.Fatal(err)
	}
	info, err = js.Context().StreamInfo("Events")
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != 0 || info.Config.MaxMsgs != 100 {
		t.Fatal("purge did not preserve the empty stream")
	}
	deadline, cancelDeadline := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancelDeadline()
	err = js.ReadHistory(deadline, "Events", "events.test", kitnats.HistoryQuery{IdleTimeout: time.Second}, func(kitnats.StoredMessage) bool { return true })
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("history ignored deadline: %v", err)
	}
}

func TestHistoryCleanupHonorsCallerDeadline(t *testing.T) {
	s, url := testutil.StartServer(t)
	defer s.Shutdown()
	c, err := kitnats.NewClient(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	js, err := c.CreateJetStream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err = js.EnsureStream(context.Background(), kitnats.StreamConfig{Name: "Cleanup", Subjects: []string{"cleanup"}}); err != nil {
		t.Fatal(err)
	}
	if _, err = js.Context().Publish("cleanup", []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	started := time.Now()
	err = js.ReadHistory(ctx, "Cleanup", "cleanup", kitnats.HistoryQuery{Limit: 1}, func(kitnats.StoredMessage) bool {
		// Lose the broker after reading, before deleting the temporary consumer.
		s.Shutdown()
		s.WaitForShutdown()
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("history cleanup exceeded the caller's deadline: %v", elapsed)
	}
}
