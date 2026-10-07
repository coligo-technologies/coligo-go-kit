package nats

import (
	"context"
	"testing"

	"github.com/coligo-technologies/coligo-go-kit/internal/testutil"
)

func TestUnsubscribeReleasesTrackedSubscriptions(t *testing.T) {
	server, url := testutil.StartServer(t)
	defer server.Shutdown()
	client, err := NewClient(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	retained, err := client.SubscribeEvent("retained", func(Message) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	for range 100 {
		sub, err := client.SubscribeEvent("temporary", func(Message) error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		if err := sub.Unsubscribe(); err != nil {
			t.Fatal(err)
		}
	}
	client.mu.Lock()
	tracked := len(client.subs)
	kept := tracked == 1 && client.subs[0] == retained.(subscription).s
	client.mu.Unlock()
	if !kept {
		t.Fatalf("expected only the active subscription to remain, tracked %d", tracked)
	}
}
