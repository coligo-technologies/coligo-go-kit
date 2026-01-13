package main

import (
	"context"
	"log"
	"time"

	kitnats "github.com/coligo-technologies/coligo-go-kit/nats"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	nc, err := kitnats.NewClient(ctx, "nats://localhost:4222")
	if err != nil {
		log.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()

	_, err = nc.Subscribe("demo.events", func(ctx context.Context, raw []byte) error {
		log.Printf("got message: %s", string(raw))
		return nil
	})
	if err != nil {
		log.Fatalf("subscribe failed: %v", err)
	}

	if err := nc.Publish("demo.events", map[string]any{"hello": "world"}); err != nil {
		log.Fatalf("publish failed: %v", err)
	}

	js, err := nc.CreateJetStream(ctx)
	if err != nil {
		log.Fatalf("Failed to create JetStream: %v", err)
	}

	kv, err := js.KV(ctx, "demo_bucket")
	if err != nil {
		log.Fatalf("Failed to open KV bucket: %v", err)
	}

	if err := kv.Save(ctx, "config", []byte(`{"a":1}`)); err != nil {
		log.Fatalf("kv save failed: %v", err)
	}

	b, err := kv.Load(ctx, "config")
	if err != nil {
		log.Fatalf("kv load failed: %v", err)
	}
	log.Printf("kv loaded: %s", string(b))

	// Give subscription goroutine a moment in this toy example.
	time.Sleep(200 * time.Millisecond)
}
