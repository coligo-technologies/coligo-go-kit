package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	kitnats "github.com/coligo-technologies/coligo-go-kit/nats"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	nc, err := kitnats.NewClient(ctx, "nats://localhost:4222")
	if err != nil {
		log.Fatalf("failed to connect to NATS: %v", err)
	}
	defer nc.Close()

	_, err = nc.Subscribe("demo.events", func(raw []byte) (*kitnats.Response, error) {
		log.Printf("got request: %s", string(raw))

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
		log.Fatalf("subscribe failed: %v", err)
	}

	resp, err := nc.Request("demo.events", map[string]any{
		"hello": "world",
	})
	if err != nil {
		log.Fatalf("request failed: %v", err)
	}

	log.Printf(
		"got response: status=%d message=%q data=%v",
		resp.StatusCode,
		resp.Message,
		resp.Data,
	)

	js, err := nc.CreateJetStream(ctx)
	if err != nil {
		log.Fatalf("failed to create JetStream: %v", err)
	}

	kv, err := js.KV(ctx, "demo_bucket")
	if err != nil {
		log.Fatalf("failed to open KV bucket: %v", err)
	}

	if err := kv.Save(ctx, "config", []byte(`{"a":1}`)); err != nil {
		log.Fatalf("kv save failed: %v", err)
	}

	b, err := kv.Load(ctx, "config")
	if err != nil {
		log.Fatalf("kv load failed: %v", err)
	}
	log.Printf("kv loaded: %s", string(b))

	// Give subscription a moment in this toy example.
	time.Sleep(200 * time.Millisecond)
}
