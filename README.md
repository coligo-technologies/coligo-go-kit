# coligo-go-kit

A curated Go kit for reusable components used across COLIGO projects.

## Packages

- `nats` – helpers and conventions built on top of the official NATS Go client
  (including core pub/sub and JetStream Key-Value)

## Goals

- Reduce repetitive boilerplate
- Expose small, focused abstractions
- Stay close to upstream libraries

## Non-Goals

- Replacing upstream clients
- Abstracting infrastructure
- Becoming a generic utils dump

## Usage

### Connect

```go
ctx := context.Background()

nc, err := nats.NewClient(ctx, "nats://localhost:4222")
if err != nil {
	log.Fatalf("Failed to connect to NATS: %v", err)
}
defer nc.Close()
```

### Publish JSON (core NATS)

```go
_ = nc.Publish("events.user.created", map[string]any{
	"id": "123",
})
```

### Subscribe (core NATS)

```go
_, _ = nc.Subscribe("events.>", func(ctx context.Context, raw []byte) error {
	// raw is the message payload
	return nil
})
```

### JetStream KV

```go
js, err := nc.CreateJetStream(ctx)
if err != nil {
	log.Fatalf("Failed to create JetStream: %v", err)
}

kv, err := js.KV(ctx, "config")
if err != nil {
	log.Fatalf("Failed to open KV bucket: %v", err)
}

_ = kv.Save(ctx, "feature_flags", []byte(`{"a":true}`))

b, _ := kv.Load(ctx, "feature_flags")
_ = kv.Update(ctx, "feature_flags", []byte(`{"a":false}`))
_ = kv.Delete(ctx, "feature_flags")
```
