package nats

import (
	"context"
	"errors"
	"fmt"
	"sync"

	nc "github.com/nats-io/nats.go"
)

type KV struct {
	kv nc.KeyValue

	mu   sync.Mutex
	revs map[string]uint64 // key -> last known revision
}

func (js *JetStream) KV(ctx context.Context, bucket string) (*KV, error) {
	if js == nil || js.js == nil {
		return nil, errors.New("jetstream is nil")
	}
	if bucket == "" {
		return nil, errors.New("bucket must not be empty")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	kv, err := js.js.KeyValue(bucket)
	if err != nil {
		// Auto-create if missing.
		if errors.Is(err, nc.ErrBucketNotFound) {
			cfg := &nc.KeyValueConfig{
				Bucket:   bucket,
				History:  1,
				Storage:  nc.FileStorage,
				Replicas: 1,
			}
			kv, err = js.js.CreateKeyValue(cfg)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("open/create kv bucket %q: %w", bucket, err)
	}

	return &KV{
		kv:   kv,
		revs: make(map[string]uint64),
	}, nil
}

func (k *KV) Save(ctx context.Context, key string, value []byte) error {
	if k == nil || k.kv == nil {
		return errors.New("kv is nil")
	}
	if key == "" {
		return errors.New("key must not be empty")
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	rev, err := k.kv.Put(key, value)
	if err != nil {
		return fmt.Errorf("kv save %q: %w", key, err)
	}

	k.mu.Lock()
	k.revs[key] = rev
	k.mu.Unlock()
	return nil
}

func (k *KV) Update(ctx context.Context, key string, value []byte) error {
	if k == nil || k.kv == nil {
		return errors.New("kv is nil")
	}
	if key == "" {
		return errors.New("key must not be empty")
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	// Use last known revision; if unknown, fetch once.
	var last uint64
	k.mu.Lock()
	last = k.revs[key]
	k.mu.Unlock()

	if last == 0 {
		entry, err := k.kv.Get(key)
		if err != nil {
			// If missing, fall back to Save (creates/overwrites).
			if errors.Is(err, nc.ErrKeyNotFound) {
				return k.Save(ctx, key, value)
			}
			return fmt.Errorf("kv update %q: load current revision: %w", key, err)
		}
		last = entry.Revision()
	}

	rev, err := k.kv.Update(key, value, last)
	if err != nil {
		// Single-writer assumption: refresh revision and retry once.
		entry, gerr := k.kv.Get(key)
		if gerr == nil {
			last2 := entry.Revision()
			if last2 != last {
				if rev2, err2 := k.kv.Update(key, value, last2); err2 == nil {
					rev, err = rev2, nil
					last = last2
				}
			}
		}
	}
	if err != nil {
		return fmt.Errorf("kv update %q: %w", key, err)
	}

	k.mu.Lock()
	k.revs[key] = rev
	k.mu.Unlock()
	return nil
}

func (k *KV) Delete(ctx context.Context, key string) error {
	if k == nil || k.kv == nil {
		return errors.New("kv is nil")
	}
	if key == "" {
		return errors.New("key must not be empty")
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	if err := k.kv.Delete(key); err != nil {
		return fmt.Errorf("kv delete %q: %w", key, err)
	}

	k.mu.Lock()
	delete(k.revs, key)
	k.mu.Unlock()
	return nil
}

func (k *KV) Load(ctx context.Context, key string) ([]byte, error) {
	if k == nil || k.kv == nil {
		return nil, errors.New("kv is nil")
	}
	if key == "" {
		return nil, errors.New("key must not be empty")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	entry, err := k.kv.Get(key)
	if err != nil {
		return nil, fmt.Errorf("kv load %q: %w", key, err)
	}

	k.mu.Lock()
	k.revs[key] = entry.Revision()
	k.mu.Unlock()

	// Copy to decouple from underlying buffer usage.
	val := append([]byte(nil), entry.Value()...)
	return val, nil
}

func (k *KV) LoadAll(ctx context.Context) (map[string][]byte, error) {
	if k == nil || k.kv == nil {
		return nil, errors.New("kv is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	keys, err := k.kv.Keys()
	if err != nil {
		// Empty bucket is not an error; return empty result.
		if errors.Is(err, nc.ErrNoKeysFound) {
			return map[string][]byte{}, nil
		}
		return nil, fmt.Errorf("kv loadAll: list keys: %w", err)
	}

	out := make(map[string][]byte, len(keys))
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if key == "" {
			continue
		}

		entry, err := k.kv.Get(key)
		if err != nil {
			if errors.Is(err, nc.ErrKeyNotFound) {
				continue
			}
			return nil, fmt.Errorf("kv loadAll %q: %w", key, err)
		}

		if entry.Operation() == nc.KeyValueDelete || entry.Operation() == nc.KeyValuePurge {
			continue
		}

		k.mu.Lock()
		k.revs[key] = entry.Revision()
		k.mu.Unlock()

		out[key] = append([]byte(nil), entry.Value()...)
	}

	return out, nil
}
