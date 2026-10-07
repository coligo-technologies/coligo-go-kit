package nats

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	nc "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

var (
	ErrKVKeyNotFound      = errors.New("nats kv key not found")
	ErrKVRevisionConflict = errors.New("nats kv revision conflict")
)

type KVEntry struct {
	Value    []byte
	Revision uint64
}

type KVStatus struct {
	Bucket  string
	History int64
	TTL     time.Duration
}

type KV struct {
	kv jetstream.KeyValue
	js jetstream.JetStream

	mu   sync.Mutex
	revs map[string]uint64 // key -> last known revision
}

func (js *JetStream) KV(ctx context.Context, bucket string) (*KV, error) {
	if js == nil || js.api == nil {
		return nil, errors.New("jetstream is nil")
	}
	if bucket == "" {
		return nil, errors.New("bucket must not be empty")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	kv, err := js.api.KeyValue(ctx, bucket)
	if err != nil {
		// Auto-create if missing.
		if errors.Is(err, jetstream.ErrBucketNotFound) {
			cfg := jetstream.KeyValueConfig{
				Bucket:   bucket,
				History:  1,
				Storage:  jetstream.FileStorage,
				Replicas: 1,
			}
			kv, err = js.api.CreateKeyValue(ctx, cfg)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("open/create kv bucket %q: %w", bucket, transportError(err))
	}

	return &KV{
		kv:   kv,
		js:   js.api,
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

	rev, err := k.kv.Put(ctx, key, value)
	if err != nil {
		return fmt.Errorf("kv save %q: %w", key, transportError(err))
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
		entry, err := k.kv.Get(ctx, key)
		if err != nil {
			// If missing, fall back to Save (creates/overwrites).
			if errors.Is(err, jetstream.ErrKeyNotFound) {
				return k.Save(ctx, key, value)
			}
			return fmt.Errorf("kv update %q: load current revision: %w", key, transportError(err))
		}
		last = entry.Revision()
	}

	rev, err := k.kv.Update(ctx, key, value, last)
	if err != nil {
		// Single-writer assumption: refresh revision and retry once.
		entry, gerr := k.kv.Get(ctx, key)
		if gerr == nil {
			last2 := entry.Revision()
			if last2 != last {
				if rev2, err2 := k.kv.Update(ctx, key, value, last2); err2 == nil {
					rev, err = rev2, nil
					last = last2
				}
			}
		}
	}
	if err != nil {
		return fmt.Errorf("kv update %q: %w", key, transportError(err))
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

	if err := k.kv.Delete(ctx, key); err != nil {
		return fmt.Errorf("kv delete %q: %w", key, transportError(err))
	}

	k.mu.Lock()
	delete(k.revs, key)
	k.mu.Unlock()
	return nil
}

// Clear removes all values and history while keeping the bucket configuration.
// Callers must stop writes to the bucket before clearing it.
func (k *KV) Clear(ctx context.Context) error {
	if k == nil || k.kv == nil || k.js == nil {
		return errors.New("kv is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	stream, err := k.js.Stream(ctx, "KV_"+k.kv.Bucket())
	if err != nil {
		return fmt.Errorf("kv clear %q: %w", k.kv.Bucket(), transportError(err))
	}
	if err := stream.Purge(ctx); err != nil {
		return fmt.Errorf("kv clear %q: %w", k.kv.Bucket(), transportError(err))
	}
	k.mu.Lock()
	clear(k.revs)
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

	entry, err := k.kv.Get(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) || errors.Is(err, jetstream.ErrKeyDeleted) {
			err = errors.Join(ErrKVKeyNotFound, nc.ErrKeyNotFound, err)
		}
		return nil, fmt.Errorf("kv load %q: %w", key, transportError(err))
	}

	k.mu.Lock()
	k.revs[key] = entry.Revision()
	k.mu.Unlock()

	// Copy to decouple from underlying buffer usage.
	val := append([]byte(nil), entry.Value()...)
	return val, nil
}

// LoadEntry loads a value together with its server-assigned revision.
func (k *KV) LoadEntry(ctx context.Context, key string) (KVEntry, error) {
	if k == nil || k.kv == nil {
		return KVEntry{}, errors.New("kv is nil")
	}
	if key == "" {
		return KVEntry{}, errors.New("key must not be empty")
	}
	if err := ctx.Err(); err != nil {
		return KVEntry{}, err
	}

	entry, err := k.kv.Get(ctx, key)
	if errors.Is(err, jetstream.ErrKeyNotFound) || errors.Is(err, jetstream.ErrKeyDeleted) {
		return KVEntry{}, fmt.Errorf("kv load %q: %w", key, ErrKVKeyNotFound)
	}
	if err != nil {
		return KVEntry{}, fmt.Errorf("kv load %q: %w", key, transportError(err))
	}

	return KVEntry{
		Value:    append([]byte(nil), entry.Value()...),
		Revision: entry.Revision(),
	}, nil
}

// Create creates a key only when it does not already exist.
func (k *KV) Create(ctx context.Context, key string, value []byte) (uint64, error) {
	if k == nil || k.kv == nil {
		return 0, errors.New("kv is nil")
	}
	if key == "" {
		return 0, errors.New("key must not be empty")
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	revision, err := k.kv.Create(ctx, key, value)
	if isKVRevisionConflict(err) {
		return 0, fmt.Errorf("kv create %q: %w", key, ErrKVRevisionConflict)
	}
	if err != nil {
		return 0, fmt.Errorf("kv create %q: %w", key, transportError(err))
	}
	return revision, nil
}

// UpdateRevision updates a key only when expectedRevision is still current.
func (k *KV) UpdateRevision(ctx context.Context, key string, value []byte, expectedRevision uint64) (uint64, error) {
	if k == nil || k.kv == nil {
		return 0, errors.New("kv is nil")
	}
	if key == "" {
		return 0, errors.New("key must not be empty")
	}
	if expectedRevision == 0 {
		return 0, errors.New("expected revision must be positive")
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	revision, err := k.kv.Update(ctx, key, value, expectedRevision)
	if isKVRevisionConflict(err) {
		return 0, fmt.Errorf("kv update %q: %w", key, ErrKVRevisionConflict)
	}
	if err != nil {
		return 0, fmt.Errorf("kv update %q: %w", key, transportError(err))
	}
	return revision, nil
}

// Status returns the bucket properties needed for readiness checks.
func (k *KV) Status(ctx context.Context) (KVStatus, error) {
	if k == nil || k.kv == nil {
		return KVStatus{}, errors.New("kv is nil")
	}
	if err := ctx.Err(); err != nil {
		return KVStatus{}, err
	}

	status, err := k.kv.Status(ctx)
	if err != nil {
		return KVStatus{}, fmt.Errorf("kv status: %w", transportError(err))
	}
	return KVStatus{
		Bucket:  status.Bucket(),
		History: status.History(),
		TTL:     status.TTL(),
	}, nil
}

func isKVRevisionConflict(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, jetstream.ErrKeyExists) {
		return true
	}
	var jetStreamError jetstream.JetStreamError
	return errors.As(err, &jetStreamError) &&
		jetStreamError.APIError() != nil &&
		jetStreamError.APIError().ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence
}

func (k *KV) LoadAll(ctx context.Context) (map[string][]byte, error) {
	if k == nil || k.kv == nil {
		return nil, errors.New("kv is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	keys, err := k.kv.Keys(ctx)
	if err != nil {
		// Empty bucket is not an error; return empty result.
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return map[string][]byte{}, nil
		}
		return nil, fmt.Errorf("kv loadAll: list keys: %w", transportError(err))
	}

	out := make(map[string][]byte, len(keys))
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if key == "" {
			continue
		}

		entry, err := k.kv.Get(ctx, key)
		if err != nil {
			if errors.Is(err, jetstream.ErrKeyNotFound) {
				continue
			}
			return nil, fmt.Errorf("kv loadAll %q: %w", key, transportError(err))
		}

		if entry.Operation() == jetstream.KeyValueDelete || entry.Operation() == jetstream.KeyValuePurge {
			continue
		}

		k.mu.Lock()
		k.revs[key] = entry.Revision()
		k.mu.Unlock()

		out[key] = append([]byte(nil), entry.Value()...)
	}

	return out, nil
}
