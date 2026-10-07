package nats

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	nc "github.com/nats-io/nats.go"
)

// StreamConfig contains the retention settings used by the stack's event streams.
type StreamConfig struct {
	Name     string
	Subjects []string
	MaxAge   time.Duration
	MaxMsgs  int64
	MaxBytes int64
}

// EnsureStream creates a file-backed stream if it is absent. Existing retention
// settings are preserved, since changing them can discard stored data.
func (j *JetStream) EnsureStream(ctx context.Context, config StreamConfig) error {
	if _, err := j.js.StreamInfo(config.Name, nc.Context(ctx)); err == nil {
		return nil
	} else if !errors.Is(err, nc.ErrStreamNotFound) {
		return transportError(err)
	}
	_, err := j.js.AddStream(&nc.StreamConfig{Name: config.Name, Subjects: config.Subjects, MaxAge: config.MaxAge, MaxMsgs: config.MaxMsgs, MaxBytes: config.MaxBytes, Storage: nc.FileStorage}, nc.Context(ctx))
	return transportError(err)
}

func (j *JetStream) PurgeStream(ctx context.Context, name string) error {
	stream, err := j.api.Stream(ctx, name)
	if err != nil {
		return transportError(err)
	}
	return transportError(stream.Purge(ctx))
}

type HistoryQuery struct {
	StartTime     time.Time
	StartSequence uint64
	Limit         int
	IdleTimeout   time.Duration
}

type StoredMessage struct {
	Data     []byte
	Sequence uint64
}

// ReadHistory visits matching messages until the accepted-message limit or idle timeout.
// The caller's context limits the entire query, including consumer creation.
func (j *JetStream) ReadHistory(ctx context.Context, stream, subject string, query HistoryQuery, accept func(StoredMessage) bool) error {
	config := &nc.ConsumerConfig{FilterSubject: subject, DeliverSubject: nc.NewInbox(), AckPolicy: nc.AckNonePolicy, InactiveThreshold: 5 * time.Second}
	if !query.StartTime.IsZero() {
		config.DeliverPolicy, config.OptStartTime = nc.DeliverByStartTimePolicy, &query.StartTime
	} else if query.StartSequence > 0 {
		config.DeliverPolicy, config.OptStartSeq = nc.DeliverByStartSequencePolicy, query.StartSequence
	}
	consumer, err := j.js.AddConsumer(stream, config, nc.Context(ctx))
	if err != nil {
		return transportError(err)
	}
	// Own the consumer explicitly so deletion uses the caller's deadline.
	defer func() {
		if err := j.js.DeleteConsumer(stream, consumer.Name, nc.Context(ctx)); err != nil {
			log.Printf("nats: delete history consumer failed: %v", err)
		}
	}()
	sub, err := j.js.SubscribeSync(subject, nc.Bind(stream, consumer.Name), nc.Context(ctx))
	if err != nil {
		return transportError(err)
	}
	defer sub.Unsubscribe()
	idle := query.IdleTimeout
	if idle <= 0 {
		idle = 500 * time.Millisecond
	}
	accepted := 0
	for query.Limit <= 0 || accepted < query.Limit {
		wait, cancel := context.WithTimeout(ctx, idle)
		message, err := sub.NextMsgWithContext(wait)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, nc.ErrTimeout) {
				break
			}
			return transportError(err)
		}
		metadata, err := message.Metadata()
		if err != nil {
			return fmt.Errorf("read stream metadata: %w", err)
		}
		if accept(StoredMessage{Data: message.Data, Sequence: metadata.Sequence.Stream}) {
			accepted++
		}
	}
	return nil
}
