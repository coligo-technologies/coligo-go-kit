package nats

import (
	"context"
	"errors"
	"fmt"
	nc "github.com/nats-io/nats.go"
)

var (
	ErrTimeout      = errors.New("nats timeout")
	ErrNoResponders = errors.New("nats no responders")
	ErrNoData       = errors.New("no data in NatsResponse")
)

// StatusError is a service response failure, distinct from transport failures.
type StatusError struct {
	StatusCode int
	Message    string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("unexpected status %d: %s", e.StatusCode, e.Message)
}

func transportError(err error) error {
	if errors.Is(err, nc.ErrTimeout) || errors.Is(err, context.DeadlineExceeded) {
		return errors.Join(ErrTimeout, err)
	}
	if errors.Is(err, nc.ErrNoResponders) {
		return errors.Join(ErrNoResponders, err)
	}
	return err
}
