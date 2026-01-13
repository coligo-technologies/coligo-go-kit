package natskit

import (
	"context"
	"math/rand"
	"time"
)

type Backoff struct {
	Min    time.Duration
	Max    time.Duration
	Factor float64
	Jitter float64 // 0.0 - 1.0
}

func DefaultBackoff() Backoff {
	return Backoff{
		Min:    200 * time.Millisecond,
		Max:    5 * time.Second,
		Factor: 1.6,
		Jitter: 0.20,
	}
}

func Sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func (b Backoff) Duration(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	// exponential growth
	d := float64(b.Min)
	for i := 0; i < attempt; i++ {
		d *= b.Factor
		if d > float64(b.Max) {
			d = float64(b.Max)
			break
		}
	}

	// jitter
	if b.Jitter > 0 {
		j := (rand.Float64()*2 - 1) * b.Jitter // [-J, +J]
		d = d * (1 + j)
		if d < float64(10*time.Millisecond) {
			d = float64(10 * time.Millisecond)
		}
	}

	return time.Duration(d)
}
