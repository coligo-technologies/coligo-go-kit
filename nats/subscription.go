package nats

import nc "github.com/nats-io/nats.go"

type Subscription interface {
	Unsubscribe() error
}

type subscription struct {
	s *nc.Subscription
}

func (s subscription) Unsubscribe() error {
	if s.s == nil {
		return nil
	}
	return s.s.Unsubscribe()
}
