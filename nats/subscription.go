package nats

import (
	"fmt"
	"slices"

	nc "github.com/nats-io/nats.go"
)

type Subscription interface {
	Unsubscribe() error
}

type subscription struct {
	s      *nc.Subscription
	client *Client
}

func (c *Client) subscribe(subject string, handler nc.MsgHandler) (Subscription, error) {
	if c == nil {
		return nil, errClientClosed
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return nil, errClientClosed
	}

	sub, err := c.conn.Subscribe(subject, handler)
	if err != nil {
		return nil, fmt.Errorf("subscribe to %q: %w", subject, err)
	}
	c.subs = append(c.subs, sub)
	return subscription{s: sub, client: c}, nil
}

func (s subscription) Unsubscribe() error {
	if s.s == nil {
		return nil
	}
	err := s.s.Unsubscribe()
	if err == nil {
		s.client.mu.Lock()
		s.client.subs = slices.DeleteFunc(s.client.subs, func(sub *nc.Subscription) bool { return sub == s.s })
		s.client.mu.Unlock()
	}
	return err
}
