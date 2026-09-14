package nats

import (
	"errors"
	"log"
	"runtime/debug"

	nc "github.com/nats-io/nats.go"
)

// Message is a Core NATS event delivered to a subscription handler.
type Message struct {
	Subject string
	Data    []byte
}

// MessageHandler processes a Core NATS event. Handlers run synchronously on
// the NATS subscription callback and should return quickly.
type MessageHandler func(Message) error

// SubscribeEvent subscribes to fire-and-forget Core NATS messages. Unlike
// Subscribe, it does not publish a response and exposes the matched subject.
func (c *Client) SubscribeEvent(subject string, handler MessageHandler) (Subscription, error) {
	if subject == "" {
		return nil, errors.New("subject must not be empty")
	}
	if handler == nil {
		return nil, errors.New("handler must not be nil")
	}

	return c.subscribe(subject, func(message *nc.Msg) {
		defer func() {
			if recovered := recover(); recovered != nil {
				log.Printf("nats: panic in event handler for %q: %v\n%s", message.Subject, recovered, string(debug.Stack()))
			}
		}()

		if err := handler(Message{Subject: message.Subject, Data: message.Data}); err != nil {
			log.Printf("nats: event handler error for %q: %v", message.Subject, err)
		}
	})
}
