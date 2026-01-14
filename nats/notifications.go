package nats

import (
	"encoding/json"
	"errors"
	"time"
)

const ISO8601MillisZ = "2006-01-02T15:04:05.000Z"

type notificationLevel int

type NotificationLevelType = notificationLevel

const (
	notificationLevelDebug notificationLevel = iota*10 + 10
	notificationLevelInfo
	notificationLevelWarning
	notificationLevelError
	notificationLevelCritical
)

// NotificationLevel is the enum namespace.
var NotificationLevel = struct {
	Debug    NotificationLevelType
	Info     NotificationLevelType
	Warning  NotificationLevelType
	Error    NotificationLevelType
	Critical NotificationLevelType
}{
	Debug:    notificationLevelDebug,
	Info:     notificationLevelInfo,
	Warning:  notificationLevelWarning,
	Error:    notificationLevelError,
	Critical: notificationLevelCritical,
}

// Notification is the org-wide notification contract.
type Notification struct {
	Msg     string                `json:"msg"`
	Time    string                `json:"time"`
	Level   NotificationLevelType `json:"level"`
	Group   string                `json:"group"`
	Service string                `json:"service"`
	Data    any                   `json:"data,omitempty"`
}

type NotificationOptions struct {
	Group   string
	Service string
	Time    func() time.Time
}

// NewNotification creates a notification.
// Group is required and must not be empty.
func NewNotification(
	msg string,
	level NotificationLevelType,
	data any,
	opt NotificationOptions,
) (Notification, error) {
	if opt.Group == "" {
		return Notification{}, errors.New("notification group must not be empty")
	}
	if opt.Service == "" {
		return Notification{}, errors.New("notification service must not be empty")
	}

	now := time.Now
	if opt.Time != nil {
		now = opt.Time
	}

	return Notification{
		Msg:     msg,
		Time:    now().UTC().Format(ISO8601MillisZ),
		Level:   level,
		Group:   opt.Group,
		Service: opt.Service,
		Data:    data,
	}, nil
}

func (c *Client) PublishNotification(subject string, n Notification) error {
	if c == nil || c.conn == nil {
		return errors.New("nats client is nil or closed")
	}

	b, err := json.Marshal(n)
	if err != nil {
		return err
	}
	return c.conn.Publish(subject, b)
}
