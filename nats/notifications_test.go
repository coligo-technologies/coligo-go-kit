package nats_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	kitnats "github.com/coligo-technologies/coligo-go-kit/nats"
	natssrv "github.com/nats-io/nats-server/v2/server"
	natsgo "github.com/nats-io/nats.go"
)

func startTestServer(t *testing.T) *natssrv.Server {
	t.Helper()

	s, err := natssrv.NewServer(&natssrv.Options{
		Host: "127.0.0.1",
		Port: -1,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	go s.Start()
	if !s.ReadyForConnections(10 * time.Second) {
		s.Shutdown()
		t.Fatalf("server not ready for connections")
	}

	t.Cleanup(s.Shutdown)
	return s
}

func TestNewNotification_RequiresGroup(t *testing.T) {
	_, err := kitnats.NewNotification(
		"hello",
		kitnats.NotificationLevel.Info,
		map[string]any{"a": 1},
		kitnats.NotificationOptions{Group: "", Service: "svc"},
	)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "group") {
		t.Fatalf("expected group error, got %v", err)
	}
}

func TestNewNotification_RequiresService(t *testing.T) {
	_, err := kitnats.NewNotification(
		"hello",
		kitnats.NotificationLevel.Info,
		map[string]any{"a": 1},
		kitnats.NotificationOptions{Group: "collect", Service: ""},
	)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "service") {
		t.Fatalf("expected service error, got %v", err)
	}
}

func TestNewNotification_SetsFieldsAndTime(t *testing.T) {
	fixed := time.Date(2026, 1, 14, 10, 11, 12, 123_000_000, time.UTC)

	n, err := kitnats.NewNotification(
		"cpu high",
		kitnats.NotificationLevel.Warning,
		map[string]any{"usage": 91.2},
		kitnats.NotificationOptions{
			Group:   "collect",
			Service: "system-manager",
			Time: func() time.Time {
				return fixed
			},
		},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if n.Msg != "cpu high" {
		t.Fatalf("Msg: got %q", n.Msg)
	}
	if n.Level != kitnats.NotificationLevel.Warning {
		t.Fatalf("Level: got %v", n.Level)
	}
	if n.Group != "collect" {
		t.Fatalf("Group: got %q", n.Group)
	}
	if n.Service != "system-manager" {
		t.Fatalf("Service: got %q", n.Service)
	}
	if n.Time != fixed.Format(kitnats.ISO8601MillisZ) {
		t.Fatalf("Time: got %q want %q", n.Time, fixed.Format(kitnats.ISO8601MillisZ))
	}
}

func TestNotification_JSON_OmitsNilData(t *testing.T) {
	fixed := time.Date(2026, 1, 14, 10, 11, 12, 0, time.UTC)

	n, err := kitnats.NewNotification(
		"ok",
		kitnats.NotificationLevel.Info,
		nil, // nil interface => should be omitted due to omitempty
		kitnats.NotificationOptions{
			Group:   "collect",
			Service: "svc",
			Time: func() time.Time {
				return fixed
			},
		},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	b, err := json.Marshal(n)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if strings.Contains(string(b), `"data"`) {
		t.Fatalf("expected data to be omitted, got: %s", string(b))
	}
}

func TestPublishNotification_NilClient(t *testing.T) {
	var c *kitnats.Client
	err := c.PublishNotification("x", kitnats.Notification{Group: "g", Service: "s"})
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
}

func TestPublishNotification_MarshalError(t *testing.T) {
	s := startTestServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c, err := kitnats.NewClient(ctx, s.ClientURL())
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	// json.Marshal cannot encode funcs
	n := kitnats.Notification{
		Msg:     "bad",
		Time:    time.Now().UTC().Format(kitnats.ISO8601MillisZ),
		Level:   kitnats.NotificationLevel.Info,
		Group:   "collect",
		Service: "svc",
		Data:    func() {},
	}

	if err := c.PublishNotification("demo.notifications", n); err == nil {
		t.Fatalf("expected marshal error, got nil")
	}
}

func TestPublishNotification_Publishes(t *testing.T) {
	s := startTestServer(t)

	// Use a raw subscriber conn so we can assert on the actual payload.
	subConn, err := natsgo.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("nats.Connect subscriber: %v", err)
	}
	defer subConn.Close()

	subject := "demo.notifications"

	gotCh := make(chan *natsgo.Msg, 1)
	sub, err := subConn.Subscribe(subject, func(m *natsgo.Msg) {
		gotCh <- m
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pubClient, err := kitnats.NewClient(ctx, s.ClientURL())
	if err != nil {
		t.Fatalf("NewClient publisher: %v", err)
	}
	defer pubClient.Close()

	fixed := time.Date(2026, 1, 14, 10, 11, 12, 123_000_000, time.UTC)
	n, err := kitnats.NewNotification(
		"disk high",
		kitnats.NotificationLevel.Error,
		map[string]any{"usedPercent": 93.4},
		kitnats.NotificationOptions{
			Group:   "collect",
			Service: "system-manager",
			Time: func() time.Time {
				return fixed
			},
		},
	)
	if err != nil {
		t.Fatalf("NewNotification: %v", err)
	}

	if err := pubClient.PublishNotification(subject, n); err != nil {
		t.Fatalf("PublishNotification: %v", err)
	}

	select {
	case msg := <-gotCh:
		var decoded kitnats.Notification
		if err := json.Unmarshal(msg.Data, &decoded); err != nil {
			t.Fatalf("Unmarshal published payload: %v\npayload=%s", err, string(msg.Data))
		}

		if decoded.Msg != n.Msg {
			t.Fatalf("Msg: got %q want %q", decoded.Msg, n.Msg)
		}
		if decoded.Level != n.Level {
			t.Fatalf("Level: got %v want %v", decoded.Level, n.Level)
		}
		if decoded.Group != n.Group {
			t.Fatalf("Group: got %q want %q", decoded.Group, n.Group)
		}
		if decoded.Service != n.Service {
			t.Fatalf("Service: got %q want %q", decoded.Service, n.Service)
		}
		if decoded.Time != n.Time {
			t.Fatalf("Time: got %q want %q", decoded.Time, n.Time)
		}
		if decoded.Data == nil {
			t.Fatalf("expected Data, got nil")
		}

	case <-time.After(2 * time.Second):
		t.Fatalf("did not receive published notification")
	}
}
