package testutil

import (
	"net"
	"testing"
	"time"

	natssrv "github.com/nats-io/nats-server/v2/server"
)

// StartServer starts an embedded NATS server on a random free port.
// It returns the server and a connection URL like nats://127.0.0.1:port.
func StartServer(t *testing.T) (*natssrv.Server, string) {
	t.Helper()

	opts := &natssrv.Options{
		Host:           "127.0.0.1",
		Port:           -1, // random free port
		NoLog:          true,
		NoSigs:         true,
		MaxControlLine: 4096,

		JetStream: true,
		StoreDir:  t.TempDir(),
	}

	s, err := natssrv.NewServer(opts)
	if err != nil {
		t.Fatalf("failed to create nats-server: %v", err)
	}

	go s.Start()

	if !s.ReadyForConnections(5 * time.Second) {
		s.Shutdown()
		t.Fatalf("nats-server not ready for connections")
	}

	// Server chooses a port when Port = -1; resolve into a usable URL.
	addr := s.Addr()
	if addr == nil {
		s.Shutdown()
		t.Fatalf("nats-server addr is nil")
	}

	host, port, err := net.SplitHostPort(addr.String())
	if err != nil {
		s.Shutdown()
		t.Fatalf("failed to parse nats-server addr %q: %v", addr.String(), err)
	}
	if host == "" {
		host = "127.0.0.1"
	}

	return s, "nats://" + net.JoinHostPort(host, port)
}
