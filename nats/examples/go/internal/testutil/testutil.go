// Package testutil holds helpers for the integration tests.
//
// By default every test starts its own embedded nats-server with JetStream
// (single node, store in t.TempDir()), so `go test ./...` needs no Docker and
// no running cluster. Set NATS_URL to run the same tests against an external
// server or the three-node cluster from module 2.5 instead.
package testutil

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"

	"github.com/jwm1rr0rb10/NATSFREECOURSE/examples/go/internal/conn"
)

// External reports whether the tests run against an external server ($NATS_URL).
func External() bool { return os.Getenv("NATS_URL") != "" }

// Replicas returns $NATS_REPLICAS, otherwise 3 for an external server (the
// course cluster) and 1 for the embedded single node.
func Replicas() int {
	if v, err := strconv.Atoi(os.Getenv("NATS_REPLICAS")); err == nil && v > 0 {
		return v
	}
	if External() {
		return 3
	}
	return 1
}

// RunServer starts an embedded nats-server with JetStream that is shut down
// when the test ends, and returns its client URL.
func RunServer(t *testing.T) string {
	t.Helper()
	opts := &server.Options{
		ServerName: "course-test",
		Host:       "127.0.0.1",
		Port:       -1, // random free port
		JetStream:  true,
		StoreDir:   t.TempDir(),
		NoLog:      true,
		NoSigs:     true,
	}
	s, err := server.NewServer(opts)
	if err != nil {
		t.Fatalf("embedded nats-server: %v", err)
	}
	go s.Start()
	if !s.ReadyForConnections(10 * time.Second) {
		t.Fatal("embedded nats-server did not start")
	}
	t.Cleanup(func() {
		s.Shutdown()
		s.WaitForShutdown()
	})
	return s.ClientURL()
}

// Connect opens a connection and JetStream context, closed at test end.
//
// Without $NATS_URL it connects to a fresh embedded server. With $NATS_URL it
// connects there; if that fails the test is skipped, unless
// COURSE_REQUIRE_BROKER=1 (CI), in which case it fails.
func Connect(t *testing.T) (*nats.Conn, jetstream.JetStream) {
	t.Helper()
	var (
		nc  *nats.Conn
		err error
	)
	if External() {
		nc, err = conn.Connect("course-test-"+t.Name(), nats.MaxReconnects(0))
		if err != nil {
			if os.Getenv("COURSE_REQUIRE_BROKER") == "1" {
				t.Fatalf("connect to %s: %v (COURSE_REQUIRE_BROKER=1)", conn.URLs(), err)
			}
			t.Skipf("connect to %s: %v (is the cluster running? unset NATS_URL to use an embedded server)", conn.URLs(), err)
		}
	} else {
		nc, err = nats.Connect(RunServer(t), nats.Name("course-test-"+t.Name()))
		if err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	return nc, js
}

// Name returns a unique, stream-safe name with the given prefix.
func Name(prefix string) string {
	return prefix + "_" + strings.ToUpper(nuid.Next()[12:])
}

// Ctx returns a context that times out and is cancelled at test end.
func Ctx(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

// Stream creates a stream that is deleted when the test ends.
func Stream(t *testing.T, js jetstream.JetStream, cfg jetstream.StreamConfig) jetstream.Stream {
	t.Helper()
	if cfg.Replicas == 0 {
		cfg.Replicas = Replicas()
	}
	ctx := Ctx(t, 30*time.Second)
	s, err := js.CreateStream(ctx, cfg)
	if err != nil {
		t.Fatalf("create stream %s: %v", cfg.Name, err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = js.DeleteStream(ctx, cfg.Name)
	})
	return s
}
