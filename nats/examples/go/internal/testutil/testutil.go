// Package testutil holds helpers for the integration tests.
package testutil

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"

	"github.com/jwm1rr0rb10/NATSFREECOURSE/examples/go/internal/conn"
)

// Replicas returns $NATS_REPLICAS or 3 (the course cluster).
func Replicas() int {
	if v, err := strconv.Atoi(os.Getenv("NATS_REPLICAS")); err == nil && v > 0 {
		return v
	}
	return 3
}

// Connect opens a connection and JetStream context, closed at test end.
func Connect(t *testing.T) (*nats.Conn, jetstream.JetStream) {
	t.Helper()
	nc, err := conn.Connect("course-test-" + t.Name())
	if err != nil {
		t.Fatalf("connect to %s: %v (is the cluster running?)", conn.URLs(), err)
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
