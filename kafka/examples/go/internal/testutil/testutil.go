// Package testutil holds helpers for the integration tests.
package testutil

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/jwm1rr0rb10/KAFKAFREECOURSE/examples/go/internal/conn"
)

// RF returns $KAFKA_RF or 3 (the course cluster).
func RF() int16 {
	if v, err := strconv.Atoi(os.Getenv("KAFKA_RF")); err == nil && v > 0 {
		return int16(v)
	}
	return 3
}

// Name returns a unique topic or group name.
func Name(prefix string) string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return prefix + "." + hex.EncodeToString(b)
}

// Ctx returns a context cancelled at test end.
func Ctx(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

// Client opens a client that is closed at test end.
func Client(t *testing.T, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	cl, err := kgo.NewClient(conn.Opts("course-test", opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return cl
}

// Admin returns an admin client.
func Admin(t *testing.T) *kadm.Client {
	t.Helper()
	return kadm.NewClient(Client(t))
}

// Topic creates a topic that is deleted at test end.
func Topic(t *testing.T, partitions int32, configs map[string]*string) string {
	t.Helper()
	name := Name("course-test")
	adm := Admin(t)
	ctx := Ctx(t, 30*time.Second)
	resp, err := adm.CreateTopic(ctx, partitions, RF(), configs, name)
	if err == nil {
		err = resp.Err
	}
	if err != nil {
		t.Fatalf("create topic %s: %v (is the cluster running on %v?)", name, err, conn.Brokers())
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = adm.DeleteTopics(ctx, name)
	})
	return name
}

// Produce writes records synchronously and fails the test on error.
func Produce(t *testing.T, cl *kgo.Client, recs ...*kgo.Record) {
	t.Helper()
	if err := cl.ProduceSync(Ctx(t, 30*time.Second), recs...).FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}
}

// Poll collects up to n records or stops after wait.
func Poll(t *testing.T, cl *kgo.Client, n int, wait time.Duration) []*kgo.Record {
	t.Helper()
	var out []*kgo.Record
	deadline := time.Now().Add(wait)
	for len(out) < n && time.Now().Before(deadline) {
		ctx, cancel := context.WithDeadline(context.Background(), deadline)
		fs := cl.PollRecords(ctx, n-len(out))
		cancel()
		fs.EachError(func(tp string, p int32, err error) {
			if ctx.Err() == nil {
				t.Logf("fetch error %s/%d: %v", tp, p, err)
			}
		})
		out = append(out, fs.Records()...)
	}
	return out
}
