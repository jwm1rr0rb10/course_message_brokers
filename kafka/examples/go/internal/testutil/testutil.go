// Package testutil holds helpers for the integration tests.
//
// Where the tests connect:
//
//   - by default, to the course cluster ($KAFKA_BROKERS or conn.DefaultBrokers).
//     If it does not answer, every test is skipped with the reason, unless
//     COURSE_REQUIRE_BROKER=1 (set in CI), in which case every test fails;
//   - with KAFKA_FAKE=1, to an in-process kfake cluster (franz-go's fake Kafka)
//     started once per test binary. No Docker needed. It checks the client-side
//     and protocol-level claims; tests of broker behaviour kfake does not model
//     call RequireRealBroker and are skipped.
package testutil

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/jwm1rr0rb10/KAFKAFREECOURSE/examples/go/internal/conn"
)

// Fake reports whether the tests run against the in-process kfake cluster.
func Fake() bool { return os.Getenv("KAFKA_FAKE") != "" }

// RequireRealBroker skips the test on kfake: use it for claims about broker
// behaviour that kfake does not implement.
func RequireRealBroker(t *testing.T, why string) {
	t.Helper()
	if Fake() {
		t.Skip("needs a real broker (KAFKA_FAKE is set): " + why)
	}
}

var (
	brokersOnce sync.Once
	brokers     []string
	brokersErr  error
)

// Brokers returns the seed brokers for the tests, skipping or failing the test
// when no cluster is reachable (see the package comment).
func Brokers(t *testing.T) []string {
	t.Helper()
	brokersOnce.Do(func() {
		if Fake() {
			c, err := kfake.NewCluster(kfake.NumBrokers(3))
			if err != nil {
				brokersErr = fmt.Errorf("start kfake: %w", err)
				return
			}
			brokers = c.ListenAddrs() // lives until the test binary exits
			return
		}
		brokers = conn.Brokers()
		brokersErr = ping(brokers)
	})
	if brokersErr != nil {
		if os.Getenv("COURSE_REQUIRE_BROKER") == "1" {
			t.Fatalf("Kafka is required (COURSE_REQUIRE_BROKER=1) but not reachable on %v: %v", brokers, brokersErr)
		}
		t.Skipf("Kafka not reachable on %v (%v); start examples/cluster, set KAFKA_FAKE=1, or COURSE_REQUIRE_BROKER=1 to fail instead", brokers, brokersErr)
	}
	return brokers
}

func ping(seeds []string) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(seeds...), kgo.ClientID("course-test-ping"))
	if err != nil {
		return err
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return cl.Ping(ctx)
}

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
	seeds := Brokers(t)
	cl, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(seeds...), kgo.ClientID("course-test")}, opts...)...)
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
		t.Fatalf("create topic %s: %v", name, err)
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
