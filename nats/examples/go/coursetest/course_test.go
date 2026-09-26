//go:build integration

// Package coursetest checks that statements made in the course hold on a real
// cluster. Each test names the module it verifies. If a NATS release changes
// behaviour, CI fails here before readers hit it.
package coursetest

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/jwm1rr0rb10/NATSFREECOURSE/examples/go/internal/testutil"
)

// 4.6 / 4.8: with no subscribers a request fails immediately with "no responders".
func TestNoResponders(t *testing.T) {
	nc, _ := testutil.Connect(t)
	start := time.Now()
	_, err := nc.Request("course.nobody."+testutil.Name("x"), []byte("hi"), 2*time.Second)
	if !errors.Is(err, nats.ErrNoResponders) {
		t.Fatalf("err = %v, want ErrNoResponders", err)
	}
	if time.Since(start) > time.Second {
		t.Fatalf("no responders took %v, should be immediate", time.Since(start))
	}
}

// 4.5: in a queue group exactly one member receives each message.
func TestQueueGroupDeliversOnce(t *testing.T) {
	nc, _ := testutil.Connect(t)
	subj := "course.queue." + testutil.Name("q")
	counts := make([]atomic.Int64, 3)
	for i := range counts {
		i := i
		sub, err := nc.QueueSubscribe(subj, "workers", func(*nats.Msg) { counts[i].Add(1) })
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Unsubscribe()
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 90; i++ {
		_ = nc.Publish(subj, []byte("job"))
	}
	_ = nc.Flush()
	time.Sleep(500 * time.Millisecond)
	a, b, c := counts[0].Load(), counts[1].Load(), counts[2].Load()
	if total := a + b + c; total != 90 {
		t.Fatalf("group received %d messages in total, want exactly 90 (%d/%d/%d)", total, a, b, c)
	}
}

// 7.3: the same Nats-Msg-Id inside the duplicate window is stored once.
func TestPublishDeduplication(t *testing.T) {
	_, js := testutil.Connect(t)
	ctx := testutil.Ctx(t, 30*time.Second)
	name := testutil.Name("T_DEDUP")
	s := testutil.Stream(t, js, jetstream.StreamConfig{
		Name: name, Subjects: []string{"tdedup." + name}, Duplicates: 2 * time.Minute,
	})

	for i := 0; i < 3; i++ {
		ack, err := js.Publish(ctx, "tdedup."+name, []byte("evt"), jetstream.WithMsgID("evt-1"))
		if err != nil {
			t.Fatal(err)
		}
		if want := i > 0; ack.Duplicate != want {
			t.Fatalf("publish %d: Duplicate = %v, want %v", i+1, ack.Duplicate, want)
		}
	}
	info, _ := s.Info(ctx)
	if info.State.Msgs != 1 {
		t.Fatalf("stream has %d messages, want 1", info.State.Msgs)
	}
}

// 7.4: ExpectLastSequencePerSubject rejects a write based on stale state.
func TestOptimisticConcurrency(t *testing.T) {
	_, js := testutil.Connect(t)
	ctx := testutil.Ctx(t, 30*time.Second)
	name := testutil.Name("T_OCC")
	subj := "tocc." + name + ".order-123"
	testutil.Stream(t, js, jetstream.StreamConfig{Name: name, Subjects: []string{"tocc." + name + ".>"}})

	first, err := js.Publish(ctx, subj, []byte("v1"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := js.Publish(ctx, subj, []byte("v2"), jetstream.WithExpectLastSequencePerSubject(first.Sequence)); err != nil {
		t.Fatalf("write with the current sequence failed: %v", err)
	}
	// someone else already wrote v2, so a writer still holding `first` must fail
	if _, err := js.Publish(ctx, subj, []byte("v2-stale"), jetstream.WithExpectLastSequencePerSubject(first.Sequence)); err == nil {
		t.Fatal("stale write was accepted")
	}
}

// 5.3: interest retention deletes messages immediately when there are no consumers.
func TestInterestRetentionWithoutConsumers(t *testing.T) {
	_, js := testutil.Connect(t)
	ctx := testutil.Ctx(t, 30*time.Second)
	name := testutil.Name("T_INTEREST")
	s := testutil.Stream(t, js, jetstream.StreamConfig{
		Name: name, Subjects: []string{"tint." + name}, Retention: jetstream.InterestPolicy,
	})
	if _, err := js.Publish(ctx, "tint."+name, []byte("lost")); err != nil {
		t.Fatal(err)
	}
	info, _ := s.Info(ctx)
	if info.State.Msgs != 0 {
		t.Fatalf("interest stream without consumers kept %d messages", info.State.Msgs)
	}
}

// 5.3: a workqueue stream rejects a second consumer with an overlapping filter.
func TestWorkqueueRejectsOverlappingConsumers(t *testing.T) {
	_, js := testutil.Connect(t)
	ctx := testutil.Ctx(t, 30*time.Second)
	name := testutil.Name("T_WQ")
	testutil.Stream(t, js, jetstream.StreamConfig{
		Name: name, Subjects: []string{"twq." + name + ".>"}, Retention: jetstream.WorkQueuePolicy,
	})
	if _, err := js.CreateConsumer(ctx, name, jetstream.ConsumerConfig{
		Durable: "A", AckPolicy: jetstream.AckExplicitPolicy, FilterSubject: "twq." + name + ".>",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := js.CreateConsumer(ctx, name, jetstream.ConsumerConfig{
		Durable: "B", AckPolicy: jetstream.AckExplicitPolicy, FilterSubject: "twq." + name + ".jobs",
	}); err == nil {
		t.Fatal("overlapping workqueue consumer was accepted")
	}
}

// 8.4: max_msgs_per_subject=1 keeps only the latest state per subject.
func TestLatestPerSubject(t *testing.T) {
	_, js := testutil.Connect(t)
	ctx := testutil.Ctx(t, 30*time.Second)
	name := testutil.Name("T_PROFILES")
	s := testutil.Stream(t, js, jetstream.StreamConfig{
		Name: name, Subjects: []string{"tprof." + name + ".*"}, MaxMsgsPerSubject: 1,
	})
	for _, p := range []struct{ subj, data string }{
		{"user-1", "Ann"}, {"user-2", "Bob"}, {"user-1", "Anna"},
	} {
		if _, err := js.Publish(ctx, "tprof."+name+"."+p.subj, []byte(p.data)); err != nil {
			t.Fatal(err)
		}
	}
	info, _ := s.Info(ctx)
	if info.State.Msgs != 2 {
		t.Fatalf("stream has %d messages, want 2", info.State.Msgs)
	}
	last, err := s.GetLastMsgForSubject(ctx, "tprof."+name+".user-1")
	if err != nil {
		t.Fatal(err)
	}
	if string(last.Data) != "Anna" {
		t.Fatalf("user-1 = %q, want Anna", last.Data)
	}
}

// 9.3 / 9.5: Create fails on an existing key, Update fails on a stale revision.
func TestKVCreateAndUpdate(t *testing.T) {
	_, js := testutil.Connect(t)
	ctx := testutil.Ctx(t, 30*time.Second)
	bucket := testutil.Name("T_KV")
	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: bucket, History: 5, Replicas: testutil.Replicas()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = js.DeleteKeyValue(ctx, bucket) })

	rev, err := kv.Create(ctx, "lock.nightly-report", []byte("worker-1"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kv.Create(ctx, "lock.nightly-report", []byte("worker-2")); !errors.Is(err, jetstream.ErrKeyExists) {
		t.Fatalf("second Create: err = %v, want ErrKeyExists", err)
	}
	if _, err := kv.Update(ctx, "lock.nightly-report", []byte("worker-1"), rev); err != nil {
		t.Fatalf("Update with current revision: %v", err)
	}
	if _, err := kv.Update(ctx, "lock.nightly-report", []byte("worker-3"), rev); err == nil {
		t.Fatal("Update with stale revision was accepted")
	}
}

// 6.4: the server rejects max_deliver smaller than the number of backoff entries.
func TestBackoffNeedsEnoughDeliveries(t *testing.T) {
	_, js := testutil.Connect(t)
	ctx := testutil.Ctx(t, 30*time.Second)
	name := testutil.Name("T_BACKOFF")
	testutil.Stream(t, js, jetstream.StreamConfig{Name: name, Subjects: []string{"tbo." + name}})

	backoff := []time.Duration{time.Second, 5 * time.Second, 30 * time.Second}
	if _, err := js.CreateConsumer(ctx, name, jetstream.ConsumerConfig{
		Durable: "TOO_FEW", AckPolicy: jetstream.AckExplicitPolicy, MaxDeliver: 2, BackOff: backoff,
	}); err == nil {
		t.Fatal("max_deliver=2 with 3 backoff entries was accepted")
	}
	if _, err := js.CreateConsumer(ctx, name, jetstream.ConsumerConfig{
		Durable: "OK", AckPolicy: jetstream.AckExplicitPolicy, MaxDeliver: 4, BackOff: backoff,
	}); err != nil {
		t.Fatalf("max_deliver=4 with 3 backoff entries rejected: %v", err)
	}
}

// 6.3 / 7.6: without an ack the message comes back after AckWait; DoubleAck confirms.
func TestRedeliveryAndDoubleAck(t *testing.T) {
	_, js := testutil.Connect(t)
	ctx := testutil.Ctx(t, 30*time.Second)
	name := testutil.Name("T_REDELIVER")
	testutil.Stream(t, js, jetstream.StreamConfig{Name: name, Subjects: []string{"tre." + name}})
	cons, err := js.CreateConsumer(ctx, name, jetstream.ConsumerConfig{
		Durable: "W", AckPolicy: jetstream.AckExplicitPolicy, AckWait: time.Second, MaxDeliver: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := js.Publish(ctx, "tre."+name, []byte("job")); err != nil {
		t.Fatal(err)
	}

	first, err := cons.Next(jetstream.FetchMaxWait(5 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	_ = first // simulate a crash: no ack

	second, err := cons.Next(jetstream.FetchMaxWait(5 * time.Second))
	if err != nil {
		t.Fatalf("message was not redelivered after AckWait: %v", err)
	}
	meta, _ := second.Metadata()
	if meta.NumDelivered != 2 {
		t.Fatalf("NumDelivered = %d, want 2", meta.NumDelivered)
	}
	if err := second.DoubleAck(ctx); err != nil {
		t.Fatalf("DoubleAck: %v", err)
	}
	if _, err := cons.Next(jetstream.FetchMaxWait(2 * time.Second)); err == nil {
		t.Fatal("message delivered again after DoubleAck")
	}
}
