// Package coursetest checks that statements made in the course hold on a real
// server. Each test names the module it verifies. If a NATS release changes
// behaviour, CI fails here before readers hit it.
//
// By default the tests start an embedded nats-server (see testutil); set
// NATS_URL to run them against the course cluster instead.
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

// 3.2: publishing to a "wildcard" is not a fan-out. The server (outside pedantic
// mode) accepts `shop.orders.*` as a literal subject: only wildcard
// subscriptions that cover it receive the message, `shop.orders.created` does not.
func TestPublishToWildcardIsLiteral(t *testing.T) {
	nc, _ := testutil.Connect(t)
	base := "course.wc." + testutil.Name("w")
	var exact, wild atomic.Int64
	s1, err := nc.Subscribe(base+".orders.created", func(*nats.Msg) { exact.Add(1) })
	if err != nil {
		t.Fatal(err)
	}
	defer s1.Unsubscribe()
	s2, err := nc.Subscribe(base+".orders.>", func(*nats.Msg) { wild.Add(1) })
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Unsubscribe()
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := nc.Publish(base+".orders.*", []byte("x")); err != nil {
		t.Fatal(err)
	}
	_ = nc.Flush()
	time.Sleep(300 * time.Millisecond)
	if exact.Load() != 0 {
		t.Fatal("publishing to orders.* reached the orders.created subscriber")
	}
	if wild.Load() != 1 {
		t.Fatalf("orders.> subscriber got %d messages, want 1 (literal subject orders.*)", wild.Load())
	}
}

// 6.4 / 14.2: with BackOff set the server overwrites AckWait with BackOff[0];
// the wait after delivery k is BackOff[k-1], the last value repeats.
func TestBackOffOverridesAckWait(t *testing.T) {
	_, js := testutil.Connect(t)
	ctx := testutil.Ctx(t, 30*time.Second)
	name := testutil.Name("T_BOAW")
	testutil.Stream(t, js, jetstream.StreamConfig{Name: name, Subjects: []string{"tboaw." + name}})

	backoff := []time.Duration{300 * time.Millisecond, 1500 * time.Millisecond}
	cons, err := js.CreateConsumer(ctx, name, jetstream.ConsumerConfig{
		Durable: "W", AckPolicy: jetstream.AckExplicitPolicy,
		AckWait: 20 * time.Second, BackOff: backoff, MaxDeliver: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	info, err := cons.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.Config.AckWait != backoff[0] {
		t.Fatalf("AckWait = %v, want it replaced by BackOff[0] = %v", info.Config.AckWait, backoff[0])
	}
	if _, err := js.Publish(ctx, "tboaw."+name, []byte("job")); err != nil {
		t.Fatal(err)
	}

	gaps := deliveryGaps(t, cons, 3, nil)
	// delivery 2 comes after BackOff[0], not after the configured 20s AckWait
	if gaps[0] < 200*time.Millisecond || gaps[0] > 1200*time.Millisecond {
		t.Errorf("wait after delivery 1 = %v, want ~%v", gaps[0], backoff[0])
	}
	if gaps[1] < 1200*time.Millisecond || gaps[1] > 3*time.Second {
		t.Errorf("wait after delivery 2 = %v, want ~%v", gaps[1], backoff[1])
	}
}

// 6.4 / 14.2: BackOff only applies when AckWait expires. A plain Nak is
// redelivered immediately, whatever the BackOff says.
func TestNakIgnoresBackOff(t *testing.T) {
	_, js := testutil.Connect(t)
	ctx := testutil.Ctx(t, 30*time.Second)
	name := testutil.Name("T_NAKBO")
	testutil.Stream(t, js, jetstream.StreamConfig{Name: name, Subjects: []string{"tnakbo." + name}})
	cons, err := js.CreateConsumer(ctx, name, jetstream.ConsumerConfig{
		Durable: "W", AckPolicy: jetstream.AckExplicitPolicy,
		BackOff: []time.Duration{5 * time.Second, 10 * time.Second}, MaxDeliver: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := js.Publish(ctx, "tnakbo."+name, []byte("job")); err != nil {
		t.Fatal(err)
	}
	gaps := deliveryGaps(t, cons, 3, func(m jetstream.Msg) { _ = m.Nak() })
	for i, g := range gaps {
		if g > time.Second {
			t.Errorf("redelivery %d after Nak took %v; Nak must not wait for BackOff", i+1, g)
		}
	}
}

// 6.4: NakWithDelay uses its own delay, but on a consumer with BackOff the
// server offsets it by the BackOff entries (d + BackOff[k-1] - BackOff[0]).
// That is why the course says to pick one of the two, not both.
func TestNakWithDelayOnBackOffConsumer(t *testing.T) {
	_, js := testutil.Connect(t)
	ctx := testutil.Ctx(t, 30*time.Second)
	name := testutil.Name("T_NAKD")
	testutil.Stream(t, js, jetstream.StreamConfig{Name: name, Subjects: []string{"tnakd." + name}})
	cons, err := js.CreateConsumer(ctx, name, jetstream.ConsumerConfig{
		Durable: "W", AckPolicy: jetstream.AckExplicitPolicy,
		BackOff: []time.Duration{200 * time.Millisecond, 1500 * time.Millisecond}, MaxDeliver: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := js.Publish(ctx, "tnakd."+name, []byte("job")); err != nil {
		t.Fatal(err)
	}
	const d = 500 * time.Millisecond
	gaps := deliveryGaps(t, cons, 3, func(m jetstream.Msg) { _ = m.NakWithDelay(d) })
	if gaps[0] < 400*time.Millisecond || gaps[0] > 1200*time.Millisecond {
		t.Errorf("first NakWithDelay(%v) took %v", d, gaps[0])
	}
	// 500ms + 1500ms - 200ms = 1.8s instead of 500ms
	if gaps[1] < 1300*time.Millisecond {
		t.Errorf("second NakWithDelay(%v) took %v; expected the BackOff offset (~1.8s)", d, gaps[1])
	}
}

// 14.3: on a workqueue stream Term counts as an ack, so the message is deleted
// immediately and the MSG_TERMINATED advisory points at nothing.
func TestTermOnWorkqueueDeletesMessage(t *testing.T) {
	_, js := testutil.Connect(t)
	ctx := testutil.Ctx(t, 30*time.Second)
	name := testutil.Name("T_WQTERM")
	s := testutil.Stream(t, js, jetstream.StreamConfig{
		Name: name, Subjects: []string{"twqterm." + name}, Retention: jetstream.WorkQueuePolicy,
	})
	cons, err := js.CreateConsumer(ctx, name, jetstream.ConsumerConfig{Durable: "W", AckPolicy: jetstream.AckExplicitPolicy})
	if err != nil {
		t.Fatal(err)
	}
	ack, err := js.Publish(ctx, "twqterm."+name, []byte("{not json"))
	if err != nil {
		t.Fatal(err)
	}
	m, err := cons.Next(jetstream.FetchMaxWait(5 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := m.TermWithReason("invalid json"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool {
		_, err := s.GetMsg(ctx, ack.Sequence)
		return errors.Is(err, jetstream.ErrMsgNotFound)
	}, "terminated message is still in the workqueue stream")
}

// 14.3: a message that exhausted max_deliver is NOT acked, so on a workqueue
// stream it stays and the DLQ mover can still copy it.
func TestMaxDeliverOnWorkqueueKeepsMessage(t *testing.T) {
	_, js := testutil.Connect(t)
	ctx := testutil.Ctx(t, 30*time.Second)
	name := testutil.Name("T_WQMAX")
	s := testutil.Stream(t, js, jetstream.StreamConfig{
		Name: name, Subjects: []string{"twqmax." + name}, Retention: jetstream.WorkQueuePolicy,
	})
	cons, err := js.CreateConsumer(ctx, name, jetstream.ConsumerConfig{
		Durable: "W", AckPolicy: jetstream.AckExplicitPolicy, MaxDeliver: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	ack, err := js.Publish(ctx, "twqmax."+name, []byte("job"))
	if err != nil {
		t.Fatal(err)
	}
	deliveryGaps(t, cons, 2, func(m jetstream.Msg) { _ = m.Nak() })
	if _, err := cons.Next(jetstream.FetchMaxWait(time.Second)); err == nil {
		t.Fatal("message delivered beyond max_deliver")
	}
	if _, err := s.GetMsg(ctx, ack.Sequence); err != nil {
		t.Fatalf("exhausted message is gone from the workqueue stream: %v", err)
	}
}

// deliveryGaps fetches the same message n times, calling react after each
// delivery (nil = no reply at all), and returns the time between deliveries.
func deliveryGaps(t *testing.T, cons jetstream.Consumer, n int, react func(jetstream.Msg)) []time.Duration {
	t.Helper()
	var gaps []time.Duration
	var last time.Time
	for i := 1; i <= n; i++ {
		m, err := cons.Next(jetstream.FetchMaxWait(10 * time.Second))
		if err != nil {
			t.Fatalf("delivery %d: %v", i, err)
		}
		now := time.Now()
		meta, err := m.Metadata()
		if err != nil {
			t.Fatal(err)
		}
		if meta.NumDelivered != uint64(i) {
			t.Fatalf("NumDelivered = %d, want %d", meta.NumDelivered, i)
		}
		if i > 1 {
			gaps = append(gaps, now.Sub(last))
		}
		last = now
		if react != nil {
			react(m)
		}
	}
	return gaps
}

func waitFor(t *testing.T, d time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal(msg)
}
