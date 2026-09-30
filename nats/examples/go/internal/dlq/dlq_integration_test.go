package dlq_test

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/jwm1rr0rb10/NATSFREECOURSE/examples/go/internal/dlq"
	"github.com/jwm1rr0rb10/NATSFREECOURSE/examples/go/internal/testutil"
)

// Module 14.3 end to end: a message that exhausts MaxDeliver and a message
// that is Term()'d both reach the DLQ, even though the mover only starts
// AFTER the failures happened (advisories are captured in a stream).
func TestDLQCatchesUpAfterMoverWasDown(t *testing.T) {
	_, js := testutil.Connect(t)
	ctx := testutil.Ctx(t, 90*time.Second)

	cfg := dlq.Config{Replicas: testutil.Replicas()}
	mover, err := dlq.Setup(ctx, js, cfg)
	if err != nil {
		t.Fatal(err)
	}

	name := testutil.Name("T_DLQ")
	subj := "tdlq." + name
	testutil.Stream(t, js, jetstream.StreamConfig{Name: name, Subjects: []string{subj + ".>"}})

	cons, err := js.CreateConsumer(ctx, name, jetstream.ConsumerConfig{
		Durable:    "WORKER",
		AckPolicy:  jetstream.AckExplicitPolicy,
		AckWait:    time.Second,
		MaxDeliver: 2,
	})
	if err != nil {
		t.Fatal(err)
	}

	// no headers at all: the original course code panicked on this one
	if _, err := js.Publish(ctx, subj+".exhausted", []byte(`{"order_id":"a"}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := js.Publish(ctx, subj+".poison", []byte(`{not json`), jetstream.WithMsgID("poison-"+name)); err != nil {
		t.Fatal(err)
	}

	// the "worker": Nak the valid one until it is exhausted, Term the poison
	deadline := time.Now().Add(20 * time.Second)
	exhaustedDeliveries := 0
	termed := false
	for time.Now().Before(deadline) && (exhaustedDeliveries < 2 || !termed) {
		batch, err := cons.Fetch(10, jetstream.FetchMaxWait(time.Second))
		if err != nil {
			t.Fatal(err)
		}
		for m := range batch.Messages() {
			switch m.Subject() {
			case subj + ".exhausted":
				exhaustedDeliveries++
				_ = m.Nak()
			case subj + ".poison":
				_ = m.TermWithReason("invalid json")
				termed = true
			}
		}
	}
	if exhaustedDeliveries < 2 || !termed {
		t.Fatalf("worker did not finish: deliveries=%d termed=%v", exhaustedDeliveries, termed)
	}

	// only now does the mover start
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	go func() { _ = dlq.Run(runCtx, js, mover, cfg) }()

	got := collectDLQ(t, js, "dlq."+name+".WORKER", 2, 45*time.Second)

	bySubject := map[string]*nats.Header{}
	for _, m := range got {
		h := m.Headers()
		bySubject[h.Get("Dlq-Original-Subject")] = &h
	}
	ex := bySubject[subj+".exhausted"]
	if ex == nil {
		t.Fatal("exhausted message did not reach the DLQ")
	}
	if ex.Get("Dlq-Deliveries") != "2" {
		t.Errorf("Dlq-Deliveries = %q, want 2", ex.Get("Dlq-Deliveries"))
	}
	po := bySubject[subj+".poison"]
	if po == nil {
		t.Fatal("terminated message did not reach the DLQ")
	}
	if po.Get("Dlq-Reason") != "invalid json" {
		t.Errorf("Dlq-Reason = %q", po.Get("Dlq-Reason"))
	}
	if id := po.Get(jetstream.MsgIDHeader); id == "poison-"+name {
		t.Errorf("original Nats-Msg-Id leaked into the DLQ")
	}
}

func collectDLQ(t *testing.T, js jetstream.JetStream, subject string, want int, wait time.Duration) []jetstream.Msg {
	t.Helper()
	ctx := testutil.Ctx(t, wait+5*time.Second)
	var got []jetstream.Msg
	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) && len(got) < want {
		oc, err := js.OrderedConsumer(ctx, dlq.DLQStream, jetstream.OrderedConsumerConfig{
			FilterSubjects: []string{subject},
		})
		if err != nil {
			t.Fatal(err)
		}
		got = got[:0]
		// fetch more than wanted, so that callers can also detect duplicates
		batch, err := oc.Fetch(want+10, jetstream.FetchMaxWait(2*time.Second))
		if err != nil {
			t.Fatal(err)
		}
		for m := range batch.Messages() {
			got = append(got, m)
		}
	}
	if len(got) < want {
		t.Fatalf("DLQ has %d messages on %s, want %d", len(got), subject, want)
	}
	return got
}

// Module 14.3, the workqueue trap: a bare Term deletes the message, so the
// mover has nothing to copy; dlq.Terminate publishes it first. On a limits
// stream the mover sees the same message too, and the shared Nats-Msg-Id keeps
// a single copy in the DLQ.
func TestTerminateReachesDLQ(t *testing.T) {
	for _, retention := range []jetstream.RetentionPolicy{jetstream.WorkQueuePolicy, jetstream.LimitsPolicy} {
		t.Run(retention.String(), func(t *testing.T) {
			_, js := testutil.Connect(t)
			ctx := testutil.Ctx(t, 60*time.Second)

			cfg := dlq.Config{Replicas: testutil.Replicas()}
			mover, err := dlq.Setup(ctx, js, cfg)
			if err != nil {
				t.Fatal(err)
			}
			runCtx, stop := context.WithCancel(ctx)
			defer stop()
			go func() { _ = dlq.Run(runCtx, js, mover, cfg) }()

			name := testutil.Name("T_TERM")
			subj := "tterm." + name
			testutil.Stream(t, js, jetstream.StreamConfig{Name: name, Subjects: []string{subj + ".>"}, Retention: retention})
			cons, err := js.CreateConsumer(ctx, name, jetstream.ConsumerConfig{
				Durable: "WORKER", AckPolicy: jetstream.AckExplicitPolicy, MaxDeliver: 3,
			})
			if err != nil {
				t.Fatal(err)
			}

			// bare Term first (only checked on workqueue), then Terminate
			for _, s := range []string{".bare", ".helper"} {
				if _, err := js.Publish(ctx, subj+s, []byte(`{not json`)); err != nil {
					t.Fatal(err)
				}
			}
			for i := 0; i < 2; i++ {
				m, err := cons.Next(jetstream.FetchMaxWait(5 * time.Second))
				if err != nil {
					t.Fatal(err)
				}
				if m.Subject() == subj+".bare" {
					if err := m.TermWithReason("invalid json"); err != nil {
						t.Fatal(err)
					}
					continue
				}
				if err := dlq.Terminate(ctx, js, m, "invalid json"); err != nil {
					t.Fatal(err)
				}
			}

			collectDLQ(t, js, "dlq."+name+".WORKER", 1, 15*time.Second)
			// give the mover time to process both advisories, then count again
			time.Sleep(2 * time.Second)
			got := collectDLQ(t, js, "dlq."+name+".WORKER", 1, 5*time.Second)

			bySubject := map[string]int{}
			for _, m := range got {
				bySubject[m.Headers().Get("Dlq-Original-Subject")]++
			}
			if bySubject[subj+".helper"] != 1 {
				t.Fatalf("Terminate: %d copies in the DLQ, want exactly 1 (%v)", bySubject[subj+".helper"], bySubject)
			}
			if retention == jetstream.WorkQueuePolicy && bySubject[subj+".bare"] != 0 {
				t.Fatalf("bare Term on a workqueue stream unexpectedly reached the DLQ")
			}
			if retention == jetstream.LimitsPolicy && bySubject[subj+".bare"] != 1 {
				t.Fatalf("bare Term on a limits stream: %d copies in the DLQ, want 1", bySubject[subj+".bare"])
			}
		})
	}
}
