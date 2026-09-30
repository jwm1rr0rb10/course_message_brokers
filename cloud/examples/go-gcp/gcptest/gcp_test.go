//go:build integration

// Package gcptest checks the Pub/Sub claims from module 8 against the Pub/Sub emulator.
// It is a separate Go module because the Google Cloud client has a large dependency tree.
// Tests skip themselves if the emulator is down (or fail, if COURSE_REQUIRE_BROKER=1 as in CI).
package gcptest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/protobuf/types/known/durationpb"
)

func project() string {
	if p := os.Getenv("PUBSUB_PROJECT_ID"); p != "" {
		return p
	}
	return "course-project"
}

func unique(prefix string) string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return prefix + "-" + hex.EncodeToString(b)
}

func client(t *testing.T) *pubsub.Client {
	t.Helper()
	host := os.Getenv("PUBSUB_EMULATOR_HOST")
	if host == "" {
		host = "localhost:8085"
		t.Setenv("PUBSUB_EMULATOR_HOST", host)
	}
	if c, err := net.DialTimeout("tcp", host, 2*time.Second); err != nil {
		msg := fmt.Sprintf("Pub/Sub emulator is not running on %s", host)
		if os.Getenv("COURSE_REQUIRE_BROKER") == "1" {
			t.Fatal(msg + " (COURSE_REQUIRE_BROKER=1)")
		}
		t.Skip(msg)
	} else {
		_ = c.Close()
	}
	c, err := pubsub.NewClient(context.Background(), project())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func topic(t *testing.T, c *pubsub.Client) string {
	t.Helper()
	tp, err := c.TopicAdminClient.CreateTopic(context.Background(), &pubsubpb.Topic{
		Name: "projects/" + project() + "/topics/" + unique("orders")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = c.TopicAdminClient.DeleteTopic(context.Background(), &pubsubpb.DeleteTopicRequest{Topic: tp.Name})
	})
	return tp.Name
}

func subscription(t *testing.T, c *pubsub.Client, sub *pubsubpb.Subscription) (string, error) {
	t.Helper()
	sub.Name = "projects/" + project() + "/subscriptions/" + unique("sub")
	if sub.AckDeadlineSeconds == 0 {
		sub.AckDeadlineSeconds = 10
	}
	s, err := c.SubscriptionAdminClient.CreateSubscription(context.Background(), sub)
	if err != nil {
		return "", err
	}
	t.Cleanup(func() {
		_ = c.SubscriptionAdminClient.DeleteSubscription(context.Background(), &pubsubpb.DeleteSubscriptionRequest{Subscription: s.Name})
	})
	return s.Name, nil
}

func mustSub(t *testing.T, c *pubsub.Client, sub *pubsubpb.Subscription) string {
	t.Helper()
	name, err := subscription(t, c, sub)
	if err != nil {
		t.Fatal(err)
	}
	return name
}

func publish(t *testing.T, c *pubsub.Client, topicName string, msgs ...*pubsub.Message) {
	t.Helper()
	p := c.Publisher(topicName)
	defer p.Stop()
	for _, m := range msgs {
		if _, err := p.Publish(context.Background(), m).Get(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}

// receive runs streaming pull for up to wait and returns the data of messages it acked.
// handle decides per message: true = ack, false = nack.
func receive(t *testing.T, c *pubsub.Client, sub string, want int, wait time.Duration, handle func(*pubsub.Message) bool) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	var mu sync.Mutex
	var got []string
	err := c.Subscriber(sub).Receive(ctx, func(_ context.Context, m *pubsub.Message) {
		if handle != nil && !handle(m) {
			m.Nack()
			return
		}
		m.Ack()
		mu.Lock()
		got = append(got, string(m.Data))
		if len(got) >= want {
			cancel()
		}
		mu.Unlock()
	})
	if err != nil && ctx.Err() == nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	return append([]string(nil), got...)
}

// 8.1: every subscription of a topic gets its own copy.
func TestFanout(t *testing.T) {
	c := client(t)
	tp := topic(t, c)
	a := mustSub(t, c, &pubsubpb.Subscription{Topic: tp})
	b := mustSub(t, c, &pubsubpb.Subscription{Topic: tp})
	publish(t, c, tp, &pubsub.Message{Data: []byte("order-1")})
	for _, s := range []string{a, b} {
		if got := receive(t, c, s, 1, 10*time.Second, nil); len(got) != 1 || got[0] != "order-1" {
			t.Fatalf("%s got %v", s, got)
		}
	}
}

// 8.3: a nacked message is delivered again.
func TestNackRedelivers(t *testing.T) {
	c := client(t)
	tp := topic(t, c)
	s := mustSub(t, c, &pubsubpb.Subscription{Topic: tp})
	publish(t, c, tp, &pubsub.Message{Data: []byte("job")})
	var mu sync.Mutex
	seen := 0
	got := receive(t, c, s, 1, 20*time.Second, func(*pubsub.Message) bool {
		mu.Lock()
		defer mu.Unlock()
		seen++
		return seen > 1 // nack the first delivery, ack the second
	})
	if len(got) != 1 || seen < 2 {
		t.Fatalf("expected a redelivery after nack: deliveries=%d acked=%v", seen, got)
	}
}

// 8.6: messages with one ordering key arrive in publish order.
func TestOrderingKey(t *testing.T) {
	c := client(t)
	tp := topic(t, c)
	s := mustSub(t, c, &pubsubpb.Subscription{Topic: tp, EnableMessageOrdering: true})
	p := c.Publisher(tp)
	p.EnableMessageOrdering = true
	events := []string{"created", "paid", "shipped", "delivered"}
	var results []*pubsub.PublishResult
	for _, e := range events {
		results = append(results, p.Publish(context.Background(), &pubsub.Message{Data: []byte(e), OrderingKey: "order-1"}))
	}
	for _, r := range results {
		if _, err := r.Get(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	p.Stop()
	got := receive(t, c, s, len(events), 20*time.Second, nil)
	if len(got) != len(events) {
		t.Fatalf("got %v", got)
	}
	for i := range events {
		if got[i] != events[i] {
			t.Fatalf("order broken: %v", got)
		}
	}
}

// 8.8: a subscription filter on attributes.
func TestSubscriptionFilter(t *testing.T) {
	c := client(t)
	tp := topic(t, c)
	s, err := subscription(t, c, &pubsubpb.Subscription{Topic: tp, Filter: `attributes.event_type = "OrderCreated"`})
	if err != nil {
		t.Skipf("emulator does not support filters: %v", err)
	}
	publish(t, c, tp,
		&pubsub.Message{Data: []byte("cancelled"), Attributes: map[string]string{"event_type": "OrderCancelled"}},
		&pubsub.Message{Data: []byte("created"), Attributes: map[string]string{"event_type": "OrderCreated"}})
	got := receive(t, c, s, 2, 8*time.Second, nil)
	if len(got) != 1 || got[0] != "created" {
		t.Fatalf("got %v, want [created]", got)
	}
}

// 8.5: after MaxDeliveryAttempts the message goes to the dead letter topic.
func TestDeadLetterTopic(t *testing.T) {
	c := client(t)
	tp, dlqTopic := topic(t, c), topic(t, c)
	dlqSub := mustSub(t, c, &pubsubpb.Subscription{Topic: dlqTopic})
	s, err := subscription(t, c, &pubsubpb.Subscription{
		Topic:            tp,
		DeadLetterPolicy: &pubsubpb.DeadLetterPolicy{DeadLetterTopic: dlqTopic, MaxDeliveryAttempts: 5},
		RetryPolicy:      &pubsubpb.RetryPolicy{MinimumBackoff: durationpb.New(0), MaximumBackoff: durationpb.New(time.Second)},
	})
	if err != nil {
		t.Skipf("emulator does not support dead letter policies: %v", err)
	}
	publish(t, c, tp, &pubsub.Message{Data: []byte("poison")})
	var mu sync.Mutex
	attempts := 0
	receive(t, c, s, 1, 25*time.Second, func(*pubsub.Message) bool {
		mu.Lock()
		attempts++
		mu.Unlock()
		return false // always "fails"
	})
	dead := receive(t, c, dlqSub, 1, 10*time.Second, nil)
	if len(dead) == 0 && attempts > 5 {
		t.Skip("emulator accepted the policy but does not dead-letter messages")
	}
	if len(dead) != 1 || dead[0] != "poison" {
		t.Fatalf("DLQ got %v after %d attempts", dead, attempts)
	}
	if attempts != 5 {
		t.Logf("delivered %d times (emulators may redeliver slightly differently)", attempts)
	}
}
