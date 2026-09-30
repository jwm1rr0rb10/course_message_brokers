//go:build integration

package cloudtest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"

	"github.com/jwm1rr0rb10/CLOUDBROKERSCOURSE/examples/go/internal/emu"
)

// Entities come from examples/emulators/servicebus-config.json:
// queues tasks (MaxDeliveryCount 3), orders-ordered (sessions), dedup (duplicate detection),
// topic orders with subscriptions billing (SQL filter) and audit (everything).

func sbClient(t *testing.T) *azservicebus.Client {
	t.Helper()
	if !emu.Reachable("localhost:5672") {
		emu.Unavailable(t, "Service Bus emulator is not running on localhost:5672")
	}
	c, err := azservicebus.NewClientFromConnectionString(emu.ServiceBusConnectionString(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close(context.Background()) })
	return c
}

func sbSend(t *testing.T, c *azservicebus.Client, entity string, m *azservicebus.Message) {
	t.Helper()
	s, err := c.NewSender(entity, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(context.Background())
	if err := s.SendMessage(context.Background(), m, nil); err != nil {
		t.Fatal(err)
	}
}

func marked(marker string) *azservicebus.Message {
	return &azservicebus.Message{Body: []byte("job"), ApplicationProperties: map[string]any{"marker": marker}}
}

// sbFind receives until the message with the marker shows up; other messages are completed.
func sbFind(t *testing.T, r *azservicebus.Receiver, marker string, wait time.Duration) *azservicebus.ReceivedMessage {
	t.Helper()
	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		msgs, err := r.ReceiveMessages(ctx, 10, nil)
		cancel()
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
		for _, m := range msgs {
			if m.ApplicationProperties["marker"] == marker {
				return m
			}
			_ = r.CompleteMessage(context.Background(), m, nil)
		}
	}
	return nil
}

func receiver(t *testing.T, c *azservicebus.Client, queue string, opts *azservicebus.ReceiverOptions) *azservicebus.Receiver {
	t.Helper()
	r, err := c.NewReceiverForQueue(queue, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close(context.Background()) })
	return r
}

// 6.3: abandon returns the message and increments DeliveryCount.
func TestServiceBusAbandon(t *testing.T) {
	c := sbClient(t)
	marker := unique("abandon")
	sbSend(t, c, "tasks", marked(marker))
	r := receiver(t, c, "tasks", nil)
	first := sbFind(t, r, marker, 10*time.Second)
	if first == nil {
		t.Fatal("message not received")
	}
	if err := r.AbandonMessage(context.Background(), first, nil); err != nil {
		t.Fatal(err)
	}
	again := sbFind(t, r, marker, 10*time.Second)
	if again == nil || again.DeliveryCount != first.DeliveryCount+1 {
		t.Fatalf("expected a redelivery with DeliveryCount+1, got %+v", again)
	}
	_ = r.CompleteMessage(context.Background(), again, nil)
}

// 6.3: after MaxDeliveryCount (3) the message is dead-lettered with MaxDeliveryCountExceeded.
func TestServiceBusMaxDeliveryCount(t *testing.T) {
	c := sbClient(t)
	marker := unique("maxdc")
	sbSend(t, c, "tasks", marked(marker))
	r := receiver(t, c, "tasks", nil)
	deliveries := 0
	for m := sbFind(t, r, marker, 5*time.Second); m != nil; m = sbFind(t, r, marker, 5*time.Second) {
		deliveries++
		_ = r.AbandonMessage(context.Background(), m, nil)
	}
	if deliveries != 3 {
		t.Fatalf("delivered %d times with MaxDeliveryCount 3", deliveries)
	}
	dlq := receiver(t, c, "tasks", &azservicebus.ReceiverOptions{SubQueue: azservicebus.SubQueueDeadLetter})
	m := sbFind(t, dlq, marker, 10*time.Second)
	if m == nil || m.DeadLetterReason == nil || *m.DeadLetterReason != "MaxDeliveryCountExceeded" {
		t.Fatalf("expected the message in the DLQ with MaxDeliveryCountExceeded, got %+v", m)
	}
	_ = dlq.CompleteMessage(context.Background(), m, nil)
}

// 6.3 / 6.4: explicit dead-lettering keeps reason and description.
func TestServiceBusExplicitDeadLetter(t *testing.T) {
	c := sbClient(t)
	marker := unique("dl")
	sbSend(t, c, "tasks", marked(marker))
	r := receiver(t, c, "tasks", nil)
	m := sbFind(t, r, marker, 10*time.Second)
	if m == nil {
		t.Fatal("message not received")
	}
	if err := r.DeadLetterMessage(context.Background(), m, &azservicebus.DeadLetterOptions{
		Reason: to.Ptr("InvalidPayload"), ErrorDescription: to.Ptr("bad json")}); err != nil {
		t.Fatal(err)
	}
	dlq := receiver(t, c, "tasks", &azservicebus.ReceiverOptions{SubQueue: azservicebus.SubQueueDeadLetter})
	d := sbFind(t, dlq, marker, 10*time.Second)
	if d == nil || d.DeadLetterReason == nil || *d.DeadLetterReason != "InvalidPayload" ||
		d.DeadLetterErrorDescription == nil || *d.DeadLetterErrorDescription != "bad json" {
		t.Fatalf("unexpected DLQ message %+v", d)
	}
	_ = dlq.CompleteMessage(context.Background(), d, nil)
}

// 6.5: messages of one session arrive in order.
func TestServiceBusSessionOrder(t *testing.T) {
	c := sbClient(t)
	session := unique("order")
	for _, ev := range []string{"OrderCreated", "OrderPaid", "OrderShipped"} {
		sbSend(t, c, "orders-ordered", &azservicebus.Message{Body: []byte(ev), SessionID: to.Ptr(session)})
	}
	sr, err := c.AcceptSessionForQueue(context.Background(), "orders-ordered", session, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer sr.Close(context.Background())
	var got []string
	for len(got) < 3 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		msgs, err := sr.ReceiveMessages(ctx, 3, nil)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range msgs {
			got = append(got, string(m.Body))
			_ = sr.CompleteMessage(context.Background(), m, nil)
		}
	}
	if len(got) != 3 || got[0] != "OrderCreated" || got[1] != "OrderPaid" || got[2] != "OrderShipped" {
		t.Fatalf("got %v", got)
	}
}

// 6.6: duplicate detection drops a resend with the same MessageID.
func TestServiceBusDuplicateDetection(t *testing.T) {
	c := sbClient(t)
	marker := unique("dup")
	for i := 0; i < 2; i++ {
		m := marked(marker)
		m.MessageID = to.Ptr("evt-" + marker)
		sbSend(t, c, "dedup", m)
	}
	r := receiver(t, c, "dedup", nil)
	first := sbFind(t, r, marker, 10*time.Second)
	if first == nil {
		t.Fatal("message not received")
	}
	_ = r.CompleteMessage(context.Background(), first, nil)
	if sbFind(t, r, marker, 3*time.Second) != nil {
		t.Fatal("the duplicate must be dropped")
	}
}

// 6.7: a SQL filter on a subscription passes only matching messages.
func TestServiceBusSubscriptionFilter(t *testing.T) {
	c := sbClient(t)
	marker := unique("filter")
	for _, ev := range []string{"OrderCreated", "OrderCancelled"} {
		sbSend(t, c, "orders", &azservicebus.Message{Body: []byte(ev),
			ApplicationProperties: map[string]any{"EventType": ev, "marker": marker}})
	}
	collect := func(sub string) map[string]bool {
		r, err := c.NewReceiverForSubscription("orders", sub, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close(context.Background())
		got := map[string]bool{}
		deadline := time.Now().Add(8 * time.Second)
		for time.Now().Before(deadline) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			msgs, _ := r.ReceiveMessages(ctx, 10, nil)
			cancel()
			for _, m := range msgs {
				if m.ApplicationProperties["marker"] == marker {
					got[string(m.Body)] = true
				}
				_ = r.CompleteMessage(context.Background(), m, nil)
			}
		}
		return got
	}
	if got := collect("billing"); len(got) != 1 || !got["OrderCreated"] {
		t.Fatalf("billing got %v, want only OrderCreated", got)
	}
	if got := collect("audit"); len(got) != 2 {
		t.Fatalf("audit got %v, want both events", got)
	}
}
