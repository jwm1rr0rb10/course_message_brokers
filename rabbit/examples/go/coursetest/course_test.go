//go:build integration

// Package coursetest checks that statements made in the RabbitMQ course hold on
// a real broker. Each test names the module it verifies.
package coursetest

import (
	"errors"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/jwm1rr0rb10/RABBITMQFREECOURSE/examples/go/internal/testutil"
)

var quorum = amqp.Table{"x-queue-type": "quorum"}

// 3.3: topic patterns: * is one word, # is zero or more words.
func TestTopicRouting(t *testing.T) {
	c := testutil.Dial(t)
	ex := testutil.Exchange(t, c, "topic", nil)
	star := testutil.Queue(t, c, "course-star", quorum)
	hash := testutil.Queue(t, c, "course-hash", quorum)
	testutil.Bind(t, c, star, "order.*", ex)
	testutil.Bind(t, c, hash, "order.#", ex)

	ch := testutil.ConfirmCh(t, c)
	testutil.Publish(t, ch, ex, "order.created", "two words")
	testutil.Publish(t, ch, ex, "order.created.eu", "three words")

	if got := testutil.Bodies(testutil.Collect(t, c, star, 2, 3*time.Second, true, nil)); len(got) != 1 || got[0] != "two words" {
		t.Fatalf("order.* got %v, want only 'two words'", got)
	}
	if got := testutil.Collect(t, c, hash, 2, 3*time.Second, true, nil); len(got) != 2 {
		t.Fatalf("order.# got %d messages, want 2", len(got))
	}
}

// 1.3 / 2.4 / 5.4: an unroutable message is confirmed and silently dropped;
// with mandatory=true it comes back as basic.return (NO_ROUTE).
func TestUnroutableMessages(t *testing.T) {
	c := testutil.Dial(t)
	ex := testutil.Exchange(t, c, "direct", nil)
	ch := testutil.ConfirmCh(t, c)
	returns := ch.NotifyReturn(make(chan amqp.Return, 1))

	if !testutil.Publish(t, ch, ex, "nobody", "lost") {
		t.Fatal("unroutable message was nacked; the broker acks it")
	}
	select {
	case r := <-returns:
		t.Fatalf("got a return without mandatory: %v", r.ReplyText)
	case <-time.After(300 * time.Millisecond):
	}

	dc, err := ch.PublishWithDeferredConfirm(ex, "nobody", true, false, amqp.Publishing{Body: []byte("returned")})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-returns:
		if r.ReplyCode != 312 {
			t.Fatalf("reply code %d, want 312 NO_ROUTE", r.ReplyCode)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no basic.return for a mandatory unroutable message")
	}
	<-dc.Done()
}

// 3.7: an alternate exchange catches messages that match no binding.
func TestAlternateExchange(t *testing.T) {
	c := testutil.Dial(t)
	ae := testutil.Exchange(t, c, "fanout", nil)
	unrouted := testutil.Queue(t, c, "course-unrouted", quorum)
	testutil.Bind(t, c, unrouted, "", ae)
	ex := testutil.Exchange(t, c, "topic", amqp.Table{"alternate-exchange": ae})

	testutil.Publish(t, testutil.ConfirmCh(t, c), ex, "ordr.created", "typo")
	if got := testutil.Bodies(testutil.Collect(t, c, unrouted, 1, 5*time.Second, true, nil)); len(got) != 1 {
		t.Fatalf("alternate exchange queue got %v", got)
	}
}

// 6.2 / 6.3: an unacked message comes back with redelivered=true when the channel closes.
func TestRedeliveryAfterChannelClose(t *testing.T) {
	c := testutil.Dial(t)
	q := testutil.Queue(t, c, "course-redeliver", quorum)
	testutil.Publish(t, testutil.ConfirmCh(t, c), "", q, "job")

	ch, _ := c.Channel()
	msgs, err := ch.Consume(q, "", false, false, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	first := <-msgs
	if first.Redelivered {
		t.Fatal("first delivery marked redelivered")
	}
	_ = ch.Close() // "crash" without ack

	again := testutil.Collect(t, c, q, 1, 5*time.Second, true, nil)
	if len(again) != 1 || !again[0].Redelivered {
		t.Fatalf("expected one redelivered message, got %d", len(again))
	}
}

// 6.4: prefetch limits how many unacked messages a consumer holds.
func TestPrefetchLimitsUnacked(t *testing.T) {
	c := testutil.Dial(t)
	q := testutil.Queue(t, c, "course-prefetch", quorum)
	pub := testutil.ConfirmCh(t, c)
	for i := 0; i < 5; i++ {
		testutil.Publish(t, pub, "", q, "job")
	}
	ch := testutil.Ch(t, c)
	if err := ch.Qos(2, 0, false); err != nil {
		t.Fatal(err)
	}
	msgs, err := ch.Consume(q, "", false, false, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	got := 0
	timeout := time.After(time.Second)
loop:
	for {
		select {
		case <-msgs:
			got++
		case <-timeout:
			break loop
		}
	}
	if got != 2 {
		t.Fatalf("received %d unacked messages with prefetch 2", got)
	}
}

// 8.1 / 8.3: reject(requeue=false) sends the message to the DLX with x-death reason "rejected".
func TestRejectGoesToDLX(t *testing.T) {
	c := testutil.Dial(t)
	dlx := testutil.Exchange(t, c, "fanout", nil)
	dead := testutil.Queue(t, c, "course-dead", quorum)
	testutil.Bind(t, c, dead, "", dlx)
	q := testutil.Queue(t, c, "course-work", amqp.Table{"x-queue-type": "quorum", "x-dead-letter-exchange": dlx})

	testutil.Publish(t, testutil.ConfirmCh(t, c), "", q, "{not json")
	d := testutil.Collect(t, c, q, 1, 5*time.Second, false, nil)
	if len(d) != 1 {
		t.Fatal("no message to reject")
	}
	if err := d[0].Reject(false); err != nil {
		t.Fatal(err)
	}
	got := testutil.Collect(t, c, dead, 1, 5*time.Second, true, nil)
	if len(got) != 1 {
		t.Fatal("rejected message did not reach the DLX")
	}
	if reason := firstDeathReason(got[0]); reason != "rejected" {
		t.Fatalf("death reason %q, want rejected (headers %v)", reason, got[0].Headers)
	}
}

// 4.3 / 6.3 / 8.9: an explicit delivery-limit stops a requeue loop and dead-letters the message.
func TestExplicitDeliveryLimit(t *testing.T) {
	c := testutil.Dial(t)
	deliveries, reason := requeueUntilDeadLettered(t, c, amqp.Table{"x-delivery-limit": 2})
	if deliveries < 2 || deliveries > 3 {
		t.Fatalf("message delivered %d times with delivery-limit 2", deliveries)
	}
	if reason != "delivery_limit" {
		t.Fatalf("death reason %q, want delivery_limit", reason)
	}
}

// 4.3 / 6.3: since 4.0 quorum queues have a default delivery limit of 20.
func TestDefaultDeliveryLimitIs20(t *testing.T) {
	c := testutil.Dial(t)
	testutil.RequireVersion(t, c, 4, 0)
	deliveries, _ := requeueUntilDeadLettered(t, c, nil)
	if deliveries < 20 || deliveries > 21 {
		t.Fatalf("message delivered %d times without an explicit limit, want about 20", deliveries)
	}
}

// 4.5: x-overflow=reject-publish makes the broker nack publishes to a full queue.
// Classic queues enforce the limit exactly; quorum queues treat it as a soft
// limit and may accept one extra message before they start rejecting.
func TestOverflowRejectPublishNacks(t *testing.T) {
	c := testutil.Dial(t)
	for _, qt := range []string{"classic", "quorum"} {
		q := testutil.Queue(t, c, "course-full-"+qt, amqp.Table{
			"x-queue-type": qt, "x-max-length": 1, "x-overflow": "reject-publish"})
		ch := testutil.ConfirmCh(t, c)
		acks := 0
		nacked := false
		for i := 0; i < 5 && !nacked; i++ {
			if testutil.Publish(t, ch, "", q, "job") {
				acks++
			} else {
				nacked = true
			}
		}
		if !nacked {
			t.Fatalf("%s: 5 publishes to a queue with max-length 1 were all acked", qt)
		}
		if qt == "classic" && acks != 1 {
			t.Fatalf("classic: %d publishes acked before the first nack, want exactly 1", acks)
		}
		if acks > 2 {
			t.Fatalf("%s: %d publishes acked before the first nack", qt, acks)
		}
		t.Logf("%s: %d acked, then nack", qt, acks)
	}
}

// 4.5: the default overflow behaviour (drop-head) silently drops the oldest message.
func TestOverflowDropHeadByDefault(t *testing.T) {
	c := testutil.Dial(t)
	q := testutil.Queue(t, c, "course-drophead", amqp.Table{"x-queue-type": "quorum", "x-max-length": 1})
	ch := testutil.ConfirmCh(t, c)
	if !testutil.Publish(t, ch, "", q, "old") || !testutil.Publish(t, ch, "", q, "new") {
		t.Fatal("drop-head queue nacked a publish")
	}
	if got := testutil.Bodies(testutil.Collect(t, c, q, 2, 2*time.Second, true, nil)); len(got) != 1 || got[0] != "new" {
		t.Fatalf("queue contains %v, want only 'new'", got)
	}
}

// 1.6 / 2.7: since 4.3 non-durable, non-exclusive queues are rejected by default.
func TestTransientNonExclusiveQueueRejected(t *testing.T) {
	c := testutil.Dial(t)
	testutil.RequireVersion(t, c, 4, 3)
	ch := testutil.Ch(t, c)
	name := testutil.Name("course-transient")
	_, err := ch.QueueDeclare(name, false, false, false, false, nil)
	if err == nil {
		_, _ = testutil.Ch(t, c).QueueDelete(name, false, false, false)
		t.Fatal("non-durable non-exclusive queue was accepted")
	}
	var amqpErr *amqp.Error
	if errors.As(err, &amqpErr) {
		t.Logf("rejected: %d %s", amqpErr.Code, amqpErr.Reason)
	}
	// an exclusive transient queue is still fine
	if _, err := testutil.Ch(t, c).QueueDeclare("", false, true, true, false, nil); err != nil {
		t.Fatalf("exclusive transient queue rejected: %v", err)
	}
}

// 8.6: a retry queue with a queue TTL and a DLX pointing back delays redelivery.
func TestTTLRetryQueue(t *testing.T) {
	c := testutil.Dial(t)
	work := testutil.Queue(t, c, "course-retry-work", quorum)
	retry := testutil.Queue(t, c, "course-retry-1s", amqp.Table{
		"x-queue-type": "quorum", "x-message-ttl": 1000,
		"x-dead-letter-exchange": "", "x-dead-letter-routing-key": work})

	start := time.Now()
	testutil.Publish(t, testutil.ConfirmCh(t, c), "", retry, "try again")
	got := testutil.Collect(t, c, work, 1, 10*time.Second, true, nil)
	if len(got) != 1 {
		t.Fatal("message never came back from the retry queue")
	}
	if waited := time.Since(start); waited < 900*time.Millisecond {
		t.Fatalf("message came back after %v, before the 1s TTL", waited)
	}
	if reason := firstDeathReason(got[0]); reason != "expired" {
		t.Fatalf("death reason %q, want expired", reason)
	}
}

// 11.3: RPC over direct reply-to.
func TestDirectReplyToRPC(t *testing.T) {
	c := testutil.Dial(t)
	q := testutil.Queue(t, c, "course-rpc", quorum)

	server := testutil.Ch(t, c)
	reqs, err := server.Consume(q, "", false, false, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for d := range reqs {
			_ = server.Publish("", d.ReplyTo, false, false, amqp.Publishing{
				CorrelationId: d.CorrelationId, Body: []byte(strings.ToUpper(string(d.Body)))})
			_ = d.Ack(false)
		}
	}()

	client := testutil.Ch(t, c)
	replies, err := client.Consume("amq.rabbitmq.reply-to", "", true, false, false, false, nil) // must be auto-ack
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Publish("", q, false, false, amqp.Publishing{
		ReplyTo: "amq.rabbitmq.reply-to", CorrelationId: "42", Expiration: "5000", Body: []byte("ping")}); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-replies:
		if r.CorrelationId != "42" || string(r.Body) != "PING" {
			t.Fatalf("reply %q with correlation %q", r.Body, r.CorrelationId)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no RPC reply")
	}
}

// 4.4 / 10.3: a stream keeps messages after they are read; every consumer can replay from "first".
func TestStreamReplay(t *testing.T) {
	c := testutil.Dial(t)
	s := testutil.Queue(t, c, "course-stream", amqp.Table{"x-queue-type": "stream"})
	pub := testutil.ConfirmCh(t, c)
	for _, b := range []string{"a", "b", "c"} {
		testutil.Publish(t, pub, "", s, b)
	}
	for round := 1; round <= 2; round++ {
		got := testutil.Bodies(testutil.Collect(t, c, s, 3, 5*time.Second, true, amqp.Table{"x-stream-offset": "first"}))
		if strings.Join(got, "") != "abc" {
			t.Fatalf("round %d read %v, want [a b c]", round, got)
		}
	}
}

// requeueUntilDeadLettered nacks with requeue until the message shows up in the DLX.
func requeueUntilDeadLettered(t *testing.T, c *amqp.Connection, extra amqp.Table) (int, string) {
	t.Helper()
	dlx := testutil.Exchange(t, c, "fanout", nil)
	dead := testutil.Queue(t, c, "course-limit-dead", quorum)
	testutil.Bind(t, c, dead, "", dlx)
	args := amqp.Table{"x-queue-type": "quorum", "x-dead-letter-exchange": dlx}
	for k, v := range extra {
		args[k] = v
	}
	q := testutil.Queue(t, c, "course-limit", args)
	testutil.Publish(t, testutil.ConfirmCh(t, c), "", q, "poison")

	ch := testutil.Ch(t, c)
	if err := ch.Qos(1, 0, false); err != nil {
		t.Fatal(err)
	}
	msgs, err := ch.Consume(q, "", false, false, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	deliveries := 0
	for deliveries < 100 {
		select {
		case d := <-msgs:
			deliveries++
			_ = d.Nack(false, true) // always "fails" and requeues
		case <-time.After(3 * time.Second):
			got := testutil.Collect(t, c, dead, 1, 5*time.Second, true, nil)
			if len(got) != 1 {
				t.Fatalf("after %d deliveries the message is neither redelivered nor dead-lettered", deliveries)
			}
			return deliveries, firstDeathReason(got[0])
		}
	}
	t.Fatalf("message delivered %d times and still requeued: no delivery limit", deliveries)
	return 0, ""
}

func firstDeathReason(d amqp.Delivery) string {
	if r, ok := d.Headers["x-first-death-reason"].(string); ok {
		return r
	}
	if deaths, ok := d.Headers["x-death"].([]interface{}); ok && len(deaths) > 0 {
		if m, ok := deaths[0].(amqp.Table); ok {
			if r, ok := m["reason"].(string); ok {
				return r
			}
		}
	}
	return ""
}
