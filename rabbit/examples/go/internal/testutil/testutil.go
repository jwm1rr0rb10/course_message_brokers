// Package testutil holds helpers for the integration tests.
package testutil

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/jwm1rr0rb10/RABBITMQFREECOURSE/examples/go/internal/conn"
)

// Name returns a unique name for a queue or exchange.
func Name(prefix string) string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return prefix + "." + hex.EncodeToString(b)
}

// Dial opens a connection closed at test end.
func Dial(t *testing.T) *amqp.Connection {
	t.Helper()
	c, err := amqp.Dial(conn.URL())
	if err != nil {
		t.Fatalf("dial %s: %v (is RabbitMQ running?)", conn.URL(), err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// Ch opens a channel. Protocol errors close channels, so tests use fresh ones.
func Ch(t *testing.T, c *amqp.Connection) *amqp.Channel {
	t.Helper()
	ch, err := c.Channel()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ch.Close() })
	return ch
}

// Version returns the broker's major and minor version.
func Version(c *amqp.Connection) (int, int) {
	v, _ := c.Properties["version"].(string)
	parts := strings.SplitN(v, ".", 3)
	if len(parts) < 2 {
		return 0, 0
	}
	major, _ := strconv.Atoi(parts[0])
	minor, _ := strconv.Atoi(parts[1])
	return major, minor
}

// RequireVersion skips the test on brokers older than major.minor.
func RequireVersion(t *testing.T, c *amqp.Connection, major, minor int) {
	t.Helper()
	ma, mi := Version(c)
	if ma < major || (ma == major && mi < minor) {
		t.Skipf("needs RabbitMQ %d.%d+, server is %d.%d", major, minor, ma, mi)
	}
}

// Queue declares a durable queue deleted at test end.
func Queue(t *testing.T, c *amqp.Connection, prefix string, args amqp.Table) string {
	t.Helper()
	name := Name(prefix)
	if _, err := Ch(t, c).QueueDeclare(name, true, false, false, false, args); err != nil {
		t.Fatalf("declare %s: %v", name, err)
	}
	t.Cleanup(func() {
		if ch, err := c.Channel(); err == nil {
			_, _ = ch.QueueDelete(name, false, false, false)
			_ = ch.Close()
		}
	})
	return name
}

// Exchange declares a durable exchange deleted at test end.
func Exchange(t *testing.T, c *amqp.Connection, kind string, args amqp.Table) string {
	t.Helper()
	name := Name("course-test-" + kind)
	if err := Ch(t, c).ExchangeDeclare(name, kind, true, false, false, false, args); err != nil {
		t.Fatalf("declare exchange %s: %v", name, err)
	}
	t.Cleanup(func() {
		if ch, err := c.Channel(); err == nil {
			_ = ch.ExchangeDelete(name, false, false)
			_ = ch.Close()
		}
	})
	return name
}

// Bind binds a queue to an exchange.
func Bind(t *testing.T, c *amqp.Connection, queue, key, exchange string) {
	t.Helper()
	if err := Ch(t, c).QueueBind(queue, key, exchange, false, nil); err != nil {
		t.Fatalf("bind %s -> %s (%s): %v", exchange, queue, key, err)
	}
}

// Publish sends one persistent message on a confirm-mode channel and returns the ack.
func Publish(t *testing.T, ch *amqp.Channel, exchange, key string, body string, extra ...func(*amqp.Publishing)) bool {
	t.Helper()
	p := amqp.Publishing{DeliveryMode: amqp.Persistent, Body: []byte(body)}
	for _, f := range extra {
		f(&p)
	}
	dc, err := ch.PublishWithDeferredConfirm(exchange, key, false, false, p)
	if err != nil {
		t.Fatal(err)
	}
	if dc == nil {
		t.Fatal("channel is not in confirm mode")
	}
	select {
	case <-dc.Done():
		return dc.Acked()
	case <-time.After(10 * time.Second):
		t.Fatal("no confirm within 10s")
		return false
	}
}

// ConfirmCh returns a channel in confirm mode.
func ConfirmCh(t *testing.T, c *amqp.Connection) *amqp.Channel {
	t.Helper()
	ch := Ch(t, c)
	if err := ch.Confirm(false); err != nil {
		t.Fatal(err)
	}
	return ch
}

// Collect consumes from a queue with manual ack and returns up to n deliveries
// received within wait (they are acked unless ack is false).
func Collect(t *testing.T, c *amqp.Connection, queue string, n int, wait time.Duration, ack bool, args amqp.Table) []amqp.Delivery {
	t.Helper()
	ch := Ch(t, c)
	if err := ch.Qos(100, 0, false); err != nil {
		t.Fatal(err)
	}
	msgs, err := ch.Consume(queue, "", false, false, false, false, args)
	if err != nil {
		t.Fatalf("consume %s: %v", queue, err)
	}
	var out []amqp.Delivery
	deadline := time.After(wait)
	for len(out) < n {
		select {
		case d, ok := <-msgs:
			if !ok {
				return out
			}
			if ack {
				_ = d.Ack(false)
			}
			out = append(out, d)
		case <-deadline:
			return out
		}
	}
	return out
}

// Bodies returns the message bodies as strings.
func Bodies(ds []amqp.Delivery) []string {
	out := make([]string, len(ds))
	for i, d := range ds {
		out[i] = string(d.Body)
	}
	return out
}

// Fmt is a small helper for failure messages.
func Fmt(v any) string { return fmt.Sprintf("%v", v) }
