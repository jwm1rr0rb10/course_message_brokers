package dlq

import (
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Regression test for the original course code, which assigned orig.Header
// directly and panicked when the original message had no headers.
func TestBuildDLQMessageWithoutHeaders(t *testing.T) {
	orig := &jetstream.RawStreamMsg{Subject: "shop.orders.created", Sequence: 7, Data: []byte("x")}
	msg := BuildDLQMessage(Advisory{Stream: "ORDERS", Consumer: "BILLING", StreamSeq: 7, Deliveries: 5}, orig)

	if msg.Subject != "dlq.ORDERS.BILLING" {
		t.Fatalf("subject = %q", msg.Subject)
	}
	if got := msg.Header.Get("Dlq-Original-Subject"); got != "shop.orders.created" {
		t.Fatalf("Dlq-Original-Subject = %q", got)
	}
	if got := msg.Header.Get("Dlq-Deliveries"); got != "5" {
		t.Fatalf("Dlq-Deliveries = %q", got)
	}
}

func TestBuildDLQMessageReplacesMsgID(t *testing.T) {
	orig := &jetstream.RawStreamMsg{
		Subject: "shop.orders.created",
		Header:  nats.Header{jetstream.MsgIDHeader: []string{"order-1-created"}, "Trace-Id": []string{"abc"}},
		Data:    []byte("x"),
	}
	msg := BuildDLQMessage(Advisory{Stream: "ORDERS", Consumer: "BILLING", StreamSeq: 42, Reason: "bad json"}, orig)

	if got := msg.Header.Get(jetstream.MsgIDHeader); got != "dlq-ORDERS-BILLING-42" {
		t.Fatalf("Nats-Msg-Id = %q, want dlq-ORDERS-BILLING-42", got)
	}
	// another consumer failing the same message gets its own DLQ entry
	other := BuildDLQMessage(Advisory{Stream: "ORDERS", Consumer: "SHIPPING", StreamSeq: 42}, orig)
	if other.Header.Get(jetstream.MsgIDHeader) == msg.Header.Get(jetstream.MsgIDHeader) {
		t.Fatal("DLQ id must include the consumer")
	}
	if got := msg.Header.Get("Trace-Id"); got != "abc" {
		t.Fatalf("Trace-Id not copied: %q", got)
	}
	if got := msg.Header.Get("Dlq-Reason"); got != "bad json" {
		t.Fatalf("Dlq-Reason = %q", got)
	}
	// the original message must not be modified
	if got := orig.Header.Get(jetstream.MsgIDHeader); got != "order-1-created" {
		t.Fatalf("original header modified: %q", got)
	}
}
