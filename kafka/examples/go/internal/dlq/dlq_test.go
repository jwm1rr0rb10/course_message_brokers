package dlq

import (
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestRecordKeepsDataAndAddsContext(t *testing.T) {
	src := &kgo.Record{
		Topic: "shop.orders.events", Partition: 2, Offset: 41,
		Key: []byte("order-1"), Value: []byte("{not json"),
		Headers: []kgo.RecordHeader{{Key: "trace-id", Value: []byte("abc")}},
	}
	d := Record(src, "billing", errors.New("invalid json"), 3)

	if d.Topic != "shop.orders.events.dlq" {
		t.Fatalf("topic = %q", d.Topic)
	}
	if string(d.Key) != "order-1" || string(d.Value) != "{not json" {
		t.Fatalf("key/value changed: %q %q", d.Key, d.Value)
	}
	for k, want := range map[string]string{
		"trace-id": "abc", "dlq-original-topic": "shop.orders.events",
		"dlq-original-partition": "2", "dlq-original-offset": "41",
		"dlq-error-message": "invalid json", "dlq-attempts": "3", "dlq-consumer-group": "billing",
	} {
		if got, ok := Header(d, k); !ok || got != want {
			t.Errorf("header %s = %q, want %q", k, got, want)
		}
	}
	if len(src.Headers) != 1 {
		t.Fatalf("source headers modified: %d", len(src.Headers))
	}
}

func TestRecordNilError(t *testing.T) {
	d := Record(&kgo.Record{Topic: "t"}, "g", nil, 1)
	if got, _ := Header(d, "dlq-error-message"); got != "<nil>" {
		t.Fatalf("got %q", got)
	}
}
