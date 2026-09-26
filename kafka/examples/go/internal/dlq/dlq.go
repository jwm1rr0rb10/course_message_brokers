// Package dlq builds dead-letter records the way module 13.4 describes:
// the original key and value unchanged, original headers kept, context added.
package dlq

import (
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Topic returns the DLQ topic for a source topic.
func Topic(source string) string { return source + ".dlq" }

// Record builds the DLQ record for a failed source record.
func Record(r *kgo.Record, group string, cause error, attempts int) *kgo.Record {
	headers := make([]kgo.RecordHeader, 0, len(r.Headers)+7)
	headers = append(headers, r.Headers...) // keep the original headers
	msg := "<nil>"
	if cause != nil {
		msg = cause.Error()
	}
	headers = append(headers,
		kgo.RecordHeader{Key: "dlq-original-topic", Value: []byte(r.Topic)},
		kgo.RecordHeader{Key: "dlq-original-partition", Value: []byte(strconv.Itoa(int(r.Partition)))},
		kgo.RecordHeader{Key: "dlq-original-offset", Value: []byte(strconv.FormatInt(r.Offset, 10))},
		kgo.RecordHeader{Key: "dlq-error-message", Value: []byte(msg)},
		kgo.RecordHeader{Key: "dlq-attempts", Value: []byte(strconv.Itoa(attempts))},
		kgo.RecordHeader{Key: "dlq-consumer-group", Value: []byte(group)},
		kgo.RecordHeader{Key: "dlq-failed-at", Value: []byte(time.Now().UTC().Format(time.RFC3339))},
	)
	return &kgo.Record{
		Topic:   Topic(r.Topic),
		Key:     r.Key,   // same key: the DLQ keeps per-key ordering
		Value:   r.Value, // exactly the bytes that failed, even if they do not parse
		Headers: headers,
	}
}

// Header returns the value of the first header with the given key.
func Header(r *kgo.Record, key string) (string, bool) {
	for _, h := range r.Headers {
		if h.Key == key {
			return string(h.Value), true
		}
	}
	return "", false
}
