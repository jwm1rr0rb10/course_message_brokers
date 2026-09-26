//go:build integration

// Package coursetest checks that statements made in the Kafka course hold on
// a real cluster. Each test names the module it verifies.
package coursetest

import (
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/jwm1rr0rb10/KAFKAFREECOURSE/examples/go/internal/dlq"
	"github.com/jwm1rr0rb10/KAFKAFREECOURSE/examples/go/internal/testutil"
)

// 1.4 / 2.4: records with the same key land in one partition, in order.
func TestSameKeySamePartitionInOrder(t *testing.T) {
	topic := testutil.Topic(t, 6, nil)
	cl := testutil.Client(t, kgo.DefaultProduceTopic(topic))

	var recs []*kgo.Record
	for i := 0; i < 10; i++ {
		recs = append(recs, &kgo.Record{Key: []byte("order-1"), Value: []byte(fmt.Sprint(i))})
	}
	testutil.Produce(t, cl, recs...)
	for i := 1; i < len(recs); i++ {
		if recs[i].Partition != recs[0].Partition {
			t.Fatalf("record %d went to partition %d, first to %d", i, recs[i].Partition, recs[0].Partition)
		}
		if recs[i].Offset <= recs[i-1].Offset {
			t.Fatalf("offsets not increasing: %d then %d", recs[i-1].Offset, recs[i].Offset)
		}
	}
}

// 3.5: adding partitions changes where existing keys go.
func TestAddingPartitionsMovesKeys(t *testing.T) {
	topic := testutil.Topic(t, 3, nil)
	cl := testutil.Client(t, kgo.DefaultProduceTopic(topic))
	ctx := testutil.Ctx(t, 60*time.Second)

	keys := make([]string, 30)
	before := map[string]int32{}
	for i := range keys {
		keys[i] = fmt.Sprintf("order-%d", i)
		r := &kgo.Record{Key: []byte(keys[i]), Value: []byte("v1")}
		testutil.Produce(t, cl, r)
		before[keys[i]] = r.Partition
	}

	resp, err := testutil.Admin(t).UpdatePartitions(ctx, 6, topic)
	if err == nil {
		err = resp.Error()
	}
	if err != nil {
		t.Fatal(err)
	}
	cl.ForceMetadataRefresh()

	moved := 0
	deadline := time.Now().Add(20 * time.Second)
	for moved == 0 && time.Now().Before(deadline) {
		for _, k := range keys {
			r := &kgo.Record{Key: []byte(k), Value: []byte("v2")}
			testutil.Produce(t, cl, r)
			if r.Partition != before[k] {
				moved++
			}
		}
		if moved == 0 {
			time.Sleep(time.Second) // metadata may not show the new partitions yet
		}
	}
	if moved == 0 {
		t.Fatal("no key changed partition after going from 3 to 6 partitions")
	}
	t.Logf("%d of %d writes landed in a different partition than before", moved, len(keys))
}

// 3.4 / FAQ: the number of partitions cannot be decreased.
func TestPartitionsCannotDecrease(t *testing.T) {
	topic := testutil.Topic(t, 6, nil)
	resp, err := testutil.Admin(t).UpdatePartitions(testutil.Ctx(t, 30*time.Second), 3, topic)
	if err == nil {
		err = resp.Error()
	}
	if err == nil {
		t.Fatal("decreasing partitions from 6 to 3 was accepted")
	}
}

// 5.2: without a commit the group reads the same records again (at-least-once);
// after a commit it continues from the committed offset.
func TestGroupResumesFromCommittedOffset(t *testing.T) {
	topic := testutil.Topic(t, 1, nil)
	group := testutil.Name("course-group")
	prod := testutil.Client(t, kgo.DefaultProduceTopic(topic))
	for i := 0; i < 5; i++ {
		testutil.Produce(t, prod, &kgo.Record{Value: []byte(fmt.Sprint(i))})
	}

	consumer := func() *kgo.Client {
		return testutil.Client(t, kgo.ConsumerGroup(group), kgo.ConsumeTopics(topic),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.DisableAutoCommit())
	}

	c1 := consumer()
	first := testutil.Poll(t, c1, 5, 20*time.Second)
	if len(first) != 5 {
		t.Fatalf("first consumer got %d records, want 5", len(first))
	}
	c1.Close() // "crash" without committing

	c2 := consumer()
	again := testutil.Poll(t, c2, 5, 20*time.Second)
	if len(again) != 5 || again[0].Offset != first[0].Offset {
		t.Fatalf("without a commit the group should start over: got %d records", len(again))
	}
	if err := c2.CommitRecords(testutil.Ctx(t, 10*time.Second), again[2]); err != nil {
		t.Fatal(err)
	}
	c2.Close()

	c3 := consumer()
	rest := testutil.Poll(t, c3, 2, 20*time.Second)
	if len(rest) == 0 || rest[0].Offset != again[2].Offset+1 {
		t.Fatalf("after committing offset %d the group should continue from %d, got %v",
			again[2].Offset, again[2].Offset+1, offsets(rest))
	}
}

// 5.3: a new group reading from the end does not see older records.
func TestNewGroupWithLatestMissesOldRecords(t *testing.T) {
	topic := testutil.Topic(t, 1, nil)
	prod := testutil.Client(t, kgo.DefaultProduceTopic(topic))
	testutil.Produce(t, prod, &kgo.Record{Value: []byte("old")})

	c := testutil.Client(t, kgo.ConsumerGroup(testutil.Name("course-latest")), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))
	if got := testutil.Poll(t, c, 1, 5*time.Second); len(got) != 0 {
		t.Fatalf("group with 'latest' read an old record: %q", got[0].Value)
	}
}

// 6.3: read_committed hides records of aborted transactions, read_uncommitted shows them.
func TestReadCommittedHidesAbortedTransaction(t *testing.T) {
	topic := testutil.Topic(t, 1, nil)
	ctx := testutil.Ctx(t, 60*time.Second)
	txn := testutil.Client(t, kgo.TransactionalID(testutil.Name("course-txn")), kgo.DefaultProduceTopic(topic))

	for _, tc := range []struct {
		value  string
		commit kgo.TransactionEndTry
	}{{"aborted", kgo.TryAbort}, {"committed", kgo.TryCommit}} {
		if err := txn.BeginTransaction(); err != nil {
			t.Fatal(err)
		}
		if err := txn.ProduceSync(ctx, &kgo.Record{Value: []byte(tc.value)}).FirstErr(); err != nil {
			t.Fatal(err)
		}
		if err := txn.EndTransaction(ctx, tc.commit); err != nil {
			t.Fatal(err)
		}
	}

	read := func(level kgo.IsolationLevel) []string {
		c := testutil.Client(t, kgo.ConsumeTopics(topic), kgo.FetchIsolationLevel(level),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
		var vals []string
		for _, r := range testutil.Poll(t, c, 2, 10*time.Second) {
			vals = append(vals, string(r.Value))
		}
		return vals
	}
	if got := read(kgo.ReadCommitted()); len(got) != 1 || got[0] != "committed" {
		t.Fatalf("read_committed saw %v, want [committed]", got)
	}
	if got := read(kgo.ReadUncommitted()); len(got) != 2 {
		t.Fatalf("read_uncommitted saw %v, want both records", got)
	}
}

// 7.3: a compacted topic rejects records without a key.
func TestCompactedTopicRejectsNullKey(t *testing.T) {
	if os.Getenv("KAFKA_FAKE") != "" {
		t.Skip("kfake does not implement this broker-side validation")
	}
	topic := testutil.Topic(t, 1, map[string]*string{"cleanup.policy": kadm.StringPtr("compact")})
	cl := testutil.Client(t, kgo.DefaultProduceTopic(topic), kgo.RecordRetries(1))
	err := cl.ProduceSync(testutil.Ctx(t, 30*time.Second), &kgo.Record{Value: []byte("no key")}).FirstErr()
	if err == nil {
		t.Fatal("compacted topic accepted a record without a key")
	}
	if !errors.Is(err, kerr.CorruptMessage) && !errors.Is(err, kerr.InvalidRecord) {
		t.Logf("rejected with %v", err)
	}
}

// 13.4: a DLQ record keeps key, value and headers and adds the context.
func TestDLQRecordRoundTrip(t *testing.T) {
	src := testutil.Topic(t, 1, nil)
	dlqTopic := dlq.Topic(src)
	adm := testutil.Admin(t)
	ctx := testutil.Ctx(t, 60*time.Second)
	if resp, err := adm.CreateTopic(ctx, 1, testutil.RF(), nil, dlqTopic); err != nil || resp.Err != nil {
		t.Fatalf("create %s: %v %v", dlqTopic, err, resp.Err)
	}
	t.Cleanup(func() { _, _ = adm.DeleteTopics(ctx, dlqTopic) })

	cl := testutil.Client(t)
	poison := &kgo.Record{Topic: src, Key: []byte("order-9"), Value: []byte("{not json"),
		Headers: []kgo.RecordHeader{{Key: "trace-id", Value: []byte("t-1")}}}
	testutil.Produce(t, cl, poison)
	testutil.Produce(t, cl, dlq.Record(poison, "billing", errors.New("invalid json"), 1))

	c := testutil.Client(t, kgo.ConsumeTopics(dlqTopic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	got := testutil.Poll(t, c, 1, 15*time.Second)
	if len(got) != 1 {
		t.Fatal("nothing in the DLQ")
	}
	r := got[0]
	if string(r.Key) != "order-9" || string(r.Value) != "{not json" {
		t.Fatalf("key/value changed: %q %q", r.Key, r.Value)
	}
	for k, want := range map[string]string{
		"trace-id": "t-1", "dlq-original-topic": src,
		"dlq-original-offset": fmt.Sprint(poison.Offset), "dlq-error-message": "invalid json",
	} {
		if v, _ := dlq.Header(r, k); v != want {
			t.Errorf("header %s = %q, want %q", k, v, want)
		}
	}
}

func offsets(rs []*kgo.Record) []int64 {
	out := make([]int64, len(rs))
	for i, r := range rs {
		out[i] = r.Offset
	}
	return out
}
