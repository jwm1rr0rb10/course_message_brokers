// consumer reads shop.orders.events in the "billing" group (module 5.5):
// manual commit after processing, BlockRebalanceOnPoll so a rebalance never
// happens in the middle of a batch, and invalid messages go to the DLQ
// BEFORE the offset is committed (module 13.4).
//
//	go run ./cmd/consumer
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/jwm1rr0rb10/KAFKAFREECOURSE/examples/go/internal/conn"
	"github.com/jwm1rr0rb10/KAFKAFREECOURSE/examples/go/internal/dlq"
)

type event struct {
	Event   string `json:"event"`
	OrderID string `json:"order_id"`
}

func main() {
	topic := flag.String("topic", "shop.orders.events", "topic")
	group := flag.String("group", "billing", "consumer group")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cl, err := kgo.NewClient(conn.Opts("billing",
		kgo.ConsumerGroup(*group),
		kgo.ConsumeTopics(*topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), // earliest
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
	)...)
	if err != nil {
		log.Fatal(err)
	}
	// Leaves the group right away. With BlockRebalanceOnPoll a plain Close can
	// hang if the last poll is still holding a rebalance, hence ...AllowingRebalance.
	defer cl.CloseAllowingRebalance()

	for {
		fetches := cl.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return // whatever this poll returned is not committed: it is re-read after restart
		}
		fetches.EachError(func(t string, p int32, err error) {
			log.Printf("fetch error %s/%d: %v", t, p, err)
		})

		var dead []*kgo.Record
		fetches.EachRecord(func(r *kgo.Record) {
			var e event
			if err := json.Unmarshal(r.Value, &e); err != nil {
				dead = append(dead, dlq.Record(r, *group, err, 1)) // permanent: no retries
				return
			}
			log.Printf("p=%d off=%d %s %s", r.Partition, r.Offset, e.OrderID, e.Event)
		})

		// DLQ first, commit second: a crash in between means reprocessing, not loss
		if len(dead) > 0 {
			if err := writeDLQ(ctx, cl, dead); err != nil {
				// Never skip the batch: the next commit would move past the poison
				// record and lose it. Exit uncommitted; the batch is re-read on restart.
				log.Printf("DLQ write failed, exiting without commit: %v", err)
				return
			}
			log.Printf("%d message(s) moved to DLQ", len(dead))
		}
		// ctx may already be cancelled (SIGTERM) while the batch was processed:
		// the batch is done, so commit it anyway with a context of its own.
		commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		if err := cl.CommitUncommittedOffsets(commitCtx); err != nil {
			log.Println("commit:", err)
		}
		cancel()
		cl.AllowRebalance()
	}
}

// writeDLQ retries until the DLQ accepts every record or ctx is done. A retry
// after a partial failure may duplicate records in the DLQ, which is fine: the
// DLQ is at-least-once like everything else. While we retry, rebalances stay
// blocked; if that exceeds the rebalance timeout the member is kicked out and
// the commit fails, which again means reprocessing, not loss.
func writeDLQ(ctx context.Context, cl *kgo.Client, recs []*kgo.Record) error {
	backoff := time.Second
	for {
		fresh := make([]*kgo.Record, len(recs)) // new records per attempt, nothing left from the failed one
		for i, r := range recs {
			fresh[i] = &kgo.Record{Topic: r.Topic, Key: r.Key, Value: r.Value, Headers: r.Headers}
		}
		err := cl.ProduceSync(ctx, fresh...).FirstErr()
		if err == nil {
			return nil
		}
		log.Printf("DLQ write failed, retrying in %v: %v", backoff, err)
		select {
		case <-ctx.Done():
			return err
		case <-time.After(backoff):
		}
		backoff = min(2*backoff, 30*time.Second)
	}
}
