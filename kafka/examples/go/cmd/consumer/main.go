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
	defer cl.Close() // leaves the group right away

	for {
		fetches := cl.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return
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
			if err := cl.ProduceSync(ctx, dead...).FirstErr(); err != nil {
				log.Printf("DLQ write failed, not committing: %v", err)
				cl.AllowRebalance()
				continue
			}
			log.Printf("%d message(s) moved to DLQ", len(dead))
		}
		if err := cl.CommitUncommittedOffsets(ctx); err != nil {
			log.Println("commit:", err)
		}
		cl.AllowRebalance()
	}
}
