// producer sends keyed order events (module 4.7): acks=all and idempotence
// are franz-go defaults, written out explicitly as documentation.
//
//	go run ./cmd/producer -n 10
//	go run ./cmd/producer -n 3 -poison     # plus one invalid message for the DLQ demo
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/jwm1rr0rb10/KAFKAFREECOURSE/examples/go/internal/conn"
)

func main() {
	n := flag.Int("n", 10, "number of orders")
	topic := flag.String("topic", "shop.orders.events", "topic")
	poison := flag.Bool("poison", false, "also send one message that is not valid JSON")
	flag.Parse()

	cl, err := kgo.NewClient(conn.Opts("order-service",
		kgo.DefaultProduceTopic(*topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(10*time.Millisecond),
		kgo.ProducerBatchCompression(kgo.ZstdCompression()),
	)...)
	if err != nil {
		log.Fatal(err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for i := 1; i <= *n; i++ {
		id := fmt.Sprintf("order-%d", i)
		for _, ev := range []string{"OrderCreated", "OrderPaid"} {
			r := &kgo.Record{
				Key:     []byte(id),
				Value:   []byte(fmt.Sprintf(`{"event":%q,"order_id":%q}`, ev, id)),
				Headers: []kgo.RecordHeader{{Key: "event-type", Value: []byte(ev)}},
			}
			// ProduceSync keeps the example simple; see module 4.6 on async sends
			if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
				log.Fatalf("%s %s not written: %v", id, ev, err)
			}
			log.Printf("%s %-12s -> partition=%d offset=%d", id, ev, r.Partition, r.Offset)
		}
	}
	if *poison {
		r := &kgo.Record{Key: []byte("order-poison"), Value: []byte("{not json")}
		if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
			log.Fatal(err)
		}
		log.Printf("poison -> partition=%d offset=%d", r.Partition, r.Offset)
	}
}
