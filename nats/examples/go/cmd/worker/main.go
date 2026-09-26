// worker reads the BILLING pull consumer (module 6.5) and shows the three
// outcomes: Ack on success, NakWithDelay on a transient error, Term on a
// message that can never be processed. Terminated and exhausted messages end
// up in the DLQ when cmd/dlq-mover is running (module 14.3).
//
//	go run ./cmd/worker -fail-rate 0.3
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log"
	"math/rand/v2"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/jwm1rr0rb10/NATSFREECOURSE/examples/go/internal/conn"
)

var errTemporary = errors.New("temporary failure")

type order struct {
	OrderID string `json:"order_id"`
	Amount  int    `json:"amount"`
}

func main() {
	failRate := flag.Float64("fail-rate", 0, "share of messages that fail with a transient error")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	nc, err := conn.Connect("billing-worker")
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Drain()

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatal(err)
	}

	cons, err := js.CreateOrUpdateConsumer(ctx, "ORDERS", jetstream.ConsumerConfig{
		Durable:       "BILLING",
		FilterSubject: "shop.orders.created",
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckWait:       30 * time.Second,
		MaxAckPending: 500,
		// backoff replaces AckWait for redeliveries; MaxDeliver must not be
		// smaller than len(BackOff), and len+1 uses every delay (module 6.4)
		BackOff:    []time.Duration{time.Second, 5 * time.Second, 30 * time.Second},
		MaxDeliver: 4,
	})
	if err != nil {
		log.Fatal("consumer: ", err)
	}

	cc, err := cons.Consume(func(msg jetstream.Msg) {
		meta, _ := msg.Metadata()
		err := process(msg.Data(), *failRate)
		switch {
		case err == nil:
			_ = msg.Ack()
			log.Printf("ack   seq=%d deliveries=%d", meta.Sequence.Stream, meta.NumDelivered)
		case errors.Is(err, errTemporary):
			_ = msg.NakWithDelay(2 * time.Second)
			log.Printf("nak   seq=%d deliveries=%d", meta.Sequence.Stream, meta.NumDelivered)
		default:
			_ = msg.TermWithReason(err.Error())
			log.Printf("term  seq=%d reason=%v", meta.Sequence.Stream, err)
		}
	}, jetstream.PullMaxMessages(128))
	if err != nil {
		log.Fatal(err)
	}

	log.Println("worker running, Ctrl+C to stop")
	<-ctx.Done()
	cc.Drain() // finish in-flight messages before exiting
	<-cc.Closed()
}

func process(data []byte, failRate float64) error {
	var o order
	if err := json.Unmarshal(data, &o); err != nil {
		return err // permanent: retrying will never help
	}
	if rand.Float64() < failRate {
		return errTemporary
	}
	return nil
}
