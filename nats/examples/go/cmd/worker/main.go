// worker reads the BILLING pull consumer (module 6.5) and shows the three
// outcomes: Ack on success, NakWithDelay with a growing delay on a transient
// error, and Term on a message that can never be processed. Exhausted
// messages end up in the DLQ when cmd/dlq-mover is running; terminated ones
// are published to the DLQ by the worker itself before Term (module 14.3),
// which also works on workqueue and interest streams.
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

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/jwm1rr0rb10/NATSFREECOURSE/examples/go/internal/conn"
	"github.com/jwm1rr0rb10/NATSFREECOURSE/examples/go/internal/dlq"
)

var errTemporary = errors.New("temporary failure")

// retryDelays is the client-side retry schedule for transient errors: the
// first failure waits 1s, the second 5s, and so on; the last value repeats.
// The consumer has no server-side BackOff on purpose: BackOff only applies
// when AckWait expires (crash, hang), plain Nak ignores it, and NakWithDelay
// on a BackOff consumer is offset by the BackOff entries (module 6.4).
var retryDelays = []time.Duration{time.Second, 5 * time.Second, 30 * time.Second, time.Minute}

// retryDelay returns the delay for a message that has been delivered n times.
func retryDelay(n uint64) time.Duration {
	if n == 0 {
		n = 1
	}
	if n > uint64(len(retryDelays)) {
		return retryDelays[len(retryDelays)-1]
	}
	return retryDelays[n-1]
}

type order struct {
	OrderID string `json:"order_id"`
	Amount  int    `json:"amount"`
}

func main() {
	failRate := flag.Float64("fail-rate", 0, "share of messages that fail with a transient error")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	closed := make(chan struct{})
	nc, err := conn.Connect("billing-worker", nats.ClosedHandler(func(*nats.Conn) { close(closed) }))
	if err != nil {
		log.Fatal(err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatal(err)
	}

	cons, err := js.CreateOrUpdateConsumer(ctx, "ORDERS", jetstream.ConsumerConfig{
		Durable:       "BILLING",
		FilterSubject: "shop.orders.created",
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		// ~ p99 of processing: how long the server waits for an ack before it
		// redelivers a message whose worker crashed or hung
		AckWait:       30 * time.Second,
		MaxAckPending: 500,
		// 1 attempt + one retry per entry of retryDelays
		MaxDeliver: len(retryDelays) + 1,
	})
	if err != nil {
		log.Fatal("consumer: ", err)
	}

	cc, err := cons.Consume(func(msg jetstream.Msg) {
		meta, err := msg.Metadata()
		if err != nil {
			// not a JetStream message: nothing sensible to ack
			log.Printf("metadata: %v", err)
			return
		}
		err = process(msg.Data(), *failRate)
		switch {
		case err == nil:
			if err := msg.Ack(); err != nil {
				log.Printf("ack   seq=%d: %v", meta.Sequence.Stream, err)
				return
			}
			log.Printf("ack   seq=%d deliveries=%d", meta.Sequence.Stream, meta.NumDelivered)
		case errors.Is(err, errTemporary):
			delay := retryDelay(meta.NumDelivered)
			if err := msg.NakWithDelay(delay); err != nil {
				log.Printf("nak   seq=%d: %v", meta.Sequence.Stream, err)
				return
			}
			log.Printf("nak   seq=%d deliveries=%d retry in %v", meta.Sequence.Stream, meta.NumDelivered, delay)
		default:
			pctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := dlq.Terminate(pctx, js, msg, err.Error()); err != nil {
				// DLQ not reachable: retry later instead of losing the message
				log.Printf("term  seq=%d: %v", meta.Sequence.Stream, err)
				if err := msg.NakWithDelay(retryDelay(meta.NumDelivered)); err != nil {
					log.Printf("nak   seq=%d: %v", meta.Sequence.Stream, err)
				}
				return
			}
			log.Printf("term  seq=%d reason=%v (copied to DLQ)", meta.Sequence.Stream, err)
		}
	}, jetstream.PullMaxMessages(128))
	if err != nil {
		log.Fatal(err)
	}

	log.Println("worker running, Ctrl+C to stop")
	<-ctx.Done()

	// finish in-flight messages, then flush acks and close the connection
	cc.Drain()
	shutdown := time.After(30 * time.Second)
	select {
	case <-cc.Closed():
	case <-shutdown:
		log.Println("consumer drain timed out")
	}
	if err := nc.Drain(); err != nil {
		log.Printf("drain: %v", err)
	}
	select {
	case <-closed:
	case <-shutdown:
		log.Println("connection drain timed out")
	}
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
