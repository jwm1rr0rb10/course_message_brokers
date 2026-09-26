// publisher creates the ORDERS stream (module 5.5) and publishes orders with a
// stable Nats-Msg-Id, so running it twice does not create duplicates (7.3).
//
//	go run ./cmd/publisher -n 10
//	go run ./cmd/publisher -n 3 -poison   # adds a message the worker will Term()
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/jwm1rr0rb10/NATSFREECOURSE/examples/go/internal/conn"
)

type order struct {
	OrderID string `json:"order_id"`
	Amount  int    `json:"amount"`
}

func main() {
	n := flag.Int("n", 10, "number of orders")
	replicas := flag.Int("replicas", 3, "stream replicas")
	poison := flag.Bool("poison", false, "also publish one invalid message")
	flag.Parse()

	nc, err := conn.Connect("publisher")
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Drain()

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:       "ORDERS",
		Subjects:   []string{"shop.orders.>"},
		Storage:    jetstream.FileStorage,
		Retention:  jetstream.LimitsPolicy,
		Replicas:   *replicas,
		MaxAge:     30 * 24 * time.Hour,
		MaxBytes:   1 << 30,
		Duplicates: 2 * time.Minute,
	}); err != nil {
		log.Fatal("stream: ", err)
	}

	for i := 1; i <= *n; i++ {
		data, _ := json.Marshal(order{OrderID: fmt.Sprintf("order-%d", i), Amount: 1000 * i})
		msg := &nats.Msg{
			Subject: "shop.orders.created",
			Header:  nats.Header{},
			Data:    data,
		}
		// stable per business event, NOT a fresh random id per attempt
		msg.Header.Set(jetstream.MsgIDHeader, fmt.Sprintf("order-%d-created", i))

		ack, err := js.PublishMsg(ctx, msg)
		if err != nil {
			log.Fatalf("publish order-%d: %v (not stored, retry with the same id)", i, err)
		}
		log.Printf("order-%d -> stream=%s seq=%d duplicate=%v", i, ack.Stream, ack.Sequence, ack.Duplicate)
	}

	if *poison {
		ack, err := js.Publish(ctx, "shop.orders.created", []byte("{not json"),
			jetstream.WithMsgID("poison-1"))
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("poison -> seq=%d duplicate=%v", ack.Sequence, ack.Duplicate)
	}
}
