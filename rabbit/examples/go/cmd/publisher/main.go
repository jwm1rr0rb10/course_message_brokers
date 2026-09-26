// publisher sends order events to the shop.events topic exchange with
// publisher confirms and mandatory=true (module 5.6).
//
//	go run ./cmd/publisher -n 5
//	go run ./cmd/publisher -n 1 -key ordr.created   # typo: returned as unroutable
//	go run ./cmd/publisher -n 1 -poison             # invalid JSON for the DLX demo
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/jwm1rr0rb10/RABBITMQFREECOURSE/examples/go/internal/conn"
)

func main() {
	n := flag.Int("n", 5, "number of orders")
	key := flag.String("key", "order.created", "routing key")
	poison := flag.Bool("poison", false, "send an invalid JSON body")
	flag.Parse()

	c, err := amqp.Dial(conn.URL())
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()
	ch, err := c.Channel()
	if err != nil {
		log.Fatal(err)
	}
	defer ch.Close()

	if err := ch.ExchangeDeclare("shop.events", "topic", true, false, false, false, nil); err != nil {
		log.Fatal(err)
	}
	if err := ch.Confirm(false); err != nil {
		log.Fatal(err)
	}
	returns := ch.NotifyReturn(make(chan amqp.Return, *n))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var pending []*amqp.DeferredConfirmation
	for i := 1; i <= *n; i++ {
		body := fmt.Sprintf(`{"order_id":"order-%d","amount":%d}`, i, 1000*i)
		if *poison {
			body = "{not json"
		}
		dc, err := ch.PublishWithDeferredConfirmWithContext(ctx, "shop.events", *key, true, false,
			amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				ContentType:  "application/json",
				MessageId:    fmt.Sprintf("order-%d-created", i),
				Body:         []byte(body),
			})
		if err != nil {
			log.Fatal(err)
		}
		pending = append(pending, dc) // publish the batch first, wait for confirms afterwards
	}
	for i, dc := range pending {
		acked, err := dc.WaitContext(ctx)
		if err != nil || !acked {
			log.Fatalf("message %d not confirmed: acked=%v err=%v", i+1, acked, err)
		}
	}
	log.Printf("%d message(s) confirmed by the broker", len(pending))

	// returns arrive before the matching ack, so they are already buffered here
	for {
		select {
		case r := <-returns:
			log.Printf("RETURNED: %s (%d), routing key %q", r.ReplyText, r.ReplyCode, r.RoutingKey)
		default:
			return
		}
	}
}
