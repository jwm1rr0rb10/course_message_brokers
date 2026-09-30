// publisher sends order events to the shop.events topic exchange with
// publisher confirms and mandatory=true (module 5.6).
//
// A message counts as sent only after the broker's ack. If the connection
// breaks or a message is nacked, the publisher dials again and re-sends every
// message that was not confirmed, with the same message_id, so consumers can
// drop duplicates (module 7). After -attempts failed rounds it exits with an
// error listing how many messages stayed unconfirmed.
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
	attempts := flag.Int("attempts", 5, "publish rounds before giving up on unconfirmed messages")
	flag.Parse()

	var pending []amqp.Publishing
	for i := 1; i <= *n; i++ {
		body := fmt.Sprintf(`{"order_id":"order-%d","amount":%d}`, i, 1000*i)
		if *poison {
			body = "{not json"
		}
		pending = append(pending, amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
			MessageId:    fmt.Sprintf("order-%d-created", i), // stable id: a re-send is recognisable
			Body:         []byte(body),
		})
	}

	backoff := 500 * time.Millisecond
	for round := 1; len(pending) > 0; round++ {
		if round > *attempts {
			log.Fatalf("%d message(s) still unconfirmed after %d attempts", len(pending), *attempts)
		}
		if round > 1 {
			log.Printf("re-sending %d unconfirmed message(s) in %v", len(pending), backoff)
			time.Sleep(backoff)
			backoff *= 2
		}
		var err error
		pending, err = publish(*key, pending)
		if err != nil {
			log.Printf("attempt %d: %v", round, err)
		}
	}
	log.Printf("%d message(s) confirmed by the broker", *n)
}

// publish sends msgs on a fresh connection and returns the ones the broker did
// not confirm: nacked, or left without an answer when the connection broke.
func publish(key string, msgs []amqp.Publishing) (unconfirmed []amqp.Publishing, err error) {
	c, err := amqp.Dial(conn.URL())
	if err != nil {
		return msgs, fmt.Errorf("dial: %w", err)
	}
	defer c.Close()
	ch, err := c.Channel()
	if err != nil {
		return msgs, fmt.Errorf("channel: %w", err)
	}
	defer ch.Close()

	if err := ch.ExchangeDeclare("shop.events", "topic", true, false, false, false, nil); err != nil {
		return msgs, fmt.Errorf("declare exchange: %w", err)
	}
	if err := ch.Confirm(false); err != nil {
		return msgs, fmt.Errorf("confirm mode: %w", err)
	}
	returns := ch.NotifyReturn(make(chan amqp.Return, len(msgs)))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// publish the batch first, wait for confirms afterwards
	var notSent []amqp.Publishing
	confirms := make([]*amqp.DeferredConfirmation, 0, len(msgs))
	for i, m := range msgs {
		dc, err := ch.PublishWithDeferredConfirmWithContext(ctx, "shop.events", key, true, false, m)
		if err != nil {
			notSent = msgs[i:] // this and all later messages; wait for the earlier ones below
			break
		}
		confirms = append(confirms, dc)
	}
	// a broken channel completes outstanding confirmations as not acked
	for i, dc := range confirms {
		acked, werr := dc.WaitContext(ctx)
		if werr != nil || !acked {
			unconfirmed = append(unconfirmed, msgs[i])
		}
	}
	unconfirmed = append(unconfirmed, notSent...)

	// returns arrive before the matching ack, so they are already buffered here
	for done := false; !done; {
		select {
		case r := <-returns:
			log.Printf("RETURNED: %s (%d), routing key %q", r.ReplyText, r.ReplyCode, r.RoutingKey)
		default:
			done = true
		}
	}
	if len(unconfirmed) > 0 {
		return unconfirmed, fmt.Errorf("%d message(s) not confirmed", len(unconfirmed))
	}
	return nil, nil
}
