// consumer reads the payments queue (module 6.6): manual ack, prefetch,
// invalid messages rejected without requeue so they go to the DLX (module 8).
//
// amqp091-go does not reconnect by default, so main runs a small supervisor:
// on a lost connection or channel, or a consumer cancelled by the broker, it
// waits with exponential backoff, dials again, re-declares the topology and
// subscribes again. SIGINT/SIGTERM stops it gracefully: the consumer is
// cancelled, deliveries already received are processed and acked, then the
// channel and connection are closed.
//
//	go run ./cmd/consumer
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os/signal"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/jwm1rr0rb10/RABBITMQFREECOURSE/examples/go/internal/conn"
)

const (
	queue       = "payments"
	consumerTag = "billing-1"
	prefetch    = 20

	minBackoff = 500 * time.Millisecond
	maxBackoff = 30 * time.Second
	// how long a graceful shutdown may take; after that unacked messages are left to the broker
	shutdownTimeout = 10 * time.Second
)

type order struct {
	OrderID string `json:"order_id"`
	Amount  int    `json:"amount"`
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	backoff := minBackoff
	for {
		connected, err := consume(ctx)
		if ctx.Err() != nil { // SIGINT/SIGTERM: consume has already shut down
			if err != nil {
				log.Printf("shutdown: %v", err)
			}
			log.Println("stopped")
			return
		}
		if connected {
			backoff = minBackoff // the last session worked: start again with a short pause
		}
		log.Printf("%v; reconnecting in %v", err, backoff)
		select {
		case <-ctx.Done():
			log.Println("stopped")
			return
		case <-time.After(backoff):
		}
		backoff = nextBackoff(backoff)
	}
}

// nextBackoff doubles the pause up to maxBackoff.
func nextBackoff(d time.Duration) time.Duration {
	if d *= 2; d > maxBackoff {
		return maxBackoff
	}
	return d
}

// consume runs one session: dial, declare the topology, subscribe, process deliveries.
// It returns when the session breaks (connected reports whether it got as far as
// consuming) or, after a graceful shutdown, when ctx is cancelled.
func consume(ctx context.Context) (connected bool, err error) {
	c, err := amqp.Dial(conn.URL())
	if err != nil {
		return false, fmt.Errorf("dial: %w", err)
	}
	defer c.Close()
	ch, err := c.Channel()
	if err != nil {
		return false, fmt.Errorf("channel: %w", err)
	}
	defer ch.Close()

	// buffered, so the library never blocks while notifying us
	connClosed := c.NotifyClose(make(chan *amqp.Error, 1))
	chClosed := ch.NotifyClose(make(chan *amqp.Error, 1))
	cancelled := ch.NotifyCancel(make(chan string, 1))

	// declarations are idempotent, so they are repeated on every reconnect
	if err := declareTopology(ch); err != nil {
		return false, fmt.Errorf("declare topology: %w", err)
	}
	if err := ch.Qos(prefetch, 0, false); err != nil {
		return false, fmt.Errorf("qos: %w", err)
	}
	msgs, err := ch.Consume(queue, consumerTag, false, false, false, false, nil)
	if err != nil {
		return false, fmt.Errorf("consume: %w", err)
	}
	log.Printf("consuming %s, Ctrl+C to stop", queue)

	for {
		select {
		case <-ctx.Done():
			return true, shutdown(ch, msgs)
		case err := <-connClosed:
			return true, fmt.Errorf("connection closed: %v", err)
		case err := <-chClosed:
			return true, fmt.Errorf("channel closed: %v", err)
		case tag := <-cancelled:
			// the queue was deleted or its node went away: subscribe again
			return true, fmt.Errorf("consumer %s cancelled by the broker", tag)
		case d, ok := <-msgs:
			if !ok {
				return true, errors.New("delivery channel closed")
			}
			if err := handle(d); err != nil {
				return true, err // ack failed: the channel is gone, the message will be redelivered
			}
		}
	}
}

// shutdown stops new deliveries, finishes the ones already received and acks them.
func shutdown(ch *amqp.Channel, msgs <-chan amqp.Delivery) error {
	log.Println("shutting down: cancelling the consumer")
	if err := ch.Cancel(consumerTag, false); err != nil {
		return fmt.Errorf("cancel: %w", err)
	}
	// after basic.cancel-ok the library closes msgs once the buffered deliveries are read
	deadline := time.After(shutdownTimeout)
	for {
		select {
		case d, ok := <-msgs:
			if !ok {
				return nil
			}
			if err := handle(d); err != nil {
				return err
			}
		case <-deadline:
			return errors.New("shutdown timeout: unacked messages go back to the queue")
		}
	}
}

// declareTopology declares the topology from modules 3.4 and 8.2:
// payments <- shop.events (order.created), DLX shop.dlx -> shop.dead.
func declareTopology(ch *amqp.Channel) error {
	if err := ch.ExchangeDeclare("shop.events", "topic", true, false, false, false, nil); err != nil {
		return err
	}
	if err := ch.ExchangeDeclare("shop.dlx", "topic", true, false, false, false, nil); err != nil {
		return err
	}
	if _, err := ch.QueueDeclare("shop.dead", true, false, false, false, amqp.Table{"x-queue-type": "quorum"}); err != nil {
		return err
	}
	if err := ch.QueueBind("shop.dead", "#", "shop.dlx", false, nil); err != nil {
		return err
	}
	if _, err := ch.QueueDeclare(queue, true, false, false, false, amqp.Table{
		"x-queue-type":           "quorum",
		"x-dead-letter-exchange": "shop.dlx",
		"x-delivery-limit":       5,
	}); err != nil {
		return err
	}
	return ch.QueueBind(queue, "order.created", "shop.events", false, nil)
}

// handle processes one delivery; an error means the ack or reject could not be sent.
func handle(d amqp.Delivery) error {
	var o order
	if err := json.Unmarshal(d.Body, &o); err != nil {
		log.Printf("invalid message %q -> DLX", d.MessageId)
		return d.Reject(false)
	}
	log.Printf("charged %s: %d (redelivered=%v)", o.OrderID, o.Amount, d.Redelivered)
	return d.Ack(false) // after processing
}
