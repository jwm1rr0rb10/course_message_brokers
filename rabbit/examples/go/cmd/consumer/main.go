// consumer reads the payments queue (module 6.6): manual ack, prefetch,
// invalid messages rejected without requeue so they go to the DLX (module 8).
//
//	go run ./cmd/consumer
package main

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/jwm1rr0rb10/RABBITMQFREECOURSE/examples/go/internal/conn"
)

type order struct {
	OrderID string `json:"order_id"`
	Amount  int    `json:"amount"`
}

func main() {
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

	// topology from modules 3.4 and 8.2: payments <- shop.events (order.created), DLX shop.dlx -> shop.dead
	must(ch.ExchangeDeclare("shop.events", "topic", true, false, false, false, nil))
	must(ch.ExchangeDeclare("shop.dlx", "topic", true, false, false, false, nil))
	_, err = ch.QueueDeclare("shop.dead", true, false, false, false, amqp.Table{"x-queue-type": "quorum"})
	must(err)
	must(ch.QueueBind("shop.dead", "#", "shop.dlx", false, nil))
	_, err = ch.QueueDeclare("payments", true, false, false, false, amqp.Table{
		"x-queue-type":           "quorum",
		"x-dead-letter-exchange": "shop.dlx",
		"x-delivery-limit":       5,
	})
	must(err)
	must(ch.QueueBind("payments", "order.created", "shop.events", false, nil))

	must(ch.Qos(20, 0, false))
	msgs, err := ch.Consume("payments", "billing-1", false, false, false, false, nil)
	must(err)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	closed := c.NotifyClose(make(chan *amqp.Error, 1))

	log.Println("consuming payments, Ctrl+C to stop")
	for {
		select {
		case <-stop:
			return
		case err := <-closed:
			log.Fatalf("connection closed: %v (a real service would reconnect here)", err)
		case d, ok := <-msgs:
			if !ok {
				log.Fatal("delivery channel closed")
			}
			var o order
			if err := json.Unmarshal(d.Body, &o); err != nil {
				log.Printf("invalid message %q -> DLX", d.MessageId)
				must(d.Reject(false))
				continue
			}
			log.Printf("charged %s: %d (redelivered=%v)", o.OrderID, o.Amount, d.Redelivered)
			must(d.Ack(false))
		}
	}
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
