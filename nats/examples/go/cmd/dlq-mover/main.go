// dlq-mover runs the dead-letter queue from module 14.3.
//
//	go run ./cmd/dlq-mover
package main

import (
	"context"
	"flag"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/jwm1rr0rb10/NATSFREECOURSE/examples/go/internal/conn"
	"github.com/jwm1rr0rb10/NATSFREECOURSE/examples/go/internal/dlq"
)

func main() {
	replicas := flag.Int("replicas", 3, "replicas for the ADVISORIES and DLQ streams")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	closed := make(chan struct{})
	nc, err := conn.Connect("dlq-mover", nats.ClosedHandler(func(*nats.Conn) { close(closed) }))
	if err != nil {
		log.Fatal(err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatal(err)
	}

	cfg := dlq.Config{
		Replicas: *replicas,
		AlertFunc: func(a dlq.Advisory) {
			log.Printf("ALERT: %s/%s seq=%d moved to DLQ after %d deliveries %s",
				a.Stream, a.Consumer, a.StreamSeq, a.Deliveries, a.Reason)
		},
	}
	cons, err := dlq.Setup(ctx, js, cfg)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("dlq-mover running, Ctrl+C to stop")
	if err := dlq.Run(ctx, js, cons, cfg); err != nil {
		log.Fatal(err)
	}
	// Run has drained the consumer; now flush the last acks and wait for it
	if err := nc.Drain(); err != nil {
		log.Printf("drain: %v", err)
	}
	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		log.Println("connection drain timed out")
	}
}
