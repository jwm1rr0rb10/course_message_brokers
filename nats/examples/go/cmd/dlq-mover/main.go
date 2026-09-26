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

	"github.com/nats-io/nats.go/jetstream"

	"github.com/jwm1rr0rb10/NATSFREECOURSE/examples/go/internal/conn"
	"github.com/jwm1rr0rb10/NATSFREECOURSE/examples/go/internal/dlq"
)

func main() {
	replicas := flag.Int("replicas", 3, "replicas for the ADVISORIES and DLQ streams")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	nc, err := conn.Connect("dlq-mover")
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Drain()

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
}
