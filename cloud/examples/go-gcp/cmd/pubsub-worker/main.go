// pubsub-worker is the Pub/Sub consumer from module 8.4 in Go: streaming
// pull with flow control; the client extends ack deadlines automatically.
//
//	PUBSUB_EMULATOR_HOST=localhost:8085 go run ./cmd/pubsub-worker -subscription billing
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"cloud.google.com/go/pubsub/v2"
)

func main() {
	subID := flag.String("subscription", "billing", "subscription id")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	client, err := pubsub.NewClient(ctx, project())
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	sub := client.Subscriber(*subID)
	sub.ReceiveSettings.MaxOutstandingMessages = 100 // the equivalent of prefetch

	err = sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
		var v map[string]any
		if err := json.Unmarshal(m.Data, &v); err != nil {
			m.Ack() // garbage: don't retry (or publish it to your own DLQ)
			return
		}
		log.Println("processed", v, "attributes", m.Attributes)
		m.Ack() // must be idempotent: without exactly-once a message can come twice
	})
	if err != nil && ctx.Err() == nil {
		log.Fatal(err)
	}
}

// project returns $PUBSUB_PROJECT_ID; with PUBSUB_EMULATOR_HOST set the client talks to the emulator.
func project() string {
	if p := os.Getenv("PUBSUB_PROJECT_ID"); p != "" {
		return p
	}
	return "course-project"
}
