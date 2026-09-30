// sqs-worker is the SQS consumer from module 3.7, extended with the visibility
// heartbeat from 3.8 and the retry backoff from 3.10: long polling, delete after
// success, growing visibility on failure, DLQ via the queue's RedrivePolicy.
//
//	go run ./cmd/sqs-worker -queue tasks
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/jwm1rr0rb10/CLOUDBROKERSCOURSE/examples/go/internal/emu"
)

const (
	visibility    = 60               // seconds; longer than processing one message
	heartbeat     = 20 * time.Second // extend visibility this often while processing (3.8)
	maxRetryDelay = 900              // seconds; SQS allows up to 43200 (12 h)
	settleTimeout = 10 * time.Second
)

func main() {
	queue := flag.String("queue", "tasks", "queue name")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(emu.Env("AWS_DEFAULT_REGION", "eu-central-1")))
	if err != nil {
		log.Fatal(err)
	}
	client := sqs.NewFromConfig(cfg, func(o *sqs.Options) { o.BaseEndpoint = aws.String(emu.AWSEndpoint()) })

	q, err := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: queue})
	if err != nil {
		log.Fatal(err)
	}
	log.Println("polling", *q.QueueUrl)

	backoff := time.Second
	for ctx.Err() == nil {
		out, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            q.QueueUrl,
			MaxNumberOfMessages: 10,
			WaitTimeSeconds:     20, // long polling
			VisibilityTimeout:   visibility,
			MessageSystemAttributeNames: []types.MessageSystemAttributeName{
				types.MessageSystemAttributeNameApproximateReceiveCount,
			},
		})
		if err != nil {
			if ctx.Err() == nil {
				// don't hammer the API while it (or the network) is down: 1 s, 2 s, 4 s ... 30 s
				log.Printf("receive: %v, retry in %s", err, backoff)
				sleep(ctx, backoff)
				backoff = min(backoff*2, 30*time.Second)
			}
			continue
		}
		backoff = time.Second
		for _, m := range out.Messages {
			if ctx.Err() != nil {
				break // shutting down: unprocessed messages come back after the visibility timeout
			}
			handle(ctx, client, q.QueueUrl, m)
		}
	}
}

func handle(ctx context.Context, client *sqs.Client, queueURL *string, m types.Message) {
	// Settle even after SIGTERM: with the cancelled ctx a finished message would not be
	// deleted and would be processed again.
	settle := context.WithoutCancel(ctx)
	setVisibility := func(seconds int32) error {
		c, cancel := context.WithTimeout(settle, settleTimeout)
		defer cancel()
		_, err := client.ChangeMessageVisibility(c, &sqs.ChangeMessageVisibilityInput{
			QueueUrl: queueURL, ReceiptHandle: m.ReceiptHandle, VisibilityTimeout: seconds,
		})
		return err
	}

	// heartbeat: keep the message invisible while we are still working on it
	done := make(chan struct{})
	go func() {
		t := time.NewTicker(heartbeat)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				if err := setVisibility(visibility); err != nil {
					log.Println("extend visibility:", err)
				}
			}
		}
	}()
	err := process(aws.ToString(m.Body))
	close(done)

	if err != nil {
		// retry later with a growing delay: 10 s, 20 s, 40 s ... up to 15 minutes
		attempt, _ := strconv.Atoi(m.Attributes["ApproximateReceiveCount"])
		delay := int32(min(5<<min(max(attempt, 1), 8), maxRetryDelay)) // capped shift can't overflow
		log.Printf("attempt %d failed (%v), retry in %ds", attempt, err, delay)
		if err := setVisibility(delay); err != nil {
			log.Println("change visibility:", err)
		}
		return // after maxReceiveCount the queue moves it to the DLQ
	}
	c, cancel := context.WithTimeout(settle, settleTimeout)
	defer cancel()
	if _, err := client.DeleteMessage(c, &sqs.DeleteMessageInput{QueueUrl: queueURL, ReceiptHandle: m.ReceiptHandle}); err != nil {
		log.Println("delete:", err) // the message will come again: processing must be idempotent
	}
}

// sleep waits for d or until ctx is cancelled.
func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

func process(body string) error {
	var v map[string]any
	if err := json.Unmarshal([]byte(body), &v); err != nil {
		return err
	}
	log.Println("processed", v)
	return nil
}
