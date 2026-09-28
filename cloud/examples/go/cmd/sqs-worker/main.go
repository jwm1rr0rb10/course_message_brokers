// sqs-worker is the SQS consumer from module 3.7, extended with the retry
// backoff from 3.10: long polling, delete after success, growing visibility
// on failure, DLQ via the queue's RedrivePolicy.
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

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/jwm1rr0rb10/CLOUDBROKERSCOURSE/examples/go/internal/emu"
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

	for ctx.Err() == nil {
		out, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            q.QueueUrl,
			MaxNumberOfMessages: 10,
			WaitTimeSeconds:     20, // long polling
			VisibilityTimeout:   60, // longer than processing a batch
			MessageSystemAttributeNames: []types.MessageSystemAttributeName{
				types.MessageSystemAttributeNameApproximateReceiveCount,
			},
		})
		if err != nil {
			if ctx.Err() == nil {
				log.Println("receive:", err)
			}
			continue
		}
		for _, m := range out.Messages {
			attempt, _ := strconv.Atoi(m.Attributes["ApproximateReceiveCount"])
			if err := process(aws.ToString(m.Body)); err != nil {
				// retry later with a growing delay: 10 s, 20 s, 40 s ... up to 15 minutes
				delay := int32(min(5<<attempt, 900))
				log.Printf("attempt %d failed (%v), retry in %ds", attempt, err, delay)
				if _, err := client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
					QueueUrl: q.QueueUrl, ReceiptHandle: m.ReceiptHandle, VisibilityTimeout: delay,
				}); err != nil {
					log.Println("change visibility:", err)
				}
				continue // after maxReceiveCount the queue moves it to the DLQ
			}
			if _, err := client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
				QueueUrl: q.QueueUrl, ReceiptHandle: m.ReceiptHandle,
			}); err != nil {
				log.Println("delete:", err)
			}
		}
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
