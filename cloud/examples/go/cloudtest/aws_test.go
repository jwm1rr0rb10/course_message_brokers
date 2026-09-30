//go:build integration

package cloudtest

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/jwm1rr0rb10/CLOUDBROKERSCOURSE/examples/go/internal/emu"
)

func awsConfig(t *testing.T) aws.Config {
	t.Helper()
	if !emu.Reachable(emu.AWSEndpoint()) {
		emu.Unavailable(t, "AWS emulator is not running on %s", emu.AWSEndpoint())
	}
	t.Setenv("AWS_ACCESS_KEY_ID", emu.Env("AWS_ACCESS_KEY_ID", "test"))
	t.Setenv("AWS_SECRET_ACCESS_KEY", emu.Env("AWS_SECRET_ACCESS_KEY", "test"))
	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion("eu-central-1"))
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func sqsClient(t *testing.T) *sqs.Client {
	return sqs.NewFromConfig(awsConfig(t), func(o *sqs.Options) { o.BaseEndpoint = aws.String(emu.AWSEndpoint()) })
}

func makeQueue(t *testing.T, c *sqs.Client, name string, attrs map[string]string) (url, arn string) {
	t.Helper()
	ctx := context.Background()
	out, err := c.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(name), Attributes: attrs})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = c.DeleteQueue(context.Background(), &sqs.DeleteQueueInput{QueueUrl: out.QueueUrl}) })
	a, err := c.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: out.QueueUrl, AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn}})
	if err != nil {
		t.Fatal(err)
	}
	return *out.QueueUrl, a.Attributes["QueueArn"]
}

func receive(t *testing.T, c *sqs.Client, url string, wait int32) []types.Message {
	t.Helper()
	out, err := c.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{
		QueueUrl: aws.String(url), MaxNumberOfMessages: 10, WaitTimeSeconds: wait,
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameApproximateReceiveCount},
	})
	if err != nil {
		t.Fatal(err)
	}
	return out.Messages
}

// receiveOne fails the test instead of panicking when nothing arrives.
func receiveOne(t *testing.T, c *sqs.Client, url string, wait int32) types.Message {
	t.Helper()
	msgs := receive(t, c, url, wait)
	if len(msgs) != 1 {
		t.Fatalf("got %d messages, want 1", len(msgs))
	}
	return msgs[0]
}

// 3.1: a received message is invisible until the visibility timeout expires, then comes back.
func TestSQSVisibilityTimeout(t *testing.T) {
	c := sqsClient(t)
	url, _ := makeQueue(t, c, unique("go-vis"), map[string]string{"VisibilityTimeout": "2"})
	send(t, c, url, "job")
	first := receive(t, c, url, 1)
	if len(first) != 1 {
		t.Fatalf("got %d messages", len(first))
	}
	if got := receive(t, c, url, 0); len(got) != 0 {
		t.Fatal("message must be invisible while in flight")
	}
	time.Sleep(2500 * time.Millisecond)
	again := receive(t, c, url, 1)
	if len(again) != 1 || again[0].Attributes["ApproximateReceiveCount"] != "2" {
		t.Fatalf("expected a redelivery with receive count 2, got %+v", again)
	}
	if aws.ToString(again[0].ReceiptHandle) == aws.ToString(first[0].ReceiptHandle) {
		t.Fatal("each receive must have its own receipt handle")
	}
}

// 3.9: after maxReceiveCount receives without delete the message moves to the DLQ.
func TestSQSRedriveToDLQ(t *testing.T) {
	c := sqsClient(t)
	dlqURL, dlqARN := makeQueue(t, c, unique("go-dlq"), nil)
	policy, _ := json.Marshal(map[string]string{"deadLetterTargetArn": dlqARN, "maxReceiveCount": "2"})
	url, _ := makeQueue(t, c, unique("go-work"), map[string]string{"VisibilityTimeout": "1", "RedrivePolicy": string(policy)})
	send(t, c, url, "poison")

	depth := func() string {
		a, err := c.GetQueueAttributes(context.Background(), &sqs.GetQueueAttributesInput{
			QueueUrl: aws.String(dlqURL), AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameApproximateNumberOfMessages}})
		if err != nil {
			t.Fatal(err)
		}
		return a.Attributes["ApproximateNumberOfMessages"]
	}
	receives := 0
	for i := 0; i < 10 && depth() == "0"; i++ {
		receives += len(receive(t, c, url, 1)) // "fail": never delete
		time.Sleep(1200 * time.Millisecond)
	}
	if receives != 2 {
		t.Fatalf("received %d times with maxReceiveCount 2", receives)
	}
	if dead := receive(t, c, dlqURL, 1); len(dead) != 1 || aws.ToString(dead[0].Body) != "poison" {
		t.Fatalf("DLQ holds %+v", dead)
	}
}

// 3.2: FIFO keeps order within a group and drops a resend with the same deduplication id.
func TestSQSFIFO(t *testing.T) {
	c := sqsClient(t)
	url, _ := makeQueue(t, c, unique("go-orders")+".fifo", map[string]string{"FifoQueue": "true"})
	for _, b := range []string{"created", "paid", "shipped", "paid"} {
		_, err := c.SendMessage(context.Background(), &sqs.SendMessageInput{
			QueueUrl: aws.String(url), MessageBody: aws.String(b),
			MessageGroupId: aws.String("order-1"), MessageDeduplicationId: aws.String("order-1-" + b),
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	var bodies []string
	for i := 0; i < 5 && len(bodies) < 4; i++ {
		msgs := receive(t, c, url, 1)
		if len(msgs) == 0 && len(bodies) >= 3 {
			break
		}
		for _, m := range msgs {
			bodies = append(bodies, aws.ToString(m.Body))
			_, _ = c.DeleteMessage(context.Background(), &sqs.DeleteMessageInput{QueueUrl: aws.String(url), ReceiptHandle: m.ReceiptHandle})
		}
	}
	if fmt.Sprint(bodies) != "[created paid shipped]" {
		t.Fatalf("got %v", bodies)
	}
}

// 11.4: a batch call can partially fail while the request itself succeeds.
func TestSQSBatchPartialFailure(t *testing.T) {
	c := sqsClient(t)
	url, _ := makeQueue(t, c, unique("go-batch"), nil)
	send(t, c, url, "job")
	m := receiveOne(t, c, url, 1)
	out, err := c.DeleteMessageBatch(context.Background(), &sqs.DeleteMessageBatchInput{
		QueueUrl: aws.String(url),
		Entries: []types.DeleteMessageBatchRequestEntry{
			{Id: aws.String("good"), ReceiptHandle: m.ReceiptHandle},
			{Id: aws.String("bad"), ReceiptHandle: aws.String("not-a-valid-receipt-handle")},
		},
	})
	if err != nil {
		t.Fatalf("the request itself must succeed: %v", err)
	}
	if len(out.Successful) != 1 || len(out.Failed) != 1 || aws.ToString(out.Failed[0].Id) != "bad" {
		t.Fatalf("successful=%d failed=%+v", len(out.Successful), out.Failed)
	}
}

// 4.2–4.4: SNS fan-out to SQS with raw delivery and a filter policy.
func TestSNSFanoutRawAndFilter(t *testing.T) {
	cfg := awsConfig(t)
	c := sqsClient(t)
	s := sns.NewFromConfig(cfg, func(o *sns.Options) { o.BaseEndpoint = aws.String(emu.AWSEndpoint()) })
	ctx := context.Background()
	topic, err := s.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String(unique("go-orders"))})
	if err != nil {
		t.Fatal(err)
	}
	// deleting a topic also deletes its subscriptions
	t.Cleanup(func() { _, _ = s.DeleteTopic(context.Background(), &sns.DeleteTopicInput{TopicArn: topic.TopicArn}) })
	auditURL, auditARN := makeQueue(t, c, unique("go-audit"), nil)
	billingURL, billingARN := makeQueue(t, c, unique("go-billing"), nil)
	filter, _ := json.Marshal(map[string][]string{"event_type": {"OrderCreated"}})
	for _, sub := range []struct {
		arn   string
		attrs map[string]string
	}{
		{auditARN, map[string]string{"RawMessageDelivery": "true"}},
		{billingARN, map[string]string{"RawMessageDelivery": "true", "FilterPolicy": string(filter)}},
	} {
		if _, err := s.Subscribe(ctx, &sns.SubscribeInput{TopicArn: topic.TopicArn, Protocol: aws.String("sqs"),
			Endpoint: aws.String(sub.arn), Attributes: sub.attrs}); err != nil {
			t.Fatal(err)
		}
	}
	for _, ev := range []string{"OrderCreated", "OrderCancelled"} {
		_, err := s.Publish(ctx, &sns.PublishInput{TopicArn: topic.TopicArn, Message: aws.String(ev),
			MessageAttributes: map[string]snstypes.MessageAttributeValue{
				"event_type": {DataType: aws.String("String"), StringValue: aws.String(ev)}}})
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := receive(t, c, auditURL, 2); len(got) != 2 {
		t.Fatalf("audit got %d messages, want 2", len(got))
	}
	got := receive(t, c, billingURL, 2)
	if len(got) != 1 || aws.ToString(got[0].Body) != "OrderCreated" {
		t.Fatalf("billing got %+v, want only the raw OrderCreated body", got)
	}
}

// 5.2: an EventBridge rule with a numeric comparison routes only matching events to SQS.
func TestEventBridgeNumericRule(t *testing.T) {
	cfg := awsConfig(t)
	c := sqsClient(t)
	eb := eventbridge.NewFromConfig(cfg, func(o *eventbridge.Options) { o.BaseEndpoint = aws.String(emu.AWSEndpoint()) })
	ctx := context.Background()
	bus, rule := unique("go-shop"), unique("go-big")
	if _, err := eb.CreateEventBus(ctx, &eventbridge.CreateEventBusInput{Name: aws.String(bus)}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = eb.DeleteEventBus(context.Background(), &eventbridge.DeleteEventBusInput{Name: aws.String(bus)})
	})
	url, arn := makeQueue(t, c, unique("go-fraud"), nil)
	pattern := `{"source":["shop.orders"],"detail":{"amount":[{"numeric":[">",10000]}]}}`
	if _, err := eb.PutRule(ctx, &eventbridge.PutRuleInput{Name: aws.String(rule), EventBusName: aws.String(bus), EventPattern: aws.String(pattern)}); err != nil {
		t.Fatal(err)
	}
	// cleanups run in reverse order: targets, then the rule, then the bus
	t.Cleanup(func() {
		_, _ = eb.DeleteRule(context.Background(), &eventbridge.DeleteRuleInput{Name: aws.String(rule), EventBusName: aws.String(bus)})
	})
	if _, err := eb.PutTargets(ctx, &eventbridge.PutTargetsInput{Rule: aws.String(rule), EventBusName: aws.String(bus),
		Targets: []ebtypes.Target{{Id: aws.String("fraud"), Arn: aws.String(arn)}}}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = eb.RemoveTargets(context.Background(), &eventbridge.RemoveTargetsInput{
			Rule: aws.String(rule), EventBusName: aws.String(bus), Ids: []string{"fraud"}})
	})
	out, err := eb.PutEvents(ctx, &eventbridge.PutEventsInput{Entries: []ebtypes.PutEventsRequestEntry{
		{Source: aws.String("shop.orders"), DetailType: aws.String("OrderCreated"), EventBusName: aws.String(bus), Detail: aws.String(`{"order_id":"big","amount":14990}`)},
		{Source: aws.String("shop.orders"), DetailType: aws.String("OrderCreated"), EventBusName: aws.String(bus), Detail: aws.String(`{"order_id":"small","amount":500}`)},
	}})
	if err != nil {
		t.Fatalf("put events: %v", err)
	}
	if out.FailedEntryCount != 0 {
		t.Fatalf("put events: %d entries failed: %+v", out.FailedEntryCount, out.Entries)
	}
	got := receive(t, c, url, 2)
	if len(got) != 1 {
		t.Fatalf("got %d events, want 1", len(got))
	}
	var ev struct {
		Detail struct {
			OrderID string `json:"order_id"`
		} `json:"detail"`
	}
	if err := json.Unmarshal([]byte(aws.ToString(got[0].Body)), &ev); err != nil {
		t.Fatalf("event body: %v", err)
	}
	if ev.Detail.OrderID != "big" {
		t.Fatalf("routed %q, want big", ev.Detail.OrderID)
	}
}

func send(t *testing.T, c *sqs.Client, url, body string) {
	t.Helper()
	if _, err := c.SendMessage(context.Background(), &sqs.SendMessageInput{QueueUrl: aws.String(url), MessageBody: aws.String(body)}); err != nil {
		t.Fatal(err)
	}
}
