// servicebus-worker is the Service Bus consumer from module 6.4 in Go:
// peek-lock, complete after success, abandon on a transient error,
// dead-letter with a reason for invalid messages.
//
//	go run ./cmd/servicebus-worker -queue tasks
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log"
	"os/signal"
	"syscall"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"

	"github.com/jwm1rr0rb10/CLOUDBROKERSCOURSE/examples/go/internal/emu"
)

var errTemporary = errors.New("temporary failure")

func main() {
	queue := flag.String("queue", "tasks", "queue name")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// in the cloud: azservicebus.NewClient("<ns>.servicebus.windows.net", azidentity credential, nil)
	client, err := azservicebus.NewClientFromConnectionString(emu.ServiceBusConnectionString(), nil)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close(context.Background())

	receiver, err := client.NewReceiverForQueue(*queue, nil) // peek-lock by default
	if err != nil {
		log.Fatal(err)
	}
	defer receiver.Close(context.Background())

	for ctx.Err() == nil {
		msgs, err := receiver.ReceiveMessages(ctx, 10, nil)
		if err != nil {
			if ctx.Err() == nil {
				log.Println("receive:", err)
			}
			continue
		}
		for _, m := range msgs {
			switch err := process(m.Body); {
			case err == nil:
				if err := receiver.CompleteMessage(ctx, m, nil); err != nil {
					log.Println("complete:", err)
				}
			case errors.Is(err, errTemporary):
				_ = receiver.AbandonMessage(ctx, m, nil) // delivery count +1, DLQ after MaxDeliveryCount
			default:
				_ = receiver.DeadLetterMessage(ctx, m, &azservicebus.DeadLetterOptions{
					Reason: to.Ptr("InvalidPayload"), ErrorDescription: to.Ptr(err.Error()),
				})
			}
		}
	}
}

func process(body []byte) error {
	var v map[string]any
	if err := json.Unmarshal(body, &v); err != nil {
		return err
	}
	log.Println("processed", v)
	return nil
}
