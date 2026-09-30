// servicebus-worker is the Service Bus consumer from module 6.4 in Go:
// peek-lock, lock renewal while processing, complete after success,
// abandon on a transient error, dead-letter with a reason for invalid messages.
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
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"

	"github.com/jwm1rr0rb10/CLOUDBROKERSCOURSE/examples/go/internal/emu"
)

const (
	renewEvery    = 20 * time.Second // well below the queue's LockDuration (PT1M)
	settleTimeout = 10 * time.Second
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

	backoff := time.Second
	for ctx.Err() == nil {
		msgs, err := receiver.ReceiveMessages(ctx, 10, nil)
		if err != nil {
			if ctx.Err() == nil {
				// don't spin while the namespace (or the network) is down: 1 s, 2 s, 4 s ... 30 s
				log.Printf("receive: %v, retry in %s", err, backoff)
				sleep(ctx, backoff)
				backoff = min(backoff*2, 30*time.Second)
			}
			continue
		}
		backoff = time.Second
		for _, m := range msgs {
			if ctx.Err() != nil {
				break // shutting down: the rest come back when their lock expires
			}
			handle(ctx, receiver, m)
		}
	}
}

func handle(ctx context.Context, receiver *azservicebus.Receiver, m *azservicebus.ReceivedMessage) {
	// Settle even after SIGTERM: with the cancelled ctx a finished message would not be
	// completed and would be delivered again.
	settle := context.WithoutCancel(ctx)

	// renew the lock while processing may take longer than LockDuration (6.4)
	done := make(chan struct{})
	go func() {
		t := time.NewTicker(renewEvery)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				c, cancel := context.WithTimeout(settle, settleTimeout)
				if err := receiver.RenewMessageLock(c, m, nil); err != nil {
					log.Println("renew lock:", err)
				}
				cancel()
			}
		}
	}()
	procErr := process(m.Body)
	close(done)

	c, cancel := context.WithTimeout(settle, settleTimeout)
	defer cancel()
	var err error
	switch {
	case procErr == nil:
		err = receiver.CompleteMessage(c, m, nil)
	case errors.Is(procErr, errTemporary):
		err = receiver.AbandonMessage(c, m, nil) // delivery count +1, DLQ after MaxDeliveryCount
	default:
		err = receiver.DeadLetterMessage(c, m, &azservicebus.DeadLetterOptions{
			Reason: to.Ptr("InvalidPayload"), ErrorDescription: to.Ptr(procErr.Error()),
		})
	}
	if err != nil {
		// e.g. the lock was lost: the message comes back after the lock expires, so processing must be idempotent
		log.Printf("settle message %s: %v", m.MessageID, err)
	}
}

// sleep waits for d or until ctx is cancelled.
func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
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
