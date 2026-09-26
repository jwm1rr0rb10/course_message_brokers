// kprobe sends one record with the given acks and exits 0 on success, 1 on
// failure. The smoke test uses it to show min.insync.replicas in action (8.2).
//
//	kprobe -topic t -acks all
//	kprobe -topic t -acks leader
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/jwm1rr0rb10/KAFKAFREECOURSE/examples/go/internal/conn"
)

func main() {
	topic := flag.String("topic", "", "topic")
	acks := flag.String("acks", "all", "all or leader")
	timeout := flag.Duration("timeout", 15*time.Second, "delivery timeout")
	flag.Parse()

	opts := conn.Opts("kprobe", kgo.RecordDeliveryTimeout(*timeout), kgo.ProduceRequestTimeout(5*time.Second))
	switch *acks {
	case "all":
		opts = append(opts, kgo.RequiredAcks(kgo.AllISRAcks()))
	case "leader":
		// acks=1 is incompatible with idempotence, so it has to be disabled
		opts = append(opts, kgo.RequiredAcks(kgo.LeaderAck()), kgo.DisableIdempotentWrite())
	default:
		fmt.Fprintln(os.Stderr, "acks must be all or leader")
		os.Exit(2)
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout+5*time.Second)
	defer cancel()
	r := &kgo.Record{Topic: *topic, Key: []byte("probe"), Value: []byte("probe")}
	if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
		fmt.Println("FAILED:", err)
		os.Exit(1)
	}
	fmt.Printf("OK partition=%d offset=%d\n", r.Partition, r.Offset)
}
