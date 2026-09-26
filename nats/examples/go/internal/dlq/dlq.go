// Package dlq implements the dead-letter queue from module 14.3 of the course.
//
// Advisories are Core NATS messages, so they are first captured into the
// ADVISORIES stream; a durable consumer then moves each failed message from
// its original stream into the DLQ stream. If the mover is down when a
// message fails, nothing is lost: it catches up when it starts again.
package dlq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	AdvisoryStream = "ADVISORIES"
	DLQStream      = "DLQ"
	MoverConsumer  = "DLQ_MOVER"

	maxDeliveriesSubject = "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.>"
	terminatedSubject    = "$JS.EVENT.ADVISORY.CONSUMER.MSG_TERMINATED.>"
)

// Config tunes the streams. Zero values fall back to the course defaults.
type Config struct {
	Replicas int
	// AlertFunc is called after a message was moved. Optional.
	AlertFunc func(Advisory)
}

// Advisory holds the fields shared by MAX_DELIVERIES and MSG_TERMINATED.
type Advisory struct {
	Stream     string `json:"stream"`
	Consumer   string `json:"consumer"`
	StreamSeq  uint64 `json:"stream_seq"`
	Deliveries uint64 `json:"deliveries"`
	Reason     string `json:"reason"` // MSG_TERMINATED only
}

// Setup creates (or updates) the ADVISORIES and DLQ streams and the mover consumer.
func Setup(ctx context.Context, js jetstream.JetStream, cfg Config) (jetstream.Consumer, error) {
	replicas := cfg.Replicas
	if replicas == 0 {
		replicas = 3
	}

	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     AdvisoryStream,
		Subjects: []string{maxDeliveriesSubject, terminatedSubject},
		Storage:  jetstream.FileStorage,
		Replicas: replicas,
		MaxAge:   7 * 24 * time.Hour,
	}); err != nil {
		return nil, fmt.Errorf("advisory stream: %w", err)
	}

	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:       DLQStream,
		Subjects:   []string{"dlq.>"},
		Storage:    jetstream.FileStorage,
		Replicas:   replicas,
		MaxAge:     30 * 24 * time.Hour,
		Duplicates: 10 * time.Minute,
	}); err != nil {
		return nil, fmt.Errorf("dlq stream: %w", err)
	}

	cons, err := js.CreateOrUpdateConsumer(ctx, AdvisoryStream, jetstream.ConsumerConfig{
		Durable:    MoverConsumer,
		AckPolicy:  jetstream.AckExplicitPolicy,
		MaxDeliver: 20,
		AckWait:    30 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("mover consumer: %w", err)
	}
	return cons, nil
}

// Run consumes advisories until ctx is cancelled.
func Run(ctx context.Context, js jetstream.JetStream, cons jetstream.Consumer, cfg Config) error {
	cc, err := cons.Consume(func(m jetstream.Msg) {
		handle(js, m, cfg)
	})
	if err != nil {
		return err
	}
	<-ctx.Done()
	cc.Drain()
	return nil
}

func handle(js jetstream.JetStream, m jetstream.Msg, cfg Config) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var adv Advisory
	if err := json.Unmarshal(m.Data(), &adv); err != nil {
		log.Printf("dlq: bad advisory: %v", err)
		_ = m.Term()
		return
	}
	if adv.Stream == DLQStream || adv.Stream == AdvisoryStream {
		_ = m.Ack() // never loop on our own streams
		return
	}

	stream, err := js.Stream(ctx, adv.Stream)
	if err != nil {
		_ = m.NakWithDelay(5 * time.Second)
		return
	}
	orig, err := stream.GetMsg(ctx, adv.StreamSeq)
	if err != nil {
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			log.Printf("dlq: %s seq %d already removed by retention", adv.Stream, adv.StreamSeq)
			_ = m.Ack()
			return
		}
		_ = m.NakWithDelay(5 * time.Second)
		return
	}

	dlqMsg := BuildDLQMessage(adv, orig)
	if _, err := js.PublishMsg(ctx, dlqMsg); err != nil {
		_ = m.NakWithDelay(5 * time.Second)
		return
	}
	if cfg.AlertFunc != nil {
		cfg.AlertFunc(adv)
	}
	_ = m.Ack()
}

// BuildDLQMessage copies the original into a new message for the DLQ.
//
// Headers are copied into a NEW map: orig.Header may be nil, and the
// original Nats-Msg-Id must not trigger deduplication inside the DLQ.
func BuildDLQMessage(adv Advisory, orig *jetstream.RawStreamMsg) *nats.Msg {
	h := nats.Header{}
	for k, v := range orig.Header {
		if k != jetstream.MsgIDHeader {
			h[k] = v
		}
	}
	h.Set("Dlq-Original-Subject", orig.Subject)
	h.Set("Dlq-Original-Stream", adv.Stream)
	h.Set("Dlq-Original-Seq", strconv.FormatUint(adv.StreamSeq, 10))
	h.Set("Dlq-Deliveries", strconv.FormatUint(adv.Deliveries, 10))
	if adv.Reason != "" {
		h.Set("Dlq-Reason", adv.Reason)
	}
	// a stable id makes the move itself idempotent if the mover retries
	h.Set(jetstream.MsgIDHeader, fmt.Sprintf("dlq-%s-%d", adv.Stream, adv.StreamSeq))

	return &nats.Msg{
		Subject: "dlq." + adv.Stream + "." + adv.Consumer,
		Header:  h,
		Data:    orig.Data,
	}
}
