// Package dlq implements the dead-letter queue from module 14.3 of the course.
//
// Advisories are Core NATS messages, so they are first captured into the
// ADVISORIES stream; a durable consumer then moves each failed message from
// its original stream into the DLQ stream. If the mover is down when a
// message fails, nothing is lost: it catches up when it starts again.
//
// One exception: on workqueue and interest streams the server treats Term as
// an ack and deletes the message at once, so the mover finds nothing to copy.
// Use Terminate there: it publishes the message to the DLQ itself and only
// then calls Term. Both paths use the same Nats-Msg-Id, so on a limits stream,
// where the mover sees the message too, the DLQ still stores it once.
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
	cc.Drain() // let in-flight advisories finish
	select {
	case <-cc.Closed():
	case <-time.After(10 * time.Second):
		log.Printf("dlq: drain did not finish in 10s")
	}
	return nil
}

// Terminate moves a message that can never be processed to the DLQ and then
// calls TermWithReason. Use it instead of a bare Term on workqueue and
// interest streams, where Term deletes the message before the mover can read
// it. If publishing fails the message is not terminated and err is returned:
// Nak it (or let AckWait expire) and try again later.
func Terminate(ctx context.Context, js jetstream.JetStream, msg jetstream.Msg, reason string) error {
	meta, err := msg.Metadata()
	if err != nil {
		return fmt.Errorf("metadata: %w", err)
	}
	adv := Advisory{
		Stream:     meta.Stream,
		Consumer:   meta.Consumer,
		StreamSeq:  meta.Sequence.Stream,
		Deliveries: meta.NumDelivered,
		Reason:     reason,
	}
	orig := &jetstream.RawStreamMsg{
		Subject:  msg.Subject(),
		Sequence: meta.Sequence.Stream,
		Header:   msg.Headers(),
		Data:     msg.Data(),
	}
	if _, err := js.PublishMsg(ctx, BuildDLQMessage(adv, orig)); err != nil {
		return fmt.Errorf("publish to DLQ: %w", err)
	}
	return msg.TermWithReason(reason)
}

func handle(js jetstream.JetStream, m jetstream.Msg, cfg Config) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var adv Advisory
	if err := json.Unmarshal(m.Data(), &adv); err != nil {
		log.Printf("dlq: bad advisory: %v", err)
		if err := m.Term(); err != nil {
			log.Printf("dlq: term advisory: %v", err)
		}
		return
	}
	if adv.Stream == DLQStream || adv.Stream == AdvisoryStream {
		ack(m) // never loop on our own streams
		return
	}

	stream, err := js.Stream(ctx, adv.Stream)
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			log.Printf("dlq: stream %s no longer exists, dropping advisory for seq %d", adv.Stream, adv.StreamSeq)
			ack(m)
			return
		}
		retry(m, "stream lookup", err)
		return
	}
	orig, err := stream.GetMsg(ctx, adv.StreamSeq)
	if err != nil {
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			// removed by retention, or Term on a workqueue/interest stream
			// (then Terminate has already put it into the DLQ)
			log.Printf("dlq: %s seq %d is no longer in the stream", adv.Stream, adv.StreamSeq)
			ack(m)
			return
		}
		retry(m, "get original", err)
		return
	}

	dlqMsg := BuildDLQMessage(adv, orig)
	if _, err := js.PublishMsg(ctx, dlqMsg); err != nil {
		retry(m, "publish to DLQ", err)
		return
	}
	if cfg.AlertFunc != nil {
		cfg.AlertFunc(adv)
	}
	ack(m)
}

func ack(m jetstream.Msg) {
	if err := m.Ack(); err != nil {
		log.Printf("dlq: ack advisory: %v", err)
	}
}

func retry(m jetstream.Msg, what string, err error) {
	log.Printf("dlq: %s: %v (retrying)", what, err)
	if err := m.NakWithDelay(5 * time.Second); err != nil {
		log.Printf("dlq: nak advisory: %v", err)
	}
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
	// a stable id makes the move itself idempotent if the mover retries; the
	// consumer is part of it because several consumers can fail the same message
	h.Set(jetstream.MsgIDHeader, fmt.Sprintf("dlq-%s-%s-%d", adv.Stream, adv.Consumer, adv.StreamSeq))

	return &nats.Msg{
		Subject: "dlq." + adv.Stream + "." + adv.Consumer,
		Header:  h,
		Data:    orig.Data,
	}
}
