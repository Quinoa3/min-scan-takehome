package processing

import (
	"context"
	"log"
	"strconv"
	"time"

	"cloud.google.com/go/pubsub"

	"github.com/censys/scan-takehome/pkg/storage"
)

// ScanDB represents the storage dependency used by the handler.
type ScanDB interface {
	UpsertLatest(ctx context.Context, record storage.ScanRecord) error
}

// DLQPublisher publishes malformed messages to a dead-letter topic.
type DLQPublisher interface {
	Publish(ctx context.Context, msg *pubsub.Message, reason string) error
}

// PubSubDLQPublisher implements DLQPublisher using a Pub/Sub topic.
type PubSubDLQPublisher struct {
	topic *pubsub.Topic
}

// NewPubSubDLQPublisher constructs a DLQ publisher for the given topic. If the
// topic is nil, publishes are treated as no-ops.
func NewPubSubDLQPublisher(topic *pubsub.Topic) *PubSubDLQPublisher {
	return &PubSubDLQPublisher{topic: topic}
}

// Publish sends the message to the DLQ topic. If topic is nil, it is a no-op.
func (p *PubSubDLQPublisher) Publish(ctx context.Context, msg *pubsub.Message, reason string) error {
	if p.topic == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := p.topic.Publish(ctx, &pubsub.Message{
		Data: msg.Data,
		Attributes: map[string]string{
			"reason":           reason,
			"orig_msg_id":      msg.ID,
			"delivery_attempt": strconv.Itoa(*msg.DeliveryAttempt),
		},
	}).Get(ctx)
	return err
}

// NoopDLQPublisher is used when no DLQ topic is configured.
type NoopDLQPublisher struct{}

func (n *NoopDLQPublisher) Publish(ctx context.Context, msg *pubsub.Message, reason string) error {
	return nil
}

// HandleMessage processes a Pub/Sub message and returns true if it should be
// acked (even when sent to DLQ) or false to Nack (for retriable errors).
func HandleMessage(ctx context.Context, db ScanDB, dlq DLQPublisher, msg *pubsub.Message) bool {
	if dlq == nil {
		dlq = &NoopDLQPublisher{}
	}
	scan, err := ParseScanMessage(msg.Data)
	if err != nil {
		log.Printf("pushing message to DLQ: %v", err)
		if err := dlq.Publish(ctx, msg, "parse_error"); err != nil {
			log.Printf("error publishing to DLQ: %v", err)
			return false
		}
		return true
	}

	resp, err := scan.ResponseString()
	if err != nil {
		log.Printf("pushing message to DLQ: %q:%d/%s: %v", scan.Ip, scan.Port, scan.Service, err)
		if err := dlq.Publish(ctx, msg, "normalize_error"); err != nil {
			log.Printf("error publishing to DLQ: %v", err)
			return false
		}
		return true
	}

	record := storage.ScanRecord{
		IP:        scan.Ip,
		Port:      scan.Port,
		Service:   scan.Service,
		Timestamp: time.Unix(scan.Timestamp, 0).UTC(),
		Response:  resp,
	}

	if err := db.UpsertLatest(ctx, record); err != nil {
		log.Printf("upsert failed %q:%d/%s: %v", scan.Ip, scan.Port, scan.Service, err)
		return false
	}

	return true
}
