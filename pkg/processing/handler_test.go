package processing

import (
	"context"
	"encoding/json"
	"testing"

	"cloud.google.com/go/pubsub"

	"github.com/censys/scan-takehome/pkg/storage"
)

type stubDB struct {
	called int
	err    error
	record storage.ScanRecord
}

func (s *stubDB) UpsertLatest(ctx context.Context, record storage.ScanRecord) error {
	s.called++
	s.record = record
	return s.err
}

type stubDLQ struct {
	called  int
	reasons []string
	data    [][]byte
	err     error
}

func (s *stubDLQ) Publish(ctx context.Context, msg *pubsub.Message, reason string) error {
	s.called++
	s.reasons = append(s.reasons, reason)
	s.data = append(s.data, msg.Data)
	return s.err
}

func TestHandleMessage_MalformedJSON(t *testing.T) {
	ctx := context.Background()
	db := &stubDB{}
	dlq := &stubDLQ{}

	msg := &pubsub.Message{Data: []byte("{not json")}

	ack := HandleMessage(ctx, db, dlq, msg)
	if !ack {
		t.Fatalf("expected ack despite DLQ, got nack")
	}
	if db.called != 0 {
		t.Fatalf("expected repo not called, got %d", db.called)
	}
	if dlq.called != 1 || dlq.reasons[0] != "parse_error" {
		t.Fatalf("unexpected dlq calls: %+v", dlq.reasons)
	}
}

func TestHandleMessage_BadResponseString(t *testing.T) {
	ctx := context.Background()
	db := &stubDB{}
	dlq := &stubDLQ{}

	payload := map[string]any{
		"ip":           "1.1.1.1",
		"port":         80,
		"service":      "HTTP",
		"timestamp":    1,
		"data_version": 99,
		"data":         map[string]any{},
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	msg := &pubsub.Message{Data: raw}

	ack := HandleMessage(ctx, db, dlq, msg)
	if !ack {
		t.Fatalf("expected ack despite DLQ, got nack")
	}
	if db.called != 0 {
		t.Fatalf("expected repo not called, got %d", db.called)
	}
	if dlq.called != 1 || dlq.reasons[0] != "normalize_error" {
		t.Fatalf("unexpected dlq calls: %+v", dlq.reasons)
	}
}

func TestHandleMessage_GoodMessage(t *testing.T) {
	ctx := context.Background()
	db := &stubDB{}
	dlq := &stubDLQ{}

	payload := map[string]any{
		"ip":           "1.1.1.1",
		"port":         80,
		"service":      "HTTP",
		"timestamp":    123,
		"data_version": 2,
		"data": map[string]any{
			"response_str": "ok",
		},
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	msg := &pubsub.Message{Data: raw}

	ack := HandleMessage(ctx, db, dlq, msg)
	if !ack {
		t.Fatalf("expected ack on success")
	}
	if db.called != 1 {
		t.Fatalf("expected repo called once, got %d", db.called)
	}
	if db.record.Response != "ok" {
		t.Fatalf("unexpected response stored: %q", db.record.Response)
	}
	if dlq.called != 0 {
		t.Fatalf("expected no dlq publish, got %d", dlq.called)
	}
}
