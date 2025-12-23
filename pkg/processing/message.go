package processing

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/censys/scan-takehome/pkg/scanning"
)

// ScanMessage is a lightweight view of an incoming scan payload.
// It uses RawMessage for the data blob so we can decode based on data_version.
type ScanMessage struct {
	Ip          string          `json:"ip"`
	Port        uint32          `json:"port"`
	Service     string          `json:"service"`
	Timestamp   int64           `json:"timestamp"`
	DataVersion int             `json:"data_version"`
	Data        json.RawMessage `json:"data"`
}

// ParseScanMessage unmarshals the JSON payload into a ScanMessage.
func ParseScanMessage(raw []byte) (ScanMessage, error) {
	var msg ScanMessage
	if err := json.Unmarshal(raw, &msg); err != nil {
		return ScanMessage{}, fmt.Errorf("unmarshal scan: %w", err)
	}
	if len(msg.Data) == 0 {
		return ScanMessage{}, errors.New("missing data field")
	}
	return msg, nil
}

// ResponseString extracts the service response as a UTF-8 string, normalizing
// different data versions to a single representation.
func (s ScanMessage) ResponseString() (string, error) {
	switch s.DataVersion {
	case scanning.V1:
		var payload struct {
			ResponseBytesUtf8 []byte `json:"response_bytes_utf8"`
		}
		if err := json.Unmarshal(s.Data, &payload); err != nil {
			return "", fmt.Errorf("decode v1 data: %w", err)
		}
		return string(payload.ResponseBytesUtf8), nil
	case scanning.V2:
		var payload struct {
			ResponseStr string `json:"response_str"`
		}
		if err := json.Unmarshal(s.Data, &payload); err != nil {
			return "", fmt.Errorf("decode v2 data: %w", err)
		}
		return payload.ResponseStr, nil
	default:
		return "", errors.New("unsupported data_version")
	}
}
