package processing

import (
	"encoding/json"
	"testing"
)

func TestResponseString(t *testing.T) {
	tests := []struct {
		name        string
		payload     map[string]any
		want        string
		expectError bool
	}{
		{
			name: "v1 base64 bytes",
			payload: map[string]any{
				"ip":           "1.1.1.1",
				"port":         80,
				"service":      "HTTP",
				"timestamp":    1,
				"data_version": 1,
				"data": map[string]any{
					"response_bytes_utf8": "aGVsbG8gd29ybGQ=",
				},
			},
			want: "hello world",
		},
		{
			name: "v2 plain string",
			payload: map[string]any{
				"ip":           "1.1.1.1",
				"port":         22,
				"service":      "SSH",
				"timestamp":    1,
				"data_version": 2,
				"data": map[string]any{
					"response_str": "ok",
				},
			},
			want: "ok",
		},
		{
			name: "unknown version",
			payload: map[string]any{
				"ip":           "1.1.1.1",
				"port":         22,
				"service":      "SSH",
				"timestamp":    1,
				"data_version": 99,
				"data":         map[string]any{},
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw, err := json.Marshal(tt.payload)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			msg, err := ParseScanMessage(raw)
			if err != nil {
				if tt.expectError {
					return
				}
				t.Fatalf("ParseScanMessage error: %v", err)
			}
			got, err := msg.ResponseString()
			if tt.expectError {
				if err == nil {
					t.Fatalf("expected error, got %q", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ResponseString error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("got %q want %q", got, tt.want)
			}
		})
	}
}
