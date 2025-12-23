package storage

import (
	"context"
	"time"
)

// ScanRecord holds the normalized scan fields we persist.
type ScanRecord struct {
	IP        string
	Port      uint32
	Service   string
	Timestamp time.Time
	Response  string
}

// Repository defines persistence operations for scans.
type Repository interface {
	UpsertLatest(ctx context.Context, record ScanRecord) error
}
