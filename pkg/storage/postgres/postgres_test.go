package postgres

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/censys/scan-takehome/pkg/storage"
)

// Integration test that ensures the upsert keeps the newest timestamp only.
func TestUpsertLatestHonorsNewestTimestamp(t *testing.T) {
	t.Parallel()

	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5433/scans_test?sslmode=disable"
	}

	ctx := context.Background()
	pool, err := NewDB(ctx, dsn)
	if err != nil {
		t.Fatalf("database unavailable: %v", err)
	}
	defer pool.Close()

	if err := EnsureSchema(ctx, pool); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	if _, err := pool.Exec(ctx, "TRUNCATE scans"); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	repo := NewRepository(pool)

	base := time.Now().UTC().Truncate(time.Second)

	old := storage.ScanRecord{
		IP:        "1.1.1.1",
		Port:      80,
		Service:   "HTTP",
		Timestamp: base,
		Response:  "old",
	}
	newer := storage.ScanRecord{
		IP:        "1.1.1.1",
		Port:      80,
		Service:   "HTTP",
		Timestamp: base.Add(time.Hour),
		Response:  "new",
	}
	older := storage.ScanRecord{
		IP:        "1.1.1.1",
		Port:      80,
		Service:   "HTTP",
		Timestamp: base.Add(-time.Hour),
		Response:  "older",
	}

	if err := repo.UpsertLatest(ctx, old); err != nil {
		t.Fatalf("upsert old: %v", err)
	}
	if err := repo.UpsertLatest(ctx, newer); err != nil {
		t.Fatalf("upsert newer: %v", err)
	}
	if err := repo.UpsertLatest(ctx, older); err != nil {
		t.Fatalf("upsert older: %v", err)
	}

	var got storage.ScanRecord
	row := pool.QueryRow(ctx, `SELECT ip, port, service, last_scanned, response FROM scans WHERE ip=$1 AND port=$2 AND service=$3`, "1.1.1.1", 80, "HTTP")
	if err := row.Scan(&got.IP, &got.Port, &got.Service, &got.Timestamp, &got.Response); err != nil {
		t.Fatalf("scan row: %v", err)
	}

	if !got.Timestamp.Equal(newer.Timestamp) {
		t.Fatalf("timestamp mismatch: got %s want %s", got.Timestamp, newer.Timestamp)
	}
	if got.Response != newer.Response {
		t.Fatalf("response mismatch: got %q want %q", got.Response, newer.Response)
	}
}
