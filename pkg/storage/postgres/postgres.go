package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/censys/scan-takehome/pkg/storage"
)

type Repository struct {
	pool *pgxpool.Pool
}

// NewRepository wraps an existing pool. Call EnsureSchema before using it.
func NewRepository(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

// EnsureSchema creates the scans table if it is missing.
func EnsureSchema(ctx context.Context, pool *pgxpool.Pool) error {
	ddl := `
CREATE TABLE IF NOT EXISTS scans (
  ip TEXT NOT NULL,
  port INTEGER NOT NULL,
  service TEXT NOT NULL,
  last_scanned TIMESTAMPTZ NOT NULL,
  response TEXT NOT NULL,
  PRIMARY KEY (ip, port, service)
);`
	if _, err := pool.Exec(ctx, ddl); err != nil {
		return fmt.Errorf("ERROR creating scans table: %w", err)
	}
	return nil
}

// UpsertLatest persists the newest scan for a (ip, port, service) tuple. Older
// scans are ignored to protect against out-of-order deliveries.
func (r *Repository) UpsertLatest(ctx context.Context, record storage.ScanRecord) error {
	const query = `
INSERT INTO scans (ip, port, service, last_scanned, response)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (ip, port, service)
DO UPDATE SET
  last_scanned = EXCLUDED.last_scanned,
  response = EXCLUDED.response
WHERE EXCLUDED.last_scanned >= scans.last_scanned;
`
	_, err := r.pool.Exec(ctx, query,
		record.IP,
		int(record.Port),
		record.Service,
		record.Timestamp.UTC(),
		record.Response,
	)
	if err != nil {
		return fmt.Errorf("upsert scan: %w", err)
	}
	return nil
}

// Close helps when wiring Repository to a lifecycle manager.
func (r *Repository) Close() {
	r.pool.Close()
}

// NewDB opens a pgx pool with tuned defaults.
func NewDB(ctx context.Context, connString string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("parse db config: %w", err)
	}
	// Keep a small, steady pool; processor is lightweight.
	cfg.MaxConns = 10
	cfg.MinConns = 2
	cfg.MaxConnIdleTime = 5 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping db: %w", err)
	}
	return pool, nil
}
