package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"syscall"

	"cloud.google.com/go/pubsub"

	"github.com/censys/scan-takehome/pkg/processing"
	pgstore "github.com/censys/scan-takehome/pkg/storage/postgres"
)

type config struct {
	ProjectID      string
	SubscriptionID string
	DLQTopicID     string
	DatabaseURL    string
	WorkerCount    int
	MaxOutstanding int
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.LUTC)

	cfg := loadConfig()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgstore.NewDB(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("db connect: %v", err)
	}
	defer pool.Close()

	if err := pgstore.EnsureSchema(ctx, pool); err != nil {
		log.Fatalf("db schema: %v", err)
	}

	db := pgstore.NewRepository(pool)

	client, err := pubsub.NewClient(ctx, cfg.ProjectID)
	if err != nil {
		log.Fatalf("pubsub client: %v", err)
	}
	defer client.Close()

	var dlqPublisher processing.DLQPublisher
	if cfg.DLQTopicID != "" {
		dlqPublisher = processing.NewPubSubDLQPublisher(client.Topic(cfg.DLQTopicID))
	} else {
		dlqPublisher = &processing.NoopDLQPublisher{}
	}

	sub := client.Subscription(cfg.SubscriptionID)
	sub.ReceiveSettings.NumGoroutines = cfg.WorkerCount
	sub.ReceiveSettings.MaxOutstandingMessages = cfg.MaxOutstanding

	log.Printf("processor started project=%s subscription=%s workers=%d", cfg.ProjectID, cfg.SubscriptionID, cfg.WorkerCount)

	err = sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		if processing.HandleMessage(ctx, db, dlqPublisher, msg) {
			msg.Ack()
		} else {
			msg.Nack()
		}
	})
	if err != nil {
		log.Fatalf("subscription receive ended: %v", err)
	}
}

func loadConfig() config {
	return config{
		ProjectID:      getEnv("PUBSUB_PROJECT_ID", "test-project"),
		SubscriptionID: getEnv("PUBSUB_SUBSCRIPTION_ID", "scan-sub"),
		DLQTopicID:     getEnv("PUBSUB_DLQ_TOPIC", ""),
		DatabaseURL:    getEnv("DATABASE_URL", "postgres://postgres:postgres@postgres:5432/scans?sslmode=disable"),
		WorkerCount:    getEnvInt("PROCESSOR_WORKERS", runtime.NumCPU()),
		MaxOutstanding: getEnvInt("PROCESSOR_MAX_OUTSTANDING", 200),
	}
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if val := os.Getenv(key); val != "" {
		n, err := strconv.Atoi(val)
		if err == nil && n > 0 {
			return n
		}
	}
	return fallback
}
