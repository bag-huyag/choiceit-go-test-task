package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"swaps-rt/internal/store"

	kafka "github.com/segmentio/kafka-go"
)

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

type wireEvent struct {
	ID        string    `json:"id"`
	Token     string    `json:"token"`
	Amount    float64   `json:"amount"`
	USD       float64   `json:"usd"`
	Side      string    `json:"side"`
	EventTime time.Time `json:"event_time"`
}

func main() {
	ctx := context.Background()
	dsn := os.Getenv("DB_DSN")
	graceSec, _ := strconv.Atoi(env("GRACE_SECONDS", "180"))
	pg, err := store.NewPG(ctx, dsn, time.Duration(graceSec)*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	defer pg.Close()

	brokers := strings.Split(env("KAFKA_BROKERS", "localhost:9092"), ",")
	topic := env("KAFKA_TOPIC", "swaps.v1")
	group := env("KAFKA_GROUP", "ingest-consumer")

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        group,
		MinBytes:       1,
		MaxBytes:       10 << 20,
		StartOffset:    kafka.FirstOffset,
		CommitInterval: 0,
	})
	defer reader.Close()

	log.Println("ingest started")

	for {
		m, err := reader.FetchMessage(ctx)
		if err != nil {
			log.Fatal(err)
		}

		var ev wireEvent
		if err := json.Unmarshal(m.Value, &ev); err != nil {
			_ = reader.CommitMessages(ctx, m)
			continue
		}

		token := strings.ToUpper(ev.Token)
		side := strings.ToLower(ev.Side)

		if _, err := pg.UpsertEvent(ctx, ev.ID, token, side, ev.Amount, ev.USD, ev.EventTime); err != nil {
			log.Printf("upsert error: %v", err)
			continue
		}
		if err := reader.CommitMessages(ctx, m); err != nil {
			log.Printf("commit error: %v", err)
		}
	}
}
