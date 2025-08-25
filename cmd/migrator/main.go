package main

import (
	"context"
	"log"
	"os"

	"swaps-rt/internal/store"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	dsn := os.Getenv("DB_DSN")
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	if _, err := pool.Exec(ctx, store.SchemaSQL); err != nil {
		log.Fatal(err)
	}
	log.Println("migrations applied")
}
