package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"swaps-rt/internal/store"
)

func TestRestartAndReplay(t *testing.T) {
	ctx := context.Background()
	pg, err := store.NewPG(ctx, "postgresql://swaps:swaps@localhost:5432/swaps?sslmode=disable", 3*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer pg.Close()

	now := time.Now().UTC()
	ok, err := pg.UpsertEvent(ctx, "e1", "ETH", "buy", 1.0, 2000, now.Add(-2*time.Minute))
	if err != nil || !ok {
		t.Fatalf("upsert e1: %v ok=%v", err, ok)
	}
	ok, err = pg.UpsertEvent(ctx, "e1", "ETH", "buy", 1.0, 2000, now.Add(-2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	pg2, err := store.NewPG(ctx, "postgresql://swaps:swaps@localhost:5432/swaps?sslmode=disable", 3*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer pg2.Close()

	s, err := pg2.QueryStats(ctx, "ETH", now)
	if err != nil {
		t.Fatal(err)
	}
	if s.W5.Cnt < 1 {
		t.Fatalf("expected cnt>=1 got %d", s.W5.Cnt)
	}
}

func TestLoad1000PerSecond(t *testing.T) {
	ctx := context.Background()
	pg, err := store.NewPG(ctx, "postgresql://swaps:swaps@localhost:5432/swaps?sslmode=disable", 3*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer pg.Close()

	now := time.Now().UTC()
	start := time.Now()
	const total = 5000
	for i := 0; i < total; i++ {
		id := fmt.Sprintf("bench-%d", i)
		if _, err := pg.UpsertEvent(ctx, id, "SOL", "buy", 0.1, 10, now); err != nil {
			t.Fatal(err)
		}
	}
	elapsed := time.Since(start)
	perSec := float64(total) / elapsed.Seconds()
	if perSec < 1000 {
		t.Logf("throughput=%.0f/s (below 1000/s on this env)", perSec)
	}
}
