package store

import (
	"context"
	"time"
)

type Window struct {
	VolumeToken float64 `json:"volume_token"`
	VolumeUSD   float64 `json:"volume_usd"`
	Cnt         int64   `json:"cnt"`
	Buy         int64   `json:"buy"`
	Sell        int64   `json:"sell"`
}

type Stats struct {
	Token     string    `json:"token"`
	Now       time.Time `json:"now"`
	W5        Window    `json:"w5"`
	W1        Window    `json:"w1"`
	W24       Window    `json:"w24"`
	Watermark time.Time `json:"watermark"`
}

type Store interface {
	UpsertEvent(ctx context.Context, evID, token, side string, amt, usd float64, eventTime time.Time) (bool, error)
	QueryStats(ctx context.Context, token string, now time.Time) (*Stats, error)
	ListenUpdates(ctx context.Context) (<-chan string, func(), error)
	Close()
}
