package model

import "time"

type SwapEvent struct {
	ID        string    `json:"id"`
	Token     string    `json:"token"`
	Amount    float64   `json:"amount"`
	USD       float64   `json:"usd"`
	Side      string    `json:"side"`
	EventTime time.Time `json:"event_time"`
}
