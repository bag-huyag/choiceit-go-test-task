package agg

import (
	"time"

	"swaps-rt/internal/model"
)

type Aggregator struct {
	LateGrace time.Duration
}

func New(grace time.Duration) *Aggregator { return &Aggregator{LateGrace: grace} }

func (a *Aggregator) Accept(watermark time.Time, ev model.SwapEvent) bool {
	if watermark.IsZero() {
		return true
	}
	return watermark.Sub(ev.EventTime) <= a.LateGrace
}

func MinuteKey(t time.Time) time.Time { return t.UTC().Truncate(time.Minute) }
