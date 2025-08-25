package agg

import (
	"testing"
	"time"

	"swaps-rt/internal/model"
)

func TestDuplicates(t *testing.T) {
	a := New(0)
	ev := model.SwapEvent{EventTime: time.Now()}
	if !a.Accept(time.Time{}, ev) {
		t.Fatal("first event should be accepted")
	}
	if a.Accept(time.Time{}, ev) {
		t.Fatal("second event should be rejected")
	}
}

func TestOutOfOrderWithinGrace(t *testing.T) {
	a := New(3 * time.Minute)
	wm := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	ev := model.SwapEvent{EventTime: wm.Add(-2 * time.Minute)}
	if !a.Accept(wm, ev) {
		t.Fatal("should accept within grace")
	}
}

func TestOutOfOrderBeyondGrace(t *testing.T) {
	a := New(3 * time.Minute)
	wm := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	ev := model.SwapEvent{EventTime: wm.Add(-10 * time.Minute)}
	if a.Accept(wm, ev) {
		t.Fatal("should reject beyond grace")
	}
}
