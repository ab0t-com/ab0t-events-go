package metrics

import (
	"context"
	"testing"
	"time"

	"github.com/ab0t-com/ab0t-events-go/events"
)

func TestCollector_TalliesAttempts(t *testing.T) {
	c := New()
	ctx := context.Background()
	c.OnAttempt(ctx, events.Delivery{Status: events.StatusFailed}, events.Attempt{Code: 500, Latency: 10 * time.Millisecond})
	c.OnAttempt(ctx, events.Delivery{Status: events.StatusDead}, events.Attempt{Code: 500, Latency: 30 * time.Millisecond})
	c.OnAttempt(ctx, events.Delivery{Status: events.StatusDelivered}, events.Attempt{Code: 200, Latency: 20 * time.Millisecond})

	s := c.Snapshot()
	if s.Attempts != 3 || s.Delivered != 1 || s.Failed != 1 || s.Dead != 1 {
		t.Fatalf("counts = %+v", s)
	}
	if s.ByCode[500] != 2 || s.ByCode[200] != 1 {
		t.Fatalf("byCode = %+v", s.ByCode)
	}
	if s.LatencyMax != 30*time.Millisecond {
		t.Fatalf("latencyMax = %v", s.LatencyMax)
	}
	if s.MeanLatency() != 20*time.Millisecond {
		t.Fatalf("mean = %v, want 20ms", s.MeanLatency())
	}
	if got := s.SuccessRate(); got < 0.33 || got > 0.34 {
		t.Fatalf("success rate = %v, want ~0.333", got)
	}
	// Snapshot is a copy: further attempts don't mutate it.
	c.OnAttempt(ctx, events.Delivery{Status: events.StatusDelivered}, events.Attempt{Code: 200})
	if s.Attempts != 3 {
		t.Fatal("snapshot mutated after the fact")
	}
}

func TestSnapshot_EmptyIsSafe(t *testing.T) {
	s := New().Snapshot()
	if s.MeanLatency() != 0 || s.SuccessRate() != 0 {
		t.Fatalf("empty snapshot not zero: %+v", s)
	}
}
