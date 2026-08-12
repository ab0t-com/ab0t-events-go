package events

import (
	"testing"
	"time"
)

func TestBackoff_Exponential(t *testing.T) {
	p := RetryPolicy{BaseDelay: time.Second, MaxDelay: time.Hour, Multiplier: 2, JitterFrac: 0}
	// jitter 0.5 => factor 1.0 (no change), so pure exponential.
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second}
	for i, w := range want {
		if got := p.Backoff(i+1, 0.5); got != w {
			t.Fatalf("attempt %d: got %v want %v", i+1, got, w)
		}
	}
}

func TestBackoff_CapAndJitterBounds(t *testing.T) {
	p := RetryPolicy{BaseDelay: time.Second, MaxDelay: 3 * time.Second, Multiplier: 2, JitterFrac: 0.5}
	// Attempt 10 would be huge; must cap at MaxDelay before jitter.
	base := 3 * time.Second
	for _, j := range []float64{0, 0.25, 0.5, 0.75, 0.999} {
		got := p.Backoff(10, j)
		lo := time.Duration(float64(base) * 0.5)
		hi := time.Duration(float64(base) * 1.5)
		if got < lo || got > hi {
			t.Fatalf("jitter %v: got %v outside [%v,%v]", j, got, lo, hi)
		}
	}
	// jitter extremes: 0 => -50%, ~1 => +50%.
	if got := p.Backoff(10, 0); got != base/2 {
		t.Fatalf("jitter 0: got %v want %v", got, base/2)
	}
}

func TestBackoff_ConstantWhenMultiplierLow(t *testing.T) {
	p := RetryPolicy{BaseDelay: 2 * time.Second, Multiplier: 0, JitterFrac: 0}
	for i := 1; i <= 3; i++ {
		if got := p.Backoff(i, 0.5); got != 2*time.Second {
			t.Fatalf("attempt %d: got %v want 2s", i, got)
		}
	}
}
