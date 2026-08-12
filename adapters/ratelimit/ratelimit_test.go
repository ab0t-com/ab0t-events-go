package ratelimit

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ab0t-com/ab0t-events-go/events"
)

func TestReserve_TokenBucketMath(t *testing.T) {
	// 10 tokens/sec, burst 2: two immediate, then ~100ms spacing.
	l := NewLimiter(Config{PerSecond: 10, Burst: 2})
	now := time.Unix(0, 0)

	if d := l.Reserve("k", now); d != 0 {
		t.Fatalf("reserve 1 = %v, want 0", d)
	}
	if d := l.Reserve("k", now); d != 0 {
		t.Fatalf("reserve 2 (burst) = %v, want 0", d)
	}
	// Third depletes the burst: must wait ~1/10s.
	if d := l.Reserve("k", now); d != 100*time.Millisecond {
		t.Fatalf("reserve 3 = %v, want 100ms", d)
	}
	// Fourth queued behind it: ~200ms.
	if d := l.Reserve("k", now); d != 200*time.Millisecond {
		t.Fatalf("reserve 4 = %v, want 200ms", d)
	}
	// After 500ms, tokens refill (5 accrued, capped at burst 2) → immediate again.
	if d := l.Reserve("k", now.Add(500*time.Millisecond)); d != 0 {
		t.Fatalf("reserve after refill = %v, want 0", d)
	}
}

// TestReserve_EvictsIdleBuckets is the proving test for audit Finding 8: idle,
// fully-refilled per-key buckets are evicted so the map cannot grow without bound
// as a tenant registers subscriptions across many distinct hosts.
func TestReserve_EvictsIdleBuckets(t *testing.T) {
	l := NewLimiter(Config{PerSecond: 100, Burst: 5})
	base := time.Unix(0, 0)

	// Touch 50 distinct keys at t=0.
	for i := 0; i < 50; i++ {
		l.Reserve(key(i), base)
	}
	if n := l.NumBuckets(); n != 50 {
		t.Fatalf("want 50 buckets, got %d", n)
	}

	// After the idle TTL, a new request (which triggers a sweep) evicts every idle
	// bucket. Only the just-touched key survives.
	after := base.Add(idleEvictionTTL + sweepInterval + time.Second)
	l.Reserve("fresh", after)
	if n := l.NumBuckets(); n != 1 {
		t.Fatalf("idle buckets not evicted: want 1, got %d", n)
	}
	if !hasKey(l, "fresh") {
		t.Fatal("the just-touched bucket must survive")
	}

	// A bucket touched within the TTL is retained: recency, not just a sweep, gates
	// eviction. Touch "fresh" again just before the next sweep, then trigger one.
	l.Reserve("fresh", after.Add(sweepInterval/2))
	l.Reserve("fresh", after.Add(sweepInterval+time.Second)) // triggers a sweep
	if !hasKey(l, "fresh") {
		t.Fatal("a recently-touched bucket must not be evicted")
	}
}

func key(i int) string { return "host-" + strconv.Itoa(i) }

func hasKey(l *Limiter, k string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, ok := l.buckets[k]
	return ok
}

func TestReserve_PerKeyIsolation(t *testing.T) {
	l := NewLimiter(Config{PerSecond: 1, Burst: 1})
	now := time.Unix(0, 0)
	if d := l.Reserve("a", now); d != 0 {
		t.Fatalf("key a first = %v, want 0", d)
	}
	// A different key has its own full bucket.
	if d := l.Reserve("b", now); d != 0 {
		t.Fatalf("key b first = %v, want 0", d)
	}
	// Key a is now depleted.
	if d := l.Reserve("a", now); d != time.Second {
		t.Fatalf("key a second = %v, want 1s", d)
	}
}

func TestReserve_DisabledPassesThrough(t *testing.T) {
	l := NewLimiter(Config{PerSecond: 0})
	for i := 0; i < 100; i++ {
		if d := l.Reserve("k", time.Unix(0, 0)); d != 0 {
			t.Fatalf("disabled limiter returned wait %v", d)
		}
	}
}

// countingDeliverer records how many Post calls happened and when.
type countingDeliverer struct {
	mu    sync.Mutex
	times []time.Time
}

func (c *countingDeliverer) Post(_ context.Context, _ events.DeliveryRequest) (events.DeliveryResponse, error) {
	c.mu.Lock()
	c.times = append(c.times, time.Now())
	c.mu.Unlock()
	return events.DeliveryResponse{StatusCode: 200}, nil
}
func (c *countingDeliverer) count() int { c.mu.Lock(); defer c.mu.Unlock(); return len(c.times) }

func TestDeliverer_ThrottlesBurst(t *testing.T) {
	inner := &countingDeliverer{}
	// 50/sec, burst 1: three requests to one host take ~40ms total (2 waits of 20ms).
	d := WrapDeliverer(inner, Config{PerSecond: 50, Burst: 1})
	req := events.DeliveryRequest{URL: "https://sub.example/hook"}

	start := time.Now()
	for i := 0; i < 3; i++ {
		if _, err := d.Post(context.Background(), req); err != nil {
			t.Fatalf("post %d: %v", i, err)
		}
	}
	elapsed := time.Since(start)
	if inner.count() != 3 {
		t.Fatalf("delivered %d, want 3", inner.count())
	}
	if elapsed < 30*time.Millisecond {
		t.Fatalf("burst was not throttled (elapsed %v)", elapsed)
	}
}

func TestDeliverer_ContextCancelStopsWaiting(t *testing.T) {
	inner := &countingDeliverer{}
	d := WrapDeliverer(inner, Config{PerSecond: 1, Burst: 1})
	req := events.DeliveryRequest{URL: "https://sub.example/hook"}

	// Use up the single burst token.
	_, _ = d.Post(context.Background(), req)

	// The next call must wait ~1s; cancel quickly and confirm it returns the ctx error.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
	defer cancel()
	if _, err := d.Post(ctx, req); err == nil {
		t.Fatal("expected context error while rate-limited")
	}
	if inner.count() != 1 {
		t.Fatalf("delivered %d, want 1 (second was cancelled)", inner.count())
	}
}

func TestByHost(t *testing.T) {
	if got := ByHost(events.DeliveryRequest{URL: "https://a.example:8443/x?y=1"}); got != "a.example:8443" {
		t.Fatalf("ByHost = %q", got)
	}
}
