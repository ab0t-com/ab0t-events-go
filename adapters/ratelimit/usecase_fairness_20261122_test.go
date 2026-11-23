package ratelimit_test

// Use-case scenario tests, 2026-11-22: each test is one job a service or its customer does with the library.
//
// Job: "one tenant's bulk import must not flood a receiver host or delay everybody else's webhooks". The limiter is
// keyed by destination host (ByHost): a noisy host queues only itself, and a host shared by several tenants (a
// hosted automation platform, say) is ONE bucket across those tenants. Deterministic: Reserve with explicit
// instants, and an already-cancelled context for the "worker gives up while throttled" path.

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ab0t-com/ab0t-events-go/adapters/ratelimit"
	"github.com/ab0t-com/ab0t-events-go/events"
)

func TestUseCase_RateLimit_NoisyHostQueuesOnlyItself(t *testing.T) {
	t0 := time.Unix(1_763_800_000, 0)
	l := ratelimit.NewLimiter(ratelimit.Config{PerSecond: 1, Burst: 2})

	// A bulk import fires 4 webhooks at once to one host: 2 go now (burst), then one per second.
	want := []time.Duration{0, 0, time.Second, 2 * time.Second}
	for i, w := range want {
		if got := l.Reserve("bulk-importer.example", t0); got != w {
			t.Fatalf("noisy host request %d: wait %v, want %v", i+1, got, w)
		}
	}
	// A different customer's host at the same instant is not delayed at all.
	if got := l.Reserve("quiet-customer.example", t0); got != 0 {
		t.Fatalf("quiet host waited %v behind the noisy one, want 0", got)
	}
	// Two seconds later the noisy host has earned 2 tokens but owed 2, so the next request waits 1s.
	if got := l.Reserve("bulk-importer.example", t0.Add(2*time.Second)); got != time.Second {
		t.Fatalf("noisy host after 2s: wait %v, want 1s", got)
	}
	// After an idle hour the bucket is full again — but never above Burst.
	far := t0.Add(time.Hour)
	a, b, c := l.Reserve("bulk-importer.example", far), l.Reserve("bulk-importer.example", far), l.Reserve("bulk-importer.example", far)
	if a != 0 || b != 0 || c != time.Second {
		t.Fatalf("after idle: waits %v %v %v, want 0 0 1s (refill capped at Burst)", a, b, c)
	}
}

type ucRecorder struct {
	mu   sync.Mutex
	urls []string
}

func (r *ucRecorder) Post(_ context.Context, req events.DeliveryRequest) (events.DeliveryResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.urls = append(r.urls, req.URL)
	return events.DeliveryResponse{StatusCode: 200}, nil
}

func (r *ucRecorder) seen() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.urls...)
}

func TestUseCase_RateLimit_SharedReceiverHostIsOneBucketAcrossTenants(t *testing.T) {
	for _, tc := range []struct{ url, key string }{
		{"https://hooks.example.com/org_a/endpoint", "hooks.example.com"},
		{"https://hooks.example.com/org_b/endpoint?token=placeholder", "hooks.example.com"},
		{"https://hooks.example.com:8443/org_c", "hooks.example.com:8443"},
		{"https://other.example.net/hook", "other.example.net"},
	} {
		if got := ratelimit.ByHost(events.DeliveryRequest{URL: tc.url}); got != tc.key {
			t.Fatalf("ByHost(%q) = %q, want %q", tc.url, got, tc.key)
		}
	}

	rec := &ucRecorder{}
	d := ratelimit.WrapDeliverer(rec, ratelimit.Config{PerSecond: 0.001, Burst: 1}) // one token, then ~1000s per token
	if _, err := d.Post(context.Background(), events.DeliveryRequest{URL: "https://hooks.example.com/org_a/endpoint"}); err != nil {
		t.Fatalf("tenant A, first request: %v", err)
	}
	// Tenant B targets the SAME host: it must wait for A's bucket. Its worker is shutting down (context already
	// cancelled), so it gives up without POSTing — the core records a transient failure and retries later.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := d.Post(ctx, events.DeliveryRequest{URL: "https://hooks.example.com/org_b/endpoint"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("tenant B on the shared host: want context.Canceled while throttled, got %v", err)
	}
	// A different host is unaffected by the shared host's bucket.
	if _, err := d.Post(context.Background(), events.DeliveryRequest{URL: "https://other.example.net/hook"}); err != nil {
		t.Fatalf("other host: %v", err)
	}
	got := rec.seen()
	if len(got) != 2 || got[0] != "https://hooks.example.com/org_a/endpoint" || got[1] != "https://other.example.net/hook" {
		t.Fatalf("forwarded %v, want tenant A's request then the other host's (tenant B throttled, not forwarded)", got)
	}
}
