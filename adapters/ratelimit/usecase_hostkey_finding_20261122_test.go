package ratelimit_test

// REGRESSION TEST (found 2026-11-22 by the use-case test sweep; fixed in v0.2.1). Fails on v0.2.0.
//
// F-4 "ByHost splits one receiver host into several buckets". In v0.2.0 ByHost returned url.URL.Host verbatim,
// so spellings that reach the SAME endpoint got DIFFERENT token buckets: letter case (DNS names are
// case-insensitive), a trailing root dot, and an explicit default port. The package promises "one subscriber host
// cannot be flooded regardless of how many subscriptions point at it" (ratelimit.go ByHost doc); a tenant who
// registered the same receiver under N spellings got N× the configured rate against it.
// Fix (additive, no API change): key on the lower-cased host without a trailing dot, plus the port only when it is
// not the scheme default. Severity: low (abuse needs deliberate URL variants).

import (
	"testing"
	"time"

	"github.com/ab0t-com/ab0t-events-go/adapters/ratelimit"
	"github.com/ab0t-com/ab0t-events-go/events"
)

func TestUseCaseFinding_ByHostGivesOneReceiverHostSeveralBuckets(t *testing.T) {
	canonical := ratelimit.ByHost(events.DeliveryRequest{URL: "https://hooks.example.com/a"})
	for _, u := range []string{
		"https://HOOKS.EXAMPLE.COM/a",
		"https://Hooks.Example.com/b",
		"https://hooks.example.com./a",
		"https://hooks.example.com:443/a",
	} {
		if got := ratelimit.ByHost(events.DeliveryRequest{URL: u}); got != canonical {
			t.Errorf("FINDING F-4: %q is keyed %q, not %q — the same receiver host gets a fresh token bucket", u, got, canonical)
		}
	}

	// The consequence, in limiter terms: with Burst 1, the second request to the same host should have to wait.
	l := ratelimit.NewLimiter(ratelimit.Config{PerSecond: 0.001, Burst: 1})
	t0 := time.Unix(1_763_800_000, 0)
	first := l.Reserve(canonical, t0)
	evasive := l.Reserve(ratelimit.ByHost(events.DeliveryRequest{URL: "https://HOOKS.EXAMPLE.COM/a"}), t0)
	if first != 0 || evasive == 0 {
		t.Errorf("FINDING F-4: a case-variant URL got an immediate token (wait %v) from a host whose only token was just spent", evasive)
	}
}
