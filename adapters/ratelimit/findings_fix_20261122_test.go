package ratelimit_test

// Regression test for the F-4 fix (canonical ByHost key, v0.2.1): the
// canonical ByHost key must merge only spellings of the SAME endpoint. A port that is not the scheme's default is a
// different endpoint and keeps its own bucket; IPv6 literals keep the bracketed form url.URL.Host uses; input that
// has no host to canonicalise is keyed as before.

import (
	"testing"

	"github.com/ab0t-com/ab0t-events-go/adapters/ratelimit"
	"github.com/ab0t-com/ab0t-events-go/events"
)

func TestFindingsFix_ByHostCanonicalKeyKeepsDistinctEndpointsApart(t *testing.T) {
	for _, tc := range []struct{ url, key string }{
		{"http://Hooks.Example.com:80/x", "hooks.example.com"},          // http default port dropped
		{"https://hooks.example.com:80/x", "hooks.example.com:80"},      // 80 is NOT https's default: kept
		{"http://hooks.example.com:443/x", "hooks.example.com:443"},     // 443 is NOT http's default: kept
		{"HTTPS://hooks.example.com:443/x", "hooks.example.com"},        // scheme compared case-insensitively
		{"https://HOOKS.example.com.:8443/x", "hooks.example.com:8443"}, // canonical name, non-default port kept
		{"https://[2001:DB8::1]/x", "[2001:db8::1]"},                    // IPv6 keeps its brackets
		{"https://[2001:db8::1]:443/x", "[2001:db8::1]"},
		{"https://[2001:db8::1]:8443/x", "[2001:db8::1]:8443"},
		{"/relative/path", ""}, // no host: unchanged from v0.2.0
		{"://not a url", "://not a url"},
	} {
		if got := ratelimit.ByHost(events.DeliveryRequest{URL: tc.url}); got != tc.key {
			t.Errorf("ByHost(%q) = %q, want %q", tc.url, got, tc.key)
		}
	}
}
