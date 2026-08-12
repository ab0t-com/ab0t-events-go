package events

import (
	"context"
	"testing"
)

func TestPayloadFilter_Matches(t *testing.T) {
	payload := []byte(`{
		"status": "active",
		"size": 42,
		"ratio": 0.5,
		"public": true,
		"deleted": null,
		"file": {"owner": {"id": "u_9"}},
		"tags": ["a","b"]
	}`)

	cases := []struct {
		path, equals string
		want         bool
	}{
		{"status", "active", true},
		{"status", "ACTIVE", false},    // case-sensitive
		{"size", "42", true},           // number → exact text
		{"size", "42.0", false},        // no float coercion
		{"ratio", "0.5", true},         // fractional number
		{"public", "true", true},       // bool
		{"public", "false", false},     // bool mismatch
		{"deleted", "null", true},      // JSON null
		{"file.owner.id", "u_9", true}, // nested dot-path
		{"file.owner.id", "u_8", false},
		{"file.missing", "x", false},       // absent key
		{"file", "x", false},               // object is not scalar-comparable
		{"tags", "a", false},               // array is not scalar-comparable
		{"file.owner.id.deep", "x", false}, // descends through a non-object (string)
		{"", "x", false},                   // empty path never matches
	}
	for _, c := range cases {
		got := PayloadFilter{Path: c.path, Equals: c.equals}.matches(payload)
		if got != c.want {
			t.Errorf("PayloadFilter{%q==%q}.matches = %v, want %v", c.path, c.equals, got, c.want)
		}
	}
}

func TestSubscription_MatchesPayload_ANDAndEmpty(t *testing.T) {
	payload := []byte(`{"status":"active","tier":"gold"}`)

	// No filters → matches everything (backward compatible).
	if !(Subscription{}).MatchesPayload(payload) {
		t.Fatal("no filters should match any payload")
	}
	// AND semantics: both must match.
	both := Subscription{PayloadFilters: []PayloadFilter{{"status", "active"}, {"tier", "gold"}}}
	if !both.MatchesPayload(payload) {
		t.Fatal("both filters match → should match")
	}
	oneFails := Subscription{PayloadFilters: []PayloadFilter{{"status", "active"}, {"tier", "silver"}}}
	if oneFails.MatchesPayload(payload) {
		t.Fatal("one filter fails → should not match")
	}
	// Malformed / empty payload with a filter → no match.
	if both.MatchesPayload(nil) || both.MatchesPayload([]byte("not json")) {
		t.Fatal("empty/invalid payload must not match a filter")
	}
}

// TestEmit_PayloadFilterFanOut is the behavioral proof: within one org and event
// type, Emit fans out only to subscriptions whose payload filters match, and
// creates no delivery (log or queue) for the ones that don't.
func TestEmit_PayloadFilterFanOut(t *testing.T) {
	h := newHarness(DefaultRetryPolicy())
	// Three subs, all subscribed to the same type via "*":
	h.store.add(Subscription{ID: "match", OrgID: "org1", EventTypes: []string{"*"}, URL: "https://match", Active: true,
		PayloadFilters: []PayloadFilter{{"region", "eu"}}})
	h.store.add(Subscription{ID: "nomatch", OrgID: "org1", EventTypes: []string{"*"}, URL: "https://nomatch", Active: true,
		PayloadFilters: []PayloadFilter{{"region", "us"}}})
	h.store.add(Subscription{ID: "nofilter", OrgID: "org1", EventTypes: []string{"*"}, URL: "https://nofilter", Active: true})

	ev := mkEvent("e1", "drive.file.created", "org1", map[string]any{"region": "eu"})
	if err := h.svc.Emit(context.Background(), ev); err != nil {
		t.Fatal(err)
	}

	// Only "match" and "nofilter" get a queued job; "nomatch" is filtered out.
	pending := h.queue.pending()
	got := map[string]bool{}
	for _, j := range pending {
		got[j.Subscription.ID] = true
	}
	if len(pending) != 2 || !got["match"] || !got["nofilter"] || got["nomatch"] {
		t.Fatalf("expected fan-out to {match,nofilter}, got %v (n=%d)", got, len(pending))
	}
	// And the filtered-out subscription got NO delivery recorded in the log.
	if _, ok := h.log.get(DeliveryID("nomatch", "e1")); ok {
		t.Fatal("filtered-out subscription must not have a delivery record")
	}
	if _, ok := h.log.get(DeliveryID("match", "e1")); !ok {
		t.Fatal("matching subscription should have a delivery record")
	}
}
