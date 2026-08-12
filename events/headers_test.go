package events

import (
	"context"
	"testing"
	"time"
)

func eventWithHeaders(id, org string, headers map[string]string) Event {
	return Event{
		ID: id, Type: "drive.file.created", OrgID: org,
		OccurredAt: time.Unix(1_700_000_000, 0).UTC(),
		Payload:    []byte(`{"n":1}`),
		Headers:    headers,
	}
}

// TestProcess_CustomHeadersMergedAndReservedProtected is the behavioral proof:
// custom headers (e.g. a W3C traceparent) ride along on the outbound request,
// but a caller can never override the core/signer-owned reserved headers.
func TestProcess_CustomHeadersMergedAndReservedProtected(t *testing.T) {
	h := newHarness(DefaultRetryPolicy())
	h.store.add(Subscription{ID: "s1", OrgID: "org1", EventTypes: []string{"*"}, URL: "https://x", Active: true})

	ev := eventWithHeaders("e1", "org1", map[string]string{
		"traceparent":  "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
		"X-Custom":     "hello",
		"X-Event-Id":   "FORGED",     // reserved — must be ignored
		"X-Signature":  "forged-sig", // reserved (signer-owned)
		"Content-Type": "text/evil",  // reserved
		"X-Attempt":    "999",        // reserved
	})
	if err := h.svc.Emit(context.Background(), ev); err != nil {
		t.Fatal(err)
	}
	if err := h.queue.drain(context.Background(), h.svc, h.clock, 5); err != nil {
		t.Fatal(err)
	}

	got := h.deliv.lastHeaders()
	if got == nil {
		t.Fatal("no delivery was made")
	}
	// Custom headers are present.
	if got["traceparent"] != "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01" {
		t.Errorf("traceparent not propagated: %q", got["traceparent"])
	}
	if got["X-Custom"] != "hello" {
		t.Errorf("custom header not propagated: %q", got["X-Custom"])
	}
	// Reserved headers keep the core/signer values, not the forged ones.
	if got[HeaderEventID] != "e1" {
		t.Errorf("X-Event-Id overridden: %q", got[HeaderEventID])
	}
	if got["Content-Type"] != "application/json" {
		t.Errorf("Content-Type overridden: %q", got["Content-Type"])
	}
	if got[HeaderAttempt] != "1" {
		t.Errorf("X-Attempt overridden: %q", got[HeaderAttempt])
	}
	if got[HeaderSignature] == "forged-sig" || got[HeaderSignature] == "" {
		t.Errorf("X-Signature was forged or missing: %q", got[HeaderSignature])
	}
}

// TestRehydrate_RestoresHeaders proves custom headers are persisted in the event
// snapshot and survive a durable-queue round trip (JobRef → Rehydrate), so a
// trace context propagates even off-process.
func TestRehydrate_RestoresHeaders(t *testing.T) {
	h := newHarness(DefaultRetryPolicy())
	h.store.add(Subscription{ID: "s1", OrgID: "org1", EventTypes: []string{"*"}, URL: "https://x", Active: true})

	ev := eventWithHeaders("e1", "org1", map[string]string{"traceparent": "00-trace-span-01"})
	if err := h.svc.Emit(context.Background(), ev); err != nil {
		t.Fatal(err)
	}

	ref := JobRef{OrgID: "org1", DeliveryID: DeliveryID("s1", "e1"), SubscriptionID: "s1", EventID: "e1", AttemptN: 1}
	job, err := h.svc.Rehydrate(context.Background(), ref)
	if err != nil {
		t.Fatalf("rehydrate: %v", err)
	}
	if job.Event.Headers["traceparent"] != "00-trace-span-01" {
		t.Fatalf("headers not restored from snapshot: %+v", job.Event.Headers)
	}
}

func TestIsReservedHeader(t *testing.T) {
	for _, name := range []string{"Content-Type", "content-type", "X-Event-Id", "x-signature", "X-Attempt", "webhook-signature"} {
		if !isReservedHeader(name) {
			t.Errorf("%q should be reserved", name)
		}
	}
	for _, name := range []string{"traceparent", "X-Custom", "Baggage", "X-Tenant-Trace"} {
		if isReservedHeader(name) {
			t.Errorf("%q should NOT be reserved", name)
		}
	}
}
