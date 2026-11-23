package events

// REGRESSION TEST (found 2026-11-22 by the use-case test sweep; fixed in v0.2.1). Fails on v0.2.0.
//
// F-3 "in-process retries act on a stale subscription snapshot". A Job carries a copy of the Subscription taken at
// Emit time (ports.go Job doc). In v0.2.0 Process signed and POSTed with job.Subscription and a retry was
// `next := job`, so every retry reused that copy. On the in-process queue (inproc, and therefore the webhooks
// facade) that meant, for the whole retry horizon (default policy: up to ~4 attempts over minutes):
//   - RotateSecret after a leak: retries were still signed with the REVOKED secret, so a receiver that installed the
//     new secret rejected them (400 → permanent → dead-letter), and they counted toward autodisable pausing the endpoint;
//   - Unsubscribe: retries still POSTed to the endpoint the customer deleted (and a paused subscription kept receiving).
// The durable path (ProcessRef → Rehydrate) reloads the subscription and gets both right
// (usecase_rotation_20261122_test.go), so the two queue adapters disagreed on the same scenario. Rotation must be
// immediate: every attempt should use the subscription as it is stored now.
//
// Fix (keeps direct Process callers working): Process re-reads the subscription from the store for a retry
// (AttemptN > 1). Reproduces with the same fake queue the core's own retry tests use (it is what inproc does).

import (
	"context"
	"strings"
	"testing"
)

func TestUseCaseFinding_InProcessRetryAfterRotateSecretIsSignedWithTheRevokedSecret(t *testing.T) {
	ctx := context.Background()
	h := newHarness(fastPolicy)
	sub, err := h.svc.Subscribe(ctx, NewSubscription{OrgID: "org_a", EventTypes: []string{"*"}, URL: "https://a.example/hook", Secret: "leaked-placeholder-secret"})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	h.deliv.byURL["https://a.example/hook"] = 503
	if err := h.svc.Emit(ctx, mkEvent("evt_1", "contact.updated", "org_a", nil)); err != nil {
		t.Fatalf("emit: %v", err)
	}
	if err := h.queue.drain(ctx, h.svc, h.clock, 1); err != nil { // attempt 1 fails; the retry waits in the queue
		t.Fatalf("attempt 1: %v", err)
	}
	newSecret, err := h.svc.RotateSecret(ctx, "org_a", sub.ID) // the customer rotates after a leak
	if err != nil {
		t.Fatalf("rotate: %v", err)
	}
	h.deliv.byURL["https://a.example/hook"] = 200
	if err := h.queue.drain(ctx, h.svc, h.clock, 1); err != nil { // the already-scheduled retry fires
		t.Fatalf("attempt 2: %v", err)
	}
	sig := h.deliv.lastHeaders()["X-Signature"]
	if !strings.HasPrefix(sig, "sig("+newSecret+",") {
		usedOld := strings.HasPrefix(sig, "sig(leaked-placeholder-secret,")
		t.Fatalf("FINDING F-3: a retry that fired AFTER RotateSecret was not signed with the new secret (signed with the revoked one: %v)", usedOld)
	}
}

func TestUseCaseFinding_InProcessRetryAfterUnsubscribeStillPostsToTheDeletedEndpoint(t *testing.T) {
	ctx := context.Background()
	h := newHarness(fastPolicy)
	sub, err := h.svc.Subscribe(ctx, NewSubscription{OrgID: "org_a", EventTypes: []string{"*"}, URL: "https://old-crm.example/hook", Secret: "placeholder-secret"})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	h.deliv.byURL["https://old-crm.example/hook"] = 500
	if err := h.svc.Emit(ctx, mkEvent("evt_1", "contact.updated", "org_a", nil)); err != nil {
		t.Fatalf("emit: %v", err)
	}
	if err := h.queue.drain(ctx, h.svc, h.clock, 1); err != nil {
		t.Fatalf("attempt 1: %v", err)
	}
	if err := h.svc.Unsubscribe(ctx, "org_a", sub.ID); err != nil { // the customer decommissions the endpoint
		t.Fatalf("unsubscribe: %v", err)
	}
	if err := h.queue.drain(ctx, h.svc, h.clock, 10); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if n := len(h.deliv.calls); n != 1 {
		t.Fatalf("FINDING F-3: the deleted endpoint received %d POSTs after Unsubscribe (want 0 more; total %d, want 1)", n-1, n)
	}
}
