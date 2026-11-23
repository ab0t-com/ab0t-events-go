package events

// Use-case scenario tests, 2026-11-22: each test is one job a service or its customer does with the library.
//
// Job: "a customer rotates a leaked secret, or deletes an endpoint, while a retry is already scheduled". On the
// DURABLE path (id-only JobRef → ProcessRef, e.g. sqsqueue) every attempt reloads the subscription, so rotation
// and deletion take effect on the very next attempt. These lock that behaviour in.
// (Since v0.2.1 the in-process path does the same for retries — see usecase_rotation_finding_20261122_test.go.)

import (
	"context"
	"strings"
	"testing"
)

func TestUseCase_DurablePath_RetryAfterRotateSecretIsSignedWithTheNewSecret(t *testing.T) {
	ctx := context.Background()
	h := newHarness(fastPolicy)
	sub, err := h.svc.Subscribe(ctx, NewSubscription{OrgID: "org_a", EventTypes: []string{"*"}, URL: "https://a.example/hook", Secret: "old-placeholder-secret"})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	h.deliv.byURL["https://a.example/hook"] = 503
	if err := h.svc.Emit(ctx, mkEvent("evt_1", "contact.updated", "org_a", nil)); err != nil {
		t.Fatalf("emit: %v", err)
	}
	first := h.queue.pending()[0]
	h.queue.jobs = nil
	if err := h.svc.ProcessRef(ctx, first.Ref()); err != nil {
		t.Fatalf("attempt 1: %v", err)
	}
	retry := h.queue.pending()[0]
	h.queue.jobs = nil

	newSecret, err := h.svc.RotateSecret(ctx, "org_a", sub.ID)
	if err != nil {
		t.Fatalf("rotate: %v", err)
	}
	h.deliv.byURL["https://a.example/hook"] = 200
	if err := h.svc.ProcessRef(ctx, retry.Ref()); err != nil {
		t.Fatalf("attempt 2: %v", err)
	}
	// recordingSigner encodes the secret it was handed as "sig(<secret>,<len>)".
	if sig := h.deliv.lastHeaders()["X-Signature"]; !strings.HasPrefix(sig, "sig("+newSecret+",") {
		t.Fatalf("durable retry after rotation was not signed with the new secret (signature %q)", sig)
	}
	if e, _ := h.log.get(DeliveryID(sub.ID, "evt_1")); e.delivery.Status != StatusDelivered {
		t.Fatalf("status = %s, want delivered", e.delivery.Status)
	}
}

func TestUseCase_DurablePath_RetryAfterUnsubscribeIsDroppedWithoutAPost(t *testing.T) {
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
	first := h.queue.pending()[0]
	h.queue.jobs = nil
	if err := h.svc.ProcessRef(ctx, first.Ref()); err != nil {
		t.Fatalf("attempt 1: %v", err)
	}
	retry := h.queue.pending()[0]
	h.queue.jobs = nil

	if err := h.svc.Unsubscribe(ctx, "org_a", sub.ID); err != nil {
		t.Fatalf("unsubscribe: %v", err)
	}
	if err := h.svc.ProcessRef(ctx, retry.Ref()); err != nil {
		t.Fatalf("a retry for a deleted subscription must be dropped quietly (nil), got %v", err)
	}
	if n := len(h.deliv.calls); n != 1 {
		t.Fatalf("endpoint received %d POSTs, want 1 (none after the customer deleted it)", n)
	}
	if n := len(h.queue.pending()); n != 0 {
		t.Fatalf("%d further retries scheduled for a deleted subscription, want 0", n)
	}
}
