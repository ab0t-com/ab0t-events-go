package events

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestRetryAfter_AbsoluteCeiling is the proving test for audit Finding 6: a
// hostile Retry-After is clamped to AbsoluteMaxRetryDelay even when the policy's
// MaxDelay is 0 ("uncapped").
func TestRetryAfter_AbsoluteCeiling(t *testing.T) {
	policy := DefaultRetryPolicy()
	policy.MaxDelay = 0 // uncapped by MaxDelay — the absolute ceiling must still apply
	h := newHarness(policy)
	h.store.add(Subscription{ID: "s1", OrgID: "org1", EventTypes: []string{"*"}, URL: "https://slow", Secret: "k", Active: true})
	// Subscriber returns 503 with an outrageous Retry-After (100 days).
	h.deliv.byURL["https://slow"] = 503
	h.deliv.retryAfter["https://slow"] = 100 * 24 * time.Hour

	if err := h.svc.Emit(context.Background(), mkEvent("e1", "t", "org1", nil)); err != nil {
		t.Fatal(err)
	}
	// Process one attempt; it should schedule a retry clamped to the ceiling.
	job := h.queue.pending()[0]
	before := h.clock.Now()
	if err := h.svc.Process(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	// The re-enqueued job's NotBefore must be no further out than the ceiling.
	var retry *Job
	for _, j := range h.queue.posted {
		if j.AttemptN == 2 {
			jj := j
			retry = &jj
		}
	}
	if retry == nil {
		t.Fatal("no retry was scheduled")
	}
	gotDelay := retry.NotBefore.Sub(before)
	if gotDelay > AbsoluteMaxRetryDelay {
		t.Fatalf("retry delay %v exceeds absolute ceiling %v", gotDelay, AbsoluteMaxRetryDelay)
	}
	if gotDelay != AbsoluteMaxRetryDelay {
		t.Fatalf("expected the hostile Retry-After to be clamped exactly to %v, got %v", AbsoluteMaxRetryDelay, gotDelay)
	}
}

// TestEmit_MaxPayloadBytes is the proving test for audit Finding 9: an event whose
// payload exceeds Deps.MaxPayloadBytes is rejected up front, before any fan-out.
func TestEmit_MaxPayloadBytes(t *testing.T) {
	h := &harness{
		store: newStore(), queue: newQueue(), deliv: newDeliverer(),
		log: newLog(), clock: newClock(), idgen: &seqIDGen{},
	}
	h.svc = NewService(Deps{
		Store: h.store, Queue: h.queue, Deliverer: h.deliv, Signer: recordingSigner{},
		Log: h.log, Clock: h.clock, IDGen: h.idgen, Rand: fixedRand{v: 0.5},
		MaxPayloadBytes: 16,
	})
	h.store.add(Subscription{ID: "s1", OrgID: "org1", EventTypes: []string{"*"}, URL: "https://x", Active: true})

	// Oversized payload → rejected, nothing enqueued or logged.
	big := Event{ID: "e1", Type: "t", OrgID: "org1", Payload: []byte(`{"data":"way too long to fit"}`)}
	if err := h.svc.Emit(context.Background(), big); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("want ErrPayloadTooLarge, got %v", err)
	}
	if len(h.queue.pending()) != 0 {
		t.Fatal("oversized emit must not enqueue")
	}

	// A payload within the cap flows normally.
	ok := Event{ID: "e2", Type: "t", OrgID: "org1", Payload: []byte(`{"n":1}`)}
	if err := h.svc.Emit(context.Background(), ok); err != nil {
		t.Fatalf("in-bounds payload should emit: %v", err)
	}
	if len(h.queue.pending()) != 1 {
		t.Fatalf("expected 1 job, got %d", len(h.queue.pending()))
	}
}

// TestReplay_ResetsLogStatusForDurableQueues is the proving test for audit
// Finding 7: Replay persists a pending status to the log, so a rehydrated
// (durable-queue) job is non-terminal and ProcessRef re-delivers it instead of
// dropping it as a duplicate.
func TestReplay_ResetsLogStatusForDurableQueues(t *testing.T) {
	h := newHarness(DefaultRetryPolicy())
	h.store.add(Subscription{ID: "s1", OrgID: "org1", EventTypes: []string{"*"}, URL: "https://x", Secret: "k", Active: true})

	// Emit + deliver so the delivery reaches a terminal (delivered) state.
	if err := h.svc.Emit(context.Background(), mkEvent("e1", "t", "org1", nil)); err != nil {
		t.Fatal(err)
	}
	if err := h.queue.drain(context.Background(), h.svc, h.clock, 5); err != nil {
		t.Fatal(err)
	}
	id := DeliveryID("s1", "e1")
	if e, _ := h.log.get(id); e.delivery.Status != StatusDelivered {
		t.Fatalf("precondition: want delivered, got %q", e.delivery.Status)
	}
	postsBefore := len(h.deliv.calls)

	// Replay, then simulate the DURABLE path: rehydrate from a JobRef (which reads
	// the log status) and dispatch via ProcessRef.
	if err := h.svc.Replay(context.Background(), "org1", id); err != nil {
		t.Fatalf("replay: %v", err)
	}
	// Without the fix the log status would still be "delivered" and ProcessRef
	// would drop this as terminal. With the fix it is "pending".
	if e, _ := h.log.get(id); e.delivery.Status != StatusPending {
		t.Fatalf("replay must reset log status to pending, got %q", e.delivery.Status)
	}
	ref := JobRef{OrgID: "org1", DeliveryID: id, SubscriptionID: "s1", EventID: "e1", AttemptN: 1}
	if err := h.svc.ProcessRef(context.Background(), ref); err != nil {
		t.Fatalf("processRef: %v", err)
	}
	if len(h.deliv.calls) != postsBefore+1 {
		t.Fatalf("replay through the durable path did not re-deliver (posts before=%d after=%d)", postsBefore, len(h.deliv.calls))
	}
}
