package events

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
)

// newTransformHarness wires a Service to the shared fakes plus a Transformer.
func newTransformHarness(tr Transformer) *harness {
	h := &harness{
		store: newStore(), queue: newQueue(), deliv: newDeliverer(),
		log: newLog(), clock: newClock(), idgen: &seqIDGen{},
	}
	h.svc = NewService(Deps{
		Store: h.store, Queue: h.queue, Deliverer: h.deliv, Signer: recordingSigner{},
		Log: h.log, Clock: h.clock, IDGen: h.idgen, Rand: fixedRand{v: 0.5},
		Policy: DefaultRetryPolicy(), Transformer: tr,
	})
	return h
}

// TestProcess_TransformerReshapesBodyAndSignsTransformed is the behavioral proof:
// the per-endpoint transform rewrites the outbound body, the subscriber receives
// the transformed body, and the signature is computed over the TRANSFORMED body
// (recordingSigner encodes the body length into X-Signature).
func TestProcess_TransformerReshapesBodyAndSignsTransformed(t *testing.T) {
	tr := TransformerFunc(func(_ context.Context, sub Subscription, _ Event, body []byte) ([]byte, error) {
		// Per-endpoint: only wrap for subscription "wrap".
		if sub.ID == "wrap" {
			return []byte(`{"envelope":` + string(body) + `}`), nil
		}
		return body, nil
	})
	h := newTransformHarness(tr)
	h.store.add(Subscription{ID: "wrap", OrgID: "org1", EventTypes: []string{"*"}, URL: "https://x", Secret: "sek", Active: true})

	ev := mkEvent("e1", "drive.file.created", "org1", map[string]any{"n": 1})
	if err := h.svc.Emit(context.Background(), ev); err != nil {
		t.Fatal(err)
	}
	if err := h.queue.drain(context.Background(), h.svc, h.clock, 5); err != nil {
		t.Fatal(err)
	}

	if len(h.deliv.calls) != 1 {
		t.Fatalf("want 1 delivery, got %d", len(h.deliv.calls))
	}
	sent := h.deliv.calls[0]
	wantBody := `{"envelope":{"n":1}}`
	if string(sent.Body) != wantBody {
		t.Fatalf("body not transformed: got %q want %q", sent.Body, wantBody)
	}
	// recordingSigner signs "sig(<secret>,<len(body)>)"; the length must be the
	// TRANSFORMED body's length, proving signing happened after the transform.
	wantSig := "sig(sek," + strconv.Itoa(len(wantBody)) + ")"
	if sent.Headers[HeaderSignature] != wantSig {
		t.Fatalf("signature not over transformed body: got %q want %q", sent.Headers[HeaderSignature], wantSig)
	}
}

// TestProcess_TransformerErrorFailsAttemptWithoutSending proves a transform error
// aborts the attempt (no POST) and is recorded as a failed, retryable delivery.
func TestProcess_TransformerErrorFailsAttemptWithoutSending(t *testing.T) {
	tr := TransformerFunc(func(_ context.Context, _ Subscription, _ Event, _ []byte) ([]byte, error) {
		return nil, errors.New("boom")
	})
	h := newTransformHarness(tr)
	h.store.add(Subscription{ID: "s1", OrgID: "org1", EventTypes: []string{"*"}, URL: "https://x", Secret: "sek", Active: true})

	ev := mkEvent("e1", "drive.file.created", "org1", map[string]any{"n": 1})
	if err := h.svc.Emit(context.Background(), ev); err != nil {
		t.Fatal(err)
	}
	// Process exactly one attempt.
	job := h.queue.pending()[0]
	if err := h.svc.Process(context.Background(), job); err != nil {
		t.Fatalf("process returned error: %v", err)
	}

	// No POST happened.
	if len(h.deliv.calls) != 0 {
		t.Fatalf("transform error must abort before POST, got %d calls", len(h.deliv.calls))
	}
	// The attempt was recorded as failed, with the transform error, and a retry
	// was scheduled (status failed, not dead — attempts remain).
	entry, ok := h.log.get(DeliveryID("s1", "e1"))
	if !ok {
		t.Fatal("no delivery recorded")
	}
	if entry.delivery.Status != StatusFailed {
		t.Fatalf("want StatusFailed, got %q", entry.delivery.Status)
	}
	if !strings.Contains(entry.delivery.LastError, "transform failed: boom") {
		t.Fatalf("error not recorded: %q", entry.delivery.LastError)
	}
	if len(entry.attempts) != 1 || entry.attempts[0].Code != 0 {
		t.Fatalf("expected one attempt with code 0, got %+v", entry.attempts)
	}
}

// TestProcess_NilTransformerPassesThrough confirms the default (no transformer)
// delivers the payload verbatim.
func TestProcess_NilTransformerPassesThrough(t *testing.T) {
	h := newTransformHarness(nil)
	h.store.add(Subscription{ID: "s1", OrgID: "org1", EventTypes: []string{"*"}, URL: "https://x", Secret: "sek", Active: true})
	ev := mkEvent("e1", "drive.file.created", "org1", map[string]any{"n": 1})
	_ = h.svc.Emit(context.Background(), ev)
	_ = h.queue.drain(context.Background(), h.svc, h.clock, 5)
	if got := string(h.deliv.calls[0].Body); got != `{"n":1}` {
		t.Fatalf("payload not delivered verbatim: %q", got)
	}
}
