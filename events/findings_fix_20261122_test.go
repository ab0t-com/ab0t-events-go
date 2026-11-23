package events

// Regression tests for the fixes shipped in v0.2.1 (found by the 2026-11-22 use-case test sweep).
// The finding tests themselves (usecase_*_finding_20261122_test.go) prove each defect is gone; these guard the
// SAFETY properties the fixes had to keep, so a future "simplification" of a fix cannot silently trade one bug for
// a worse one:
//   - F-1: the stale-duplicate drop must never drop an attempt whose retry was NOT scheduled (that would lose the
//     delivery); a redelivery after a failed retry-enqueue is re-run, exactly as in v0.2.0.
//   - F-1 refactor: ProcessRef still re-asserts tenancy (it no longer goes through the public Process).
//   - F-3: a store outage on a retry's subscription re-read keeps the retry going on the job's copy (v0.2.0).
//   - F-5: only a CANCELLED attempt with no receiver answer goes unrecorded; a deadline, or a real HTTP answer that
//     arrived before the cancel, is still a recorded attempt.

import (
	"context"
	"errors"
	"testing"
	"time"
)

// flakyQueue fails the next Enqueue when failNext is set (an SQS SendMessage error after the attempt was recorded).
type flakyQueue struct {
	*fakeQueue
	failNext bool
}

var errSendFailed = errors.New("queue send failed")

func (q *flakyQueue) Enqueue(ctx context.Context, job Job) error {
	if q.failNext {
		q.failNext = false
		return errSendFailed
	}
	return q.fakeQueue.Enqueue(ctx, job)
}

func TestFindingsFix_DurableRedeliveryAfterAFailedRetryEnqueueIsReRunNotDropped(t *testing.T) {
	ctx := context.Background()
	h := newHarness(fastPolicy)
	q := &flakyQueue{fakeQueue: h.queue}
	slow := RetryPolicy{MaxAttempts: 4, BaseDelay: time.Minute, Multiplier: 2, RetryOn: DefaultRetryable} // armed path
	h.svc = NewService(Deps{
		Store: h.store, Queue: q, Deliverer: h.deliv, Signer: recordingSigner{},
		Log: h.log, Clock: h.clock, IDGen: h.idgen, Rand: fixedRand{v: 0.5}, Policy: slow,
	})
	sub, err := h.svc.Subscribe(ctx, NewSubscription{OrgID: "org_a", EventTypes: []string{"*"}, URL: "https://a.example/hook", Secret: "placeholder-secret"})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	h.deliv.byURL["https://a.example/hook"] = 503
	if err := h.svc.Emit(ctx, mkEvent("evt_1", "contact.updated", "org_a", nil)); err != nil {
		t.Fatalf("emit: %v", err)
	}
	ref := h.queue.pending()[0].Ref()
	h.queue.jobs = nil

	q.failNext = true // attempt 1 is POSTed and recorded, then scheduling attempt 2 fails
	if err := h.svc.ProcessRef(ctx, ref); !errors.Is(err, errSendFailed) {
		t.Fatalf("attempt 1 with a failed retry enqueue: want the queue error (so the message is redelivered), got %v", err)
	}
	if n := len(h.queue.pending()); n != 0 {
		t.Fatalf("precondition: %d retries scheduled, want 0 (the enqueue failed)", n)
	}

	// The queue redelivers attempt 1. Its retry was never scheduled, so dropping it as "stale" would lose the
	// delivery for good: it must be re-run and must schedule attempt 2.
	if err := h.svc.ProcessRef(ctx, ref); err != nil {
		t.Fatalf("redelivered attempt 1: %v", err)
	}
	if n := len(h.deliv.calls); n != 2 {
		t.Fatalf("endpoint received %d POSTs, want 2 (the redelivery of an unscheduled attempt is re-run)", n)
	}
	pend := h.queue.pending()
	if len(pend) != 1 || pend[0].AttemptN != 2 {
		t.Fatalf("after the re-run: pending=%d, want exactly the attempt-2 retry", len(pend))
	}
	if e, _ := h.log.get(DeliveryID(sub.ID, "evt_1")); e.delivery.Status != StatusFailed || e.delivery.Attempts != 1 {
		t.Fatalf("delivery = %+v, want failed with attempt count 1 once the retry is scheduled", e.delivery)
	}

	// And now that the retry IS scheduled, a further copy of attempt 1 is the stale duplicate F-1 drops.
	if err := h.svc.ProcessRef(ctx, ref); err != nil {
		t.Fatalf("stale copy: %v", err)
	}
	if n := len(h.deliv.calls); n != 2 || len(h.queue.pending()) != 1 {
		t.Fatalf("stale copy of attempt 1: posts=%d pending=%d, want 2 and 1 (dropped)", n, len(h.queue.pending()))
	}
}

func TestFindingsFix_ProcessRefStillRejectsACrossOrgSnapshot(t *testing.T) {
	ctx := context.Background()
	h := newHarness(fastPolicy)
	h.store.add(Subscription{ID: "s1", OrgID: "orgA", EventTypes: []string{"*"}, URL: "https://a.example/hook", Secret: "k", Active: true})
	id := DeliveryID("s1", "e1")
	_ = h.log.Record(ctx, DeliveryRecord{
		Delivery: Delivery{ID: id, OrgID: "orgA", SubscriptionID: "s1", EventID: "e1", Status: StatusPending},
		Event:    EventSnapshot{ID: "e1", Type: "t", OrgID: "orgB", Payload: []byte(`{}`)}, // a poisoned snapshot
	})
	err := h.svc.ProcessRef(ctx, JobRef{OrgID: "orgA", DeliveryID: id, SubscriptionID: "s1", EventID: "e1", AttemptN: 1})
	if !errors.Is(err, ErrTenancyViolation) {
		t.Fatalf("want ErrTenancyViolation for an event of another org, got %v", err)
	}
	if n := len(h.deliv.calls); n != 0 {
		t.Fatalf("POSTed %d times across orgs, want 0", n)
	}
}

// getErrStore fails Get (a store outage) but otherwise behaves like the fake store.
type getErrStore struct {
	*fakeStore
	getErr error
}

func (s getErrStore) Get(ctx context.Context, orgID, subID string) (Subscription, error) {
	if s.getErr != nil {
		return Subscription{}, s.getErr
	}
	return s.fakeStore.Get(ctx, orgID, subID)
}

func TestFindingsFix_RetryWhoseSubscriptionCannotBeReReadUsesTheJobCopy(t *testing.T) {
	ctx := context.Background()
	h := newHarness(fastPolicy)
	h.svc = NewService(Deps{
		Store: getErrStore{fakeStore: h.store, getErr: errors.New("store down")}, Queue: h.queue, Deliverer: h.deliv,
		Signer: recordingSigner{}, Log: h.log, Clock: h.clock, IDGen: h.idgen, Rand: fixedRand{v: 0.5}, Policy: fastPolicy,
	})
	h.store.add(Subscription{ID: "s1", OrgID: "orgA", EventTypes: []string{"*"}, URL: "https://a.example/hook", Secret: "k", Active: true})
	retry := Job{
		Delivery:     Delivery{ID: DeliveryID("s1", "e1"), OrgID: "orgA", SubscriptionID: "s1", EventID: "e1", Status: StatusFailed, Attempts: 1},
		Event:        mkEvent("e1", "t", "orgA", nil),
		Subscription: Subscription{ID: "s1", OrgID: "orgA", URL: "https://a.example/hook", Secret: "k", Active: true},
		AttemptN:     2,
	}
	if err := h.svc.Process(ctx, retry); err != nil {
		t.Fatalf("process: %v", err)
	}
	if n := len(h.deliv.calls); n != 1 {
		t.Fatalf("a retry during a store outage POSTed %d times, want 1 (kept on the job's copy, as in v0.2.0)", n)
	}
}

// observedCount returns an Observer that counts attempts.
func observedCount(n *int) Observer {
	return ObserverFunc(func(context.Context, Delivery, Attempt) { *n++ })
}

func TestFindingsFix_OnlyACancelledUnansweredAttemptGoesUnrecorded(t *testing.T) {
	cases := []struct {
		name       string
		code       int // scripted response; 0 = transport error
		ctx        func() (context.Context, context.CancelFunc)
		wantRecord bool
	}{
		{"cancelled, no answer (shutdown) — not a verdict", 0, func() (context.Context, context.CancelFunc) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			return ctx, cancel
		}, false},
		{"deadline exceeded, no answer — a real failed attempt", 0, func() (context.Context, context.CancelFunc) {
			return context.WithDeadline(context.Background(), time.Unix(1, 0))
		}, true},
		{"cancelled after the receiver answered 503 — the answer is a verdict", 503, func() (context.Context, context.CancelFunc) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			return ctx, cancel
		}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(fastPolicy)
			observed := 0
			h.svc = NewService(Deps{
				Store: h.store, Queue: h.queue, Deliverer: h.deliv, Signer: recordingSigner{}, Log: h.log,
				Clock: h.clock, IDGen: h.idgen, Rand: fixedRand{v: 0.5}, Policy: fastPolicy, Observer: observedCount(&observed),
			})
			h.store.add(Subscription{ID: "s1", OrgID: "orgA", EventTypes: []string{"*"}, URL: "https://a.example/hook", Secret: "k", Active: true})
			h.deliv.byURL["https://a.example/hook"] = tc.code
			if err := h.svc.Emit(context.Background(), mkEvent("e1", "t", "orgA", nil)); err != nil {
				t.Fatalf("emit: %v", err)
			}
			job := h.queue.pending()[0]
			h.queue.jobs = nil
			ctx, cancel := tc.ctx()
			defer cancel()
			err := h.svc.Process(ctx, job)
			e, _ := h.log.get(job.Delivery.ID)
			if !tc.wantRecord {
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("want context.Canceled returned, got %v", err)
				}
				if len(e.attempts) != 0 || observed != 0 || len(h.queue.pending()) != 0 || e.delivery.Status != StatusPending {
					t.Fatalf("cancelled attempt was recorded: attempts=%d observed=%d retries=%d status=%s, want 0/0/0/pending",
						len(e.attempts), observed, len(h.queue.pending()), e.delivery.Status)
				}
				return
			}
			if err != nil {
				t.Fatalf("process: %v", err)
			}
			if len(e.attempts) != 1 || observed != 1 || e.delivery.Status != StatusFailed || len(h.queue.pending()) != 1 {
				t.Fatalf("attempt not recorded as a failure: attempts=%d observed=%d status=%s retries=%d, want 1/1/failed/1",
					len(e.attempts), observed, e.delivery.Status, len(h.queue.pending()))
			}
		})
	}
}
