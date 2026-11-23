package events

// Use-case scenario tests, 2026-11-22: each test is one job a service or its customer does with the library.
//
// Job: "a customer endpoint is flaky (deploys, rate limits, timeouts): keep trying on a sane schedule, respect what
// the endpoint asks for, never let it park us forever, and stop at once when the answer is permanent". Deterministic:
// fake clock, fixed jitter, a scripted endpoint, the core's synchronous fake queue.

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type ucScriptedResp struct {
	code       int
	retryAfter time.Duration
	err        error
}

// ucSeqDeliverer answers call i with steps[i] (200 once the script runs out).
type ucSeqDeliverer struct {
	mu    sync.Mutex
	steps []ucScriptedResp
	calls int
}

func (d *ucSeqDeliverer) Post(_ context.Context, _ DeliveryRequest) (DeliveryResponse, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	s := ucScriptedResp{code: 200}
	if d.calls < len(d.steps) {
		s = d.steps[d.calls]
	}
	d.calls++
	if s.err != nil {
		return DeliveryResponse{}, s.err
	}
	return DeliveryResponse{StatusCode: s.code, RetryAfter: s.retryAfter, Latency: time.Millisecond}, nil
}

func (d *ucSeqDeliverer) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.calls
}

type ucRig struct {
	svc   *Service
	store *fakeStore
	queue *fakeQueue
	log   *fakeLog
	clock *fakeClock
}

func newUCRig(deliv Deliverer, policy RetryPolicy, obs Observer) *ucRig {
	r := &ucRig{store: newStore(), queue: newQueue(), log: newLog(), clock: newClock()}
	r.svc = NewService(Deps{
		Store: r.store, Queue: r.queue, Deliverer: deliv, Signer: recordingSigner{}, Log: r.log,
		Clock: r.clock, IDGen: &seqIDGen{}, Rand: fixedRand{v: 0.5}, Policy: policy, Observer: obs,
	})
	return r
}

// TestUseCase_Retry_FlappingEndpointScheduleHonoursRetryAfterAndMaxDelay: a shop's endpoint is mid-deploy. The exact
// gap before each retry is asserted: Retry-After wins only when it asks for MORE than the backoff, and a huge
// Retry-After is clamped to MaxDelay. JitterFrac is on, but the fixed jitter source (0.5) makes it a factor of 1.
func TestUseCase_Retry_FlappingEndpointScheduleHonoursRetryAfterAndMaxDelay(t *testing.T) {
	ctx := context.Background()
	policy := RetryPolicy{MaxAttempts: 6, BaseDelay: time.Second, MaxDelay: 2 * time.Minute, Multiplier: 2, JitterFrac: 0.2, RetryOn: DefaultRetryable}
	deliv := &ucSeqDeliverer{steps: []ucScriptedResp{
		{code: 503, retryAfter: 30 * time.Second}, // "deploying, come back in 30s": 30s > 1s backoff → 30s
		{code: 500}, // still warming up: plain backoff → 2s
		{code: 429, retryAfter: 500 * time.Millisecond}, // asks for LESS than the 4s backoff → backoff wins
		{err: context.DeadlineExceeded},                 // transport timeout → 8s
		{code: 429, retryAfter: 10 * time.Minute},       // asks for 10m → clamped to MaxDelay 2m
		{code: 200},
	}}
	var statuses []DeliveryStatus
	r := newUCRig(deliv, policy, ObserverFunc(func(_ context.Context, d Delivery, _ Attempt) { statuses = append(statuses, d.Status) }))
	r.store.add(Subscription{ID: "sub_shop", OrgID: "org_shop", EventTypes: []string{"order.*"}, URL: "https://shop.example/hooks", Secret: "k", Active: true})
	if err := r.svc.Emit(ctx, mkEvent("evt_o1", "order.created", "org_shop", map[string]string{"orderId": "o_1"})); err != nil {
		t.Fatalf("emit: %v", err)
	}

	wantGaps := []time.Duration{30 * time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 2 * time.Minute}
	for i, want := range wantGaps {
		if err := r.queue.drain(ctx, r.svc, r.clock, 1); err != nil {
			t.Fatalf("attempt %d: %v", i+1, err)
		}
		pend := r.queue.pending()
		if len(pend) != 1 {
			t.Fatalf("after attempt %d: %d jobs queued, want exactly the next retry", i+1, len(pend))
		}
		next := pend[0]
		if next.AttemptN != i+2 {
			t.Fatalf("after attempt %d: next AttemptN=%d, want %d", i+1, next.AttemptN, i+2)
		}
		if got := next.NotBefore.Sub(r.clock.Now()); got != want {
			t.Fatalf("gap after attempt %d = %v, want %v", i+1, got, want)
		}
	}
	if err := r.queue.drain(ctx, r.svc, r.clock, 1); err != nil {
		t.Fatalf("final attempt: %v", err)
	}
	if n := len(r.queue.pending()); n != 0 {
		t.Fatalf("%d jobs queued after a successful delivery, want 0", n)
	}

	e, ok := r.log.get(DeliveryID("sub_shop", "evt_o1"))
	if !ok {
		t.Fatal("delivery missing from the log")
	}
	if e.delivery.Status != StatusDelivered || e.delivery.Attempts != 6 || e.delivery.LastError != "" {
		t.Fatalf("final delivery = %+v, want delivered after 6 attempts with no LastError", e.delivery)
	}
	wantCodes := []int{503, 500, 429, 0, 429, 200}
	if len(e.attempts) != len(wantCodes) {
		t.Fatalf("logged %d attempts, want %d", len(e.attempts), len(wantCodes))
	}
	for i, a := range e.attempts {
		if a.N != i+1 || a.Code != wantCodes[i] {
			t.Fatalf("attempt %d logged as N=%d code=%d, want N=%d code=%d", i+1, a.N, a.Code, i+1, wantCodes[i])
		}
	}
	wantStatuses := []DeliveryStatus{StatusFailed, StatusFailed, StatusFailed, StatusFailed, StatusFailed, StatusDelivered}
	if len(statuses) != len(wantStatuses) {
		t.Fatalf("observer saw %v, want %v", statuses, wantStatuses)
	}
	for i := range wantStatuses {
		if statuses[i] != wantStatuses[i] {
			t.Fatalf("observer saw %v, want %v", statuses, wantStatuses)
		}
	}
}

// TestUseCase_Retry_PermanentAnswersDeadLetterAtOnceTransientOnesRetry: what each first-attempt answer means for a
// customer. A permanent answer (the endpoint was removed, rejects the schema, moved) is dead-lettered after ONE POST
// so we stop hammering it; a transient one is retried.
func TestUseCase_Retry_PermanentAnswersDeadLetterAtOnceTransientOnesRetry(t *testing.T) {
	cases := []struct {
		name  string
		code  int
		err   error
		retry bool
	}{
		{name: "400 bad request", code: 400},
		{name: "401 endpoint auth changed", code: 401},
		{name: "403 forbidden", code: 403},
		{name: "404 endpoint removed", code: 404},
		{name: "410 gone", code: 410},
		{name: "422 schema rejected", code: 422},
		{name: "301 moved (redirects are never followed)", code: 301},
		{name: "307 temporary redirect", code: 307},
		{name: "408 request timeout", code: 408, retry: true},
		{name: "429 rate limited", code: 429, retry: true},
		{name: "500 internal error", code: 500, retry: true},
		{name: "502 bad gateway", code: 502, retry: true},
		{name: "503 unavailable", code: 503, retry: true},
		{name: "504 gateway timeout", code: 504, retry: true},
		{name: "transport timeout", err: context.DeadlineExceeded, retry: true},
		{name: "connection refused", err: errors.New("dial tcp 203.0.113.9:443: connect: connection refused"), retry: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			deliv := &ucSeqDeliverer{steps: []ucScriptedResp{{code: tc.code, err: tc.err}}}
			r := newUCRig(deliv, DefaultRetryPolicy(), nil)
			r.store.add(Subscription{ID: "sub_1", OrgID: "org_1", EventTypes: []string{"*"}, URL: "https://customer.example/hook", Secret: "k", Active: true})
			if err := r.svc.Emit(ctx, mkEvent("evt_1", "thing.happened", "org_1", nil)); err != nil {
				t.Fatalf("emit: %v", err)
			}
			if err := r.queue.drain(ctx, r.svc, r.clock, 1); err != nil {
				t.Fatalf("process: %v", err)
			}
			e, _ := r.log.get(DeliveryID("sub_1", "evt_1"))
			if deliv.count() != 1 {
				t.Fatalf("POSTed %d times, want 1", deliv.count())
			}
			if tc.retry {
				if e.delivery.Status != StatusFailed || len(r.queue.pending()) != 1 {
					t.Fatalf("status=%s queued=%d, want failed with one retry scheduled", e.delivery.Status, len(r.queue.pending()))
				}
				return
			}
			if e.delivery.Status != StatusDead || len(r.queue.pending()) != 0 || e.delivery.LastError == "" {
				t.Fatalf("status=%s queued=%d lastError=%q, want dead immediately with a reason", e.delivery.Status, len(r.queue.pending()), e.delivery.LastError)
			}
		})
	}
}
