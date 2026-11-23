package autodisable_test

// Use-case scenario tests, 2026-11-22: each test is one job a service or its customer does with the library.
//
// Job: "a customer decommissioned their old CRM but never deleted the webhook. Stop sending to it after a few
// dead-letters, tell the org once, and do not disturb their other endpoint". Wired the way CONSUMING.md shows
// (breaker as the Service Observer, sharing the subscription store), driven synchronously through the real Service.

import (
	"context"
	"sync"
	"testing"

	"github.com/ab0t-com/ab0t-events-go/adapters/autodisable"
	"github.com/ab0t-com/ab0t-events-go/adapters/hmacsign"
	"github.com/ab0t-com/ab0t-events-go/adapters/memory"
	"github.com/ab0t-com/ab0t-events-go/adapters/std"
	"github.com/ab0t-com/ab0t-events-go/events"
)

type ucQueue struct {
	mu   sync.Mutex
	jobs []events.Job
}

func (q *ucQueue) Enqueue(_ context.Context, j events.Job) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.jobs = append(q.jobs, j)
	return nil
}

// drain processes every queued job, including retries, ignoring NotBefore (time is irrelevant to the breaker).
func (q *ucQueue) drain(t *testing.T, svc *events.Service) {
	t.Helper()
	for i := 0; i < 100; i++ {
		q.mu.Lock()
		if len(q.jobs) == 0 {
			q.mu.Unlock()
			return
		}
		j := q.jobs[0]
		q.jobs = q.jobs[1:]
		q.mu.Unlock()
		if err := svc.Process(context.Background(), j); err != nil {
			t.Fatalf("process: %v", err)
		}
	}
	t.Fatal("queue did not drain")
}

// ucEndpoints answers per URL (200 unless set) and counts POSTs per URL.
type ucEndpoints struct {
	mu    sync.Mutex
	code  map[string]int
	posts map[string]int
}

func (e *ucEndpoints) Post(_ context.Context, req events.DeliveryRequest) (events.DeliveryResponse, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.posts[req.URL]++
	c, ok := e.code[req.URL]
	if !ok {
		c = 200
	}
	return events.DeliveryResponse{StatusCode: c}, nil
}

func (e *ucEndpoints) set(url string, code int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.code[url] = code
}

func (e *ucEndpoints) count(url string) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.posts[url]
}

type ucRig struct {
	svc      *events.Service
	store    *memory.Store
	q        *ucQueue
	ep       *ucEndpoints
	mu       sync.Mutex
	disabled []string
}

func (r *ucRig) disabledCalls() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.disabled...)
}

func newUCRig(threshold int) *ucRig {
	r := &ucRig{store: memory.NewStore(), q: &ucQueue{}, ep: &ucEndpoints{code: map[string]int{}, posts: map[string]int{}}}
	breaker := autodisable.New(r.store, autodisable.Config{Threshold: threshold, OnDisable: func(_ context.Context, orgID, subID string) {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.disabled = append(r.disabled, orgID+"/"+subID)
	}})
	r.svc = events.NewService(events.Deps{
		Store: r.store, Queue: r.q, Deliverer: r.ep, Signer: hmacsign.New(), Log: memory.NewLog(),
		Clock: std.Clock{}, IDGen: std.IDGen{}, Rand: std.Rand{}, Policy: events.DefaultRetryPolicy(),
		Observer: events.MultiObserver(breaker),
	})
	return r
}

func (r *ucRig) subscribe(t *testing.T, org, url string) events.Subscription {
	t.Helper()
	sub, err := r.svc.Subscribe(context.Background(), events.NewSubscription{OrgID: org, EventTypes: []string{"*"}, URL: url, Secret: "test-placeholder-secret"})
	if err != nil {
		t.Fatalf("subscribe %s: %v", url, err)
	}
	return sub
}

func (r *ucRig) emitAndDrain(t *testing.T, org string, n int) {
	t.Helper()
	if err := r.svc.EmitNew(context.Background(), "contact.updated", org, map[string]any{"n": n}); err != nil {
		t.Fatalf("emit: %v", err)
	}
	r.q.drain(t, r.svc)
}

func (r *ucRig) active(t *testing.T, org, subID string) bool {
	t.Helper()
	sub, err := r.svc.GetSubscription(context.Background(), org, subID)
	if err != nil {
		t.Fatalf("get subscription: %v", err)
	}
	return sub.Active
}

func TestUseCase_AutoDisable_GoneEndpointIsPausedAndTheHealthySiblingKeepsReceiving(t *testing.T) {
	const org = "org_acme"
	const oldCRM, newCRM = "https://old-crm.example/hooks", "https://new-crm.example/hooks"
	r := newUCRig(3)
	broken := r.subscribe(t, org, oldCRM)
	healthy := r.subscribe(t, org, newCRM)
	r.ep.set(oldCRM, 410) // Gone: permanent, so each delivery dead-letters after one POST (no hammering)

	for i := 1; i <= 2; i++ {
		r.emitAndDrain(t, org, i)
		if !r.active(t, org, broken.ID) {
			t.Fatalf("paused after %d dead-letters, threshold is 3", i)
		}
	}
	r.emitAndDrain(t, org, 3)
	if r.active(t, org, broken.ID) {
		t.Fatal("not paused after 3 consecutive dead-letters")
	}
	if got := r.disabledCalls(); len(got) != 1 || got[0] != org+"/"+broken.ID {
		t.Fatalf("OnDisable calls = %v, want exactly one for %s", got, broken.ID)
	}
	if !r.active(t, org, healthy.ID) {
		t.Fatal("the healthy sibling endpoint was paused")
	}
	if r.ep.count(oldCRM) != 3 || r.ep.count(newCRM) != 3 {
		t.Fatalf("posts old=%d new=%d, want 3 and 3", r.ep.count(oldCRM), r.ep.count(newCRM))
	}

	// New events no longer go to the paused endpoint; the sibling still gets them.
	r.emitAndDrain(t, org, 4)
	if r.ep.count(oldCRM) != 3 || r.ep.count(newCRM) != 4 {
		t.Fatalf("after pause: posts old=%d new=%d, want 3 and 4", r.ep.count(oldCRM), r.ep.count(newCRM))
	}
	page, _, err := r.svc.ListDeliveries(context.Background(), org, events.DeliveryFilter{SubscriptionID: broken.ID})
	if err != nil || len(page) != 3 {
		t.Fatalf("paused endpoint has %d deliveries (err %v), want 3 (no new delivery records)", len(page), err)
	}
	if got := r.disabledCalls(); len(got) != 1 {
		t.Fatalf("OnDisable fired %d times, want once (on the transition only)", len(got))
	}

	// The org fixes the endpoint and uses Ping to check it: Ping bypasses the pause, so it is delivered.
	r.ep.set(oldCRM, 200)
	if _, err := r.svc.Ping(context.Background(), org, broken.ID); err != nil {
		t.Fatalf("ping: %v", err)
	}
	r.q.drain(t, r.svc)
	if r.ep.count(oldCRM) != 4 {
		t.Fatalf("ping to the paused endpoint: posts=%d, want 4", r.ep.count(oldCRM))
	}
}

func TestUseCase_AutoDisable_OnlyConsecutiveDeadLettersCount(t *testing.T) {
	const org = "org_flaky"
	const url = "https://flaky.example/hooks"
	r := newUCRig(3)
	sub := r.subscribe(t, org, url)

	// dead, dead, recovered once, dead, dead: never 3 in a row → stays active.
	for i, code := range []int{410, 410, 200, 410, 410} {
		r.ep.set(url, code)
		r.emitAndDrain(t, org, i)
	}
	if !r.active(t, org, sub.ID) || len(r.disabledCalls()) != 0 {
		t.Fatal("a success between dead-letters must reset the streak")
	}
	r.ep.set(url, 410)
	r.emitAndDrain(t, org, 99)
	if r.active(t, org, sub.ID) || len(r.disabledCalls()) != 1 {
		t.Fatal("the third consecutive dead-letter must pause the endpoint and notify once")
	}
}
