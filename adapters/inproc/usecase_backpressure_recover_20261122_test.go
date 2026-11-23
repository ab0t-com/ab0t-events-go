package inproc_test

// Use-case scenario tests, 2026-11-22: each test is one job a service or its customer does with the library.
//
// Job: "a single-process service on the in-memory queue must not lose webhooks when it is overloaded or restarted".
// The queue is volatile, the delivery log is durable: a full queue pushes back (the delivery stays pending in the
// log), and Recover re-enqueues pending/failed deliveries after the backlog drains or the process restarts. Real
// HTTP to an httptest receiver that records each delivery id and attempt number.

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/ab0t-com/ab0t-events-go/adapters/hmacsign"
	"github.com/ab0t-com/ab0t-events-go/adapters/httpdeliver"
	"github.com/ab0t-com/ab0t-events-go/adapters/inproc"
	"github.com/ab0t-com/ab0t-events-go/adapters/memory"
	"github.com/ab0t-com/ab0t-events-go/adapters/std"
	"github.com/ab0t-com/ab0t-events-go/events"
)

// ucReceiver records the X-Attempt of every request per X-Delivery-Id; the first failFirst requests get a 503.
type ucReceiver struct {
	mu        sync.Mutex
	failFirst int
	byID      map[string][]string
}

func newUCReceiver(failFirst int) *ucReceiver {
	return &ucReceiver{failFirst: failFirst, byID: map[string][]string{}}
}

func (rc *ucReceiver) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	id := r.Header.Get(events.HeaderDelivery)
	rc.byID[id] = append(rc.byID[id], r.Header.Get(events.HeaderAttempt))
	if rc.failFirst > 0 {
		rc.failFirst--
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (rc *ucReceiver) attempts(id string) []string {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return append([]string(nil), rc.byID[id]...)
}

func newUCService(store events.SubscriptionStore, log events.DeliveryLog, q events.DeliveryQueue, policy events.RetryPolicy) *events.Service {
	return events.NewService(events.Deps{
		Store: store, Queue: q, Log: log, Signer: hmacsign.New(),
		Deliverer: httpdeliver.New(httpdeliver.Config{Timeout: 2 * time.Second}), // httptest is loopback: guard off
		Clock:     std.Clock{}, IDGen: std.IDGen{}, Rand: std.Rand{}, Policy: policy,
	})
}

func ucWait(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func ucCount(t *testing.T, svc *events.Service, org string, status events.DeliveryStatus) int {
	t.Helper()
	page, _, err := svc.ListDeliveries(context.Background(), org, events.DeliveryFilter{Status: status})
	if err != nil {
		t.Fatalf("list deliveries: %v", err)
	}
	return len(page)
}

func singleAttempt(got []string) bool { return len(got) == 1 && got[0] == "1" }

func TestUseCase_TrafficSpike_FullQueuePushesBackThenRecoverDeliversTheRest(t *testing.T) {
	ctx := context.Background()
	rc := newUCReceiver(0)
	srv := httptest.NewServer(rc)
	defer srv.Close()

	const org = "org_spike"
	store, log := memory.NewStore(), memory.NewLog()
	q := inproc.New(inproc.Config{Workers: 2, MaxDepth: 2})
	svc := newUCService(store, log, q, events.DefaultRetryPolicy())
	var subs []events.Subscription
	for _, path := range []string{"/crm", "/erp", "/warehouse"} {
		sub, err := svc.Subscribe(ctx, events.NewSubscription{OrgID: org, EventTypes: []string{"order.*"}, URL: srv.URL + path, Secret: "test-placeholder-secret"})
		if err != nil {
			t.Fatalf("subscribe: %v", err)
		}
		subs = append(subs, sub)
	}

	// The spike: workers not yet draining, room for 2 jobs, the event fans out to 3 endpoints.
	ev := events.Event{ID: "evt_spike_1", Type: "order.created", OrgID: org, OccurredAt: time.Now().UTC(), Payload: []byte(`{"orderId":"o_1"}`)}
	err := svc.Emit(ctx, ev)
	if !errors.Is(err, inproc.ErrQueueFull) {
		t.Fatalf("emit into a full queue: want ErrQueueFull surfaced to the caller, got %v", err)
	}
	if n := q.Len(); n != 2 {
		t.Fatalf("queue depth = %d, want 2 (MaxDepth)", n)
	}
	if n := ucCount(t, svc, org, events.StatusPending); n != 3 {
		t.Fatalf("%d deliveries pending in the log, want all 3 (the rejected one must not be lost)", n)
	}

	stop := q.Start(ctx, svc)
	defer stop()
	ucWait(t, "the two queued deliveries", func() bool { return ucCount(t, svc, org, events.StatusDelivered) == 2 })

	n, err := svc.Recover(ctx, org) // the backlog drained: re-enqueue what the full queue refused
	if err != nil || n != 1 {
		t.Fatalf("Recover = (%d, %v), want exactly the 1 refused delivery", n, err)
	}
	ucWait(t, "all three deliveries", func() bool { return ucCount(t, svc, org, events.StatusDelivered) == 3 })
	for _, sub := range subs {
		if got := rc.attempts(events.DeliveryID(sub.ID, ev.ID)); !singleAttempt(got) {
			t.Fatalf("endpoint %s saw attempts %v, want exactly [1]", sub.URL, got)
		}
	}
}

func TestUseCase_ProcessRestart_RecoverRedeliversARetryThatOnlyLivedInMemory(t *testing.T) {
	ctx := context.Background()
	rc := newUCReceiver(1) // the first POST gets a 503
	srv := httptest.NewServer(rc)
	defer srv.Close()

	const org = "org_restart"
	store, log := memory.NewStore(), memory.NewLog() // durable state shared by both "process lifetimes"
	policy := events.RetryPolicy{MaxAttempts: 5, BaseDelay: time.Hour, Multiplier: 2, RetryOn: events.DefaultRetryable}

	// Process #1: attempt 1 fails; the retry is parked in memory for an hour.
	qA := inproc.New(inproc.Config{Workers: 1})
	svcA := newUCService(store, log, qA, policy)
	sub, err := svcA.Subscribe(ctx, events.NewSubscription{OrgID: org, EventTypes: []string{"*"}, URL: srv.URL + "/hooks", Secret: "test-placeholder-secret"})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	stopA := qA.Start(ctx, svcA)
	ev := events.Event{ID: "evt_restart_1", Type: "invoice.paid", OrgID: org, OccurredAt: time.Now().UTC(), Payload: []byte(`{"invoiceId":"in_1"}`)}
	if err := svcA.Emit(ctx, ev); err != nil {
		stopA()
		t.Fatalf("emit: %v", err)
	}
	id := events.DeliveryID(sub.ID, ev.ID)
	ucWait(t, "attempt 1 to fail and its retry to be parked", func() bool {
		d, _, atts, err := log.Get(ctx, org, id)
		return err == nil && d.Status == events.StatusFailed && len(atts) == 1 && qA.Len() == 1
	})
	stopA() // the process is killed: the parked retry is gone with it

	// Process #2 starts on the same store + log and recovers.
	qB := inproc.New(inproc.Config{Workers: 1})
	svcB := newUCService(store, log, qB, policy)
	stopB := qB.Start(ctx, svcB)
	defer stopB()
	n, err := svcB.Recover(ctx, org)
	if err != nil || n != 1 {
		t.Fatalf("Recover = (%d, %v), want 1", n, err)
	}
	ucWait(t, "the recovered delivery", func() bool {
		d, _, atts, err := log.Get(ctx, org, id)
		return err == nil && d.Status == events.StatusDelivered && len(atts) == 2
	})
	if got := rc.attempts(id); len(got) != 2 || got[0] != "1" || got[1] != "2" {
		t.Fatalf("receiver saw attempts %v for %s, want [1 2] (same delivery id, attempt numbering continues)", got, id)
	}
}
