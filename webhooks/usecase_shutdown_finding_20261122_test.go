package webhooks_test

// REGRESSION TESTS (found 2026-11-22 by the use-case test sweep; fixed in v0.2.1). Both fail on v0.2.0 (the second
// one only under -race).
//
// F-5 "Close cancels in-flight POSTs and records them as real failures". In v0.2.0 System.Close was documented as
// "stops the worker pool, blocking until in-flight deliveries drain", but inproc's stop cancels the context every
// worker passes to Process. The in-flight POST was aborted, Process recorded it as a failed attempt, and on the
// FINAL attempt dead-lettered a delivery the customer never answered — the Observer saw StatusDead, so autodisable
// counted our own deploy against the customer's endpoint. Same class as sqsqueue F-2 and the phantom-job fix in
// 0.2.0 (work dispatched/recorded across a stop): a shutdown must not turn an in-flight POST into a recorded
// failed attempt.
// Possible fixes: run Process on a context detached from the stop signal so Close really drains; or have Process
// not record an outcome when ctx was cancelled (the v0.2.1 fix). This test accepts either: it only requires that
// the delivery is not dead-lettered by the shutdown.
//
// F-6 "concurrent Close is a data race". In v0.2.0 Close read and wrote System.closed without synchronisation; a
// `defer sys.Close()` racing a signal handler's Close was reported by the race detector.
// Fix: guard with sync.Once (behaviour otherwise unchanged).

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ab0t-com/ab0t-events-go/adapters/memory"
	"github.com/ab0t-com/ab0t-events-go/events"
	"github.com/ab0t-com/ab0t-events-go/webhooks"
)

// ucShutdownDeliverer fails attempts 1 and 2 (503). Attempt 3 — the last one — is in flight when the operator deploys:
// it triggers sys.Close() and then behaves like net/http: it returns ctx.Err() if its request context is cancelled,
// otherwise the customer answers 200 (reachable only if Close lets the in-flight POST finish).
type ucShutdownDeliverer struct {
	sys    atomic.Pointer[webhooks.System]
	calls  atomic.Int32
	closed chan struct{}
}

func (d *ucShutdownDeliverer) Post(ctx context.Context, _ events.DeliveryRequest) (events.DeliveryResponse, error) {
	if d.calls.Add(1) < 3 {
		return events.DeliveryResponse{StatusCode: 503}, nil
	}
	go func() {
		d.sys.Load().Close() // SIGTERM handler
		close(d.closed)
	}()
	select {
	case <-ctx.Done():
		return events.DeliveryResponse{}, ctx.Err()
	case <-time.After(2 * time.Second):
		return events.DeliveryResponse{StatusCode: 200}, nil
	}
}

func TestUseCaseFinding_CloseDuringTheFinalAttemptDeadLettersADeliveryTheCustomerNeverAnswered(t *testing.T) {
	ctx := context.Background()
	d := &ucShutdownDeliverer{closed: make(chan struct{})}
	var mu sync.Mutex
	var seen []events.DeliveryStatus
	log := memory.NewLog()
	sys, err := webhooks.New(ctx, webhooks.Options{
		Store: memory.NewStore(), Log: log, Deliverer: d, Workers: 1,
		Policy: events.RetryPolicy{MaxAttempts: 3, BaseDelay: time.Millisecond, Multiplier: 1, RetryOn: events.DefaultRetryable},
		Observer: events.ObserverFunc(func(_ context.Context, del events.Delivery, _ events.Attempt) {
			mu.Lock()
			defer mu.Unlock()
			seen = append(seen, del.Status)
		}),
	})
	if err != nil {
		t.Fatalf("webhooks.New: %v", err)
	}
	d.sys.Store(sys)
	sub, err := sys.Subscribe(ctx, events.NewSubscription{OrgID: "org_pay", EventTypes: []string{"payment.*"}, URL: "https://merchant.example/hooks", Secret: "test-placeholder-secret"})
	if err != nil {
		sys.Close()
		t.Fatalf("subscribe: %v", err)
	}
	ev := events.Event{ID: "evt_pay_1", Type: "payment.succeeded", OrgID: "org_pay", OccurredAt: time.Now().UTC(), Payload: []byte(`{"paymentId":"pay_1"}`)}
	if err := sys.Emit(ctx, ev); err != nil {
		sys.Close()
		t.Fatalf("emit: %v", err)
	}
	select {
	case <-d.closed:
	case <-time.After(10 * time.Second):
		t.Fatal("the final attempt never started (Close was never triggered)")
	}

	del, _, atts, err := log.Get(ctx, "org_pay", events.DeliveryID(sub.ID, ev.ID))
	if err != nil {
		t.Fatalf("log get: %v", err)
	}
	if del.Status == events.StatusDead {
		t.Errorf("FINDING F-5: Close dead-lettered the delivery while its final attempt was in flight (attempts=%d, lastError=%q); the customer never answered it", len(atts), del.LastError)
	}
	mu.Lock()
	defer mu.Unlock()
	for _, s := range seen {
		if s == events.StatusDead {
			t.Errorf("FINDING F-5: the Observer saw a dead-letter caused by our own shutdown — autodisable would count it against the customer's endpoint (observed %v)", seen)
			break
		}
	}
}

// TestUseCaseFinding_ConcurrentCloseFromDeferAndSignalHandler is only meaningful under -race.
func TestUseCaseFinding_ConcurrentCloseFromDeferAndSignalHandler(t *testing.T) {
	sys, err := webhooks.New(context.Background(), webhooks.Options{Store: memory.NewStore(), Log: memory.NewLog()})
	if err != nil {
		t.Fatalf("webhooks.New: %v", err)
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ { // main's `defer sys.Close()` and the SIGTERM handler's sys.Close()
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			sys.Close()
		}()
	}
	close(start)
	wg.Wait()
}
