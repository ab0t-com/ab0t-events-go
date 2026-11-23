package webhooks_test

// Use-case scenario tests, 2026-11-22: each test is one job a service or its customer does with the library.
//
// Job: "a service owner copies the README quickstart (Store + Log only) and is production-safe without reading the
// security section". These pin the facade's defaults: the SSRF-guarded deliverer and the HMAC signer whose output
// the README receiver code verifies.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ab0t-com/ab0t-events-go/adapters/hmacsign"
	"github.com/ab0t-com/ab0t-events-go/adapters/memory"
	"github.com/ab0t-com/ab0t-events-go/events"
	"github.com/ab0t-com/ab0t-events-go/webhooks"
)

// TestUseCase_Facade_DefaultDelivererRefusesATenantURLAimedAtAnInternalService: a tenant registers a URL that points
// at an internal admin endpoint (here a real loopback server). With the README wire-in the deliverer is the guarded
// one, so the internal service never sees a request and the delivery dead-letters with a "blocked" reason.
func TestUseCase_Facade_DefaultDelivererRefusesATenantURLAimedAtAnInternalService(t *testing.T) {
	var hits atomic.Int32
	internal := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer internal.Close()

	log := memory.NewLog()
	sys, err := webhooks.New(context.Background(), webhooks.Options{
		Store: memory.NewStore(),
		Log:   log,
		// Deliverer deliberately unset: the default must be the SSRF-guarded one. Policy shortened for the test.
		Policy: events.RetryPolicy{MaxAttempts: 2, BaseDelay: time.Millisecond, Multiplier: 1, RetryOn: events.DefaultRetryable},
	})
	if err != nil {
		t.Fatalf("webhooks.New: %v", err)
	}
	defer sys.Close()

	ctx := context.Background()
	sub, err := sys.Subscribe(ctx, events.NewSubscription{OrgID: "org_tenant", EventTypes: []string{"*"}, URL: internal.URL + "/internal/admin/flush", Secret: "test-placeholder-secret"})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	ev := events.Event{ID: "evt_ssrf_1", Type: "user.created", OrgID: "org_tenant", OccurredAt: time.Now().UTC(), Payload: []byte(`{"userId":"u_1"}`)}
	if err := sys.Emit(ctx, ev); err != nil {
		t.Fatalf("emit: %v", err)
	}
	id := events.DeliveryID(sub.ID, ev.ID)
	waitFor(t, 5*time.Second, func() bool {
		d, _, atts, err := log.Get(ctx, "org_tenant", id)
		return err == nil && d.Status == events.StatusDead && len(atts) == 2
	})
	d, _, _, _ := log.Get(ctx, "org_tenant", id)
	if !strings.Contains(d.LastError, "blocked") {
		t.Fatalf("LastError = %q, want the SSRF guard's blocked-address reason", d.LastError)
	}
	if n := hits.Load(); n != 0 {
		t.Fatalf("the internal service received %d requests through the default deliverer", n)
	}
}

type ucCapture struct {
	mu   sync.Mutex
	reqs []events.DeliveryRequest
}

func (c *ucCapture) Post(_ context.Context, req events.DeliveryRequest) (events.DeliveryResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reqs = append(c.reqs, req)
	return events.DeliveryResponse{StatusCode: http.StatusOK}, nil
}

func (c *ucCapture) snapshot() []events.DeliveryRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]events.DeliveryRequest(nil), c.reqs...)
}

// TestUseCase_Facade_DefaultSignerIsWhatTheREADMEReceiverVerifies: no Signer option → hmacsign, so the receiver
// snippet in CONSUMING.md §5 accepts what the facade sends.
func TestUseCase_Facade_DefaultSignerIsWhatTheREADMEReceiverVerifies(t *testing.T) {
	capture := &ucCapture{}
	sys, err := webhooks.New(context.Background(), webhooks.Options{Store: memory.NewStore(), Log: memory.NewLog(), Deliverer: capture})
	if err != nil {
		t.Fatalf("webhooks.New: %v", err)
	}
	defer sys.Close()
	ctx := context.Background()
	secret, err := hmacsign.GenerateSecret()
	if err != nil {
		t.Fatalf("GenerateSecret: %v", err)
	}
	if _, err := sys.Subscribe(ctx, events.NewSubscription{OrgID: "org_1", EventTypes: []string{"drive.*"}, URL: "https://customer.example/webhooks", Secret: secret}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := sys.EmitNew(ctx, "drive.file.created", "org_1", map[string]any{"fileId": "f_1"}); err != nil {
		t.Fatalf("emit: %v", err)
	}
	waitFor(t, 5*time.Second, func() bool { return len(capture.snapshot()) == 1 })
	req := capture.snapshot()[0]
	h := req.Headers
	if err := hmacsign.VerifyRequest(secret, req.Body, h[events.HeaderDelivery], h[events.HeaderSignature], h[events.HeaderTimestamp], time.Now(), 5*time.Minute); err != nil {
		t.Fatalf("README receiver rejects the facade's default signature: %v", err)
	}
}
