// Package integration wires the REAL adapters together (in-process worker pool +
// net/http deliverer + HMAC signer + in-memory store/log) and drives an actual
// HTTP delivery to a local httptest server. It proves the end-to-end path with
// real goroutines and real sockets — the -race target exercises it too.
//
// The SSRF guard is intentionally OFF here: httptest binds to 127.0.0.1, which
// the production guard (NewSecure) correctly blocks. Production uses NewSecure.
package integration

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ab0t-com/ab0t-events-go/adapters/hmacsign"
	"github.com/ab0t-com/ab0t-events-go/adapters/httpdeliver"
	"github.com/ab0t-com/ab0t-events-go/adapters/inproc"
	"github.com/ab0t-com/ab0t-events-go/adapters/memory"
	"github.com/ab0t-com/ab0t-events-go/adapters/std"
	"github.com/ab0t-com/ab0t-events-go/events"
)

func TestIntegration_RealHTTPDelivery(t *testing.T) {
	const secret = "whsec_integration"

	// A subscriber that returns 2xx and verifies the HMAC signature + headers.
	var okHits int32
	var sigOK int32
	okSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := readAll(t, r)
		if hmacsign.Verify(secret, r.Header.Get(events.HeaderDelivery), r.Header.Get(events.HeaderTimestamp), body, r.Header.Get(events.HeaderSignature)) {
			atomic.AddInt32(&sigOK, 1)
		}
		if r.Header.Get(events.HeaderDelivery) == "" || r.Header.Get(events.HeaderEventID) == "" {
			t.Errorf("missing idempotency headers")
		}
		atomic.AddInt32(&okHits, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer okSrv.Close()

	// A subscriber that always fails: forces retries then dead-letter.
	var failHits int32
	failSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&failHits, 1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer failSrv.Close()

	store := memory.NewStore()
	log := memory.NewLog()
	queue := inproc.New(inproc.Config{Workers: 4})
	deliverer := httpdeliver.New(httpdeliver.Config{Timeout: 2 * time.Second, GuardPrivate: false})

	svc := events.NewService(events.Deps{
		Store:     store,
		Queue:     queue,
		Deliverer: deliverer,
		Signer:    hmacsign.New(),
		Log:       log,
		Clock:     std.Clock{},
		IDGen:     std.IDGen{},
		Rand:      std.Rand{},
		Policy:    events.RetryPolicy{MaxAttempts: 3, BaseDelay: 10 * time.Millisecond, MaxDelay: 100 * time.Millisecond, Multiplier: 2, JitterFrac: 0.2},
	})

	stop := queue.Start(context.Background(), svc)
	defer stop()

	ctx := context.Background()
	okSub, err := svc.Subscribe(ctx, events.NewSubscription{OrgID: "orgA", EventTypes: []string{"drive.file.created"}, URL: okSrv.URL, Secret: secret})
	if err != nil {
		t.Fatalf("subscribe ok: %v", err)
	}
	failSub, err := svc.Subscribe(ctx, events.NewSubscription{OrgID: "orgA", EventTypes: []string{"*"}, URL: failSrv.URL, Secret: secret})
	if err != nil {
		t.Fatalf("subscribe fail: %v", err)
	}

	payload, _ := json.Marshal(map[string]any{"fileId": "f_123", "name": "report.pdf"})
	ev := events.Event{ID: "e_int_1", Type: "drive.file.created", OrgID: "orgA", OccurredAt: time.Now().UTC(), Payload: payload}
	if err := svc.Emit(ctx, ev); err != nil {
		t.Fatalf("emit: %v", err)
	}

	okID := events.DeliveryID(okSub.ID, ev.ID)
	failID := events.DeliveryID(failSub.ID, ev.ID)

	// The 2xx delivery is logged as delivered with one successful attempt.
	waitFor(t, 5*time.Second, func() bool {
		d, _, atts, err := log.Get(ctx, "orgA", okID)
		return err == nil && d.Status == events.StatusDelivered && len(atts) == 1
	}, "ok delivery to reach 'delivered'")

	// The failing delivery is retried up to MaxAttempts then dead-lettered.
	waitFor(t, 5*time.Second, func() bool {
		d, _, atts, err := log.Get(ctx, "orgA", failID)
		return err == nil && d.Status == events.StatusDead && len(atts) == 3
	}, "fail delivery to be dead-lettered after retries")

	if atomic.LoadInt32(&sigOK) < 1 {
		t.Fatal("subscriber never received a valid HMAC signature")
	}
	if got := atomic.LoadInt32(&okHits); got != 1 {
		t.Fatalf("ok endpoint hit %d times, want 1", got)
	}
	if got := atomic.LoadInt32(&failHits); got != 3 {
		t.Fatalf("fail endpoint hit %d times, want 3 (attempts)", got)
	}

	// Final attempt records must carry the real HTTP codes.
	_, _, atts, _ := log.Get(ctx, "orgA", failID)
	for i, a := range atts {
		if a.Code != http.StatusInternalServerError {
			t.Fatalf("attempt %d code = %d, want 500", i, a.Code)
		}
	}
}

func waitFor(t *testing.T, d time.Duration, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func readAll(t *testing.T, r *http.Request) []byte {
	t.Helper()
	b, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return b
}
