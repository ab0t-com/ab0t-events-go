package webhooks_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ab0t-com/ab0t-events-go/adapters/httpdeliver"
	"github.com/ab0t-com/ab0t-events-go/adapters/memory"
	"github.com/ab0t-com/ab0t-events-go/events"
	"github.com/ab0t-com/ab0t-events-go/webhooks"
)

// The facade must reject a missing required dependency.
func TestNew_RequiresStoreAndLog(t *testing.T) {
	if _, err := webhooks.New(context.Background(), webhooks.Options{}); err == nil {
		t.Fatal("expected error when Store and Log are missing")
	}
}

// End-to-end through the facade: 5-line wire-in, real HTTP, snippet capture, and
// the observer hook all working together.
func TestFacade_EndToEnd(t *testing.T) {
	var observed atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "endpoint on fire: teapot exploded", http.StatusInternalServerError)
	}))
	defer srv.Close()

	log := memory.NewLog()
	sys, err := webhooks.New(context.Background(), webhooks.Options{
		Store:     memory.NewStore(),
		Log:       log,
		Deliverer: httpdeliver.New(httpdeliver.Config{GuardPrivate: false}), // httptest is 127.0.0.1
		Policy:    events.RetryPolicy{MaxAttempts: 2, BaseDelay: 5 * time.Millisecond, Multiplier: 2, RetryOn: events.DefaultRetryable},
		Observer:  events.ObserverFunc(func(_ context.Context, _ events.Delivery, _ events.Attempt) { observed.Add(1) }),
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer sys.Close()

	ctx := context.Background()
	sub, err := sys.Subscribe(ctx, events.NewSubscription{OrgID: "org1", EventTypes: []string{"drive.*"}, URL: srv.URL, Secret: "whsec_x"})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	// EmitNew convenience + prefix wildcard "drive.*" matching "drive.file.created".
	if err := sys.EmitNew(ctx, "drive.file.created", "org1", map[string]any{"id": 1}); err != nil {
		t.Fatalf("emitnew: %v", err)
	}

	id := events.DeliveryID(sub.ID, mustDeliveryEventID(t, sys, log))
	waitFor(t, 3*time.Second, func() bool {
		d, _, atts, err := log.Get(ctx, "org1", id)
		return err == nil && d.Status == events.StatusDead && len(atts) == 2
	})

	d, _, atts, _ := log.Get(ctx, "org1", id)
	// The response-body snippet must be captured into the attempt error.
	if !strings.Contains(atts[len(atts)-1].Error, "teapot exploded") {
		t.Fatalf("snippet not captured in attempt error: %q", atts[len(atts)-1].Error)
	}
	if d.LastError == "" {
		t.Fatal("delivery LastError should be populated")
	}
	if got := observed.Load(); got < 2 {
		t.Fatalf("observer called %d times, want >= 2", got)
	}
}

// mustDeliveryEventID pulls the single logged delivery's event id back out (the
// facade minted the event id internally via EmitNew).
func mustDeliveryEventID(t *testing.T, sys *webhooks.System, log *memory.Log) string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		page, _, err := sys.ListDeliveries(context.Background(), "org1", events.DeliveryFilter{})
		if err == nil && len(page) == 1 {
			return page[0].EventID
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("delivery never appeared in log")
	return ""
}

func waitFor(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}
