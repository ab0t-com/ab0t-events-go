package sqsqueue

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ab0t-com/ab0t-events-go/adapters/hmacsign"
	"github.com/ab0t-com/ab0t-events-go/adapters/memory"
	"github.com/ab0t-com/ab0t-events-go/adapters/std"
	"github.com/ab0t-com/ab0t-events-go/events"
)

// recordDeliverer records what was POSTed so we can assert the rehydrated job
// carried the real payload and secret-derived signature (reloaded, not queued).
type recordDeliverer struct {
	mu       sync.Mutex
	bodies   []string
	sigSeen  bool
	statusFn func() int
}

func (d *recordDeliverer) Post(_ context.Context, req events.DeliveryRequest) (events.DeliveryResponse, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.bodies = append(d.bodies, string(req.Body))
	if req.Headers[events.HeaderSignature] != "" {
		d.sigSeen = true
	}
	code := 200
	if d.statusFn != nil {
		code = d.statusFn()
	}
	return events.DeliveryResponse{StatusCode: code}, nil
}
func (d *recordDeliverer) count() int { d.mu.Lock(); defer d.mu.Unlock(); return len(d.bodies) }

// TestDurablePath_EndToEnd proves the whole durable design: Emit records the
// delivery + snapshot, enqueues an ID-ONLY ref to SQS, and the consumer
// rehydrates the full job from the store + log and delivers it — with no payload
// or secret ever placed on the queue.
func TestDurablePath_EndToEnd(t *testing.T) {
	store := memory.NewStore()
	log := memory.NewLog()
	f := newFakeSQS()
	deliv := &recordDeliverer{}

	queue := New(Config{Client: f, QueueURL: "url", Workers: 3})
	svc := events.NewService(events.Deps{
		Store: store, Queue: queue, Deliverer: deliv, Signer: hmacsign.New(),
		Log: log, Clock: std.Clock{}, IDGen: std.IDGen{}, Rand: std.Rand{},
		Policy: events.RetryPolicy{MaxAttempts: 3, BaseDelay: 5 * time.Millisecond, Multiplier: 2, RetryOn: events.DefaultRetryable},
	})

	ctx := context.Background()
	sub, err := svc.Subscribe(ctx, events.NewSubscription{OrgID: "orgA", EventTypes: []string{"drive.*"}, URL: "https://x/hook", Secret: "whsec_secret"})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := svc.EmitNew(ctx, "drive.file.created", "orgA", map[string]any{"fileId": "f_1"}); err != nil {
		t.Fatalf("emit: %v", err)
	}

	// The SQS message must be id-only: no payload, no secret.
	if len(f.sent) != 1 {
		t.Fatalf("sent %d messages, want 1", len(f.sent))
	}
	if contains(f.sent[0].body, "whsec_secret") || contains(f.sent[0].body, "fileId") {
		t.Fatalf("queue leaked payload/secret: %s", f.sent[0].body)
	}

	stop := queue.Start(ctx, svc)
	defer stop()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		page, _, _ := svc.ListDeliveries(ctx, "orgA", events.DeliveryFilter{})
		if len(page) == 1 && page[0].Status == events.StatusDelivered {
			// The rehydrated job carried the real payload and a signature.
			if deliv.count() != 1 || !contains(deliv.bodies[0], "fileId") || !deliv.sigSeen {
				t.Fatalf("rehydrated delivery wrong: bodies=%v sig=%v", deliv.bodies, deliv.sigSeen)
			}
			_ = sub
			return
		}
		time.Sleep(3 * time.Millisecond)
	}
	t.Fatal("delivery did not reach 'delivered' via the durable SQS path")
}
