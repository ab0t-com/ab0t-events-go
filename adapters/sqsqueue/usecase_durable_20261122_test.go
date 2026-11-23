package sqsqueue

// Use-case scenario tests, 2026-11-22: each test is one job a service or its customer does with the library.
//
// Job: "webhooks must survive restarts, and a leaked queue must reveal nothing". One delivery is walked through its
// whole durable life — fail, delayed retry, early wake-up re-deferred, delivered, SQS at-least-once duplicate
// dropped, support-initiated replay — by handing each SENT message to the consumer exactly as SQS would, so there are
// no goroutines and no sleeps. The clock is shared by the queue and the service.

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ab0t-com/ab0t-events-go/adapters/hmacsign"
	"github.com/ab0t-com/ab0t-events-go/adapters/memory"
	"github.com/ab0t-com/ab0t-events-go/adapters/std"
	"github.com/ab0t-com/ab0t-events-go/events"
)

type ucClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *ucClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *ucClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// ucPoster answers call i with codes[i] (200 once the script runs out) and records every request.
type ucPoster struct {
	mu    sync.Mutex
	codes []int
	reqs  []events.DeliveryRequest
}

func (p *ucPoster) Post(_ context.Context, req events.DeliveryRequest) (events.DeliveryResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	code := 200
	if len(p.reqs) < len(p.codes) {
		code = p.codes[len(p.reqs)]
	}
	p.reqs = append(p.reqs, req)
	return events.DeliveryResponse{StatusCode: code}, nil
}

func (p *ucPoster) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.reqs)
}

func (p *ucPoster) at(i int) events.DeliveryRequest {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.reqs[i]
}

type ucDurable struct {
	f    *fakeSQS
	q    *Queue
	svc  *events.Service
	log  *memory.Log
	clk  *ucClock
	post *ucPoster
}

func newUCDurable(codes ...int) *ucDurable {
	clk := &ucClock{t: time.Unix(1_763_800_000, 0).UTC()}
	f := newFakeSQS()
	q := New(Config{Client: f, QueueURL: "https://sqs.local/000000000000/webhooks-test", Now: clk.Now})
	post := &ucPoster{codes: codes}
	log := memory.NewLog()
	svc := events.NewService(events.Deps{
		Store: memory.NewStore(), Queue: q, Deliverer: post, Signer: hmacsign.New(), Log: log,
		Clock: clk, IDGen: std.IDGen{}, Rand: std.Rand{},
		Policy: events.RetryPolicy{MaxAttempts: 4, BaseDelay: 40 * time.Second, MaxDelay: 10 * time.Minute, Multiplier: 2, RetryOn: events.DefaultRetryable},
	})
	return &ucDurable{f: f, q: q, svc: svc, log: log, clk: clk, post: post}
}

func (u *ucDurable) sentCount() int {
	u.f.mu.Lock()
	defer u.f.mu.Unlock()
	return len(u.f.sent)
}

func (u *ucDurable) sentAt(t *testing.T, i int) sentMsg {
	t.Helper()
	u.f.mu.Lock()
	defer u.f.mu.Unlock()
	if i >= len(u.f.sent) {
		t.Fatalf("only %d messages were sent, want index %d", len(u.f.sent), i)
	}
	return u.f.sent[i]
}

func (u *ucDurable) sentRef(t *testing.T, i int) events.JobRef {
	t.Helper()
	var ref events.JobRef
	if err := json.Unmarshal([]byte(u.sentAt(t, i).body), &ref); err != nil {
		t.Fatalf("message %d is not a JobRef: %v", i, err)
	}
	return ref
}

// consume hands the i-th sent message to the consumer under a fresh receipt handle, as SQS does on each receive.
func (u *ucDurable) consume(t *testing.T, i int, receipt string) {
	t.Helper()
	body := u.sentAt(t, i).body
	u.q.handle(context.Background(), u.svc, &body, &receipt)
}

func (u *ucDurable) deleted(receipt string) bool {
	u.f.mu.Lock()
	defer u.f.mu.Unlock()
	for _, r := range u.f.deleted {
		if r == receipt {
			return true
		}
	}
	return false
}

func (u *ucDurable) lastVisibility() (visCall, bool) {
	u.f.mu.Lock()
	defer u.f.mu.Unlock()
	if len(u.f.visCalls) == 0 {
		return visCall{}, false
	}
	return u.f.visCalls[len(u.f.visCalls)-1], true
}

func (u *ucDurable) status(t *testing.T, org, id string) (events.DeliveryStatus, int) {
	t.Helper()
	d, _, atts, err := u.log.Get(context.Background(), org, id)
	if err != nil {
		t.Fatalf("log get %s: %v", id, err)
	}
	return d.Status, len(atts)
}

func TestUseCase_DurableSQS_RetryRedeferDuplicateAndReplayLifecycle(t *testing.T) {
	ctx := context.Background()
	u := newUCDurable(500) // the first POST fails, everything after succeeds
	const org = "org_shop"
	const secret = "test-placeholder-signing-secret"
	sub, err := u.svc.Subscribe(ctx, events.NewSubscription{OrgID: org, EventTypes: []string{"order.*"}, URL: "https://shop.example/hooks", Secret: secret})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	payload := []byte(`{"orderId":"o_42","cardToken":"tok_placeholder"}`)
	ev := events.Event{ID: "evt_o42", Type: "order.paid", OrgID: org, OccurredAt: u.clk.Now(), Payload: payload}
	if err := u.svc.Emit(ctx, ev); err != nil {
		t.Fatalf("emit: %v", err)
	}
	id := events.DeliveryID(sub.ID, ev.ID)
	assertIDOnly := func() {
		t.Helper()
		for i := 0; i < u.sentCount(); i++ {
			b := u.sentAt(t, i).body
			if strings.Contains(b, secret) || strings.Contains(b, "o_42") || strings.Contains(b, "tok_placeholder") {
				t.Fatalf("queue message %d leaks the secret or the payload: %s", i, b)
			}
		}
	}
	if n := u.sentCount(); n != 1 {
		t.Fatalf("emit sent %d messages, want 1", n)
	}

	// 1. First attempt fails: the original message is deleted and a retry is sent with the backoff as SQS delay.
	u.consume(t, 0, "rcpt-1")
	if u.post.count() != 1 || u.sentCount() != 2 || !u.deleted("rcpt-1") {
		t.Fatalf("after attempt 1: posts=%d sent=%d deleted=%v, want 1/2/true", u.post.count(), u.sentCount(), u.deleted("rcpt-1"))
	}
	if ref := u.sentRef(t, 1); ref.AttemptN != 2 || ref.DeliveryID != id {
		t.Fatalf("retry message = %+v, want attempt 2 of %s", ref, id)
	}
	if d := u.sentAt(t, 1).delay; d != 40 {
		t.Fatalf("retry DelaySeconds = %d, want 40 (the 40s backoff)", d)
	}
	if st, _ := u.status(t, org, id); st != events.StatusFailed {
		t.Fatalf("status after attempt 1 = %s, want failed", st)
	}

	// 2. The consumer sees the retry early (10s in): it is re-deferred for the remaining 30s, not POSTed.
	u.clk.advance(10 * time.Second)
	u.consume(t, 1, "rcpt-2a")
	vis, ok := u.lastVisibility()
	if u.post.count() != 1 || !ok || vis.secs != 30 || vis.receipt != "rcpt-2a" || u.deleted("rcpt-2a") {
		t.Fatalf("early retry: posts=%d visibility=%+v deleted=%v, want no POST and a 30s re-defer", u.post.count(), vis, u.deleted("rcpt-2a"))
	}

	// 3. When due, it is delivered with the exact payload, signed with the secret reloaded from the store.
	u.clk.advance(30 * time.Second)
	u.consume(t, 1, "rcpt-2b")
	if st, n := u.status(t, org, id); st != events.StatusDelivered || n != 2 || u.post.count() != 2 || !u.deleted("rcpt-2b") || u.sentCount() != 2 {
		t.Fatalf("due retry: status=%s attempts=%d posts=%d sent=%d", st, n, u.post.count(), u.sentCount())
	}
	last := u.post.at(1)
	if !bytes.Equal(last.Body, payload) {
		t.Fatalf("rehydrated body = %s, want %s", last.Body, payload)
	}
	if last.Headers[events.HeaderAttempt] != "2" || last.Headers[events.HeaderDelivery] != id {
		t.Fatalf("rehydrated headers attempt=%q delivery=%q", last.Headers[events.HeaderAttempt], last.Headers[events.HeaderDelivery])
	}
	if !hmacsign.Verify(secret, last.Headers[events.HeaderDelivery], last.Headers[events.HeaderTimestamp], last.Body, last.Headers[events.HeaderSignature]) {
		t.Fatal("rehydrated delivery is not signed with the subscription's secret")
	}

	// 4. SQS at-least-once: the same retry message arrives again. The delivery is terminal → dropped, no duplicate.
	u.consume(t, 1, "rcpt-2c")
	if u.post.count() != 2 || !u.deleted("rcpt-2c") {
		t.Fatalf("duplicate of a delivered message: posts=%d deleted=%v, want 2 and true", u.post.count(), u.deleted("rcpt-2c"))
	}

	// 5. The customer lost the order in their own DB; support replays the delivery from the admin surface.
	if err := u.svc.Replay(ctx, org, id); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if u.sentCount() != 3 || u.sentRef(t, 2).AttemptN != 1 {
		t.Fatalf("replay did not enqueue a fresh attempt-1 message (sent=%d)", u.sentCount())
	}
	u.consume(t, 2, "rcpt-3")
	if st, n := u.status(t, org, id); st != events.StatusDelivered || n != 3 || u.post.count() != 3 {
		t.Fatalf("after replay: status=%s attempts=%d posts=%d, want delivered/3/3", st, n, u.post.count())
	}
	if got := u.post.at(2).Headers[events.HeaderDelivery]; got != id {
		t.Fatalf("replay carried delivery id %q, want the original %q (receivers dedup on it)", got, id)
	}
	assertIDOnly()
}
