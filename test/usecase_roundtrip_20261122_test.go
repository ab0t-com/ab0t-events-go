package integration

// Use-case scenario tests, 2026-11-22: each test is one job a service or its customer does with the library.
//
// Job: "a SaaS emits a domain event and a CUSTOMER's endpoint receives a signed webhook it can trust". The receiver
// below is written the way CONSUMING.md §5 tells a customer to write one (VerifyRequest with the three headers and a
// 5-minute window, 400 on failure, dedup on X-Delivery-Id), so these tests break if the producer side ever stops
// matching the documented receiver code.

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ab0t-com/ab0t-events-go/adapters/hmacsign"
	"github.com/ab0t-com/ab0t-events-go/adapters/httpdeliver"
	"github.com/ab0t-com/ab0t-events-go/adapters/memory"
	"github.com/ab0t-com/ab0t-events-go/adapters/stdwebhook"
	"github.com/ab0t-com/ab0t-events-go/events"
	"github.com/ab0t-com/ab0t-events-go/webhooks"
)

// ucReceiver is the customer's endpoint. During a secret rotation the customer holds more than one secret (current
// first) and accepts a signature made with any of them.
type ucReceiver struct {
	mu       sync.Mutex
	secrets  []string
	now      func() time.Time
	effects  map[string]int // X-Delivery-Id -> times the business side effect ran
	dupes    int            // verified requests whose delivery id was already processed
	posts    int
	rejected int
	captured []ucCaptured
}

type ucCaptured struct {
	header http.Header
	body   []byte
}

func newUCReceiver(secrets ...string) *ucReceiver {
	return &ucReceiver{secrets: secrets, now: time.Now, effects: map[string]int{}}
}

func (rc *ucReceiver) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read error", http.StatusBadRequest)
		return
	}
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.posts++
	rc.captured = append(rc.captured, ucCaptured{header: r.Header.Clone(), body: body})
	verr := hmacsign.ErrBadSignature
	for _, secret := range rc.secrets {
		verr = hmacsign.VerifyRequest(
			secret, body,
			r.Header.Get(events.HeaderDelivery),  // X-Delivery-Id  (bound into the signature)
			r.Header.Get(events.HeaderSignature), // X-Signature
			r.Header.Get(events.HeaderTimestamp), // X-Event-Timestamp (bound into the signature)
			rc.now(), 5*time.Minute,              // tolerance window (anti-replay)
		)
		if verr == nil {
			break
		}
	}
	if verr != nil {
		rc.rejected++
		http.Error(w, "bad signature", http.StatusBadRequest)
		return
	}
	id := r.Header.Get(events.HeaderDelivery)
	if rc.effects[id] > 0 {
		rc.dupes++ // dedup on X-Delivery-Id: acknowledge, do nothing
		w.WriteHeader(http.StatusOK)
		return
	}
	rc.effects[id]++ // the business side effect (mark invoice paid, ship the order, ...)
	w.WriteHeader(http.StatusOK)
}

func (rc *ucReceiver) setSecrets(secrets ...string) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.secrets = secrets
}

func (rc *ucReceiver) setNow(now func() time.Time) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.now = now
}

func (rc *ucReceiver) counts() (posts, rejected, dupes int) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return rc.posts, rc.rejected, rc.dupes
}

func (rc *ucReceiver) effectsFor(id string) int {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return rc.effects[id]
}

func (rc *ucReceiver) capturedAt(t *testing.T, i int) ucCaptured {
	t.Helper()
	rc.mu.Lock()
	defer rc.mu.Unlock()
	if i >= len(rc.captured) {
		t.Fatalf("receiver captured %d requests, want index %d", len(rc.captured), i)
	}
	return rc.captured[i]
}

// newUCSystem is the facade wired as in the README, except the deliverer: httptest binds 127.0.0.1, which the
// facade's default SSRF guard (correctly) refuses.
func newUCSystem(t *testing.T, signer events.Signer) (*webhooks.System, *memory.Log) {
	t.Helper()
	log := memory.NewLog()
	sys, err := webhooks.New(context.Background(), webhooks.Options{
		Store:     memory.NewStore(),
		Log:       log,
		Deliverer: httpdeliver.New(httpdeliver.Config{Timeout: 2 * time.Second, GuardPrivate: false}),
		Signer:    signer, // nil keeps the facade default (hmacsign)
		Policy:    events.RetryPolicy{MaxAttempts: 3, BaseDelay: 5 * time.Millisecond, MaxDelay: 50 * time.Millisecond, Multiplier: 2, RetryOn: events.DefaultRetryable},
		Workers:   2,
	})
	if err != nil {
		t.Fatalf("webhooks.New: %v", err)
	}
	t.Cleanup(sys.Close)
	return sys, log
}

func ucWaitStatus(t *testing.T, log *memory.Log, orgID, deliveryID string, want events.DeliveryStatus, attempts int) {
	t.Helper()
	waitFor(t, 5*time.Second, func() bool {
		d, _, atts, err := log.Get(context.Background(), orgID, deliveryID)
		return err == nil && d.Status == want && len(atts) == attempts
	}, "delivery "+deliveryID+" to reach "+string(want)+" after "+strconv.Itoa(attempts)+" attempt(s)")
}

// TestUseCase_RoundTrip_HMACWebhookVerifiedByTheREADMEReceiver: an invoicing SaaS emits invoice.paid for org_acme;
// Acme's endpoint verifies it with the README code and marks the invoice paid exactly once. The bytes the customer
// receives are the bytes that were emitted, the idempotency/trace headers arrive, and no header carries the secret.
func TestUseCase_RoundTrip_HMACWebhookVerifiedByTheREADMEReceiver(t *testing.T) {
	ctx := context.Background()
	secret, err := hmacsign.GenerateSecret()
	if err != nil {
		t.Fatalf("GenerateSecret: %v", err)
	}
	rc := newUCReceiver(secret)
	srv := httptest.NewServer(rc)
	defer srv.Close()
	sys, log := newUCSystem(t, nil)

	sub, err := sys.Subscribe(ctx, events.NewSubscription{OrgID: "org_acme", EventTypes: []string{"invoice.*"}, URL: srv.URL + "/webhooks/ab0t", Secret: secret})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	payload := []byte(`{"invoiceId":"in_1001","amountCents":4200,"currency":"NZD"}`)
	ev := events.Event{
		ID: "evt_rt_1001", Type: "invoice.paid", OrgID: "org_acme", OccurredAt: time.Now().UTC(), Payload: payload,
		Headers: map[string]string{"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"},
	}
	if err := sys.Emit(ctx, ev); err != nil {
		t.Fatalf("emit: %v", err)
	}
	id := events.DeliveryID(sub.ID, ev.ID)
	ucWaitStatus(t, log, "org_acme", id, events.StatusDelivered, 1)

	if posts, rejected, dupes := rc.counts(); posts != 1 || rejected != 0 || dupes != 0 {
		t.Fatalf("receiver saw posts=%d rejected=%d dupes=%d, want 1/0/0", posts, rejected, dupes)
	}
	if n := rc.effectsFor(id); n != 1 {
		t.Fatalf("side effect ran %d times for %s, want 1", n, id)
	}
	got := rc.capturedAt(t, 0)
	if !bytes.Equal(got.body, payload) {
		t.Fatalf("receiver body = %s, want the emitted payload byte-for-byte %s", got.body, payload)
	}
	want := map[string]string{
		events.HeaderEventID:   ev.ID,
		events.HeaderEventType: ev.Type,
		events.HeaderDelivery:  id,
		events.HeaderAttempt:   "1",
		"Content-Type":         "application/json",
		"User-Agent":           httpdeliver.DefaultUserAgent,
		"Traceparent":          ev.Headers["traceparent"],
	}
	for name, v := range want {
		if g := got.header.Get(name); g != v {
			t.Errorf("header %s = %q, want %q", name, g, v)
		}
	}
	for name, vals := range got.header {
		for _, v := range vals {
			if strings.Contains(v, secret) {
				t.Fatalf("header %s carries the signing secret", name)
			}
		}
	}
}

// TestUseCase_RoundTrip_StandardWebhooksReceiver: a customer on another stack verifies with a Standard Webhooks
// verifier. The service swaps in stdwebhook.New(); the receiver uses stdwebhook.VerifyRequest as CONSUMING.md shows,
// and its dedup key (webhook-id) is the library's delivery id.
func TestUseCase_RoundTrip_StandardWebhooksReceiver(t *testing.T) {
	ctx := context.Background()
	secret, err := stdwebhook.GenerateSecret()
	if err != nil {
		t.Fatalf("GenerateSecret: %v", err)
	}
	var mu sync.Mutex
	var verified, rejected int
	var sawWebhookID, sawDeliveryID string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		verr := stdwebhook.VerifyRequest(secret, body,
			r.Header.Get(stdwebhook.HeaderID), r.Header.Get(stdwebhook.HeaderTimestamp), r.Header.Get(stdwebhook.HeaderSignature),
			time.Now(), 5*time.Minute)
		mu.Lock()
		defer mu.Unlock()
		if verr != nil {
			rejected++
			http.Error(w, "bad signature", http.StatusBadRequest)
			return
		}
		verified++
		sawWebhookID, sawDeliveryID = r.Header.Get(stdwebhook.HeaderID), r.Header.Get(events.HeaderDelivery)
		w.WriteHeader(http.StatusNoContent) // any 2xx is success
	}))
	defer srv.Close()
	sys, log := newUCSystem(t, stdwebhook.New())

	if _, err := sys.Subscribe(ctx, events.NewSubscription{OrgID: "org_globex", EventTypes: []string{"*"}, URL: srv.URL + "/hooks", Secret: secret}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := sys.EmitNew(ctx, "shipment.dispatched", "org_globex", map[string]any{"shipmentId": "sh_9"}); err != nil {
		t.Fatalf("emit: %v", err)
	}
	var id string
	waitFor(t, 5*time.Second, func() bool {
		page, _, err := sys.ListDeliveries(ctx, "org_globex", events.DeliveryFilter{})
		if err != nil || len(page) != 1 {
			return false
		}
		id = page[0].ID
		return true
	}, "the delivery to be logged")
	ucWaitStatus(t, log, "org_globex", id, events.StatusDelivered, 1)

	mu.Lock()
	defer mu.Unlock()
	if verified != 1 || rejected != 0 {
		t.Fatalf("verified=%d rejected=%d, want 1/0", verified, rejected)
	}
	if sawWebhookID != id || sawDeliveryID != id {
		t.Fatalf("webhook-id=%q X-Delivery-Id=%q, both want the delivery id %q", sawWebhookID, sawDeliveryID, id)
	}
}

// TestUseCase_Receiver_RejectsCapturedReplayForgedHeadersAndStaleRequests: an attacker captured one genuine delivery
// (a logging proxy, a leaked request dump). Re-POSTing it unchanged is harmless (dedup); refreshing the timestamp,
// minting a new delivery id to dodge dedup, or editing the body all fail the signature; and the unchanged request is
// refused once it is older than the receiver's window. The side effect runs exactly once throughout.
func TestUseCase_Receiver_RejectsCapturedReplayForgedHeadersAndStaleRequests(t *testing.T) {
	ctx := context.Background()
	secret, err := hmacsign.GenerateSecret()
	if err != nil {
		t.Fatalf("GenerateSecret: %v", err)
	}
	rc := newUCReceiver(secret)
	srv := httptest.NewServer(rc)
	defer srv.Close()
	sys, log := newUCSystem(t, nil)
	sub, err := sys.Subscribe(ctx, events.NewSubscription{OrgID: "org_bank", EventTypes: []string{"payment.*"}, URL: srv.URL + "/webhooks", Secret: secret})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	ev := events.Event{ID: "evt_pay_77", Type: "payment.succeeded", OrgID: "org_bank", OccurredAt: time.Now().UTC(), Payload: []byte(`{"paymentId":"pay_77","credit":"100.00"}`)}
	if err := sys.Emit(ctx, ev); err != nil {
		t.Fatalf("emit: %v", err)
	}
	id := events.DeliveryID(sub.ID, ev.ID)
	ucWaitStatus(t, log, "org_bank", id, events.StatusDelivered, 1)
	orig := rc.capturedAt(t, 0)

	repost := func(h http.Header, body []byte) int {
		t.Helper()
		req, err := http.NewRequest(http.MethodPost, srv.URL+"/webhooks", bytes.NewReader(body))
		if err != nil {
			t.Fatalf("new request: %v", err)
		}
		req.Header = h.Clone()
		req.Header.Del("Content-Length")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("repost: %v", err)
		}
		_ = resp.Body.Close()
		return resp.StatusCode
	}

	// (a) the exact captured request, inside the window: verifies, but dedup makes it a no-op.
	if code := repost(orig.header, orig.body); code != http.StatusOK {
		t.Fatalf("exact replay inside the window: code %d, want 200 (deduplicated)", code)
	}
	// (b) timestamp refreshed to look recent: the timestamp is bound into the MAC.
	h := orig.header.Clone()
	h.Set(events.HeaderTimestamp, strconv.FormatInt(time.Now().Add(time.Minute).Unix(), 10))
	if code := repost(h, orig.body); code != http.StatusBadRequest {
		t.Fatalf("replay with a refreshed timestamp: code %d, want 400", code)
	}
	// (c) a fresh delivery id to slip past dedup: the id is bound into the MAC.
	h = orig.header.Clone()
	h.Set(events.HeaderDelivery, "dlv_00000000000000000000000000000000")
	if code := repost(h, orig.body); code != http.StatusBadRequest {
		t.Fatalf("replay with a forged delivery id: code %d, want 400", code)
	}
	// (d) body edited (credit inflated).
	if code := repost(orig.header, bytes.Replace(orig.body, []byte("100.00"), []byte("900.00"), 1)); code != http.StatusBadRequest {
		t.Fatalf("tampered body: code %d, want 400", code)
	}
	// (e) the exact request, replayed after the receiver's 5-minute window.
	rc.setNow(func() time.Time { return time.Now().Add(6 * time.Minute) })
	if code := repost(orig.header, orig.body); code != http.StatusBadRequest {
		t.Fatalf("stale replay: code %d, want 400", code)
	}

	if n := rc.effectsFor(id); n != 1 {
		t.Fatalf("side effect ran %d times, want exactly 1", n)
	}
	if _, rejected, dupes := rc.counts(); rejected != 4 || dupes != 1 {
		t.Fatalf("rejected=%d dupes=%d, want 4 rejections and 1 deduplicated replay", rejected, dupes)
	}
}

// TestUseCase_SecretRotation_ReceiverHoldsBothSecretsForTheGraceWindow: Acme suspects its secret leaked and rotates
// it. The library signs every delivery with ONE secret (the current one) — it does not dual-sign — so the customer's
// side of a zero-downtime rotation is: install the new secret, keep accepting the old one for a window, then drop it.
// A replay after rotation is signed with the CURRENT secret.
func TestUseCase_SecretRotation_ReceiverHoldsBothSecretsForTheGraceWindow(t *testing.T) {
	ctx := context.Background()
	oldSecret, err := hmacsign.GenerateSecret()
	if err != nil {
		t.Fatalf("GenerateSecret: %v", err)
	}
	rc := newUCReceiver(oldSecret)
	srv := httptest.NewServer(rc)
	defer srv.Close()
	sys, log := newUCSystem(t, nil)
	const org = "org_acme"
	sub, err := sys.Subscribe(ctx, events.NewSubscription{OrgID: org, EventTypes: []string{"*"}, URL: srv.URL + "/webhooks", Secret: oldSecret})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	emit := func(evID string) string {
		t.Helper()
		ev := events.Event{ID: evID, Type: "contact.updated", OrgID: org, OccurredAt: time.Now().UTC(), Payload: []byte(`{"contact":"` + evID + `"}`)}
		if err := sys.Emit(ctx, ev); err != nil {
			t.Fatalf("emit %s: %v", evID, err)
		}
		return events.DeliveryID(sub.ID, evID)
	}
	verifies := func(c ucCaptured, secret string) bool {
		return hmacsign.Verify(secret, c.header.Get(events.HeaderDelivery), c.header.Get(events.HeaderTimestamp), c.body, c.header.Get(events.HeaderSignature))
	}

	id1 := emit("evt_before_rotation")
	ucWaitStatus(t, log, org, id1, events.StatusDelivered, 1)

	newSecret, err := sys.RotateSecret(ctx, org, sub.ID)
	if err != nil {
		t.Fatalf("rotate: %v", err)
	}
	if newSecret == "" || newSecret == oldSecret {
		t.Fatal("RotateSecret must return a fresh secret")
	}
	rc.setSecrets(newSecret, oldSecret) // grace window: new first, old still accepted

	id2 := emit("evt_after_rotation")
	ucWaitStatus(t, log, org, id2, events.StatusDelivered, 1)
	c2 := rc.capturedAt(t, 1)
	if !verifies(c2, newSecret) {
		t.Fatal("a delivery after rotation must be signed with the new secret")
	}
	if verifies(c2, oldSecret) {
		t.Fatal("a delivery after rotation verified under the OLD secret (expected a single-secret signature)")
	}

	rc.setSecrets(newSecret) // window over
	if err := sys.Replay(ctx, org, id1); err != nil {
		t.Fatalf("replay: %v", err)
	}
	ucWaitStatus(t, log, org, id1, events.StatusDelivered, 2)
	c3 := rc.capturedAt(t, 2)
	if c3.header.Get(events.HeaderDelivery) != id1 || !verifies(c3, newSecret) {
		t.Fatal("a replay after rotation must keep its delivery id and be signed with the current secret")
	}
	if _, rejected, _ := rc.counts(); rejected != 0 {
		t.Fatalf("receiver rejected %d deliveries during a correctly-run rotation, want 0", rejected)
	}
}
