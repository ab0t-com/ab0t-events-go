package events

import (
	"context"
	"errors"
	"testing"
	"time"
)

var fastPolicy = RetryPolicy{MaxAttempts: 4, BaseDelay: time.Second, MaxDelay: time.Minute, Multiplier: 2, JitterFrac: 0}

func TestEmit_FanOutToMatchingSubs(t *testing.T) {
	h := newHarness(fastPolicy)
	h.store.add(Subscription{ID: "s1", OrgID: "orgA", EventTypes: []string{"drive.file.created"}, URL: "https://a.example/hook1", Secret: "k1", Active: true})
	h.store.add(Subscription{ID: "s2", OrgID: "orgA", EventTypes: []string{"*"}, URL: "https://a.example/hook2", Secret: "k2", Active: true})
	h.store.add(Subscription{ID: "s3", OrgID: "orgA", EventTypes: []string{"mail.message.sent"}, URL: "https://a.example/other", Secret: "k3", Active: true})
	h.store.add(Subscription{ID: "s4", OrgID: "orgA", EventTypes: []string{"*"}, URL: "https://a.example/paused", Secret: "k4", Active: false})

	if err := h.svc.Emit(context.Background(), mkEvent("e1", "drive.file.created", "orgA", map[string]string{"k": "v"})); err != nil {
		t.Fatalf("emit: %v", err)
	}
	// s1 (exact) and s2 (wildcard) match; s3 (other type) and s4 (paused) do not.
	if got := len(h.queue.pending()); got != 2 {
		t.Fatalf("expected 2 jobs enqueued, got %d", got)
	}
	if err := h.queue.drain(context.Background(), h.svc, h.clock, 20); err != nil {
		t.Fatalf("drain: %v", err)
	}
	urls := h.deliv.urls()
	if !contains(urls, "https://a.example/hook1") || !contains(urls, "https://a.example/hook2") {
		t.Fatalf("matching subs not delivered: %v", urls)
	}
	if contains(urls, "https://a.example/other") || contains(urls, "https://a.example/paused") {
		t.Fatalf("non-matching sub delivered: %v", urls)
	}
}

func TestEmit_PerOrgIsolation(t *testing.T) {
	h := newHarness(fastPolicy)
	// Correctly-partitioned subs for two different orgs, same event type.
	h.store.add(Subscription{ID: "a1", OrgID: "orgA", EventTypes: []string{"*"}, URL: "https://orgA.example/hook", Secret: "ka", Active: true})
	h.store.add(Subscription{ID: "b1", OrgID: "orgB", EventTypes: []string{"*"}, URL: "https://orgB.example/hook", Secret: "kb", Active: true})
	// Defense-in-depth: a mis-stored sub carrying orgB's id inside orgA's partition.
	h.store.add(Subscription{ID: "poison", OrgID: "orgB", EventTypes: []string{"*"}, URL: "https://orgB.example/leak", Secret: "kx", Active: true})
	h.store.subs["orgA"] = append(h.store.subs["orgA"], Subscription{ID: "poison", OrgID: "orgB", EventTypes: []string{"*"}, URL: "https://orgB.example/leak", Secret: "kx", Active: true})

	// The poison sub trips the defense-in-depth guard: Emit reports it (and
	// skips it) rather than fanning org A's event out to org B's URL.
	err := h.svc.Emit(context.Background(), mkEvent("e1", "drive.file.created", "orgA", nil))
	if !errors.Is(err, ErrTenancyViolation) {
		t.Fatalf("expected the mis-partitioned sub to trip the tenancy guard, got %v", err)
	}
	if err := h.queue.drain(context.Background(), h.svc, h.clock, 20); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if !contains(h.deliv.urls(), "https://orgA.example/hook") {
		t.Fatalf("org A's own subscription was not delivered: %v", h.deliv.urls())
	}
	for _, u := range h.deliv.urls() {
		if u == "https://orgB.example/hook" || u == "https://orgB.example/leak" {
			t.Fatalf("org A event reached org B endpoint %q — TENANCY BREACH", u)
		}
		if u != "https://orgA.example/hook" {
			t.Fatalf("unexpected delivery url %q", u)
		}
	}
	// And org A's delivery log must only ever mention org A.
	page, _, err := h.svc.ListDeliveries(context.Background(), "orgB", DeliveryFilter{})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(page) != 0 {
		t.Fatalf("org B has deliveries from org A's emit: %v", page)
	}
}

func TestEmit_Success2xxLogged(t *testing.T) {
	h := newHarness(fastPolicy)
	h.store.add(Subscription{ID: "s1", OrgID: "orgA", EventTypes: []string{"*"}, URL: "https://a.example/ok", Secret: "k1", Active: true})
	if err := h.svc.Emit(context.Background(), mkEvent("e1", "drive.file.created", "orgA", nil)); err != nil {
		t.Fatalf("emit: %v", err)
	}
	if err := h.queue.drain(context.Background(), h.svc, h.clock, 10); err != nil {
		t.Fatalf("drain: %v", err)
	}
	id := DeliveryID("s1", "e1")
	e, ok := h.log.get(id)
	if !ok {
		t.Fatalf("delivery %s not logged", id)
	}
	if e.delivery.Status != StatusDelivered {
		t.Fatalf("status = %s, want delivered", e.delivery.Status)
	}
	if len(e.attempts) != 1 || e.attempts[0].Code != 200 {
		t.Fatalf("attempts = %+v, want one 200", e.attempts)
	}
}

func TestRetry_DeadLetterWithBackoff(t *testing.T) {
	h := newHarness(fastPolicy)
	h.store.add(Subscription{ID: "s1", OrgID: "orgA", EventTypes: []string{"*"}, URL: "https://a.example/fail", Secret: "k1", Active: true})
	h.deliv.byURL["https://a.example/fail"] = 500

	if err := h.svc.Emit(context.Background(), mkEvent("e1", "drive.file.created", "orgA", nil)); err != nil {
		t.Fatalf("emit: %v", err)
	}
	if err := h.queue.drain(context.Background(), h.svc, h.clock, 50); err != nil {
		t.Fatalf("drain: %v", err)
	}

	id := DeliveryID("s1", "e1")
	e, ok := h.log.get(id)
	if !ok {
		t.Fatalf("delivery not logged")
	}
	if e.delivery.Status != StatusDead {
		t.Fatalf("status = %s, want dead", e.delivery.Status)
	}
	if len(e.attempts) != fastPolicy.MaxAttempts {
		t.Fatalf("attempts = %d, want %d", len(e.attempts), fastPolicy.MaxAttempts)
	}
	// Every attempt should be an HTTP 500.
	for i, a := range e.attempts {
		if a.N != i+1 || a.Code != 500 {
			t.Fatalf("attempt %d = %+v", i, a)
		}
	}
	// The requeued jobs must carry increasing backoff (1s, 2s, 4s) — jitter off.
	var retries []Job
	for _, j := range h.queue.posted {
		if j.AttemptN >= 2 {
			retries = append(retries, j)
		}
	}
	if len(retries) != fastPolicy.MaxAttempts-1 {
		t.Fatalf("retry jobs = %d, want %d", len(retries), fastPolicy.MaxAttempts-1)
	}
	wantDelays := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second}
	for i, r := range retries {
		got := r.NotBefore.Sub(r.Delivery.UpdatedAt)
		if got != wantDelays[i] {
			t.Fatalf("retry %d backoff = %v, want %v", i, got, wantDelays[i])
		}
	}
}

func TestIdempotency_StableDeliveryID(t *testing.T) {
	if a, b := DeliveryID("s1", "e1"), DeliveryID("s1", "e1"); a != b {
		t.Fatalf("delivery id not stable: %s vs %s", a, b)
	}
	if DeliveryID("s1", "e1") == DeliveryID("s1", "e2") {
		t.Fatal("distinct events share a delivery id")
	}

	h := newHarness(fastPolicy)
	h.store.add(Subscription{ID: "s1", OrgID: "orgA", EventTypes: []string{"*"}, URL: "https://a.example/ok", Secret: "k1", Active: true})
	ev := mkEvent("e1", "drive.file.created", "orgA", nil)
	_ = h.svc.Emit(context.Background(), ev)
	_ = h.svc.Emit(context.Background(), ev) // re-emit same event
	seen := map[string]bool{}
	for _, j := range h.queue.pending() {
		seen[j.Delivery.ID] = true
	}
	if len(seen) != 1 {
		t.Fatalf("re-emit produced %d distinct delivery ids, want 1 (dedupable)", len(seen))
	}
}

func TestReplay_ReDelivers(t *testing.T) {
	h := newHarness(fastPolicy)
	h.store.add(Subscription{ID: "s1", OrgID: "orgA", EventTypes: []string{"*"}, URL: "https://a.example/ok", Secret: "k1", Active: true})
	if err := h.svc.Emit(context.Background(), mkEvent("e1", "drive.file.created", "orgA", map[string]int{"n": 7})); err != nil {
		t.Fatalf("emit: %v", err)
	}
	if err := h.queue.drain(context.Background(), h.svc, h.clock, 10); err != nil {
		t.Fatalf("drain: %v", err)
	}
	id := DeliveryID("s1", "e1")
	before := len(h.deliv.urls())

	if err := h.svc.Replay(context.Background(), "orgA", id); err != nil {
		t.Fatalf("replay: %v", err)
	}
	// Replay must be org-scoped: another org cannot replay this delivery.
	if err := h.svc.Replay(context.Background(), "orgB", id); err == nil {
		t.Fatal("cross-org replay succeeded — tenancy breach")
	}
	if err := h.queue.drain(context.Background(), h.svc, h.clock, 10); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if len(h.deliv.urls()) != before+1 {
		t.Fatalf("replay did not re-deliver exactly once (before=%d after=%d)", before, len(h.deliv.urls()))
	}
}

func TestSubscribeAdminLifecycle(t *testing.T) {
	h := newHarness(fastPolicy)
	sub, err := h.svc.Subscribe(context.Background(), NewSubscription{OrgID: "orgA", EventTypes: []string{"drive.file.created"}, URL: "https://a.example/hook", Secret: "shhh"})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if sub.ID == "" || !sub.Active {
		t.Fatalf("bad subscription %+v", sub)
	}
	if _, err := h.svc.Subscribe(context.Background(), NewSubscription{OrgID: "orgA"}); err != ErrInvalidSubscription {
		t.Fatalf("expected ErrInvalidSubscription, got %v", err)
	}
	list, err := h.svc.ListSubscriptions(context.Background(), "orgA")
	if err != nil || len(list) != 1 {
		t.Fatalf("list = %v (%v)", list, err)
	}
	if err := h.svc.Unsubscribe(context.Background(), "orgA", sub.ID); err != nil {
		t.Fatalf("unsubscribe: %v", err)
	}
	list, _ = h.svc.ListSubscriptions(context.Background(), "orgA")
	if len(list) != 0 {
		t.Fatalf("subscription not removed: %v", list)
	}
}

func TestEmit_InvalidEvent(t *testing.T) {
	h := newHarness(fastPolicy)
	if err := h.svc.Emit(context.Background(), Event{Type: "x", OrgID: "o"}); err != ErrInvalidEvent {
		t.Fatalf("want ErrInvalidEvent, got %v", err)
	}
}

func TestSubscription_PrefixWildcard(t *testing.T) {
	sub := Subscription{Active: true, EventTypes: []string{"calendar.event.*"}}
	cases := map[string]bool{
		"calendar.event.created": true,
		"calendar.event.updated": true,
		"calendar.reminder.set":  false,
		"calendar.event":         false, // no trailing segment
		"mail.message.sent":      false,
	}
	for typ, want := range cases {
		if got := sub.Matches(typ); got != want {
			t.Errorf("Matches(%q) = %v, want %v", typ, got, want)
		}
	}
	if !(Subscription{Active: true, EventTypes: []string{"*"}}).Matches("anything.at.all") {
		t.Error(`global "*" should match everything`)
	}
	if (Subscription{Active: false, EventTypes: []string{"*"}}).Matches("x") {
		t.Error("paused subscription must match nothing")
	}
}

func TestRetry_PermanentFailureDeadLettersImmediately(t *testing.T) {
	h := newHarness(DefaultRetryPolicy()) // uses DefaultRetryable
	h.store.add(Subscription{ID: "s1", OrgID: "orgA", EventTypes: []string{"*"}, URL: "https://a.example/bad", Secret: "k1", Active: true})
	h.deliv.byURL["https://a.example/bad"] = 400 // client error: not worth retrying

	if err := h.svc.Emit(context.Background(), mkEvent("e1", "drive.file.created", "orgA", nil)); err != nil {
		t.Fatalf("emit: %v", err)
	}
	if err := h.queue.drain(context.Background(), h.svc, h.clock, 20); err != nil {
		t.Fatalf("drain: %v", err)
	}
	e, _ := h.log.get(DeliveryID("s1", "e1"))
	if e.delivery.Status != StatusDead {
		t.Fatalf("status = %s, want dead", e.delivery.Status)
	}
	if len(e.attempts) != 1 {
		t.Fatalf("attempts = %d, want 1 (no retry on 4xx)", len(e.attempts))
	}
}

func TestObserver_CalledPerAttempt(t *testing.T) {
	h := newHarness(fastPolicy)
	var mu struct {
		statuses []DeliveryStatus
	}
	obs := ObserverFunc(func(_ context.Context, d Delivery, _ Attempt) {
		mu.statuses = append(mu.statuses, d.Status)
	})
	h.svc = NewService(Deps{
		Store: h.store, Queue: h.queue, Deliverer: h.deliv, Signer: recordingSigner{},
		Log: h.log, Clock: h.clock, IDGen: h.idgen, Rand: fixedRand{v: 0.5}, Policy: fastPolicy, Observer: obs,
	})
	h.store.add(Subscription{ID: "s1", OrgID: "orgA", EventTypes: []string{"*"}, URL: "https://a.example/fail", Secret: "k1", Active: true})
	h.deliv.byURL["https://a.example/fail"] = 500

	_ = h.svc.Emit(context.Background(), mkEvent("e1", "drive.file.created", "orgA", nil))
	_ = h.queue.drain(context.Background(), h.svc, h.clock, 50)

	if len(mu.statuses) != fastPolicy.MaxAttempts {
		t.Fatalf("observer calls = %d, want %d", len(mu.statuses), fastPolicy.MaxAttempts)
	}
	if last := mu.statuses[len(mu.statuses)-1]; last != StatusDead {
		t.Fatalf("final observed status = %s, want dead", last)
	}
}

func TestEmitNew_BuildsAndEmits(t *testing.T) {
	h := newHarness(fastPolicy)
	h.store.add(Subscription{ID: "s1", OrgID: "orgA", EventTypes: []string{"drive.file.created"}, URL: "https://a.example/ok", Secret: "k1", Active: true})
	if err := h.svc.EmitNew(context.Background(), "drive.file.created", "orgA", map[string]string{"file": "x"}); err != nil {
		t.Fatalf("emitnew: %v", err)
	}
	if got := len(h.queue.pending()); got != 1 {
		t.Fatalf("expected 1 job, got %d", got)
	}
	if j := h.queue.pending()[0]; string(j.Event.Payload) != `{"file":"x"}` {
		t.Fatalf("payload = %s", j.Event.Payload)
	}
}

func TestProcess_TenancyViolationIsHardStop(t *testing.T) {
	h := newHarness(fastPolicy)
	// A malformed job whose event and subscription disagree on org must never POST.
	job := Job{
		Delivery:     Delivery{ID: "d1", OrgID: "orgA"},
		Event:        mkEvent("e1", "t", "orgA", nil),
		Subscription: Subscription{ID: "s1", OrgID: "orgB", URL: "https://evil/leak", Secret: "k"},
		AttemptN:     1,
	}
	if err := h.svc.Process(context.Background(), job); err != ErrTenancyViolation {
		t.Fatalf("want ErrTenancyViolation, got %v", err)
	}
	if len(h.deliv.urls()) != 0 {
		t.Fatalf("a cross-org job was delivered: %v", h.deliv.urls())
	}
}

func TestSubscribe_URLValidationRejects(t *testing.T) {
	h := newHarness(fastPolicy)
	blocked := errors.New("blocked by policy")
	h.svc = NewService(Deps{
		Store: h.store, Queue: h.queue, Deliverer: h.deliv, Signer: recordingSigner{},
		Log: h.log, Clock: h.clock, IDGen: h.idgen, Rand: fixedRand{v: 0.5}, Policy: fastPolicy,
		ValidateURL: func(string) error { return blocked },
	})
	_, err := h.svc.Subscribe(context.Background(), NewSubscription{
		OrgID: "orgA", EventTypes: []string{"*"}, URL: "http://169.254.169.254/", Secret: "k",
	})
	if !errors.Is(err, blocked) {
		t.Fatalf("want validation error, got %v", err)
	}
	if list, _ := h.store.ListForOrg(context.Background(), "orgA"); len(list) != 0 {
		t.Fatalf("rejected subscription was stored: %v", list)
	}
}

func TestReplay_NotFound(t *testing.T) {
	h := newHarness(fastPolicy)
	if err := h.svc.Replay(context.Background(), "orgA", "dlv_missing"); err != ErrNotFound {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestRecover_ReenqueuesNonTerminalAfterRestart(t *testing.T) {
	// Shared durable state (store + log); the queue is the volatile part lost on crash.
	store := newStore()
	log := newLog()
	deliv := newDeliverer()
	clk := newClock()
	store.add(Subscription{ID: "s1", OrgID: "orgA", EventTypes: []string{"*"}, URL: "https://a.example/ok", Secret: "k1", Active: true})

	// Instance A emits but "crashes" before its queue drains.
	qA := newQueue()
	svcA := NewService(Deps{Store: store, Queue: qA, Deliverer: deliv, Signer: recordingSigner{}, Log: log, Clock: clk, IDGen: &seqIDGen{}, Rand: fixedRand{v: 0.5}, Policy: fastPolicy})
	if err := svcA.Emit(context.Background(), mkEvent("e1", "drive.file.created", "orgA", nil)); err != nil {
		t.Fatalf("emit: %v", err)
	}
	id := DeliveryID("s1", "e1")
	if e, _ := log.get(id); e.delivery.Status != StatusPending {
		t.Fatalf("pre-crash status = %s, want pending", e.delivery.Status)
	}

	// Instance B boots with a fresh queue and recovers.
	qB := newQueue()
	svcB := NewService(Deps{Store: store, Queue: qB, Deliverer: deliv, Signer: recordingSigner{}, Log: log, Clock: clk, IDGen: &seqIDGen{}, Rand: fixedRand{v: 0.5}, Policy: fastPolicy})
	n, err := svcB.Recover(context.Background(), "orgA")
	if err != nil || n != 1 {
		t.Fatalf("recover = %d, %v; want 1, nil", n, err)
	}
	if err := qB.drain(context.Background(), svcB, clk, 10); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if e, _ := log.get(id); e.delivery.Status != StatusDelivered {
		t.Fatalf("post-recover status = %s, want delivered", e.delivery.Status)
	}
	// Recovering again re-enqueues nothing (now terminal).
	if n, _ := svcB.Recover(context.Background(), "orgA"); n != 0 {
		t.Fatalf("second recover re-enqueued %d, want 0", n)
	}
}

func TestRotateSecret(t *testing.T) {
	h := newHarness(fastPolicy)
	sub, err := h.svc.Subscribe(context.Background(), NewSubscription{OrgID: "orgA", EventTypes: []string{"*"}, URL: "https://a/ok", Secret: "whsec_old"})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	newSecret, err := h.svc.RotateSecret(context.Background(), "orgA", sub.ID)
	if err != nil {
		t.Fatalf("rotate: %v", err)
	}
	if newSecret == "whsec_old" || newSecret == "" {
		t.Fatalf("secret not rotated: %q", newSecret)
	}
	got, _ := h.store.Get(context.Background(), "orgA", sub.ID)
	if got.Secret != newSecret {
		t.Fatalf("stored secret = %q, want %q", got.Secret, newSecret)
	}
	// Cross-org rotation is denied.
	if _, err := h.svc.RotateSecret(context.Background(), "orgB", sub.ID); err != ErrNotFound {
		t.Fatalf("cross-org rotate want ErrNotFound, got %v", err)
	}
}

func TestProcess_HonorsRetryAfter(t *testing.T) {
	// Base backoff is tiny; Retry-After (5s) must win, capped by MaxDelay (1m).
	policy := RetryPolicy{MaxAttempts: 3, BaseDelay: time.Millisecond, MaxDelay: time.Minute, Multiplier: 2, JitterFrac: 0}
	h := newHarness(policy)
	h.store.add(Subscription{ID: "s1", OrgID: "orgA", EventTypes: []string{"*"}, URL: "https://a/429", Secret: "k", Active: true})
	h.deliv.byURL["https://a/429"] = 429
	h.deliv.retryAfter["https://a/429"] = 5 * time.Second

	if err := h.svc.Emit(context.Background(), mkEvent("e1", "t", "orgA", nil)); err != nil {
		t.Fatalf("emit: %v", err)
	}
	// Process just the first job (do not fully drain) to inspect the scheduled retry.
	first := h.queue.pending()[0]
	h.queue.jobs = nil
	if err := h.svc.Process(context.Background(), first); err != nil {
		t.Fatalf("process: %v", err)
	}
	retry := h.queue.pending()[0]
	if got := retry.NotBefore.Sub(retry.Delivery.UpdatedAt); got != 5*time.Second {
		t.Fatalf("retry delay = %v, want 5s (Retry-After honored)", got)
	}
}

func TestPing_DeliversRegardlessOfFilter(t *testing.T) {
	h := newHarness(fastPolicy)
	// Subscription only listens to an unrelated type — Ping must still reach it.
	sub, _ := h.svc.Subscribe(context.Background(), NewSubscription{OrgID: "orgA", EventTypes: []string{"only.this"}, URL: "https://a/ok", Secret: "k"})
	del, err := h.svc.Ping(context.Background(), "orgA", sub.ID)
	if err != nil {
		t.Fatalf("ping: %v", err)
	}
	if del.EventType != PingEventType || del.Status != StatusPending {
		t.Fatalf("bad ping delivery: %+v", del)
	}
	if err := h.queue.drain(context.Background(), h.svc, h.clock, 5); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if !contains(h.deliv.urls(), "https://a/ok") {
		t.Fatalf("ping was not delivered: %v", h.deliv.urls())
	}
	// Cross-org ping denied.
	if _, err := h.svc.Ping(context.Background(), "orgB", sub.ID); err != ErrNotFound {
		t.Fatalf("cross-org ping want ErrNotFound, got %v", err)
	}
}

func TestProcessRef_RehydratesAndProcesses(t *testing.T) {
	h := newHarness(fastPolicy)
	h.store.add(Subscription{ID: "s1", OrgID: "orgA", EventTypes: []string{"*"}, URL: "https://a/ok", Secret: "k", Active: true})
	// Emit records the delivery + snapshot and enqueues the (full) job; take its ref.
	if err := h.svc.Emit(context.Background(), mkEvent("e1", "drive.file.created", "orgA", map[string]int{"n": 1})); err != nil {
		t.Fatalf("emit: %v", err)
	}
	ref := h.queue.pending()[0].Ref()
	// The ref is id-only.
	if ref.DeliveryID == "" || ref.SubscriptionID != "s1" || ref.EventID != "e1" {
		t.Fatalf("bad ref: %+v", ref)
	}
	// ProcessRef rehydrates from store+log and delivers.
	if err := h.svc.ProcessRef(context.Background(), ref); err != nil {
		t.Fatalf("processref: %v", err)
	}
	if !contains(h.deliv.urls(), "https://a/ok") {
		t.Fatalf("rehydrated job not delivered: %v", h.deliv.urls())
	}
	if e, _ := h.log.get(ref.DeliveryID); e.delivery.Status != StatusDelivered {
		t.Fatalf("status = %s, want delivered", e.delivery.Status)
	}
}

func TestProcessRef_DropsTerminalAndMissing(t *testing.T) {
	h := newHarness(fastPolicy)
	h.store.add(Subscription{ID: "s1", OrgID: "orgA", EventTypes: []string{"*"}, URL: "https://a/ok", Secret: "k", Active: true})

	// A terminal (delivered) delivery: a duplicate ref must NOT re-deliver.
	id := DeliveryID("s1", "e1")
	_ = h.log.Record(context.Background(), DeliveryRecord{
		Delivery: Delivery{ID: id, OrgID: "orgA", SubscriptionID: "s1", EventID: "e1", Status: StatusDelivered},
		Event:    EventSnapshot{ID: "e1", Type: "t", OrgID: "orgA", Payload: []byte(`{}`)},
	})
	ref := JobRef{OrgID: "orgA", DeliveryID: id, SubscriptionID: "s1", EventID: "e1", AttemptN: 1}
	if err := h.svc.ProcessRef(context.Background(), ref); err != nil {
		t.Fatalf("processref: %v", err)
	}
	if len(h.deliv.urls()) != 0 {
		t.Fatalf("duplicate ref re-delivered a terminal delivery: %v", h.deliv.urls())
	}

	// A ref whose delivery/subscription does not exist is dropped (no error).
	missing := JobRef{OrgID: "orgA", DeliveryID: "dlv_missing", SubscriptionID: "ghost", EventID: "e9", AttemptN: 1}
	if err := h.svc.ProcessRef(context.Background(), missing); err != nil {
		t.Fatalf("processref missing: %v", err)
	}
}

// --- fault injection ---

type errStore struct {
	*fakeStore
	listErr error
}

func (e errStore) ListForOrgAndType(ctx context.Context, orgID, typ string) ([]Subscription, error) {
	if e.listErr != nil {
		return nil, e.listErr
	}
	return e.fakeStore.ListForOrgAndType(ctx, orgID, typ)
}

type errLog struct {
	*fakeLog
	recordErr error
}

func (e errLog) Record(ctx context.Context, rec DeliveryRecord) error {
	if e.recordErr != nil {
		return e.recordErr
	}
	return e.fakeLog.Record(ctx, rec)
}

func TestEmit_StoreErrorPropagates(t *testing.T) {
	h := newHarness(fastPolicy)
	boom := errors.New("store down")
	h.svc = NewService(Deps{
		Store: errStore{fakeStore: h.store, listErr: boom}, Queue: h.queue, Deliverer: h.deliv,
		Signer: recordingSigner{}, Log: h.log, Clock: h.clock, IDGen: h.idgen, Rand: fixedRand{v: 0.5}, Policy: fastPolicy,
	})
	if err := h.svc.Emit(context.Background(), mkEvent("e1", "t", "orgA", nil)); !errors.Is(err, boom) {
		t.Fatalf("want store error, got %v", err)
	}
}

func TestProcess_LogErrorReturned(t *testing.T) {
	h := newHarness(fastPolicy)
	boom := errors.New("log down")
	h.svc = NewService(Deps{
		Store: h.store, Queue: h.queue, Deliverer: h.deliv, Signer: recordingSigner{},
		Log: errLog{fakeLog: h.log, recordErr: boom}, Clock: h.clock, IDGen: h.idgen, Rand: fixedRand{v: 0.5}, Policy: fastPolicy,
	})
	job := Job{
		Delivery:     Delivery{ID: "d1", OrgID: "orgA"},
		Event:        mkEvent("e1", "t", "orgA", nil),
		Subscription: Subscription{ID: "s1", OrgID: "orgA", URL: "https://a/ok", Secret: "k"},
		AttemptN:     1,
	}
	if err := h.svc.Process(context.Background(), job); !errors.Is(err, boom) {
		t.Fatalf("want log error, got %v", err)
	}
}

func TestRecover_SkipsDeliveriesWithNoSubscription(t *testing.T) {
	h := newHarness(fastPolicy)
	// A pending delivery whose subscription does not exist in the store.
	_ = h.log.Record(context.Background(), DeliveryRecord{
		Delivery: Delivery{ID: "d1", OrgID: "orgA", SubscriptionID: "ghost", Status: StatusPending},
		Event:    EventSnapshot{ID: "e1", Type: "t", OrgID: "orgA", Payload: []byte(`{}`)},
	})
	n, err := h.svc.Recover(context.Background(), "orgA")
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if n != 0 {
		t.Fatalf("recovered %d, want 0 (subscription gone)", n)
	}
}

func contains(ss []string, want string) bool {
	for _, s := range ss {
		if s == want {
			return true
		}
	}
	return false
}
