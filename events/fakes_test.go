package events

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"time"
)

// --- deterministic testability ports ---

// fakeClock advances only when told; the core reads it via Clock.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock() *fakeClock { return &fakeClock{t: time.Unix(1_700_000_000, 0).UTC()} }
func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}
func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// seqIDGen returns id_1, id_2, ... deterministically.
type seqIDGen struct {
	mu sync.Mutex
	n  int
}

func (g *seqIDGen) NewID() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.n++
	return "id_" + strconv.Itoa(g.n)
}

// fixedRand returns a fixed jitter value (0.5 => jitter factor of exactly 1.0).
type fixedRand struct{ v float64 }

func (r fixedRand) Float64() float64 { return r.v }

// --- fake adapters ---

// fakeStore is an in-memory, org-partitioned SubscriptionStore.
type fakeStore struct {
	mu   sync.Mutex
	subs map[string][]Subscription // orgID -> subs
}

func newStore() *fakeStore { return &fakeStore{subs: map[string][]Subscription{}} }

func (s *fakeStore) add(sub Subscription) { s.subs[sub.OrgID] = append(s.subs[sub.OrgID], sub) }

func (s *fakeStore) ListForOrgAndType(_ context.Context, orgID, eventType string) ([]Subscription, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Subscription
	for _, sub := range s.subs[orgID] { // partitioned: only orgID's bucket
		if sub.Matches(eventType) {
			out = append(out, sub)
		}
	}
	return out, nil
}
func (s *fakeStore) ListForOrg(_ context.Context, orgID string) ([]Subscription, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Subscription(nil), s.subs[orgID]...), nil
}
func (s *fakeStore) Get(_ context.Context, orgID, subID string) (Subscription, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, sub := range s.subs[orgID] {
		if sub.ID == subID {
			return sub, nil
		}
	}
	return Subscription{}, ErrNotFound
}
func (s *fakeStore) Put(_ context.Context, sub Subscription) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, existing := range s.subs[sub.OrgID] {
		if existing.ID == sub.ID {
			s.subs[sub.OrgID][i] = sub // replace by id
			return nil
		}
	}
	s.add(sub)
	return nil
}
func (s *fakeStore) Delete(_ context.Context, orgID, subID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	kept := s.subs[orgID][:0]
	for _, sub := range s.subs[orgID] {
		if sub.ID != subID {
			kept = append(kept, sub)
		}
	}
	s.subs[orgID] = kept
	return nil
}

// fakeQueue records enqueued jobs and can be drained synchronously through a
// Dispatcher, so retry/backoff loops are testable without goroutines or timers.
type fakeQueue struct {
	mu     sync.Mutex
	jobs   []Job
	posted []Job // every job ever enqueued, in order
}

func newQueue() *fakeQueue { return &fakeQueue{} }

func (q *fakeQueue) Enqueue(_ context.Context, job Job) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.jobs = append(q.jobs, job)
	q.posted = append(q.posted, job)
	return nil
}
func (q *fakeQueue) pending() []Job {
	q.mu.Lock()
	defer q.mu.Unlock()
	return append([]Job(nil), q.jobs...)
}

// drain processes queued jobs (re-enqueues included) until the queue empties or
// max iterations are reached, advancing clk to each job's NotBefore first.
func (q *fakeQueue) drain(ctx context.Context, d Dispatcher, clk *fakeClock, max int) error {
	for i := 0; i < max; i++ {
		q.mu.Lock()
		if len(q.jobs) == 0 {
			q.mu.Unlock()
			return nil
		}
		job := q.jobs[0]
		q.jobs = q.jobs[1:]
		q.mu.Unlock()
		if clk != nil && job.NotBefore.After(clk.Now()) {
			clk.advance(job.NotBefore.Sub(clk.Now()))
		}
		if err := d.Process(ctx, job); err != nil {
			return err
		}
	}
	return nil
}

// scriptedDeliverer returns responses driven by a function of the URL.
type scriptedDeliverer struct {
	mu         sync.Mutex
	byURL      map[string]int           // url -> status code to return (0 => transport error)
	retryAfter map[string]time.Duration // url -> Retry-After to return on failure
	calls      []DeliveryRequest
}

func newDeliverer() *scriptedDeliverer {
	return &scriptedDeliverer{byURL: map[string]int{}, retryAfter: map[string]time.Duration{}}
}

func (d *scriptedDeliverer) Post(_ context.Context, req DeliveryRequest) (DeliveryResponse, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls = append(d.calls, req)
	code, ok := d.byURL[req.URL]
	if !ok {
		code = 200
	}
	if code == 0 {
		return DeliveryResponse{}, context.DeadlineExceeded
	}
	return DeliveryResponse{StatusCode: code, Latency: time.Millisecond, RetryAfter: d.retryAfter[req.URL]}, nil
}
func (d *scriptedDeliverer) lastHeaders() map[string]string {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.calls) == 0 {
		return nil
	}
	return d.calls[len(d.calls)-1].Headers
}
func (d *scriptedDeliverer) urls() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]string, len(d.calls))
	for i, c := range d.calls {
		out[i] = c.URL
	}
	return out
}

// recordingSigner is a trivial deterministic Signer for the core tests.
type recordingSigner struct{}

func (recordingSigner) Sign(in SigningInput) map[string]string {
	return map[string]string{
		"X-Signature":       "sig(" + in.Secret + "," + strconv.Itoa(len(in.Body)) + ")",
		"X-Event-Timestamp": strconv.FormatInt(in.Timestamp.Unix(), 10),
	}
}

// fakeLog captures every record for assertions and supports Get/List.
type fakeLog struct {
	mu    sync.Mutex
	byID  map[string]*logEntry
	order []string
}

type logEntry struct {
	delivery Delivery
	event    EventSnapshot
	attempts []Attempt
}

func newLog() *fakeLog { return &fakeLog{byID: map[string]*logEntry{}} }

func (l *fakeLog) Record(_ context.Context, rec DeliveryRecord) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	e, ok := l.byID[rec.Delivery.ID]
	if !ok {
		e = &logEntry{event: rec.Event}
		l.byID[rec.Delivery.ID] = e
		l.order = append(l.order, rec.Delivery.ID)
	}
	e.delivery = rec.Delivery
	if e.event.ID == "" {
		e.event = rec.Event
	}
	if rec.Attempt.N > 0 {
		e.attempts = append(e.attempts, rec.Attempt)
	}
	return nil
}
func (l *fakeLog) Get(_ context.Context, orgID, id string) (Delivery, EventSnapshot, []Attempt, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	e, ok := l.byID[id]
	if !ok || e.delivery.OrgID != orgID {
		return Delivery{}, EventSnapshot{}, nil, ErrNotFound
	}
	return e.delivery, e.event, append([]Attempt(nil), e.attempts...), nil
}
func (l *fakeLog) List(_ context.Context, orgID string, f DeliveryFilter) ([]Delivery, string, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []Delivery
	for _, id := range l.order {
		e := l.byID[id]
		if e.delivery.OrgID != orgID {
			continue
		}
		if f.SubscriptionID != "" && e.delivery.SubscriptionID != f.SubscriptionID {
			continue
		}
		if f.Status != "" && e.delivery.Status != f.Status {
			continue
		}
		out = append(out, e.delivery)
	}
	return out, "", nil
}
func (l *fakeLog) get(id string) (*logEntry, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	e, ok := l.byID[id]
	return e, ok
}

// harness wires a Service to all fakes.
type harness struct {
	svc   *Service
	store *fakeStore
	queue *fakeQueue
	deliv *scriptedDeliverer
	log   *fakeLog
	clock *fakeClock
	idgen *seqIDGen
}

func newHarness(policy RetryPolicy) *harness {
	h := &harness{
		store: newStore(),
		queue: newQueue(),
		deliv: newDeliverer(),
		log:   newLog(),
		clock: newClock(),
		idgen: &seqIDGen{},
	}
	h.svc = NewService(Deps{
		Store: h.store, Queue: h.queue, Deliverer: h.deliv, Signer: recordingSigner{},
		Log: h.log, Clock: h.clock, IDGen: h.idgen, Rand: fixedRand{v: 0.5}, Policy: policy,
	})
	return h
}

func mkEvent(id, typ, org string, payload any) Event {
	b, _ := json.Marshal(payload)
	return Event{ID: id, Type: typ, OrgID: org, OccurredAt: time.Unix(1_700_000_000, 0).UTC(), Payload: b}
}
