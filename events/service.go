package events

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"
)

// Header names carried on every outbound delivery. Subscribers verify the
// signature and (optionally) reject stale timestamps to defeat replay attacks.
const (
	HeaderEventID   = "X-Event-Id"
	HeaderEventType = "X-Event-Type"
	HeaderDelivery  = "X-Delivery-Id"
	HeaderTimestamp = "X-Event-Timestamp" // unix seconds, from Clock at send time
	HeaderSignature = "X-Signature"       // Signer output over the raw body
	HeaderAttempt   = "X-Attempt"         // 1-based attempt number
)

// Emitter is the typed driving API a service calls. Emit fans an event out to an
// org's matching subscriptions and drives delivery+retry+log; the remaining
// methods are admin operations. Every method is org-scoped.
type Emitter interface {
	Emit(ctx context.Context, ev Event) error

	Subscribe(ctx context.Context, in NewSubscription) (Subscription, error)
	Unsubscribe(ctx context.Context, orgID, subscriptionID string) error
	ListSubscriptions(ctx context.Context, orgID string) ([]Subscription, error)

	ListDeliveries(ctx context.Context, orgID string, f DeliveryFilter) (page []Delivery, nextCursor string, err error)
	Replay(ctx context.Context, orgID, deliveryID string) error
}

// NewSubscription is the input to Subscribe.
type NewSubscription struct {
	OrgID      string
	EventTypes []string
	URL        string
	Secret     string
	// PayloadFilters optionally narrow delivery by values inside the event
	// payload (in addition to the event-type filter). All must match; empty
	// matches every payload.
	PayloadFilters []PayloadFilter
}

// URLValidator optionally vets a subscription URL before it is stored. Supply
// one that rejects internal/metadata addresses to guard against SSRF. Nil
// disables validation (NOT recommended in production).
type URLValidator func(url string) error

// Deps bundles the ports the Service needs.
type Deps struct {
	Store     SubscriptionStore
	Queue     DeliveryQueue
	Deliverer Deliverer
	Signer    Signer
	Log       DeliveryLog
	Clock     Clock
	IDGen     IDGen
	Rand      Rand
	Policy    RetryPolicy
	// ValidateURL is invoked by Subscribe; nil disables URL validation.
	ValidateURL URLValidator
	// Observer is an optional per-attempt hook for metrics/logging; nil disables.
	Observer Observer
	// Catalog optionally declares the service's event vocabulary. When it is set
	// and strict (NewCatalog(true)), Emit rejects events whose Type it does not
	// contain (ErrUnknownEventType). Nil disables catalog enforcement.
	Catalog *Catalog
	// Transformer is an optional per-endpoint hook that reshapes the outbound body
	// before signing; nil delivers the event payload verbatim.
	Transformer Transformer
	// MaxPayloadBytes optionally caps the size of an event payload accepted by
	// Emit; a larger payload is rejected with ErrPayloadTooLarge before any fan-out
	// (fail fast instead of at the store or on the wire). 0 disables the cap.
	MaxPayloadBytes int
}

// Service is the concrete implementation of Emitter and Dispatcher. It is pure
// with respect to time and randomness: all such inputs arrive via Deps ports.
type Service struct {
	store       SubscriptionStore
	queue       DeliveryQueue
	deliverer   Deliverer
	signer      Signer
	log         DeliveryLog
	clock       Clock
	idgen       IDGen
	rand        Rand
	policy      RetryPolicy
	validate    URLValidator
	observer    Observer
	catalog     *Catalog
	transformer Transformer
	maxPayload  int
}

var (
	_ Emitter       = (*Service)(nil)
	_ Dispatcher    = (*Service)(nil)
	_ Rehydrator    = (*Service)(nil)
	_ RefDispatcher = (*Service)(nil)
)

// NewService constructs a Service. It panics if a required port is nil, since a
// misconfigured emitter is a programming error, not a runtime condition.
func NewService(d Deps) *Service {
	switch {
	case d.Store == nil:
		panic("events: Deps.Store is required")
	case d.Queue == nil:
		panic("events: Deps.Queue is required")
	case d.Deliverer == nil:
		panic("events: Deps.Deliverer is required")
	case d.Signer == nil:
		panic("events: Deps.Signer is required")
	case d.Log == nil:
		panic("events: Deps.Log is required")
	case d.Clock == nil:
		panic("events: Deps.Clock is required")
	case d.IDGen == nil:
		panic("events: Deps.IDGen is required")
	case d.Rand == nil:
		panic("events: Deps.Rand is required")
	}
	p := d.Policy
	if p.MaxAttempts == 0 {
		p = DefaultRetryPolicy()
	}
	return &Service{
		store:       d.Store,
		queue:       d.Queue,
		deliverer:   d.Deliverer,
		signer:      d.Signer,
		log:         d.Log,
		clock:       d.Clock,
		idgen:       d.IDGen,
		rand:        d.Rand,
		policy:      p,
		validate:    d.ValidateURL,
		observer:    d.Observer,
		catalog:     d.Catalog,
		transformer: d.Transformer,
		maxPayload:  d.MaxPayloadBytes,
	}
}

// Emit looks up the emitting org's matching subscriptions and enqueues a
// delivery job (with a stable, idempotent delivery id) for each. It records each
// delivery as pending in the log up-front (persisting the event snapshot so the
// delivery can later be replayed). Fan-out continues past a per-subscription
// error; the joined error is returned.
func (s *Service) Emit(ctx context.Context, ev Event) error {
	if ev.ID == "" || ev.Type == "" || ev.OrgID == "" {
		return ErrInvalidEvent
	}
	// Optional payload-size guard: reject oversized events up front rather than
	// failing later at the store (e.g. DynamoDB's 400 KB item limit) or on the wire.
	if s.maxPayload > 0 && len(ev.Payload) > s.maxPayload {
		return ErrPayloadTooLarge
	}
	// A strict catalog is the event vocabulary's gate: reject any type that was
	// never declared, so typos and undeclared events fail fast at emit time.
	if s.catalog != nil && s.catalog.Strict() && !s.catalog.Has(ev.Type) {
		return ErrUnknownEventType
	}
	subs, err := s.store.ListForOrgAndType(ctx, ev.OrgID, ev.Type)
	if err != nil {
		return err
	}
	now := s.clock.Now()
	var errs []error
	for _, sub := range subs {
		// Defense in depth: the store is the tenancy boundary, but never fan an
		// org's event out to a subscription belonging to another org.
		if sub.OrgID != ev.OrgID {
			errs = append(errs, ErrTenancyViolation)
			continue
		}
		// Content filter: the store matched the event type; also honor any
		// per-subscription payload filters before creating a delivery.
		if !sub.MatchesPayload(ev.Payload) {
			continue
		}
		del := Delivery{
			ID:             DeliveryID(sub.ID, ev.ID),
			OrgID:          ev.OrgID,
			SubscriptionID: sub.ID,
			EventID:        ev.ID,
			EventType:      ev.Type,
			Status:         StatusPending,
			Attempts:       0,
			CreatedAt:      now,
			UpdatedAt:      now,
		}
		if err := s.log.Record(ctx, DeliveryRecord{Delivery: del, Event: snapshot(ev)}); err != nil {
			errs = append(errs, err)
			continue
		}
		job := Job{Delivery: del, Event: ev, Subscription: sub, AttemptN: 1, NotBefore: now}
		if err := s.queue.Enqueue(ctx, job); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// NewEvent builds an Event with a freshly minted id and OccurredAt set from the
// clock, JSON-marshalling payload. It saves callers the boilerplate of wiring an
// IDGen/Clock and marshalling by hand. The payload may be any JSON-marshalable
// value, or a []byte / json.RawMessage which is used verbatim.
func (s *Service) NewEvent(eventType, orgID string, payload any) (Event, error) {
	raw, err := toRawJSON(payload)
	if err != nil {
		return Event{}, err
	}
	return Event{
		ID:         s.idgen.NewID(),
		Type:       eventType,
		OrgID:      orgID,
		OccurredAt: s.clock.Now(),
		Payload:    raw,
	}, nil
}

// EmitNew builds an event via NewEvent and emits it in one call.
func (s *Service) EmitNew(ctx context.Context, eventType, orgID string, payload any) error {
	ev, err := s.NewEvent(eventType, orgID, payload)
	if err != nil {
		return err
	}
	return s.Emit(ctx, ev)
}

func toRawJSON(payload any) (json.RawMessage, error) {
	switch p := payload.(type) {
	case nil:
		return json.RawMessage("null"), nil
	case json.RawMessage:
		return p, nil
	case []byte:
		return json.RawMessage(p), nil
	default:
		b, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		return b, nil
	}
}

// Process executes one delivery attempt for a job: it signs the raw body, POSTs
// it, records the attempt, and then either marks the delivery delivered,
// schedules a backed-off retry, or dead-letters it once attempts are exhausted.
// It is the handler a DeliveryQueue adapter's workers invoke.
//
// A retry (AttemptN > 1) acts on the CURRENT subscription, not the copy the job
// was built with: Process re-reads it from the store, so a retry after
// RotateSecret is signed with the new secret and a retry after Unsubscribe is
// dropped without a POST (nil) — the same as the durable ProcessRef path. If the
// store cannot be read (an error other than ErrNotFound) the job's copy is used,
// as before, rather than losing the retry.
//
// An attempt that got no answer because ctx was CANCELLED (a queue stopping, a
// process shutting down) is not a delivery verdict: nothing is recorded, the
// Observer is not called, no retry is scheduled, and ctx.Err() is returned, so
// the delivery keeps its previous (non-terminal) state for Recover or for the
// durable queue's redelivery. A deadline (context.DeadlineExceeded) is still a
// failed attempt.
func (s *Service) Process(ctx context.Context, job Job) error {
	if job.Event.OrgID != job.Subscription.OrgID {
		return ErrTenancyViolation
	}
	if job.AttemptN > 1 && job.Subscription.ID != "" {
		fresh, err := s.store.Get(ctx, job.Subscription.OrgID, job.Subscription.ID)
		switch {
		case errors.Is(err, ErrNotFound):
			return nil // deleted since the job was built — nothing to do (as ProcessRef)
		case err != nil:
			// Store unreachable: keep the job's copy (v0.2.0 behaviour) rather than drop the retry.
		case fresh.OrgID != job.Event.OrgID:
			return ErrTenancyViolation
		default:
			job.Subscription = fresh
		}
	}
	return s.process(ctx, job, false)
}

// process is one attempt. durable is true on the ProcessRef path: there a failed
// attempt's retry is ARMED in two log writes (see the retry branch) so that a
// redelivered copy of an attempt whose retry is already scheduled can be told
// apart from one whose retry was never scheduled.
func (s *Service) process(ctx context.Context, job Job, durable bool) error {
	if job.Event.OrgID != job.Subscription.OrgID {
		return ErrTenancyViolation // re-asserted for every caller (ProcessRef included)
	}
	body := []byte(job.Event.Payload)
	start := s.clock.Now()

	var resp DeliveryResponse
	var postErr error

	// Optional per-endpoint transform, applied BEFORE signing so the signature
	// covers exactly what is sent. A transform error aborts the attempt without a
	// POST; it is recorded as a failed attempt and retried by the normal policy.
	if s.transformer != nil {
		if b, terr := s.transformer.Transform(ctx, job.Subscription, job.Event, body); terr != nil {
			postErr = errors.New("events: transform failed: " + terr.Error())
		} else {
			body = b
		}
	}

	if postErr == nil {
		// Start from the event's custom headers (e.g. a W3C traceparent), skipping
		// any reserved name so a caller can never override the idempotency/auth
		// headers.
		headers := make(map[string]string, len(job.Event.Headers)+6)
		for k, v := range job.Event.Headers {
			if isReservedHeader(k) {
				continue
			}
			headers[k] = v
		}
		// Common idempotency headers set by the core; the Signer owns the signature
		// (and, per scheme, the timestamp) headers, which are merged on top. Both are
		// applied after the custom headers, so reserved names always win.
		headers["Content-Type"] = "application/json"
		headers[HeaderEventID] = job.Event.ID
		headers[HeaderEventType] = job.Event.Type
		headers[HeaderDelivery] = job.Delivery.ID
		headers[HeaderAttempt] = strconv.Itoa(job.AttemptN)
		for k, v := range s.signer.Sign(SigningInput{
			EventID:    job.Event.ID,
			DeliveryID: job.Delivery.ID,
			EventType:  job.Event.Type,
			Timestamp:  start,
			Body:       body,
			Secret:     job.Subscription.Secret,
		}) {
			headers[k] = v
		}

		resp, postErr = s.deliverer.Post(ctx, DeliveryRequest{URL: job.Subscription.URL, Body: body, Headers: headers})
	}
	end := s.clock.Now()

	// No receiver answer and OUR context was cancelled: a shutdown, not a verdict.
	// Do not record it (a final attempt would otherwise be dead-lettered and counted
	// by autodisable against an endpoint that was never asked).
	if postErr != nil && resp.StatusCode == 0 && errors.Is(ctx.Err(), context.Canceled) {
		return ctx.Err()
	}

	att := Attempt{
		N:       job.AttemptN,
		At:      start,
		Code:    resp.StatusCode,
		Latency: end.Sub(start),
	}

	success := postErr == nil && resp.StatusCode >= 200 && resp.StatusCode < 300
	if !success {
		att.Error = failureReason(resp.StatusCode, resp.Body, postErr)
	}

	del := job.Delivery
	del.Attempts = job.AttemptN
	del.UpdatedAt = end

	switch {
	case success:
		del.Status = StatusDelivered
		del.LastError = ""
	case job.AttemptN >= s.policy.maxAttempts() || !s.policy.retryable(resp.StatusCode, postErr):
		// Exhausted, or a permanent failure not worth retrying: dead-letter.
		del.Status = StatusDead
		del.LastError = att.Error
	default:
		del.Status = StatusFailed
		del.LastError = att.Error
	}

	rec := del
	if durable && del.Status == StatusFailed {
		// Durable path, write 1 of 2: append the attempt but keep the delivery's
		// attempt count at AttemptN-1 until the retry is actually scheduled (write 2
		// below; for a retry due within retryArmGuard, until that retry runs). The
		// Observer and the next job still see AttemptN, as before.
		// If the retry is never scheduled (Enqueue fails, or the process dies
		// here), a redelivered copy of this attempt still looks current and is
		// re-run — exactly as before — instead of being dropped as stale and lost.
		rec.Attempts = job.AttemptN - 1
	}
	if err := s.log.Record(ctx, DeliveryRecord{Delivery: rec, Event: snapshot(job.Event), Attempt: att}); err != nil {
		return err
	}
	if s.observer != nil {
		s.observer.OnAttempt(ctx, del, att)
	}

	if del.Status != StatusFailed {
		return nil // delivered or dead — no retry
	}

	// Re-enqueue with backoff, honoring a subscriber-supplied Retry-After as a
	// lower bound (capped by MaxDelay so a hostile value can't park a job forever).
	delay := s.policy.Backoff(job.AttemptN, s.rand.Float64())
	if resp.RetryAfter > delay {
		delay = resp.RetryAfter
	}
	if s.policy.MaxDelay > 0 && delay > s.policy.MaxDelay {
		delay = s.policy.MaxDelay
	}
	// Absolute ceiling, enforced even when MaxDelay == 0 ("uncapped"), so a hostile
	// Retry-After can never park a retry beyond AbsoluteMaxRetryDelay.
	if delay > AbsoluteMaxRetryDelay {
		delay = AbsoluteMaxRetryDelay
	}
	next := job
	next.Delivery = del
	next.AttemptN = job.AttemptN + 1
	next.NotBefore = end.Add(delay)
	if err := s.queue.Enqueue(ctx, next); err != nil {
		return err
	}
	if durable && next.NotBefore.Sub(s.clock.Now()) >= retryArmGuard {
		// Write 2 of 2: the retry is scheduled, so the attempt count advances to
		// AttemptN; from now on ProcessRef drops a redelivered copy of this attempt
		// as stale (no second POST, no second retry chain). Best effort: if this
		// write fails the job's work is still done (attempt recorded, retry
		// scheduled); the only cost is that such a duplicate is re-run as in v0.2.0.
		// Skipped when the retry is due within retryArmGuard: a consumer could then
		// run (and record) the retry before this write lands, and this older state
		// would overwrite the newer one. Unarmed, the count catches up when the retry
		// itself runs, which for such a short delay is almost at once.
		_ = s.log.Record(ctx, DeliveryRecord{Delivery: del, Event: snapshot(job.Event)})
	}
	return nil
}

// retryArmGuard is how far in the future a durable retry must be scheduled for
// process to arm stale-duplicate detection (write 2). It covers the earliest a
// durable consumer may pick the retry up (sqsqueue runs a message up to ~1.5 s
// before its NotBefore: whole-second SQS delays plus a 1 s re-defer threshold)
// plus a generous allowance for the log write's own latency.
const retryArmGuard = 5 * time.Second

// reservedHeaders are the outbound header names the core and signer own; a
// custom Event.Header may not override them. Compared case-insensitively.
var reservedHeaders = map[string]struct{}{
	"content-type":                   {},
	"host":                           {},
	"content-length":                 {},
	strings.ToLower(HeaderEventID):   {},
	strings.ToLower(HeaderEventType): {},
	strings.ToLower(HeaderDelivery):  {},
	strings.ToLower(HeaderTimestamp): {},
	strings.ToLower(HeaderSignature): {},
	strings.ToLower(HeaderAttempt):   {},
	// Standard Webhooks signer header names (protected even though the signer
	// applies them last, so a bogus value never reaches the durable snapshot path).
	"webhook-id":        {},
	"webhook-timestamp": {},
	"webhook-signature": {},
}

// isReservedHeader reports whether name is a header the core/signer owns.
func isReservedHeader(name string) bool {
	_, ok := reservedHeaders[strings.ToLower(name)]
	return ok
}

// failureReason builds a compact, secret-free description of a failed attempt.
func failureReason(code int, respBody []byte, err error) string {
	if err != nil {
		return err.Error()
	}
	msg := "http " + strconv.Itoa(code)
	if snip := snippet(respBody, 256); snip != "" {
		msg += ": " + snip
	}
	return msg
}

func snippet(b []byte, max int) string {
	s := strings.TrimSpace(string(b))
	if len(s) > max {
		s = s[:max] + "…"
	}
	// Collapse newlines so a multi-line HTML error page stays a single log line.
	return strings.Join(strings.Fields(s), " ")
}

// Subscribe validates and stores a new subscription for an org, assigning an id.
func (s *Service) Subscribe(ctx context.Context, in NewSubscription) (Subscription, error) {
	if in.OrgID == "" || in.URL == "" || in.Secret == "" || len(in.EventTypes) == 0 {
		return Subscription{}, ErrInvalidSubscription
	}
	if s.validate != nil {
		if err := s.validate(in.URL); err != nil {
			return Subscription{}, err
		}
	}
	sub := Subscription{
		ID:             s.idgen.NewID(),
		OrgID:          in.OrgID,
		EventTypes:     append([]string(nil), in.EventTypes...),
		URL:            in.URL,
		Secret:         in.Secret,
		Active:         true,
		CreatedAt:      s.clock.Now(),
		PayloadFilters: append([]PayloadFilter(nil), in.PayloadFilters...),
	}
	if err := s.store.Put(ctx, sub); err != nil {
		return Subscription{}, err
	}
	return sub, nil
}

// Unsubscribe removes a subscription within the caller's org.
func (s *Service) Unsubscribe(ctx context.Context, orgID, subscriptionID string) error {
	return s.store.Delete(ctx, orgID, subscriptionID)
}

// ListSubscriptions returns all of an org's subscriptions.
func (s *Service) ListSubscriptions(ctx context.Context, orgID string) ([]Subscription, error) {
	return s.store.ListForOrg(ctx, orgID)
}

// GetSubscription returns one of an org's subscriptions.
func (s *Service) GetSubscription(ctx context.Context, orgID, subscriptionID string) (Subscription, error) {
	sub, err := s.store.Get(ctx, orgID, subscriptionID)
	if err != nil {
		return Subscription{}, err
	}
	if sub.OrgID != orgID {
		return Subscription{}, ErrNotFound
	}
	return sub, nil
}

// GetDelivery returns one delivery with its stored event snapshot and every
// recorded attempt — the detail view behind the delivery log.
func (s *Service) GetDelivery(ctx context.Context, orgID, deliveryID string) (Delivery, EventSnapshot, []Attempt, error) {
	del, snap, atts, err := s.log.Get(ctx, orgID, deliveryID)
	if err != nil {
		return Delivery{}, EventSnapshot{}, nil, err
	}
	if del.OrgID != orgID {
		return Delivery{}, EventSnapshot{}, nil, ErrNotFound
	}
	return del, snap, atts, nil
}

// ListDeliveries returns a page of an org's deliveries (optionally filtered by
// subscription) plus an opaque cursor for the next page.
func (s *Service) ListDeliveries(ctx context.Context, orgID string, f DeliveryFilter) ([]Delivery, string, error) {
	return s.log.List(ctx, orgID, f)
}

// Rehydrate reconstructs a full Job from a durable JobRef: it reloads the event
// snapshot from the delivery log and the subscription from the store. It returns
// ErrNotFound if either is gone (the delivery was never recorded, or the
// subscription was deleted), which lets a queue adapter drop the message.
func (s *Service) Rehydrate(ctx context.Context, ref JobRef) (Job, error) {
	del, snap, _, err := s.log.Get(ctx, ref.OrgID, ref.DeliveryID)
	if err != nil {
		return Job{}, err
	}
	if del.OrgID != ref.OrgID {
		return Job{}, ErrNotFound
	}
	sub, err := s.store.Get(ctx, ref.OrgID, ref.SubscriptionID)
	if err != nil {
		return Job{}, err
	}
	if sub.OrgID != ref.OrgID {
		return Job{}, ErrTenancyViolation
	}
	attemptN := ref.AttemptN
	if attemptN < 1 {
		attemptN = 1
	}
	return Job{Delivery: del, Event: snap.event(), Subscription: sub, AttemptN: attemptN, NotBefore: ref.NotBefore}, nil
}

// ProcessRef rehydrates and dispatches a durable job reference. It is the handler
// an off-process queue adapter's workers invoke. It is safe against the
// at-least-once redelivery such queues exhibit:
//   - a reference whose delivery/subscription no longer exists is dropped (nil);
//   - a delivery already in a terminal state (delivered or dead) is dropped, so a
//     duplicate message never causes a duplicate POST;
//   - a stale reference — the delivery is failed and its log already counts this
//     attempt (ref.AttemptN <= Delivery.Attempts), i.e. the attempt ran and its
//     retry was scheduled — is dropped, so a redelivered copy of a failed attempt
//     neither re-POSTs nor forks a second retry chain. (On this path the attempt
//     count is advanced only after the retry is enqueued; see process.)
//
// Not covered: two copies of the same attempt processed CONCURRENTLY (both read
// the log before either records) can still both POST; that needs a lease.
func (s *Service) ProcessRef(ctx context.Context, ref JobRef) error {
	job, err := s.Rehydrate(ctx, ref)
	switch {
	case errors.Is(err, ErrNotFound):
		return nil // referent gone — nothing to do
	case err != nil:
		return err
	}
	if job.Delivery.Status == StatusDelivered || job.Delivery.Status == StatusDead {
		return nil // already terminal — drop duplicate
	}
	if job.Delivery.Status == StatusFailed && job.AttemptN <= job.Delivery.Attempts {
		return nil // stale duplicate — this attempt ran and its retry is scheduled
	}
	// Rehydrate just read the subscription from the store; no second read needed.
	return s.process(ctx, job, true)
}

// PingEventType is the type of the synthetic event Ping delivers.
const PingEventType = "webhook.ping"

// Ping delivers a synthetic webhook.ping event to a single subscription (no
// fan-out and regardless of the subscription's event-type filter or paused
// state), so an org can verify an endpoint from the admin UI. It returns the
// created delivery; the actual POST happens asynchronously via the queue.
func (s *Service) Ping(ctx context.Context, orgID, subscriptionID string) (Delivery, error) {
	sub, err := s.store.Get(ctx, orgID, subscriptionID)
	if err != nil {
		return Delivery{}, err
	}
	if sub.OrgID != orgID {
		return Delivery{}, ErrNotFound
	}
	now := s.clock.Now()
	ev := Event{ID: s.idgen.NewID(), Type: PingEventType, OrgID: orgID, OccurredAt: now, Payload: json.RawMessage(`{"ping":true}`)}
	del := Delivery{
		ID: DeliveryID(sub.ID, ev.ID), OrgID: orgID, SubscriptionID: sub.ID,
		EventID: ev.ID, EventType: ev.Type, Status: StatusPending, CreatedAt: now, UpdatedAt: now,
	}
	if err := s.log.Record(ctx, DeliveryRecord{Delivery: del, Event: snapshot(ev)}); err != nil {
		return Delivery{}, err
	}
	job := Job{Delivery: del, Event: ev, Subscription: sub, AttemptN: 1, NotBefore: now}
	if err := s.queue.Enqueue(ctx, job); err != nil {
		return Delivery{}, err
	}
	return del, nil
}

// Recover re-enqueues every non-terminal delivery (pending or failed) for an org
// from the delivery log, rebuilding each job from the stored event snapshot and
// the current subscription. Run it at startup so retries that were in-flight when
// an in-memory queue's process died are not lost. It returns the number of
// deliveries re-enqueued. Deliveries whose subscription no longer exists are
// skipped. Safe to run repeatedly (subscribers dedup on the stable delivery id).
func (s *Service) Recover(ctx context.Context, orgID string) (int, error) {
	requeued := 0
	cursor := ""
	for {
		page, next, err := s.log.List(ctx, orgID, DeliveryFilter{Cursor: cursor, Limit: 200})
		if err != nil {
			return requeued, err
		}
		for _, del := range page {
			if del.Status != StatusPending && del.Status != StatusFailed {
				continue
			}
			got, snap, _, err := s.log.Get(ctx, orgID, del.ID)
			if err != nil {
				continue
			}
			sub, err := s.store.Get(ctx, orgID, got.SubscriptionID)
			if err != nil || sub.OrgID != orgID {
				continue // subscription removed or cross-org — skip
			}
			attemptN := got.Attempts + 1
			if attemptN < 1 {
				attemptN = 1
			}
			job := Job{Delivery: got, Event: snap.event(), Subscription: sub, AttemptN: attemptN, NotBefore: s.clock.Now()}
			if err := s.queue.Enqueue(ctx, job); err != nil {
				return requeued, err
			}
			requeued++
		}
		if next == "" {
			return requeued, nil
		}
		cursor = next
	}
}

// RotateSecret generates a new signing secret for a subscription and persists it,
// returning the new secret. Use it to rotate a leaked or aging key without
// deleting the subscription. Org-scoped: a caller can only rotate its own org's
// subscription.
func (s *Service) RotateSecret(ctx context.Context, orgID, subscriptionID string) (string, error) {
	sub, err := s.store.Get(ctx, orgID, subscriptionID)
	if err != nil {
		return "", err
	}
	if sub.OrgID != orgID {
		return "", ErrNotFound
	}
	secret, err := s.newSecret()
	if err != nil {
		return "", err
	}
	sub.Secret = secret
	if err := s.store.Put(ctx, sub); err != nil {
		return "", err
	}
	return secret, nil
}

// newSecret mints a random "whsec_<hex>" secret using crypto/rand.
func (s *Service) newSecret() (string, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return "whsec_" + hex.EncodeToString(b[:]), nil
}

// Replay re-delivers a past delivery by id. It reconstructs the original event
// from the stored snapshot and enqueues a fresh attempt against the (current)
// subscription. The delivery id is preserved, so subscribers see it as the same
// logical delivery.
//
// It resets the delivery's log status to pending BEFORE enqueuing, so replay works
// through a durable/off-process queue too: those adapters carry only a JobRef and
// rehydrate from the log, where a terminal (delivered/dead) status would otherwise
// make ProcessRef drop the message as a duplicate — a silent no-op.
func (s *Service) Replay(ctx context.Context, orgID, deliveryID string) error {
	del, snap, _, err := s.log.Get(ctx, orgID, deliveryID)
	if err != nil {
		return err
	}
	if del.OrgID != orgID {
		return ErrNotFound
	}
	sub, err := s.store.Get(ctx, orgID, del.SubscriptionID)
	if err != nil {
		return err
	}
	if sub.OrgID != orgID {
		return ErrTenancyViolation
	}
	now := s.clock.Now()
	del.Status = StatusPending
	del.UpdatedAt = now
	// Persist the non-terminal status so a durable queue's rehydrate sees it as
	// replayable (in-process queues carry the full Job and don't rely on this).
	if err := s.log.Record(ctx, DeliveryRecord{Delivery: del, Event: snap}); err != nil {
		return err
	}
	job := Job{Delivery: del, Event: snap.event(), Subscription: sub, AttemptN: 1, NotBefore: now}
	return s.queue.Enqueue(ctx, job)
}
