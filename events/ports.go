package events

import (
	"context"
	"time"
)

// Job is one unit of work on the DeliveryQueue: a single scheduled attempt to
// deliver one Event to one Subscription. It carries the full Event and
// Subscription so an in-process worker can act without re-reading stores (a
// retry, AttemptN > 1, re-reads the subscription so rotation/deletion apply); queue
// adapters that persist off-process (e.g. SQS) should carry the JobRef instead
// (see Job.Ref) and rehydrate via Rehydrator, keeping secrets out of the queue.
type Job struct {
	Delivery     Delivery
	Event        Event
	Subscription Subscription
	AttemptN     int       // 1-based; which attempt this job represents
	NotBefore    time.Time // do not deliver before this instant (backoff)
}

// Ref returns the id-only reference for a Job — the minimal, secret-free payload
// a durable/off-process queue should persist. The event snapshot and the
// subscription (including its secret) are reloaded at consume time via Rehydrator.
func (j Job) Ref() JobRef {
	return JobRef{
		OrgID:          j.Delivery.OrgID,
		DeliveryID:     j.Delivery.ID,
		SubscriptionID: j.Delivery.SubscriptionID,
		EventID:        j.Event.ID,
		EventType:      j.Event.Type,
		AttemptN:       j.AttemptN,
		NotBefore:      j.NotBefore,
	}
}

// JobRef is a durable, secret-free reference to a queued delivery attempt. It
// holds only ids and scheduling metadata; it carries neither the event payload
// nor the subscription secret. Off-process queue adapters serialize a JobRef and
// rehydrate a full Job at consume time (see Rehydrator / RefDispatcher).
type JobRef struct {
	OrgID          string    `json:"orgId"`
	DeliveryID     string    `json:"deliveryId"`
	SubscriptionID string    `json:"subscriptionId"`
	EventID        string    `json:"eventId"`
	EventType      string    `json:"eventType,omitempty"`
	AttemptN       int       `json:"attemptN"`
	NotBefore      time.Time `json:"notBefore"`
}

// Rehydrator reconstructs a full Job from a JobRef by reloading the event
// snapshot from the DeliveryLog and the subscription from the SubscriptionStore.
// *Service implements it. Durable queue adapters depend on this instead of
// trusting queue message contents.
type Rehydrator interface {
	Rehydrate(ctx context.Context, ref JobRef) (Job, error)
}

// RefDispatcher processes a queued delivery from its id-only reference: it
// rehydrates the Job, skips references that are already terminal or whose
// subscription/delivery no longer exists, and otherwise dispatches it. *Service
// implements it; off-process queue adapters (e.g. SQS) call ProcessRef.
type RefDispatcher interface {
	ProcessRef(ctx context.Context, ref JobRef) error
}

// --- Ports the core depends on (a service supplies adapters) ---

// SubscriptionStore is the per-org subscription registry. Every method is keyed
// by orgID: the store is the hard tenancy boundary and MUST NOT return another
// org's subscriptions.
type SubscriptionStore interface {
	// ListForOrgAndType returns the active subscriptions in orgID whose event
	// types match eventType (exact or wildcard "*").
	ListForOrgAndType(ctx context.Context, orgID, eventType string) ([]Subscription, error)
	// ListForOrg returns all subscriptions (active or paused) in orgID.
	ListForOrg(ctx context.Context, orgID string) ([]Subscription, error)
	Get(ctx context.Context, orgID, subscriptionID string) (Subscription, error)
	Put(ctx context.Context, sub Subscription) error
	Delete(ctx context.Context, orgID, subscriptionID string) error
}

// DeliveryQueue accepts jobs for (eventual, at-least-once) delivery. Enqueue is
// the only method the core needs; how jobs are consumed and dispatched is an
// adapter concern (see adapters/inproc, which drives a Dispatcher).
type DeliveryQueue interface {
	Enqueue(ctx context.Context, job Job) error
}

// DeliveryRequest is a single outbound HTTP POST.
type DeliveryRequest struct {
	URL     string
	Body    []byte
	Headers map[string]string
}

// DeliveryResponse is the outcome of a DeliveryRequest.
type DeliveryResponse struct {
	StatusCode int
	Latency    time.Duration
	// Body is an optional, bounded snippet of the response body. Adapters SHOULD
	// populate it on non-2xx responses to aid debugging; it is surfaced in the
	// delivery log's attempt error. Never include secrets here.
	Body []byte
	// RetryAfter, when > 0, is the delay the subscriber asked us to wait before
	// retrying (parsed from a Retry-After header on 429/503). The core honors it
	// as a lower bound on the next backoff, capped by RetryPolicy.MaxDelay.
	RetryAfter time.Duration
}

// Deliverer performs the outbound POST. Adapters MUST apply timeouts and SHOULD
// enforce an SSRF guard on the URL (see adapters/httpdeliver).
type Deliverer interface {
	Post(ctx context.Context, req DeliveryRequest) (DeliveryResponse, error)
}

// SigningInput is everything a signing scheme may need to authenticate a
// delivery. A scheme signs some canonical string over these fields and returns
// the HTTP headers that carry the signature (and any scheme-specific id/timestamp
// headers). Passing the whole input — not just the body — lets interop schemes
// like Standard Webhooks bind the signature to the id and timestamp.
type SigningInput struct {
	EventID    string
	DeliveryID string
	EventType  string
	Timestamp  time.Time
	Body       []byte
	Secret     string
}

// Signer authenticates a delivery. It returns the auth headers to merge onto the
// request (e.g. an HMAC signature header and a timestamp header). The core sets
// the common idempotency headers; the Signer owns the signature scheme. Secrets
// must never be echoed into the returned headers.
type Signer interface {
	Sign(in SigningInput) map[string]string
}

// DeliveryRecord is one write to the delivery log: the current delivery state,
// the event snapshot (persisted on first write, enabling replay), and an
// optional attempt to append (appended when Attempt.N > 0).
type DeliveryRecord struct {
	Delivery Delivery
	Event    EventSnapshot
	Attempt  Attempt
}

// DeliveryFilter narrows and paginates a delivery-log query.
type DeliveryFilter struct {
	SubscriptionID string         // optional; empty means all subscriptions in the org
	Status         DeliveryStatus // optional; empty means any status
	Cursor         string         // opaque; empty starts at the beginning
	Limit          int            // page size; adapters may clamp
}

// DeliveryLog persists every delivery and its attempts, per org, for the
// delivery-log UI and for replay. All reads are org-scoped.
type DeliveryLog interface {
	Record(ctx context.Context, rec DeliveryRecord) error
	Get(ctx context.Context, orgID, deliveryID string) (Delivery, EventSnapshot, []Attempt, error)
	List(ctx context.Context, orgID string, f DeliveryFilter) (page []Delivery, nextCursor string, err error)
}

// --- Testability ports: the only sources of non-determinism in the core ---

// Clock is the core's sole source of the current time (no time.Now).
type Clock interface {
	Now() time.Time
}

// IDGen mints identifiers for new entities (no rand in core).
type IDGen interface {
	NewID() string
}

// Rand yields a float64 in [0,1), used for retry jitter (no rand in core).
type Rand interface {
	Float64() float64
}

// Observer is an optional hook invoked after every delivery attempt. Use it for
// metrics (counts by status, latency histograms) and structured logging without
// having to replace the DeliveryLog. It MUST NOT block or panic; it runs on the
// worker goroutine. It is called with the delivery in its post-attempt state, so
// d.Status reflects the outcome (delivered / failed / dead).
type Observer interface {
	OnAttempt(ctx context.Context, d Delivery, a Attempt)
}

// ObserverFunc adapts a function to the Observer interface.
type ObserverFunc func(ctx context.Context, d Delivery, a Attempt)

// OnAttempt implements Observer.
func (f ObserverFunc) OnAttempt(ctx context.Context, d Delivery, a Attempt) { f(ctx, d, a) }

// MultiObserver fans each attempt out to several observers in order (e.g. metrics
// plus an auto-disable breaker). Nil elements are skipped.
func MultiObserver(observers ...Observer) Observer { return multiObserver(observers) }

type multiObserver []Observer

func (m multiObserver) OnAttempt(ctx context.Context, d Delivery, a Attempt) {
	for _, o := range m {
		if o != nil {
			o.OnAttempt(ctx, d, a)
		}
	}
}

// Dispatcher processes a single queued Job: sign, POST, log, and either mark
// delivered, schedule a retry, or dead-letter. A queue adapter's worker calls
// Process; *Service implements it.
type Dispatcher interface {
	Process(ctx context.Context, job Job) error
}

// Transformer is an optional per-endpoint hook that reshapes the outbound request
// body for a specific subscription before it is signed and sent. Use it to adapt
// one canonical event to different subscriber payload shapes (wrap in an envelope,
// drop fields, rename keys) without changing the stored event. It receives the
// destination Subscription, so a single implementation can branch per endpoint.
//
// Contract:
//   - Return the (possibly rewritten) body. Return in unchanged to pass through.
//   - It runs BEFORE signing, so the signature covers exactly what is delivered.
//   - It MUST be pure w.r.t. its inputs (no wall clock / rand — the core owns
//     those) and safe for concurrent use.
//   - A non-nil error aborts the attempt without sending; it is recorded as a
//     failed attempt and retried/dead-lettered by the normal retry policy.
//   - It MUST NOT be used to inject signature/idempotency headers (custom headers
//     are Event.Headers, and reserved names are protected); it shapes the body only.
type Transformer interface {
	Transform(ctx context.Context, sub Subscription, ev Event, body []byte) ([]byte, error)
}

// TransformerFunc adapts a function to the Transformer interface.
type TransformerFunc func(ctx context.Context, sub Subscription, ev Event, body []byte) ([]byte, error)

// Transform implements Transformer.
func (f TransformerFunc) Transform(ctx context.Context, sub Subscription, ev Event, body []byte) ([]byte, error) {
	return f(ctx, sub, ev, body)
}
