package events

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"time"
)

// Event is a domain event a service emits for outbound delivery.
//
// Payload is the exact JSON body that will be signed and POSTed. It is stored
// and transmitted byte-for-byte, so the HMAC signature the subscriber verifies
// is computed over precisely these bytes.
type Event struct {
	ID         string          `json:"id"`
	Type       string          `json:"type"`  // e.g. "drive.file.created"
	OrgID      string          `json:"orgId"` // tenant; hard partition key
	OccurredAt time.Time       `json:"occurredAt"`
	Payload    json.RawMessage `json:"payload"`
	// Headers are optional custom HTTP headers attached to every outbound delivery
	// of this event (e.g. a W3C "traceparent" for trace propagation). Reserved
	// headers the core and signer own (Content-Type, the X-Event-*/X-Delivery-Id/
	// X-Attempt/X-Signature idempotency+auth headers) cannot be overridden. They
	// are persisted in the event snapshot, so they survive a durable-queue round
	// trip. Values must be valid HTTP header values.
	Headers map[string]string `json:"headers,omitempty"`
}

// Subscription is one org's registration of an external endpoint for a set of
// event types. Secret is per-subscription and MUST NOT be logged.
type Subscription struct {
	ID         string    `json:"id"`
	OrgID      string    `json:"orgId"`
	EventTypes []string  `json:"eventTypes"` // exact types, or the wildcard "*"
	URL        string    `json:"url"`
	Secret     string    `json:"-"` // signing secret; never serialized/logged
	Active     bool      `json:"active"`
	CreatedAt  time.Time `json:"createdAt"`
	// PayloadFilters optionally narrow delivery by values inside the event
	// payload, in addition to the event-type filter. All must match (AND); an
	// empty slice matches every payload. See MatchesPayload.
	PayloadFilters []PayloadFilter `json:"payloadFilters,omitempty"`
}

// Matches reports whether this subscription should receive an event of the given
// type. A paused (inactive) subscription matches nothing.
//
// A pattern matches when it is:
//   - the global wildcard "*" (matches every type), or
//   - an exact string equal to eventType, or
//   - a hierarchical prefix wildcard ending in ".*" — e.g. "calendar.event.*"
//     matches "calendar.event.created" and "calendar.event.updated" but not
//     "calendar.reminder.created".
func (s Subscription) Matches(eventType string) bool {
	if !s.Active {
		return false
	}
	for _, pattern := range s.EventTypes {
		if MatchType(pattern, eventType) {
			return true
		}
	}
	return false
}

// MatchType reports whether an event-type pattern matches a concrete event type.
// See Subscription.Matches for the pattern grammar. It is exported so admin/UI
// code can validate or preview subscription filters.
func MatchType(pattern, eventType string) bool {
	switch {
	case pattern == "*" || pattern == eventType:
		return true
	case strings.HasSuffix(pattern, ".*"):
		// Keep the trailing dot so "a.b.*" matches "a.b.x" but not "a.bx".
		return strings.HasPrefix(eventType, pattern[:len(pattern)-1])
	default:
		return false
	}
}

// DeliveryStatus is the lifecycle state of a delivery to one subscription.
type DeliveryStatus string

const (
	StatusPending   DeliveryStatus = "pending"   // created, not yet attempted
	StatusDelivered DeliveryStatus = "delivered" // 2xx received
	StatusFailed    DeliveryStatus = "failed"    // last attempt failed, retry scheduled
	StatusDead      DeliveryStatus = "dead"      // max attempts exhausted, dead-lettered
)

// Delivery is the record of delivering one Event to one Subscription. Its ID is
// stable per (SubscriptionID, EventID) so subscribers can deduplicate on it.
type Delivery struct {
	ID             string         `json:"id"`
	OrgID          string         `json:"orgId"`
	SubscriptionID string         `json:"subscriptionId"`
	EventID        string         `json:"eventId"`
	EventType      string         `json:"eventType"`
	Status         DeliveryStatus `json:"status"`
	Attempts       int            `json:"attempts"`
	LastError      string         `json:"lastError,omitempty"`
	CreatedAt      time.Time      `json:"createdAt"`
	UpdatedAt      time.Time      `json:"updatedAt"`
}

// Attempt is a single delivery attempt outcome, kept in the delivery log.
type Attempt struct {
	N       int           `json:"n"`       // 1-based attempt number
	At      time.Time     `json:"at"`      // when the attempt started
	Code    int           `json:"code"`    // HTTP status; 0 on transport error
	Latency time.Duration `json:"latency"` // round-trip time
	Error   string        `json:"error,omitempty"`
}

// EventSnapshot is the immutable copy of an event stored with its delivery so a
// past delivery can be replayed without the caller re-supplying the payload.
type EventSnapshot struct {
	ID         string            `json:"id"`
	Type       string            `json:"type"`
	OrgID      string            `json:"orgId"`
	OccurredAt time.Time         `json:"occurredAt"`
	Payload    json.RawMessage   `json:"payload"`
	Headers    map[string]string `json:"headers,omitempty"` // custom outbound headers, durable-safe
}

func snapshot(ev Event) EventSnapshot {
	return EventSnapshot{ID: ev.ID, Type: ev.Type, OrgID: ev.OrgID, OccurredAt: ev.OccurredAt, Payload: ev.Payload, Headers: ev.Headers}
}

func (s EventSnapshot) event() Event {
	return Event{ID: s.ID, Type: s.Type, OrgID: s.OrgID, OccurredAt: s.OccurredAt, Payload: s.Payload, Headers: s.Headers}
}

// DeliveryID computes the stable, idempotent delivery id for an (event,
// subscription) pair. It is deterministic (no IDGen) so re-emitting the same
// event to the same subscription yields the same id, which subscribers dedup on.
func DeliveryID(subscriptionID, eventID string) string {
	h := sha256.Sum256([]byte(subscriptionID + "\x00" + eventID))
	return "dlv_" + hex.EncodeToString(h[:16])
}
