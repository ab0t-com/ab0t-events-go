package events

import "errors"

var (
	// ErrInvalidEvent is returned by Emit when the event is missing required
	// fields (ID, Type, OrgID).
	ErrInvalidEvent = errors.New("events: invalid event (id, type and orgId are required)")
	// ErrInvalidSubscription is returned by Subscribe for a malformed input.
	ErrInvalidSubscription = errors.New("events: invalid subscription (orgId, url, secret and at least one event type are required)")
	// ErrTenancyViolation is returned when a job/record's subscription and event
	// belong to different orgs. It should never happen; it is a hard stop.
	ErrTenancyViolation = errors.New("events: tenancy violation (org mismatch between event and subscription)")
	// ErrNotFound is returned when a delivery or subscription cannot be found in
	// the caller's org.
	ErrNotFound = errors.New("events: not found")
	// ErrUnknownEventType is returned by Emit when a strict Catalog is wired via
	// Deps.Catalog and the event's Type was never registered in it.
	ErrUnknownEventType = errors.New("events: unknown event type (not registered in the strict catalog)")
	// ErrPayloadTooLarge is returned by Emit when the event payload exceeds the
	// configured Deps.MaxPayloadBytes.
	ErrPayloadTooLarge = errors.New("events: event payload exceeds the configured maximum size")
)
