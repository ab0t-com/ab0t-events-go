package events

import (
	"encoding/json"
	"sync"
)

// EventTypeDef describes one event type a service publishes: its stable type
// string, a human-readable description, an optional example payload, and whether
// it is deprecated. It is data only — registering a type does not, by itself,
// affect delivery. A strict Catalog (see NewCatalog) additionally lets Emit
// reject events whose type was never registered.
type EventTypeDef struct {
	Type        string          `json:"type"`
	Description string          `json:"description,omitempty"`
	Example     json.RawMessage `json:"example,omitempty"` // an illustrative payload; must be valid JSON if set
	Deprecated  bool            `json:"deprecated,omitempty"`
}

// Catalog is a concurrency-safe registry of the event types a service can emit.
// It documents the event vocabulary (surfaced by the admin GET /event-types
// endpoint) and, in strict mode, lets Emit reject events whose type was never
// registered — catching typos and undeclared events at emit time instead of
// silently delivering (or dropping) them.
//
// The zero value is not usable; construct a Catalog with NewCatalog. A Catalog
// is pure (no I/O, no clock) and safe for concurrent Register/Get/List/Has.
type Catalog struct {
	mu     sync.RWMutex
	strict bool
	byType map[string]EventTypeDef
	order  []string // registration order, for stable List output
}

// NewCatalog returns an empty Catalog. Pass strict=true to have Emit reject
// unknown event types when this Catalog is wired in via Deps.Catalog; a
// non-strict Catalog is documentation only and never rejects an emit.
func NewCatalog(strict bool) *Catalog {
	return &Catalog{strict: strict, byType: map[string]EventTypeDef{}}
}

// Register adds (or replaces, by Type) one or more event-type definitions and
// returns the Catalog for chaining. It panics if any Type is empty, since a
// mis-declared catalog is a programming error, not a runtime condition.
// Re-registering an existing Type updates its definition in place without
// changing its position in List order.
func (c *Catalog) Register(defs ...EventTypeDef) *Catalog {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, d := range defs {
		if d.Type == "" {
			panic("events: Catalog.Register: EventTypeDef.Type is required")
		}
		if _, exists := c.byType[d.Type]; !exists {
			c.order = append(c.order, d.Type)
		}
		c.byType[d.Type] = d
	}
	return c
}

// Get returns the definition registered for an exact event type, and whether it
// was found.
func (c *Catalog) Get(eventType string) (EventTypeDef, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	d, ok := c.byType[eventType]
	return d, ok
}

// Has reports whether an exact event type is registered.
func (c *Catalog) Has(eventType string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, ok := c.byType[eventType]
	return ok
}

// List returns every registered definition in registration order. The returned
// slice is a copy the caller may retain and mutate.
func (c *Catalog) List() []EventTypeDef {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]EventTypeDef, 0, len(c.order))
	for _, t := range c.order {
		out = append(out, c.byType[t])
	}
	return out
}

// Strict reports whether Emit should reject event types this Catalog does not
// contain. It is fixed at construction time (NewCatalog).
func (c *Catalog) Strict() bool { return c.strict }
