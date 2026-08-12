// Package autodisable is an events.Observer that pauses a subscription after it
// dead-letters too many deliveries in a row, so the system stops hammering a
// permanently-broken endpoint (mirrors Stripe's auto-disable behavior). Compose
// it with other observers via events.MultiObserver.
//
// State is kept in-process (a consecutive-dead-letter counter per subscription,
// reset on any successful delivery). In a multi-instance deployment each instance
// tracks independently; disabling is idempotent (it writes Active=false), so the
// outcome converges. For strict cross-instance counting, back it with a shared
// store instead.
package autodisable

import (
	"context"
	"sync"

	"github.com/ab0t-com/ab0t-events-go/events"
)

// Breaker implements events.Observer.
type Breaker struct {
	store     events.SubscriptionStore
	threshold int
	onDisable func(ctx context.Context, orgID, subscriptionID string)

	mu     sync.Mutex
	streak map[string]int // subscriptionID -> consecutive dead-letters
}

var _ events.Observer = (*Breaker)(nil)

// Config configures a Breaker.
type Config struct {
	// Threshold is the number of consecutive dead-lettered deliveries that
	// triggers disabling. <=0 defaults to 5.
	Threshold int
	// OnDisable, if set, is called once when a subscription is disabled (e.g. to
	// notify the org). It must not block.
	OnDisable func(ctx context.Context, orgID, subscriptionID string)
}

// New returns a Breaker that disables via the given store.
func New(store events.SubscriptionStore, cfg Config) *Breaker {
	if cfg.Threshold <= 0 {
		cfg.Threshold = 5
	}
	return &Breaker{
		store:     store,
		threshold: cfg.Threshold,
		onDisable: cfg.OnDisable,
		streak:    map[string]int{},
	}
}

// OnAttempt updates the per-subscription streak and disables at the threshold.
func (b *Breaker) OnAttempt(ctx context.Context, d events.Delivery, _ events.Attempt) {
	switch d.Status {
	case events.StatusDelivered:
		b.reset(d.SubscriptionID)
	case events.StatusDead:
		if b.bump(d.SubscriptionID) >= b.threshold {
			b.disable(ctx, d.OrgID, d.SubscriptionID)
		}
	}
}

func (b *Breaker) reset(subID string) {
	b.mu.Lock()
	delete(b.streak, subID)
	b.mu.Unlock()
}

func (b *Breaker) bump(subID string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.streak[subID]++
	return b.streak[subID]
}

func (b *Breaker) disable(ctx context.Context, orgID, subID string) {
	sub, err := b.store.Get(ctx, orgID, subID)
	if err != nil || sub.OrgID != orgID {
		return
	}
	if !sub.Active {
		return // already disabled — fire the hook only on the transition
	}
	sub.Active = false
	if err := b.store.Put(ctx, sub); err != nil {
		return
	}
	b.reset(subID)
	if b.onDisable != nil {
		b.onDisable(ctx, orgID, subID)
	}
}
