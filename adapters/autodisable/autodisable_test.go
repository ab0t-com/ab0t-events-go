package autodisable

import (
	"context"
	"testing"

	"github.com/ab0t-com/ab0t-events-go/adapters/memory"
	"github.com/ab0t-com/ab0t-events-go/events"
)

func TestBreaker_DisablesAfterThreshold(t *testing.T) {
	ctx := context.Background()
	store := memory.NewStore()
	_ = store.Put(ctx, events.Subscription{ID: "s1", OrgID: "orgA", URL: "u", Active: true})

	var disabled []string
	b := New(store, Config{Threshold: 3, OnDisable: func(_ context.Context, _, subID string) {
		disabled = append(disabled, subID)
	}})

	dead := events.Delivery{OrgID: "orgA", SubscriptionID: "s1", Status: events.StatusDead}
	// Two dead-letters: below threshold, still active.
	b.OnAttempt(ctx, dead, events.Attempt{})
	b.OnAttempt(ctx, dead, events.Attempt{})
	if sub, _ := store.Get(ctx, "orgA", "s1"); !sub.Active {
		t.Fatal("disabled too early")
	}
	// Third consecutive dead-letter crosses the threshold.
	b.OnAttempt(ctx, dead, events.Attempt{})
	if sub, _ := store.Get(ctx, "orgA", "s1"); sub.Active {
		t.Fatal("subscription was not disabled at threshold")
	}
	if len(disabled) != 1 || disabled[0] != "s1" {
		t.Fatalf("OnDisable hook fired %v, want [s1] once", disabled)
	}
	// Further dead-letters do not re-fire the hook.
	b.OnAttempt(ctx, dead, events.Attempt{})
	if len(disabled) != 1 {
		t.Fatalf("hook re-fired: %v", disabled)
	}
}

func TestBreaker_SuccessResetsStreak(t *testing.T) {
	ctx := context.Background()
	store := memory.NewStore()
	_ = store.Put(ctx, events.Subscription{ID: "s1", OrgID: "orgA", URL: "u", Active: true})
	b := New(store, Config{Threshold: 2})

	dead := events.Delivery{OrgID: "orgA", SubscriptionID: "s1", Status: events.StatusDead}
	ok := events.Delivery{OrgID: "orgA", SubscriptionID: "s1", Status: events.StatusDelivered}

	b.OnAttempt(ctx, dead, events.Attempt{})
	b.OnAttempt(ctx, ok, events.Attempt{}) // reset
	b.OnAttempt(ctx, dead, events.Attempt{})
	if sub, _ := store.Get(ctx, "orgA", "s1"); !sub.Active {
		t.Fatal("streak was not reset by a successful delivery")
	}
}
