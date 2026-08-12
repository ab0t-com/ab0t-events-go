package events

import (
	"context"
	"strconv"
	"testing"
)

// BenchmarkEmit_FanOut measures the fan-out cost of Emit across N matching
// subscriptions (store lookup + per-sub delivery record + enqueue), using fakes
// so no real I/O is involved.
func BenchmarkEmit_FanOut(b *testing.B) {
	for _, n := range []int{1, 10, 100} {
		b.Run("subs="+strconv.Itoa(n), func(b *testing.B) {
			h := newHarness(fastPolicy)
			for i := 0; i < n; i++ {
				h.store.add(Subscription{ID: "s" + strconv.Itoa(i), OrgID: "orgA", EventTypes: []string{"*"}, URL: "https://a/hook", Secret: "k", Active: true})
			}
			ev := mkEvent("e1", "drive.file.created", "orgA", map[string]int{"n": 1})
			ctx := context.Background()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				h.queue.jobs = nil
				_ = h.svc.Emit(ctx, ev)
			}
		})
	}
}

// BenchmarkProcess measures a single successful delivery attempt (sign + POST +
// log) through the fakes.
func BenchmarkProcess(b *testing.B) {
	h := newHarness(fastPolicy)
	job := Job{
		Delivery:     Delivery{ID: "d1", OrgID: "orgA"},
		Event:        mkEvent("e1", "drive.file.created", "orgA", map[string]int{"n": 1}),
		Subscription: Subscription{ID: "s1", OrgID: "orgA", URL: "https://a/ok", Secret: "k"},
		AttemptN:     1,
	}
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = h.svc.Process(ctx, job)
	}
}
