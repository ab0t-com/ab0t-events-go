package inproc

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ab0t-com/ab0t-events-go/events"
)

// recorder is a test Dispatcher that records the order and time jobs are processed.
type recorder struct {
	mu   sync.Mutex
	seen []processed
	wg   *sync.WaitGroup
}

type processed struct {
	id string
	at time.Time
}

func (r *recorder) Process(_ context.Context, job events.Job) error {
	r.mu.Lock()
	r.seen = append(r.seen, processed{id: job.Delivery.ID, at: time.Now()})
	r.mu.Unlock()
	if r.wg != nil {
		r.wg.Done()
	}
	return nil
}

func (r *recorder) snapshot() []processed {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]processed(nil), r.seen...)
}

func job(id string, notBefore time.Time) events.Job {
	return events.Job{Delivery: events.Delivery{ID: id, OrgID: "o"}, Subscription: events.Subscription{OrgID: "o"}, Event: events.Event{OrgID: "o"}, AttemptN: 1, NotBefore: notBefore}
}

func TestQueue_ProcessesAndDrains(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(3)
	rec := &recorder{wg: &wg}
	q := New(Config{Workers: 2})
	stop := q.Start(context.Background(), rec)
	defer stop()

	now := time.Now()
	for _, id := range []string{"a", "b", "c"} {
		if err := q.Enqueue(context.Background(), job(id, now)); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}
	waitWG(t, &wg, 2*time.Second)
	if got := len(rec.snapshot()); got != 3 {
		t.Fatalf("processed %d jobs, want 3", got)
	}
}

func TestQueue_RespectsNotBeforeBackoff(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(2)
	rec := &recorder{wg: &wg}
	q := New(Config{Workers: 1})
	stop := q.Start(context.Background(), rec)
	defer stop()

	start := time.Now()
	// Enqueue the delayed job FIRST to prove ordering is by NotBefore, not FIFO.
	_ = q.Enqueue(context.Background(), job("later", start.Add(120*time.Millisecond)))
	_ = q.Enqueue(context.Background(), job("now", start))

	waitWG(t, &wg, 2*time.Second)
	seen := rec.snapshot()
	if len(seen) != 2 || seen[0].id != "now" || seen[1].id != "later" {
		t.Fatalf("wrong order: %+v", seen)
	}
	if delay := seen[1].at.Sub(start); delay < 100*time.Millisecond {
		t.Fatalf("delayed job ran too early: %v after start", delay)
	}
}

func TestQueue_EnqueueAfterStopFails(t *testing.T) {
	q := New(Config{Workers: 1})
	stop := q.Start(context.Background(), &recorder{})
	stop()
	if err := q.Enqueue(context.Background(), job("x", time.Now())); err == nil {
		t.Fatal("expected error enqueueing to a stopped queue")
	}
}

func TestQueue_LenTracksPending(t *testing.T) {
	q := New(Config{Workers: 1}) // not started: jobs accumulate
	for i := 0; i < 5; i++ {
		_ = q.Enqueue(context.Background(), job("j", time.Now().Add(time.Hour)))
	}
	if q.Len() != 5 {
		t.Fatalf("Len = %d, want 5", q.Len())
	}
}

// TestQueue_MaxDepthBackpressure is the proving test for the audit unbounded-queue
// finding: once MaxDepth waiting jobs accumulate, further Enqueue returns
// ErrQueueFull instead of growing memory without bound.
func TestQueue_MaxDepthBackpressure(t *testing.T) {
	q := New(Config{Workers: 1, MaxDepth: 3}) // not started: jobs accumulate
	future := time.Now().Add(time.Hour)
	for i := 0; i < 3; i++ {
		if err := q.Enqueue(context.Background(), job("j", future)); err != nil {
			t.Fatalf("enqueue %d unexpectedly failed: %v", i, err)
		}
	}
	// The 4th enqueue is rejected with backpressure.
	if err := q.Enqueue(context.Background(), job("overflow", future)); err != ErrQueueFull {
		t.Fatalf("want ErrQueueFull at capacity, got %v", err)
	}
	if q.Len() != 3 {
		t.Fatalf("Len = %d, want 3 (overflow not stored)", q.Len())
	}
}

// TestQueue_UnboundedWhenNegative proves MaxDepth<0 disables the bound.
func TestQueue_UnboundedWhenNegative(t *testing.T) {
	q := New(Config{Workers: 1, MaxDepth: -1})
	future := time.Now().Add(time.Hour)
	for i := 0; i < DefaultMaxDepth+5; i++ {
		if err := q.Enqueue(context.Background(), job("j", future)); err != nil {
			t.Fatalf("unbounded queue rejected enqueue at %d: %v", i, err)
		}
	}
}

func waitWG(t *testing.T, wg *sync.WaitGroup, d time.Duration) {
	t.Helper()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatal("timed out waiting for jobs to process")
	}
}
