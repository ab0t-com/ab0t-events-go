package inproc

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ab0t-com/ab0t-events-go/events"
)

// slowRecorder records every job it is handed and holds each one for `hold`, so a stop() can arrive while
// a worker is still inside Process.
type slowRecorder struct {
	mu      sync.Mutex
	ids     []string
	started chan struct{}
	once    sync.Once
	hold    time.Duration
}

func (r *slowRecorder) Process(_ context.Context, j events.Job) error {
	r.mu.Lock()
	r.ids = append(r.ids, j.Delivery.ID)
	r.mu.Unlock()
	r.once.Do(func() { close(r.started) })
	time.Sleep(r.hold)
	return nil
}

func (r *slowRecorder) seen() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.ids...)
}

// TestQueue_StopNeverDispatchesAPhantomJob — 2026-11-22. `make check` (race) panicked once with
// "sync: negative WaitGroup counter" in TestQueue_ProcessesAndDrains: the dispatcher was called once more
// than jobs were enqueued. Cause: on stop the scheduler closes the work channel; a worker that was busy in
// Process during stop loops back to a select where BOTH ctx.Done() and the closed channel are ready, and
// could take the closed channel — a zero Job — and dispatch it. The worker now returns when the channel is
// closed. This stops the queue while a worker is mid-Process, many times, and fails if the dispatcher is ever
// handed a job that was never enqueued (an empty delivery id).
func TestQueue_StopNeverDispatchesAPhantomJob(t *testing.T) {
	for i := 0; i < 200; i++ {
		rec := &slowRecorder{started: make(chan struct{}), hold: 2 * time.Millisecond}
		q := New(Config{Workers: 4})
		stop := q.Start(context.Background(), rec)
		_ = q.Enqueue(context.Background(), job("real", time.Now()))
		select {
		case <-rec.started:
		case <-time.After(2 * time.Second):
			t.Fatalf("iteration %d: the real job was never dispatched", i)
		}
		stop() // arrives while the worker is still inside Process
		for _, id := range rec.seen() {
			if id != "real" {
				t.Fatalf("iteration %d: dispatcher received a job that was never enqueued (id=%q) during stop", i, id)
			}
		}
	}
}
