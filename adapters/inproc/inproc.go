// Package inproc is the in-process DeliveryQueue adapter for ab0t-events-go: a
// time-ordered scheduler feeding a fixed pool of worker goroutines that call a
// Dispatcher. Jobs whose NotBefore is in the future (backoff retries) wait until
// they are due. Delivery is at-least-once: a job is handed to at most one worker
// at a time, but a crash mid-Process may re-run it, which is safe because the
// stable delivery id lets subscribers deduplicate.
package inproc

import (
	"container/heap"
	"context"
	"errors"
	"sync"
	"time"

	"github.com/ab0t-com/ab0t-events-go/events"
)

// DefaultMaxDepth bounds the number of jobs the queue will hold when Config
// leaves MaxDepth unset. It is a memory-safety backstop for this single-process
// adapter: each job carries the full event payload and subscription (secret
// included) in memory, and failing subscribers re-enqueue delayed retries, so an
// unbounded heap is a memory-exhaustion DoS. Production should use the durable
// SQS queue.
const DefaultMaxDepth = 10000

// ErrQueueFull is returned by Enqueue when the queue is at MaxDepth. The caller
// (Emit) surfaces it; the delivery stays pending in the log and can be recovered
// later via Service.Recover once the backlog drains.
var ErrQueueFull = errors.New("inproc: queue is full (max depth reached)")

// Queue implements events.DeliveryQueue and drives an events.Dispatcher.
type Queue struct {
	mu       sync.Mutex
	pq       jobHeap
	wake     chan struct{}
	now      func() time.Time
	workers  int
	maxDepth int

	startOnce sync.Once
	closed    bool
}

var _ events.DeliveryQueue = (*Queue)(nil)

// Config configures the Queue.
type Config struct {
	Workers int              // worker goroutines; default 4
	Now     func() time.Time // clock for backoff scheduling; default time.Now
	// MaxDepth bounds the number of waiting jobs; Enqueue returns ErrQueueFull
	// beyond it (backpressure). 0 uses DefaultMaxDepth; a negative value disables
	// the bound (unbounded — not recommended in production).
	MaxDepth int
}

// New returns a Queue. Call Start to begin dispatching.
func New(cfg Config) *Queue {
	if cfg.Workers <= 0 {
		cfg.Workers = 4
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.MaxDepth == 0 {
		cfg.MaxDepth = DefaultMaxDepth
	}
	return &Queue{
		wake:     make(chan struct{}, 1),
		now:      cfg.Now,
		workers:  cfg.Workers,
		maxDepth: cfg.MaxDepth,
	}
}

// Enqueue schedules a job. Safe for concurrent use. It returns ErrQueueFull when
// the queue is at its MaxDepth (backpressure), so a burst of emits or a pile-up
// of retries cannot grow memory without bound.
func (q *Queue) Enqueue(_ context.Context, job events.Job) error {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return context.Canceled
	}
	if q.maxDepth > 0 && len(q.pq) >= q.maxDepth {
		q.mu.Unlock()
		return ErrQueueFull
	}
	heap.Push(&q.pq, job)
	q.mu.Unlock()
	q.signal()
	return nil
}

func (q *Queue) signal() {
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

// Start launches the scheduler and worker pool, dispatching to handler until ctx
// is cancelled. It returns a stop function that cancels dispatch and blocks until
// all in-flight work drains. Start may be called once.
func (q *Queue) Start(ctx context.Context, handler events.Dispatcher) (stop func()) {
	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	q.startOnce.Do(func() {
		work := make(chan events.Job)
		// Worker pool.
		for i := 0; i < q.workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-ctx.Done():
						return
					case job, ok := <-work:
						if !ok {
							// The scheduler closed `work` on stop. A receive from a closed channel is
							// immediately ready with a zero Job, so without this check a worker could
							// race ctx.Done() and dispatch a phantom empty job during shutdown.
							return
						}
						_ = handler.Process(ctx, job)
					}
				}
			}()
		}
		// Scheduler.
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer close(work)
			q.schedule(ctx, work)
		}()
	})
	return func() {
		cancel()
		q.mu.Lock()
		q.closed = true
		q.mu.Unlock()
		q.signal()
		wg.Wait()
	}
}

func (q *Queue) schedule(ctx context.Context, work chan<- events.Job) {
	var timer *time.Timer
	for {
		q.mu.Lock()
		if len(q.pq) == 0 {
			q.mu.Unlock()
			select {
			case <-ctx.Done():
				return
			case <-q.wake:
				continue
			}
		}
		wait := q.pq[0].NotBefore.Sub(q.now())
		if wait <= 0 {
			job := heap.Pop(&q.pq).(events.Job)
			q.mu.Unlock()
			select {
			case <-ctx.Done():
				return
			case work <- job:
			}
			continue
		}
		q.mu.Unlock()
		if timer == nil {
			timer = time.NewTimer(wait)
		} else {
			timer.Reset(wait)
		}
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-q.wake:
			timer.Stop()
		case <-timer.C:
		}
	}
}

// Len reports the number of jobs currently waiting (not yet handed to a worker).
func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.pq)
}

// jobHeap is a min-heap of jobs ordered by NotBefore.
type jobHeap []events.Job

func (h jobHeap) Len() int           { return len(h) }
func (h jobHeap) Less(i, j int) bool { return h[i].NotBefore.Before(h[j].NotBefore) }
func (h jobHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *jobHeap) Push(x any)        { *h = append(*h, x.(events.Job)) }
func (h *jobHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}
