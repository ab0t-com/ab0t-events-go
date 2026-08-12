// Package metrics is a dependency-free metrics Observer for ab0t-events-go. Wire
// its Collector as an events.Observer; it tallies delivery attempts by terminal
// status and HTTP code and accumulates latency, exposing a Snapshot the consumer
// can export to Prometheus, OpenTelemetry, StatsD, or logs.
package metrics

import (
	"context"
	"sync"
	"time"

	"github.com/ab0t-com/ab0t-events-go/events"
)

// Collector implements events.Observer and accumulates counters.
type Collector struct {
	mu sync.Mutex

	attempts   int64
	delivered  int64
	failed     int64 // attempts that will be retried
	dead       int64 // dead-lettered
	byCode     map[int]int64
	latencySum time.Duration
	latencyMax time.Duration
}

var _ events.Observer = (*Collector)(nil)

// New returns an empty Collector.
func New() *Collector { return &Collector{byCode: map[int]int64{}} }

// OnAttempt records one delivery attempt.
func (c *Collector) OnAttempt(_ context.Context, d events.Delivery, a events.Attempt) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.attempts++
	c.byCode[a.Code]++
	c.latencySum += a.Latency
	if a.Latency > c.latencyMax {
		c.latencyMax = a.Latency
	}
	switch d.Status {
	case events.StatusDelivered:
		c.delivered++
	case events.StatusDead:
		c.dead++
	case events.StatusFailed:
		c.failed++
	}
}

// Snapshot is an immutable copy of the collected metrics.
type Snapshot struct {
	Attempts     int64
	Delivered    int64
	Failed       int64
	Dead         int64
	ByCode       map[int]int64
	LatencyTotal time.Duration
	LatencyMax   time.Duration
}

// MeanLatency returns the average attempt latency, or 0 with no attempts.
func (s Snapshot) MeanLatency() time.Duration {
	if s.Attempts == 0 {
		return 0
	}
	return s.LatencyTotal / time.Duration(s.Attempts)
}

// SuccessRate returns delivered/attempts in [0,1].
func (s Snapshot) SuccessRate() float64 {
	if s.Attempts == 0 {
		return 0
	}
	return float64(s.Delivered) / float64(s.Attempts)
}

// Snapshot returns a consistent point-in-time copy of the counters.
func (c *Collector) Snapshot() Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	codes := make(map[int]int64, len(c.byCode))
	for k, v := range c.byCode {
		codes[k] = v
	}
	return Snapshot{
		Attempts:     c.attempts,
		Delivered:    c.delivered,
		Failed:       c.failed,
		Dead:         c.dead,
		ByCode:       codes,
		LatencyTotal: c.latencySum,
		LatencyMax:   c.latencyMax,
	}
}
