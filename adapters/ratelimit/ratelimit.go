// Package ratelimit is a Deliverer decorator that throttles outbound webhook
// requests per destination, protecting slow subscribers (and your own egress)
// from bursts. It wraps any events.Deliverer and applies a token-bucket limit
// keyed by a function of the request (by destination host by default).
//
// It is a pure-stdlib decorator: no new dependencies, and the clock is injectable
// so the bucket math is deterministically testable.
package ratelimit

import (
	"context"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/ab0t-com/ab0t-events-go/events"
)

// KeyFunc derives the rate-limit bucket key from a request. Requests sharing a
// key share a bucket.
type KeyFunc func(req events.DeliveryRequest) string

// ByHost keys the limiter by the destination host, so one subscriber host cannot
// be flooded regardless of how many subscriptions point at it.
//
// The key is canonical, so spellings that reach the same endpoint share one
// bucket: the host name is lower-cased (DNS names are case-insensitive), a
// trailing root dot is dropped, and the port is kept only when it is not the
// scheme's default (443 for https, 80 for http). "https://Hooks.Example.com.:443/a"
// and "https://hooks.example.com/b" both key as "hooks.example.com"; a
// non-default port stays part of the key ("hooks.example.com:8443").
func ByHost(req events.DeliveryRequest) string {
	u, err := url.Parse(req.URL)
	if err != nil {
		return req.URL
	}
	host := strings.TrimSuffix(strings.ToLower(u.Hostname()), ".")
	if host == "" {
		return u.Host // nothing to canonicalise (unchanged from v0.2.0)
	}
	port := u.Port()
	switch strings.ToLower(u.Scheme) + ":" + port {
	case "https:443", "http:80":
		port = ""
	}
	if port != "" {
		return net.JoinHostPort(host, port)
	}
	if strings.Contains(host, ":") {
		return "[" + host + "]" // IPv6 literal keeps its brackets, as url.URL.Host has them
	}
	return host
}

// Config configures the limiter.
type Config struct {
	// PerSecond is the sustained request rate allowed per key. <=0 disables
	// limiting (the decorator passes through).
	PerSecond float64
	// Burst is the bucket capacity (max instantaneous requests). <=0 defaults to
	// max(1, PerSecond).
	Burst float64
	// Key derives the bucket key; defaults to ByHost.
	Key KeyFunc
	// Now is the clock; defaults to time.Now.
	Now func() time.Time
}

// idleEvictionTTL is how long a bucket may go untouched before it is evicted;
// sweepInterval is the minimum time between sweeps. Evicting an idle bucket is
// behaviorally safe: after idleEvictionTTL of no traffic a bucket would refill to
// full capacity on its next use anyway, so recreating it fresh is equivalent —
// this bounds the buckets map to keys seen within roughly idleEvictionTTL, so a
// tenant registering many distinct hostnames cannot grow it without limit.
const (
	idleEvictionTTL = 10 * time.Minute
	sweepInterval   = time.Minute
)

// Limiter is a per-key token-bucket rate limiter.
type Limiter struct {
	perSecond float64
	burst     float64
	now       func() time.Time

	mu        sync.Mutex
	buckets   map[string]*bucket
	lastSweep time.Time
}

type bucket struct {
	tokens float64
	last   time.Time
}

// NewLimiter builds a Limiter from a Config.
func NewLimiter(cfg Config) *Limiter {
	burst := cfg.Burst
	if burst <= 0 {
		burst = cfg.PerSecond
		if burst < 1 {
			burst = 1
		}
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &Limiter{perSecond: cfg.PerSecond, burst: burst, now: now, buckets: map[string]*bucket{}}
}

// Reserve takes one token for key at time now and returns how long the caller
// must wait before it is valid (0 if a token was immediately available). It is
// the deterministic core of Wait, exported for testing.
func (l *Limiter) Reserve(key string, now time.Time) time.Duration {
	if l.perSecond <= 0 {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.sweepLocked(now)
	b := l.buckets[key]
	if b == nil {
		b = &bucket{tokens: l.burst, last: now}
		l.buckets[key] = b
	}
	// Refill based on elapsed time, capped at burst.
	if elapsed := now.Sub(b.last).Seconds(); elapsed > 0 {
		b.tokens += elapsed * l.perSecond
		if b.tokens > l.burst {
			b.tokens = l.burst
		}
		b.last = now
	}
	b.tokens-- // reserve one (may go negative to queue future callers)
	if b.tokens >= 0 {
		return 0
	}
	return time.Duration(-b.tokens / l.perSecond * float64(time.Second))
}

// sweepLocked evicts buckets untouched for at least idleEvictionTTL, at most once
// per sweepInterval. Dropping an idle bucket is safe: after that long without
// traffic it would refill to full capacity on its next use, so a later request for
// the key simply recreates it in the equivalent state. Callers must hold l.mu.
func (l *Limiter) sweepLocked(now time.Time) {
	if l.lastSweep.IsZero() {
		l.lastSweep = now
		return
	}
	if now.Sub(l.lastSweep) < sweepInterval {
		return
	}
	l.lastSweep = now
	for k, b := range l.buckets {
		if now.Sub(b.last) >= idleEvictionTTL {
			delete(l.buckets, k)
		}
	}
}

// NumBuckets reports the number of live per-key buckets. Exported for tests and
// for exposing memory footprint as a metric.
func (l *Limiter) NumBuckets() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.buckets)
}

// Wait blocks until a token for key is available or ctx is done.
func (l *Limiter) Wait(ctx context.Context, key string) error {
	d := l.Reserve(key, l.now())
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Deliverer wraps another events.Deliverer with per-key rate limiting.
type Deliverer struct {
	next    events.Deliverer
	limiter *Limiter
	key     KeyFunc
}

var _ events.Deliverer = (*Deliverer)(nil)

// WrapDeliverer returns next wrapped with a rate limiter built from cfg.
func WrapDeliverer(next events.Deliverer, cfg Config) *Deliverer {
	key := cfg.Key
	if key == nil {
		key = ByHost
	}
	return &Deliverer{next: next, limiter: NewLimiter(cfg), key: key}
}

// Post waits for a token (respecting ctx), then delegates. If ctx is done while
// waiting, it returns the context error: on a deadline the core treats it as a
// transient failure and retries with backoff; on a cancellation (the queue
// stopping) the core records nothing and the attempt runs again later.
func (d *Deliverer) Post(ctx context.Context, req events.DeliveryRequest) (events.DeliveryResponse, error) {
	if err := d.limiter.Wait(ctx, d.key(req)); err != nil {
		return events.DeliveryResponse{}, err
	}
	return d.next.Post(ctx, req)
}
