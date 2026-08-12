package events

import (
	"math"
	"time"
)

// AbsoluteMaxRetryDelay is a hard ceiling on any single retry delay, applied even
// when RetryPolicy.MaxDelay is 0 ("uncapped"). It bounds a hostile subscriber's
// Retry-After (and any misconfiguration) so a job can never be parked further than
// a day in the future. It is deliberately generous; set RetryPolicy.MaxDelay for a
// tighter cap.
const AbsoluteMaxRetryDelay = 24 * time.Hour

// RetryPolicy governs exponential backoff with jitter, which failures are worth
// retrying, and the dead-letter cutoff.
type RetryPolicy struct {
	MaxAttempts int           // total attempts including the first; <=0 means 1
	BaseDelay   time.Duration // delay after the first failure
	MaxDelay    time.Duration // cap on the (pre-jitter) delay; 0 means uncapped
	Multiplier  float64       // growth per attempt; <=1 means 1 (constant)
	JitterFrac  float64       // +/- fraction applied via jitter in [0,1); 0 disables

	// RetryOn decides whether a failed attempt is worth retrying, given the HTTP
	// status code (0 on transport error) and the transport error (nil on an HTTP
	// response). Nil uses DefaultRetryable. Returning false dead-letters
	// immediately, sparing endpoints that reject a payload permanently (4xx).
	RetryOn func(code int, err error) bool
}

// DefaultRetryPolicy is a sensible production default: 5 attempts, 1s base,
// doubling, capped at 5m, with +/-20% jitter, retrying transient failures only.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts: 5,
		BaseDelay:   time.Second,
		MaxDelay:    5 * time.Minute,
		Multiplier:  2.0,
		JitterFrac:  0.2,
		RetryOn:     DefaultRetryable,
	}
}

// DefaultRetryable retries transport errors/timeouts, 429 (rate limited), 408
// (request timeout) and any 5xx. Other 4xx are treated as permanent: a
// malformed or rejected payload will not succeed on a retry, so it is
// dead-lettered immediately rather than hammered.
func DefaultRetryable(code int, err error) bool {
	if err != nil {
		return true
	}
	switch {
	case code >= 500:
		return true
	case code == 429 || code == 408:
		return true
	default:
		return false
	}
}

// retryable applies the policy's classifier (or the default).
func (p RetryPolicy) retryable(code int, err error) bool {
	if p.RetryOn != nil {
		return p.RetryOn(code, err)
	}
	return DefaultRetryable(code, err)
}

func (p RetryPolicy) maxAttempts() int {
	if p.MaxAttempts < 1 {
		return 1
	}
	return p.MaxAttempts
}

// Backoff returns the delay before the retry that follows a failed attempt
// (attempt is 1-based). jitter must be in [0,1); it maps to a multiplicative
// factor in [1-JitterFrac, 1+JitterFrac). The function is pure — all randomness
// is supplied by the caller via jitter — so backoff is fully testable.
func (p RetryPolicy) Backoff(attempt int, jitter float64) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	mult := p.Multiplier
	if mult < 1 {
		mult = 1
	}
	d := float64(p.BaseDelay) * math.Pow(mult, float64(attempt-1))
	if p.MaxDelay > 0 && d > float64(p.MaxDelay) {
		d = float64(p.MaxDelay)
	}
	if p.JitterFrac > 0 {
		factor := 1 + p.JitterFrac*(2*jitter-1)
		d *= factor
	}
	if d < 0 {
		d = 0
	}
	return time.Duration(d)
}
