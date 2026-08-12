// Package std provides production implementations of the ab0t-events-go
// testability ports: a wall-clock Clock, a crypto-random IDGen, and a Rand for
// retry jitter. The core never touches time or randomness directly; wire these
// (or your own) in via events.Deps.
package std

import (
	"crypto/rand"
	"encoding/hex"
	mrand "math/rand/v2"
	"time"

	"github.com/ab0t-com/ab0t-events-go/events"
)

// Clock is a wall-clock events.Clock.
type Clock struct{}

var _ events.Clock = Clock{}

// Now returns the current UTC time.
func (Clock) Now() time.Time { return time.Now().UTC() }

// IDGen mints random, URL-safe ids with the given prefix (default "evt_").
type IDGen struct{ Prefix string }

var _ events.IDGen = IDGen{}

// NewID returns a fresh 128-bit random id, hex-encoded, with the prefix.
func (g IDGen) NewID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	p := g.Prefix
	if p == "" {
		p = "evt_"
	}
	return p + hex.EncodeToString(b[:])
}

// Rand is a non-cryptographic events.Rand for retry jitter. math/rand/v2's
// top-level source is safe for concurrent use.
type Rand struct{}

var _ events.Rand = Rand{}

// Float64 returns a pseudo-random float in [0,1).
func (Rand) Float64() float64 { return mrand.Float64() }
