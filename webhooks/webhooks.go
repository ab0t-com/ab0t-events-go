// Package webhooks is the batteries-included facade for ab0t-events-go. It wires
// the core Emitter to sensible production defaults (in-process worker pool,
// SSRF-guarded net/http deliverer, HMAC-SHA256 signer, system Clock/IDGen/Rand)
// so a service gets outbound webhooks in a few lines:
//
//	sys, err := webhooks.New(ctx, webhooks.Options{
//	    Store: memory.NewStore(),
//	    Log:   memory.NewLog(),
//	})
//	defer sys.Close()
//	sys.EmitNew(ctx, "drive.file.created", orgID, payload)
//
// Only Store and Log are required (they hold state — pick memory or dynamo).
// Every other dependency has a default and can be overridden. For full control,
// skip this package and build events.NewService(events.Deps{...}) directly.
package webhooks

import (
	"context"
	"errors"

	"github.com/ab0t-com/ab0t-events-go/adapters/hmacsign"
	"github.com/ab0t-com/ab0t-events-go/adapters/httpdeliver"
	"github.com/ab0t-com/ab0t-events-go/adapters/inproc"
	"github.com/ab0t-com/ab0t-events-go/adapters/std"
	"github.com/ab0t-com/ab0t-events-go/events"
)

// Options configures a webhook System. Store and Log are required; the rest
// default. A zero override keeps the default.
type Options struct {
	Store events.SubscriptionStore // required: the subscription registry
	Log   events.DeliveryLog       // required: the delivery/attempt log

	Deliverer   events.Deliverer    // default: httpdeliver.NewSecure() (SSRF guard on)
	Signer      events.Signer       // default: hmacsign.New()
	Policy      events.RetryPolicy  // default: events.DefaultRetryPolicy()
	Workers     int                 // default: 8
	Clock       events.Clock        // default: std.Clock{}
	IDGen       events.IDGen        // default: std.IDGen{}
	Rand        events.Rand         // default: std.Rand{}
	Observer    events.Observer     // optional metrics/logging hook
	ValidateURL events.URLValidator // optional Subscribe-time URL vetting
}

// System is a running webhook emitter plus the handle that shuts its worker pool
// down. It embeds *events.Service, so all Emitter and convenience methods
// (Emit, EmitNew, Subscribe, ListDeliveries, Replay, …) are available directly.
type System struct {
	*events.Service
	queue  *inproc.Queue
	stop   func()
	closed bool
}

// New builds and starts a webhook System. It returns an error only when a
// required field (Store, Log) is missing. Call Close to stop delivery.
func New(ctx context.Context, o Options) (*System, error) {
	if o.Store == nil || o.Log == nil {
		return nil, errors.New("webhooks: Options.Store and Options.Log are required")
	}
	if o.Deliverer == nil {
		o.Deliverer = httpdeliver.NewSecure()
	}
	if o.Signer == nil {
		o.Signer = hmacsign.New()
	}
	if o.Policy.MaxAttempts == 0 {
		o.Policy = events.DefaultRetryPolicy()
	}
	if o.Clock == nil {
		o.Clock = std.Clock{}
	}
	if o.IDGen == nil {
		o.IDGen = std.IDGen{}
	}
	if o.Rand == nil {
		o.Rand = std.Rand{}
	}

	queue := inproc.New(inproc.Config{Workers: o.Workers})
	svc := events.NewService(events.Deps{
		Store:       o.Store,
		Queue:       queue,
		Deliverer:   o.Deliverer,
		Signer:      o.Signer,
		Log:         o.Log,
		Clock:       o.Clock,
		IDGen:       o.IDGen,
		Rand:        o.Rand,
		Policy:      o.Policy,
		Observer:    o.Observer,
		ValidateURL: o.ValidateURL,
	})
	stop := queue.Start(ctx, svc)
	return &System{Service: svc, queue: queue, stop: stop}, nil
}

// Close stops the worker pool, blocking until in-flight deliveries drain. Safe to
// call more than once.
func (s *System) Close() {
	if s == nil || s.closed {
		return
	}
	s.closed = true
	s.stop()
}

// Queue exposes the underlying in-process queue (e.g. to inspect Len in tests).
func (s *System) Queue() *inproc.Queue { return s.queue }
