// Package events is the pure, I/O-free core of ab0t-events-go: an outbound
// event-webhook delivery library (Stripe/GitHub style) that any ab0t service can
// embed to deliver domain events to external subscriber endpoints.
//
// # Architecture (ports & adapters / hexagonal)
//
// The core defines the domain types (Event, Subscription, Delivery, Attempt,
// RetryPolicy), the driving API (the Emitter interface), and the PORTS it
// depends on (SubscriptionStore, DeliveryQueue, Deliverer, Signer, DeliveryLog,
// and the testability ports Clock, IDGen, Rand). A consuming service supplies
// adapters for those ports; ready-made adapters live under ../adapters.
//
// The core performs NO I/O and calls neither time.Now nor any rand source
// directly — every non-deterministic input arrives through a port, so the whole
// fan-out / retry / dead-letter / replay machinery is unit-testable with fakes.
//
// # Tenancy (hard)
//
// Subscriptions and deliveries are per-org (OrgID) partitioned. The core never
// fans one org's event out to another org's endpoint: SubscriptionStore lookups
// are keyed by OrgID and the dispatcher re-asserts OrgID equality on every job
// (defense in depth). Per-subscription Secrets are never logged.
package events
