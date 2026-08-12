// Package securestore is a SubscriptionStore decorator that encrypts a
// subscription's signing secret at rest. It wraps any events.SubscriptionStore
// (memory, dynamo, your own) with an events.SecretCodec (adapters/aesgcm,
// adapters/kmscodec): Put encrypts Subscription.Secret before delegating, and
// Get/ListForOrg/ListForOrgAndType decrypt it after reading, so the wrapped store
// — and anyone with raw access to its backing table or a backup — only ever sees
// ciphertext.
//
//	inner := dynamo.NewStore(...)
//	codec, _ := aesgcm.New(key)
//	store := securestore.New(inner, codec) // use this as your SubscriptionStore
//
// It changes nothing else: tenancy is still enforced by the wrapped store (this
// decorator adds no cross-org path), and callers see plaintext secrets exactly as
// before. An empty secret is passed through untouched (never encrypted).
package securestore

import (
	"context"

	"github.com/ab0t-com/ab0t-events-go/events"
)

// Store wraps a SubscriptionStore, encrypting secrets on write and decrypting on
// read via a SecretCodec. Construct it with New.
type Store struct {
	inner events.SubscriptionStore
	codec events.SecretCodec
}

var _ events.SubscriptionStore = (*Store)(nil)

// New returns a Store that encrypts secrets at rest in inner using codec. It
// panics if either is nil (a misconfigured store is a programming error).
func New(inner events.SubscriptionStore, codec events.SecretCodec) *Store {
	if inner == nil {
		panic("securestore: inner SubscriptionStore is required")
	}
	if codec == nil {
		panic("securestore: SecretCodec is required")
	}
	return &Store{inner: inner, codec: codec}
}

// Put encrypts the secret (if any) then delegates. The caller's Subscription is
// passed by value, so encrypting the local copy never mutates the caller's.
func (s *Store) Put(ctx context.Context, sub events.Subscription) error {
	if sub.Secret != "" {
		enc, err := s.codec.Encrypt(ctx, sub.Secret)
		if err != nil {
			return err
		}
		sub.Secret = enc
	}
	return s.inner.Put(ctx, sub)
}

// Get reads through and decrypts the secret.
func (s *Store) Get(ctx context.Context, orgID, subID string) (events.Subscription, error) {
	sub, err := s.inner.Get(ctx, orgID, subID)
	if err != nil {
		return events.Subscription{}, err
	}
	if err := s.decrypt(ctx, &sub); err != nil {
		return events.Subscription{}, err
	}
	return sub, nil
}

// ListForOrg reads through and decrypts each secret.
func (s *Store) ListForOrg(ctx context.Context, orgID string) ([]events.Subscription, error) {
	subs, err := s.inner.ListForOrg(ctx, orgID)
	return s.decryptAll(ctx, subs, err)
}

// ListForOrgAndType reads through and decrypts each secret.
func (s *Store) ListForOrgAndType(ctx context.Context, orgID, eventType string) ([]events.Subscription, error) {
	subs, err := s.inner.ListForOrgAndType(ctx, orgID, eventType)
	return s.decryptAll(ctx, subs, err)
}

// Delete delegates unchanged.
func (s *Store) Delete(ctx context.Context, orgID, subID string) error {
	return s.inner.Delete(ctx, orgID, subID)
}

func (s *Store) decrypt(ctx context.Context, sub *events.Subscription) error {
	if sub.Secret == "" {
		return nil
	}
	plain, err := s.codec.Decrypt(ctx, sub.Secret)
	if err != nil {
		return err
	}
	sub.Secret = plain
	return nil
}

func (s *Store) decryptAll(ctx context.Context, subs []events.Subscription, err error) ([]events.Subscription, error) {
	if err != nil {
		return nil, err
	}
	for i := range subs {
		if err := s.decrypt(ctx, &subs[i]); err != nil {
			return nil, err
		}
	}
	return subs, nil
}
