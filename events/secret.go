package events

import "context"

// SecretCodec encrypts and decrypts a subscription's signing secret so it can be
// held encrypted at rest by a SubscriptionStore. It is the seam the
// adapters/securestore decorator uses: on Put it Encrypts Subscription.Secret,
// and on Get/List it Decrypts it back, so the underlying store (and any backup or
// operator with raw table access) only ever sees ciphertext.
//
// Contract:
//   - Round-trip: Decrypt(ctx, Encrypt(ctx, s)) == s for any string s.
//   - Ciphertext is an opaque, storage-safe string (adapters/aesgcm and
//     adapters/kmscodec both return base64) suitable for the Secret field.
//   - Implementations MUST be safe for concurrent use.
//   - The empty string is passed through by adapters/securestore without calling
//     the codec, so a codec need not special-case it.
type SecretCodec interface {
	Encrypt(ctx context.Context, plaintext string) (ciphertext string, err error)
	Decrypt(ctx context.Context, ciphertext string) (plaintext string, err error)
}
