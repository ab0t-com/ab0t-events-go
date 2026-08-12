// Package kmscodec is an events.SecretCodec backed by AWS KMS: subscription
// secrets are encrypted and decrypted by a KMS customer master key, so the
// plaintext key material never leaves KMS and never lives in your process or
// config. Use it with adapters/securestore.
//
// # Bring your own client (adapter pattern, no assumptions)
//
// You pass the KMS client and key id. This package makes no assumptions about
// region, credentials, or endpoints (LocalStack works) — that is your
// aws.Config. Only the two KMS calls it uses are abstracted by the KMS interface,
// so you can inject *kms.Client or a mock.
//
// # Direct encryption (secrets are tiny)
//
// A signing secret is well under KMS's 4KB plaintext limit, so this codec calls
// KMS Encrypt/Decrypt directly (no envelope/data-key step). The stored value is
// base64(KMS CiphertextBlob). An optional EncryptionContext is sent as KMS
// additional authenticated data and must match on decrypt.
package kmscodec

import (
	"context"
	"encoding/base64"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kms"

	"github.com/ab0t-com/ab0t-events-go/events"
)

// KMS is the subset of the AWS KMS client this adapter uses. *kms.Client
// satisfies it; it also makes the adapter mockable.
type KMS interface {
	Encrypt(ctx context.Context, in *kms.EncryptInput, optFns ...func(*kms.Options)) (*kms.EncryptOutput, error)
	Decrypt(ctx context.Context, in *kms.DecryptInput, optFns ...func(*kms.Options)) (*kms.DecryptOutput, error)
}

// Config configures the Codec. Client and KeyID are required.
type Config struct {
	Client KMS    // required: your *kms.Client (or a mock)
	KeyID  string // required: KMS key id / ARN / alias used to Encrypt

	// EncryptionContext is optional KMS additional authenticated data. If set, the
	// same context is required to Decrypt; use it to bind ciphertext to a purpose
	// (e.g. {"app":"ab0t-events","use":"webhook-secret"}).
	EncryptionContext map[string]string
}

// Codec is a KMS-backed events.SecretCodec. Construct it with New. It is safe for
// concurrent use (the KMS client is).
type Codec struct {
	cfg Config
}

var _ events.SecretCodec = (*Codec)(nil)

// New returns a KMS codec. It panics if Client or KeyID is missing (a
// misconfigured codec is a programming error).
func New(cfg Config) *Codec {
	if cfg.Client == nil {
		panic("kmscodec: Config.Client is required")
	}
	if cfg.KeyID == "" {
		panic("kmscodec: Config.KeyID is required")
	}
	return &Codec{cfg: cfg}
}

// Encrypt calls KMS Encrypt and returns base64(CiphertextBlob).
func (c *Codec) Encrypt(ctx context.Context, plaintext string) (string, error) {
	out, err := c.cfg.Client.Encrypt(ctx, &kms.EncryptInput{
		KeyId:             aws.String(c.cfg.KeyID),
		Plaintext:         []byte(plaintext),
		EncryptionContext: c.cfg.EncryptionContext,
	})
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(out.CiphertextBlob), nil
}

// Decrypt base64-decodes the stored value and calls KMS Decrypt. The KeyID is
// passed so decryption is pinned to the expected key (defense in depth against a
// swapped ciphertext).
func (c *Codec) Decrypt(ctx context.Context, ciphertext string) (string, error) {
	blob, err := base64.StdEncoding.DecodeString(ciphertext)
	if err != nil {
		return "", err
	}
	out, err := c.cfg.Client.Decrypt(ctx, &kms.DecryptInput{
		KeyId:             aws.String(c.cfg.KeyID),
		CiphertextBlob:    blob,
		EncryptionContext: c.cfg.EncryptionContext,
	})
	if err != nil {
		return "", err
	}
	return string(out.Plaintext), nil
}
