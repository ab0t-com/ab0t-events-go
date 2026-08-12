// Package sqsqueue is a DURABLE DeliveryQueue adapter for ab0t-events-go backed
// by Amazon SQS. Unlike the in-process queue, jobs survive a process restart.
//
// # Secrets never touch the queue
//
// Each SQS message carries only an id-only events.JobRef (org, delivery,
// subscription, event ids + scheduling) — never the event payload and never the
// subscription secret. A worker rehydrates the full job at consume time via the
// events.RefDispatcher (it reloads the event snapshot from the delivery log and
// the subscription from the store). So a leaked queue reveals no payloads or
// secrets.
//
// # Bring your own client (adapter pattern, no assumptions)
//
// You pass the SQS client and queue URL. This package makes no assumptions about
// region, credentials, endpoints (LocalStack works), or standard-vs-FIFO — that
// is your aws.Config. Only the four SQS calls it uses are abstracted by the SQS
// interface, so you can inject *sqs.Client or a mock.
//
// # At-least-once
//
// A message is deleted only after the worker reports success (or a deliberate
// drop). Infrastructure errors leave the message for SQS to redeliver after the
// visibility timeout. Application-level retries are scheduled as fresh delayed
// messages, and ProcessRef drops duplicates of already-terminal deliveries, so
// at-least-once redelivery never causes a duplicate webhook.
package sqsqueue

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/ab0t-com/ab0t-events-go/events"
)

// SQS is the subset of the AWS SQS client this adapter uses. *sqs.Client
// satisfies it; it also makes the adapter mockable.
type SQS interface {
	SendMessage(ctx context.Context, in *sqs.SendMessageInput, opts ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
	ReceiveMessage(ctx context.Context, in *sqs.ReceiveMessageInput, opts ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, in *sqs.DeleteMessageInput, opts ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
	ChangeMessageVisibility(ctx context.Context, in *sqs.ChangeMessageVisibilityInput, opts ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error)
}

// maxSQSDelay is SQS's hard cap on DelaySeconds / a single visibility extension
// large enough for our purposes (15 min for delay).
const (
	maxDelaySeconds      = 900   // SQS DelaySeconds cap
	maxVisibilitySeconds = 43200 // SQS ChangeMessageVisibility cap (12h)
)

// Config configures the Queue. Client and QueueURL are required.
type Config struct {
	Client   SQS    // required: your *sqs.Client (or a mock)
	QueueURL string // required

	Workers           int              // consumer goroutines; default 4
	MaxMessages       int32            // messages per receive (1..10); default 10
	WaitTimeSeconds   int32            // long-poll wait (0..20); default 20
	VisibilityTimeout int32            // per-message visibility; default 30
	Now               func() time.Time // clock for delay math; default time.Now
	OnError           func(error)      // optional hook for receive/process errors
}

// Queue implements events.DeliveryQueue over SQS and drives an
// events.RefDispatcher.
type Queue struct {
	cfg       Config
	startOnce sync.Once
}

var _ events.DeliveryQueue = (*Queue)(nil)

// New returns a Queue. Call Start to begin consuming.
func New(cfg Config) *Queue {
	if cfg.Client == nil {
		panic("sqsqueue: Config.Client is required")
	}
	if cfg.QueueURL == "" {
		panic("sqsqueue: Config.QueueURL is required")
	}
	if cfg.Workers <= 0 {
		cfg.Workers = 4
	}
	if cfg.MaxMessages <= 0 || cfg.MaxMessages > 10 {
		cfg.MaxMessages = 10
	}
	if cfg.WaitTimeSeconds < 0 || cfg.WaitTimeSeconds > 20 {
		cfg.WaitTimeSeconds = 20
	}
	if cfg.VisibilityTimeout <= 0 {
		cfg.VisibilityTimeout = 30
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Queue{cfg: cfg}
}

// Enqueue serializes the job's id-only reference and sends it to SQS with a delay
// derived from NotBefore (capped at the SQS maximum; longer backoffs are honored
// by the consumer re-deferring the message).
func (q *Queue) Enqueue(ctx context.Context, job events.Job) error {
	body, err := json.Marshal(job.Ref())
	if err != nil {
		return err
	}
	delay := q.delaySeconds(job.NotBefore)
	_, err = q.cfg.Client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:     aws.String(q.cfg.QueueURL),
		MessageBody:  aws.String(string(body)),
		DelaySeconds: delay,
	})
	return err
}

func (q *Queue) delaySeconds(notBefore time.Time) int32 {
	d := notBefore.Sub(q.cfg.Now())
	if d <= 0 {
		return 0
	}
	secs := int64(d.Round(time.Second) / time.Second)
	if secs > maxDelaySeconds {
		secs = maxDelaySeconds
	}
	return int32(secs)
}

// Start launches the consumer worker pool, dispatching each message via handler
// until ctx is cancelled. It returns a stop function that cancels consumption and
// waits for in-flight workers to drain. Start may be called once.
func (q *Queue) Start(ctx context.Context, handler events.RefDispatcher) (stop func()) {
	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	q.startOnce.Do(func() {
		for i := 0; i < q.cfg.Workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				q.consume(ctx, handler)
			}()
		}
	})
	return func() {
		cancel()
		wg.Wait()
	}
}

func (q *Queue) consume(ctx context.Context, handler events.RefDispatcher) {
	for {
		if ctx.Err() != nil {
			return
		}
		out, err := q.cfg.Client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(q.cfg.QueueURL),
			MaxNumberOfMessages: q.cfg.MaxMessages,
			WaitTimeSeconds:     q.cfg.WaitTimeSeconds,
			VisibilityTimeout:   q.cfg.VisibilityTimeout,
		})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			q.onError(err)
			continue
		}
		for _, m := range out.Messages {
			q.handle(ctx, handler, m.Body, m.ReceiptHandle)
		}
	}
}

func (q *Queue) handle(ctx context.Context, handler events.RefDispatcher, body, receipt *string) {
	if body == nil || receipt == nil {
		return
	}
	var ref events.JobRef
	if err := json.Unmarshal([]byte(*body), &ref); err != nil {
		// Unparseable message: drop it so it doesn't poison the queue forever.
		q.onError(err)
		q.delete(ctx, receipt)
		return
	}

	// Honor a backoff longer than SQS's send delay by re-deferring the message.
	if wait := ref.NotBefore.Sub(q.cfg.Now()); wait > time.Second {
		secs := int64(wait.Round(time.Second) / time.Second)
		if secs > maxVisibilitySeconds {
			secs = maxVisibilitySeconds
		}
		_, err := q.cfg.Client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
			QueueUrl:          aws.String(q.cfg.QueueURL),
			ReceiptHandle:     receipt,
			VisibilityTimeout: int32(secs),
		})
		if err != nil {
			q.onError(err)
		}
		return // leave it in the queue; it reappears when due
	}

	if err := handler.ProcessRef(ctx, ref); err != nil {
		// Infrastructure error: leave the message for SQS to redeliver.
		q.onError(err)
		return
	}
	// Success or deliberate drop (terminal/duplicate): remove it.
	q.delete(ctx, receipt)
}

func (q *Queue) delete(ctx context.Context, receipt *string) {
	if _, err := q.cfg.Client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(q.cfg.QueueURL),
		ReceiptHandle: receipt,
	}); err != nil {
		q.onError(err)
	}
}

func (q *Queue) onError(err error) {
	if err == nil || errors.Is(err, context.Canceled) {
		return
	}
	if q.cfg.OnError != nil {
		q.cfg.OnError(err)
	}
}
