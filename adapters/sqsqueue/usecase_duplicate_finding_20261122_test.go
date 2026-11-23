package sqsqueue

// REGRESSION TESTS (found 2026-11-22 by the use-case test sweep; fixed in v0.2.1). Both fail on v0.2.0.
//
// F-1 "an SQS duplicate of a FAILED attempt re-POSTs and forks the retry chain". The package doc promises that
// at-least-once redelivery does not cause a duplicate webhook, and CONTRACTS.md says ProcessRef is "at-least-once
// safe". In v0.2.0 ProcessRef only dropped duplicates of TERMINAL deliveries. A duplicate of an attempt that FAILED
// is still non-terminal, so it was processed again: a second POST of attempt N, a second attempt-(N+1) message, and
// from then on two independent retry chains (each later failure POSTed twice and the log appending duplicate attempt
// numbers). SQS standard queues do redeliver (lost DeleteMessage, visibility timeout lapsing during a slow POST);
// a durable queue must never let an older attempt overwrite a newer schedule.
// Fix: ProcessRef drops a ref when the delivery is StatusFailed and ref.AttemptN <= Delivery.Attempts (stale).
// Replay stays safe because it resets the status to pending first; Recover enqueues Attempts+1.
//
// F-2 "the consumer keeps dispatching a received batch after shutdown". In v0.2.0 consume checked ctx only before
// ReceiveMessage; the `for _, m := range out.Messages` loop called ProcessRef for every remaining message of the
// batch with the CANCELLED context. With the real Service that POST failed at once, was recorded as a failed attempt
// that never reached the customer, and on the final attempt was dead-lettered (and counted by autodisable); if the
// retry send also failed on the cancelled context the message was redelivered and the same attempt number ran again.
// A shutdown must not turn an in-flight POST into a recorded failed attempt. Fix: stop the batch loop when ctx is
// done (the unprocessed messages simply reappear after their visibility timeout — the at-least-once contract).

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/ab0t-com/ab0t-events-go/events"
)

func TestUseCaseFinding_SQSDuplicateOfAFailedAttemptRePostsAndForksTheRetryChain(t *testing.T) {
	ctx := context.Background()
	u := newUCDurable(503, 503, 503)
	sub, err := u.svc.Subscribe(ctx, events.NewSubscription{OrgID: "org_shop", EventTypes: []string{"*"}, URL: "https://shop.example/hooks", Secret: "test-placeholder-secret"})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	ev := events.Event{ID: "evt_dup_1", Type: "order.paid", OrgID: "org_shop", OccurredAt: u.clk.Now(), Payload: []byte(`{"orderId":"o_1"}`)}
	if err := u.svc.Emit(ctx, ev); err != nil {
		t.Fatalf("emit: %v", err)
	}
	u.consume(t, 0, "rcpt-1") // attempt 1 → 503, attempt 2 scheduled as message #2
	// SQS redelivers the attempt-1 message (its DeleteMessage was lost / the visibility timeout lapsed mid-POST).
	u.consume(t, 0, "rcpt-1-redelivered")

	if n := u.post.count(); n != 1 {
		t.Errorf("FINDING F-1: the customer received attempt 1 of %s %d times (want 1: the delivery already recorded attempt 1 and scheduled attempt 2)", events.DeliveryID(sub.ID, ev.ID), n)
	}
	attempt2 := 0
	for i := 0; i < u.sentCount(); i++ {
		if u.sentRef(t, i).AttemptN == 2 {
			attempt2++
		}
	}
	if attempt2 != 1 {
		t.Errorf("FINDING F-1: %d attempt-2 messages are scheduled for one delivery — the retry chain forked; every later failure is now POSTed once per chain", attempt2)
	}
}

// ucBatchSQS returns one batch on the first receive, then long-polls until shutdown.
type ucBatchSQS struct {
	mu     sync.Mutex
	batch  []string
	served bool
}

func (b *ucBatchSQS) SendMessage(context.Context, *sqs.SendMessageInput, ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	return &sqs.SendMessageOutput{}, nil
}

func (b *ucBatchSQS) ReceiveMessage(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	b.mu.Lock()
	if !b.served {
		b.served = true
		out := make([]types.Message, 0, len(b.batch))
		for i := range b.batch {
			body, receipt := b.batch[i], "rcpt-"+strconv.Itoa(i+1)
			out = append(out, types.Message{Body: &body, ReceiptHandle: &receipt})
		}
		b.mu.Unlock()
		return &sqs.ReceiveMessageOutput{Messages: out}, nil
	}
	b.mu.Unlock()
	<-ctx.Done()
	return nil, ctx.Err()
}

func (b *ucBatchSQS) DeleteMessage(context.Context, *sqs.DeleteMessageInput, ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	return &sqs.DeleteMessageOutput{}, nil
}

func (b *ucBatchSQS) ChangeMessageVisibility(context.Context, *sqs.ChangeMessageVisibilityInput, ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	return &sqs.ChangeMessageVisibilityOutput{}, nil
}

// ucShutdownDispatcher: SIGTERM lands while the FIRST message's webhook is in flight (it cancels the consumer's
// parent context from inside ProcessRef). It records the context state every message was dispatched with.
type ucShutdownDispatcher struct {
	mu      sync.Mutex
	cancel  context.CancelFunc
	ctxErrs []error
	first   chan struct{}
}

func (d *ucShutdownDispatcher) ProcessRef(ctx context.Context, _ events.JobRef) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.ctxErrs = append(d.ctxErrs, ctx.Err())
	if len(d.ctxErrs) == 1 {
		d.cancel()
		close(d.first)
	}
	return nil
}

func TestUseCaseFinding_SQSStopMidBatchStillDispatchesTheRestWithACancelledContext(t *testing.T) {
	refBody := func(id string) string {
		b, _ := json.Marshal(events.JobRef{OrgID: "org_a", DeliveryID: id, SubscriptionID: "sub_1", EventID: "evt_" + id, AttemptN: 5})
		return string(b)
	}
	client := &ucBatchSQS{batch: []string{refBody("dlv_1"), refBody("dlv_2"), refBody("dlv_3")}}
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	disp := &ucShutdownDispatcher{cancel: cancel, first: make(chan struct{})}
	q := New(Config{Client: client, QueueURL: "https://sqs.local/000000000000/webhooks-test", Workers: 1})
	stop := q.Start(parent, disp)
	select {
	case <-disp.first:
	case <-time.After(5 * time.Second):
		stop()
		t.Fatal("the first message was never dispatched")
	}
	stop() // returns once the single worker has exited

	disp.mu.Lock()
	errs := append([]error(nil), disp.ctxErrs...)
	disp.mu.Unlock()
	for i, e := range errs {
		if e != nil {
			t.Errorf("FINDING F-2: message %d of the batch was dispatched AFTER shutdown (ctx.Err()=%v); with the real Service its final attempt would be dead-lettered without ever reaching the customer", i+1, e)
		}
	}
}
