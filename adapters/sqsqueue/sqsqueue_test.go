package sqsqueue

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/ab0t-com/ab0t-events-go/events"
)

// fakeSQS is an in-memory stand-in for the SQS client, enough to exercise the
// adapter offline (no AWS, no network).
type fakeSQS struct {
	mu       sync.Mutex
	sent     []sentMsg
	msgs     []*fakeMsg
	deleted  []string
	visCalls []visCall
	nextID   int
}

type sentMsg struct {
	body  string
	delay int32
}
type fakeMsg struct {
	body     string
	receipt  string
	inFlight bool
	deleted  bool
}
type visCall struct {
	receipt string
	secs    int32
}

func newFakeSQS() *fakeSQS { return &fakeSQS{} }

func (f *fakeSQS) SendMessage(_ context.Context, in *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextID++
	f.sent = append(f.sent, sentMsg{body: *in.MessageBody, delay: in.DelaySeconds})
	f.msgs = append(f.msgs, &fakeMsg{body: *in.MessageBody, receipt: "r" + strconv.Itoa(f.nextID)})
	return &sqs.SendMessageOutput{}, nil
}

func (f *fakeSQS) ReceiveMessage(_ context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	f.mu.Lock()
	var out []types.Message
	for _, m := range f.msgs {
		if m.inFlight || m.deleted {
			continue
		}
		m.inFlight = true
		body, receipt := m.body, m.receipt
		out = append(out, types.Message{Body: &body, ReceiptHandle: &receipt})
	}
	f.mu.Unlock()
	if len(out) == 0 {
		time.Sleep(time.Millisecond) // stand in for SQS long-poll; avoids busy-spin
	}
	return &sqs.ReceiveMessageOutput{Messages: out}, nil
}

func (f *fakeSQS) DeleteMessage(_ context.Context, in *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleted = append(f.deleted, *in.ReceiptHandle)
	for _, m := range f.msgs {
		if m.receipt == *in.ReceiptHandle {
			m.deleted = true
		}
	}
	return &sqs.DeleteMessageOutput{}, nil
}

func (f *fakeSQS) ChangeMessageVisibility(_ context.Context, in *sqs.ChangeMessageVisibilityInput, _ ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.visCalls = append(f.visCalls, visCall{receipt: *in.ReceiptHandle, secs: in.VisibilityTimeout})
	return &sqs.ChangeMessageVisibilityOutput{}, nil
}

func (f *fakeSQS) counts() (sent, deleted, vis int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.sent), len(f.deleted), len(f.visCalls)
}

// refRecorder is a fake events.RefDispatcher.
type refRecorder struct {
	mu   sync.Mutex
	refs []events.JobRef
	err  error
}

func (r *refRecorder) ProcessRef(_ context.Context, ref events.JobRef) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.refs = append(r.refs, ref)
	return r.err
}
func (r *refRecorder) count() int { r.mu.Lock(); defer r.mu.Unlock(); return len(r.refs) }

func sampleJob(notBefore time.Time) events.Job {
	return events.Job{
		Delivery:     events.Delivery{ID: "dlv_1", OrgID: "orgA", SubscriptionID: "s1", EventID: "e1"},
		Event:        events.Event{ID: "e1", Type: "drive.file.created", OrgID: "orgA", Payload: []byte(`{"secret":"payload"}`)},
		Subscription: events.Subscription{ID: "s1", OrgID: "orgA", Secret: "whsec_super_secret", URL: "https://x"},
		AttemptN:     1,
		NotBefore:    notBefore,
	}
}

func TestEnqueue_SendsIdOnlyRefNoSecrets(t *testing.T) {
	f := newFakeSQS()
	now := time.Unix(1_700_000_000, 0)
	q := New(Config{Client: f, QueueURL: "url", Now: func() time.Time { return now }})

	if err := q.Enqueue(context.Background(), sampleJob(now.Add(45*time.Second))); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if len(f.sent) != 1 {
		t.Fatalf("sent %d, want 1", len(f.sent))
	}
	msg := f.sent[0]
	if msg.delay != 45 {
		t.Fatalf("delay = %d, want 45", msg.delay)
	}
	// The body must be an id-only JobRef — no payload, no secret.
	if contains(msg.body, "super_secret") || contains(msg.body, "payload") {
		t.Fatalf("queue message leaked a secret/payload: %s", msg.body)
	}
	var ref events.JobRef
	if err := json.Unmarshal([]byte(msg.body), &ref); err != nil {
		t.Fatalf("body not a JobRef: %v", err)
	}
	if ref.DeliveryID != "dlv_1" || ref.SubscriptionID != "s1" || ref.EventID != "e1" || ref.AttemptN != 1 {
		t.Fatalf("bad ref: %+v", ref)
	}
}

func TestEnqueue_DelayCappedAtSQSMax(t *testing.T) {
	f := newFakeSQS()
	now := time.Unix(1_700_000_000, 0)
	q := New(Config{Client: f, QueueURL: "url", Now: func() time.Time { return now }})
	// 1h backoff must cap at 900s on send.
	_ = q.Enqueue(context.Background(), sampleJob(now.Add(time.Hour)))
	if f.sent[0].delay != maxDelaySeconds {
		t.Fatalf("delay = %d, want %d", f.sent[0].delay, maxDelaySeconds)
	}
}

func TestHandle_SuccessDeletesMessage(t *testing.T) {
	f := newFakeSQS()
	now := time.Unix(1_700_000_000, 0)
	q := New(Config{Client: f, QueueURL: "url", Now: func() time.Time { return now }})
	rec := &refRecorder{}

	ref := events.JobRef{OrgID: "orgA", DeliveryID: "dlv_1", SubscriptionID: "s1", EventID: "e1", AttemptN: 1, NotBefore: now}
	body, _ := json.Marshal(ref)
	receipt := "r1"
	q.handle(context.Background(), rec, ptr(string(body)), &receipt)

	if rec.count() != 1 {
		t.Fatalf("ProcessRef called %d times, want 1", rec.count())
	}
	if _, deleted, _ := f.counts(); deleted != 1 {
		t.Fatalf("message not deleted after success")
	}
}

func TestHandle_InfraErrorLeavesMessage(t *testing.T) {
	f := newFakeSQS()
	now := time.Unix(1_700_000_000, 0)
	q := New(Config{Client: f, QueueURL: "url", Now: func() time.Time { return now }})
	rec := &refRecorder{err: errors.New("store down")}

	ref := events.JobRef{OrgID: "orgA", DeliveryID: "dlv_1", NotBefore: now}
	body, _ := json.Marshal(ref)
	receipt := "r1"
	q.handle(context.Background(), rec, ptr(string(body)), &receipt)

	if _, deleted, _ := f.counts(); deleted != 0 {
		t.Fatal("message deleted despite an infrastructure error (should redeliver)")
	}
}

func TestHandle_FutureNotBeforeRedefers(t *testing.T) {
	f := newFakeSQS()
	now := time.Unix(1_700_000_000, 0)
	q := New(Config{Client: f, QueueURL: "url", Now: func() time.Time { return now }})
	rec := &refRecorder{}

	ref := events.JobRef{OrgID: "orgA", DeliveryID: "dlv_1", NotBefore: now.Add(30 * time.Minute)}
	body, _ := json.Marshal(ref)
	receipt := "r1"
	q.handle(context.Background(), rec, ptr(string(body)), &receipt)

	if rec.count() != 0 {
		t.Fatal("processed a job before its NotBefore")
	}
	_, deleted, vis := f.counts()
	if vis != 1 || deleted != 0 {
		t.Fatalf("expected a visibility change and no delete, got vis=%d deleted=%d", vis, deleted)
	}
	if f.visCalls[0].secs != 1800 {
		t.Fatalf("re-defer secs = %d, want 1800", f.visCalls[0].secs)
	}
}

func TestHandle_UnparseableMessageDropped(t *testing.T) {
	f := newFakeSQS()
	q := New(Config{Client: f, QueueURL: "url"})
	rec := &refRecorder{}
	receipt := "r1"
	q.handle(context.Background(), rec, ptr("not json"), &receipt)
	if rec.count() != 0 {
		t.Fatal("dispatched an unparseable message")
	}
	if _, deleted, _ := f.counts(); deleted != 1 {
		t.Fatal("poison message not dropped")
	}
}

func TestStart_ConsumesEnqueuedMessage(t *testing.T) {
	f := newFakeSQS()
	q := New(Config{Client: f, QueueURL: "url", Workers: 2})
	rec := &refRecorder{}

	if err := q.Enqueue(context.Background(), sampleJob(time.Now())); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	stop := q.Start(context.Background(), rec)
	defer stop()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, deleted, _ := f.counts(); deleted >= 1 && rec.count() >= 1 {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("message not consumed: processed=%d deleted via counts", rec.count())
}

func ptr(s string) *string { return &s }

func contains(hay, needle string) bool {
	return len(hay) >= len(needle) && (indexOf(hay, needle) >= 0)
}
func indexOf(hay, needle string) int {
	for i := 0; i+len(needle) <= len(hay); i++ {
		if hay[i:i+len(needle)] == needle {
			return i
		}
	}
	return -1
}
