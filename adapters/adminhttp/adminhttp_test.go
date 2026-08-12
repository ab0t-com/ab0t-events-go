package adminhttp_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/ab0t-com/ab0t-events-go/adapters/adminhttp"
	"github.com/ab0t-com/ab0t-events-go/adapters/hmacsign"
	"github.com/ab0t-com/ab0t-events-go/adapters/memory"
	"github.com/ab0t-com/ab0t-events-go/adapters/std"
	"github.com/ab0t-com/ab0t-events-go/events"
)

// --- test doubles for the two ports the admin surface indirectly touches ---

type capQueue struct {
	mu   sync.Mutex
	jobs []events.Job
}

func (q *capQueue) Enqueue(_ context.Context, j events.Job) error {
	q.mu.Lock()
	q.jobs = append(q.jobs, j)
	q.mu.Unlock()
	return nil
}
func (q *capQueue) count() int { q.mu.Lock(); defer q.mu.Unlock(); return len(q.jobs) }

type nopDeliverer struct{}

func (nopDeliverer) Post(context.Context, events.DeliveryRequest) (events.DeliveryResponse, error) {
	return events.DeliveryResponse{StatusCode: 200}, nil
}

func newHandler(t *testing.T) (*adminhttp.Handler, *events.Service, *memory.Log, *capQueue) {
	t.Helper()
	store := memory.NewStore()
	log := memory.NewLog()
	q := &capQueue{}
	svc := events.NewService(events.Deps{
		Store: store, Queue: q, Deliverer: nopDeliverer{}, Signer: hmacsign.New(),
		Log: log, Clock: std.Clock{}, IDGen: std.IDGen{}, Rand: std.Rand{},
	})
	// Org comes from the X-Org header in tests (stand-in for real auth).
	h := adminhttp.New(svc, func(r *http.Request) (string, error) {
		if org := r.Header.Get("X-Org"); org != "" {
			return org, nil
		}
		return "", http.ErrNoCookie
	})
	return h, svc, log, q
}

func req(t *testing.T, h http.Handler, method, path, org, body string) *httptest.ResponseRecorder {
	t.Helper()
	var r *http.Request
	if body != "" {
		r = httptest.NewRequest(method, path, strings.NewReader(body))
	} else {
		r = httptest.NewRequest(method, path, nil)
	}
	if org != "" {
		r.Header.Set("X-Org", org)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestAdmin_SubscriptionCRUDAndRotate(t *testing.T) {
	h, _, _, _ := newHandler(t)

	// Create — secret is auto-generated and returned once.
	w := req(t, h, "POST", "/subscriptions", "orgA", `{"eventTypes":["drive.*"],"url":"https://x/hook"}`)
	if w.Code != http.StatusCreated {
		t.Fatalf("create code = %d body=%s", w.Code, w.Body)
	}
	var created struct {
		ID     string `json:"id"`
		Secret string `json:"secret"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &created)
	if created.ID == "" || !strings.HasPrefix(created.Secret, "whsec_") {
		t.Fatalf("bad create response: %s", w.Body)
	}

	// Get — secret must NOT be present.
	w = req(t, h, "GET", "/subscriptions/"+created.ID, "orgA", "")
	if w.Code != 200 || strings.Contains(w.Body.String(), "whsec_") {
		t.Fatalf("get leaked secret or failed: %d %s", w.Code, w.Body)
	}

	// Rotate — returns a new, different secret.
	w = req(t, h, "POST", "/subscriptions/"+created.ID+"/rotate-secret", "orgA", "")
	var rot struct {
		Secret string `json:"secret"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &rot)
	if w.Code != 200 || rot.Secret == "" || rot.Secret == created.Secret {
		t.Fatalf("rotate failed: %d %s", w.Code, w.Body)
	}

	// List.
	w = req(t, h, "GET", "/subscriptions", "orgA", "")
	if !strings.Contains(w.Body.String(), created.ID) {
		t.Fatalf("list missing sub: %s", w.Body)
	}

	// Delete.
	if w := req(t, h, "DELETE", "/subscriptions/"+created.ID, "orgA", ""); w.Code != http.StatusNoContent {
		t.Fatalf("delete code = %d", w.Code)
	}
}

func TestAdmin_Ping(t *testing.T) {
	h, _, _, q := newHandler(t)
	w := req(t, h, "POST", "/subscriptions", "orgA", `{"eventTypes":["drive.*"],"url":"https://x/hook"}`)
	var created struct {
		ID string `json:"id"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &created)

	w = req(t, h, "POST", "/subscriptions/"+created.ID+"/ping", "orgA", "")
	if w.Code != http.StatusAccepted {
		t.Fatalf("ping code = %d body=%s", w.Code, w.Body)
	}
	if q.count() != 1 {
		t.Fatalf("ping enqueued %d jobs, want 1", q.count())
	}
	if !strings.Contains(w.Body.String(), "webhook.ping") {
		t.Fatalf("ping response missing event type: %s", w.Body)
	}
}

func TestAdmin_EventTypes(t *testing.T) {
	store := memory.NewStore()
	log := memory.NewLog()
	q := &capQueue{}
	svc := events.NewService(events.Deps{
		Store: store, Queue: q, Deliverer: nopDeliverer{}, Signer: hmacsign.New(),
		Log: log, Clock: std.Clock{}, IDGen: std.IDGen{}, Rand: std.Rand{},
	})
	cat := events.NewCatalog(true).Register(
		events.EventTypeDef{Type: "drive.file.created", Description: "A file was created"},
		events.EventTypeDef{Type: "drive.file.deleted", Deprecated: true},
	)
	h := adminhttp.New(svc, func(r *http.Request) (string, error) {
		if org := r.Header.Get("X-Org"); org != "" {
			return org, nil
		}
		return "", http.ErrNoCookie
	}, adminhttp.WithCatalog(cat))

	// Requires auth.
	if w := req(t, h, "GET", "/event-types", "", ""); w.Code != http.StatusUnauthorized {
		t.Fatalf("event-types without org should be 401, got %d", w.Code)
	}

	// Returns the catalog in registration order.
	w := req(t, h, "GET", "/event-types", "orgA", "")
	if w.Code != 200 {
		t.Fatalf("event-types code = %d body=%s", w.Code, w.Body)
	}
	var got struct {
		EventTypes []events.EventTypeDef `json:"eventTypes"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.EventTypes) != 2 || got.EventTypes[0].Type != "drive.file.created" || !got.EventTypes[1].Deprecated {
		t.Fatalf("unexpected event types: %+v", got.EventTypes)
	}
}

func TestAdmin_EventTypesEmptyWithoutCatalog(t *testing.T) {
	h, _, _, _ := newHandler(t) // no catalog wired
	w := req(t, h, "GET", "/event-types", "orgA", "")
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"eventTypes":[]`) {
		t.Fatalf("no-catalog event-types should be empty list: %d %s", w.Code, w.Body)
	}
}

func TestAdmin_CreateWithPayloadFilters(t *testing.T) {
	h, svc, _, q := newHandler(t)
	ctx := context.Background()

	// Create a subscription carrying a payload filter.
	w := req(t, h, "POST", "/subscriptions", "orgA",
		`{"eventTypes":["drive.*"],"url":"https://x/hook","payloadFilters":[{"path":"region","equals":"eu"}]}`)
	if w.Code != http.StatusCreated {
		t.Fatalf("create code = %d body=%s", w.Code, w.Body)
	}

	// A non-matching payload is filtered out (no delivery enqueued)...
	_ = svc.Emit(ctx, mkEvent("e1", "drive.file.created", "orgA", `{"region":"us"}`))
	if q.count() != 0 {
		t.Fatalf("non-matching payload should not enqueue, got %d", q.count())
	}
	// ...a matching payload is delivered.
	_ = svc.Emit(ctx, mkEvent("e2", "drive.file.created", "orgA", `{"region":"eu"}`))
	if q.count() != 1 {
		t.Fatalf("matching payload should enqueue 1, got %d", q.count())
	}
}

func mkEvent(id, typ, org, payload string) events.Event {
	return events.Event{ID: id, Type: typ, OrgID: org, Payload: []byte(payload)}
}

func TestAdmin_Unauthorized(t *testing.T) {
	h, _, _, _ := newHandler(t)
	if w := req(t, h, "GET", "/subscriptions", "", ""); w.Code != http.StatusUnauthorized {
		t.Fatalf("missing org should be 401, got %d", w.Code)
	}
}

func TestAdmin_Tenancy(t *testing.T) {
	h, _, _, _ := newHandler(t)
	w := req(t, h, "POST", "/subscriptions", "orgA", `{"eventTypes":["*"],"url":"https://x/hook"}`)
	var created struct {
		ID string `json:"id"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &created)
	// orgB must not see orgA's subscription.
	if w := req(t, h, "GET", "/subscriptions/"+created.ID, "orgB", ""); w.Code != http.StatusNotFound {
		t.Fatalf("cross-org get should be 404, got %d", w.Code)
	}
}

func TestAdmin_DeliveryDetailReplayAndUI(t *testing.T) {
	h, svc, log, q := newHandler(t)
	ctx := context.Background()

	// A subscription + a seeded delivery in its log.
	sub, _ := svc.Subscribe(ctx, events.NewSubscription{OrgID: "orgA", EventTypes: []string{"*"}, URL: "https://x/hook", Secret: "whsec_x"})
	id := events.DeliveryID(sub.ID, "e1")
	_ = log.Record(ctx, events.DeliveryRecord{
		Delivery: events.Delivery{ID: id, OrgID: "orgA", SubscriptionID: sub.ID, EventID: "e1", EventType: "drive.file.created", Status: events.StatusDead, Attempts: 3, LastError: "http 500"},
		Event:    events.EventSnapshot{ID: "e1", Type: "drive.file.created", OrgID: "orgA", Payload: []byte(`{}`)},
		Attempt:  events.Attempt{N: 1, Code: 500},
	})

	// List + filter by status.
	w := req(t, h, "GET", "/deliveries?status=dead", "orgA", "")
	if w.Code != 200 || !strings.Contains(w.Body.String(), id) {
		t.Fatalf("delivery list failed: %d %s", w.Code, w.Body)
	}

	// Detail includes attempts.
	w = req(t, h, "GET", "/deliveries/"+id, "orgA", "")
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"attempts"`) {
		t.Fatalf("detail failed: %d %s", w.Code, w.Body)
	}

	// HTML UI renders the delivery (still dead at this point).
	w = req(t, h, "GET", "/ui", "orgA", "")
	if w.Code != 200 || !strings.Contains(w.Header().Get("Content-Type"), "text/html") {
		t.Fatalf("ui content-type: %d %s", w.Code, w.Header().Get("Content-Type"))
	}
	if !strings.Contains(w.Body.String(), id) || !strings.Contains(w.Body.String(), "dead") {
		t.Fatalf("ui missing delivery row: %s", w.Body)
	}

	// Replay enqueues a job AND resets the delivery's log status to pending (audit
	// Finding 7) so it is redeliverable through a durable/off-process queue too.
	if w := req(t, h, "POST", "/deliveries/"+id+"/replay", "orgA", ""); w.Code != http.StatusAccepted {
		t.Fatalf("replay code = %d body=%s", w.Code, w.Body)
	}
	if q.count() != 1 {
		t.Fatalf("replay enqueued %d jobs, want 1", q.count())
	}
	if del, _, _, _ := svc.GetDelivery(ctx, "orgA", id); del.Status != events.StatusPending {
		t.Fatalf("replay should reset log status to pending, got %q", del.Status)
	}
}
