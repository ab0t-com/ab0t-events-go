// Package adminhttp is a drop-in, stdlib-only HTTP admin surface for
// ab0t-events-go: subscription CRUD, secret rotation, delivery log browsing,
// delivery detail, and replay — plus a self-contained HTML delivery-log view.
// Mount it behind your own auth; it takes the caller's org from an injectable
// OrgResolver and never trusts a client-supplied org id.
//
//	h := adminhttp.New(emitter, func(r *http.Request) (string, error) {
//	    return authctx.OrgID(r.Context()) // your auth
//	})
//	mux.Handle("/admin/webhooks/", http.StripPrefix("/admin/webhooks", h))
//
// All JSON. Secrets are returned only once, at create and rotate time; they are
// never included in list/detail responses.
package adminhttp

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/ab0t-com/ab0t-events-go/events"
)

// API is the subset of the emitter the admin surface needs. *events.Service
// satisfies it.
type API interface {
	Subscribe(ctx context.Context, in events.NewSubscription) (events.Subscription, error)
	Unsubscribe(ctx context.Context, orgID, subscriptionID string) error
	ListSubscriptions(ctx context.Context, orgID string) ([]events.Subscription, error)
	GetSubscription(ctx context.Context, orgID, subscriptionID string) (events.Subscription, error)
	RotateSecret(ctx context.Context, orgID, subscriptionID string) (string, error)
	Ping(ctx context.Context, orgID, subscriptionID string) (events.Delivery, error)
	ListDeliveries(ctx context.Context, orgID string, f events.DeliveryFilter) ([]events.Delivery, string, error)
	GetDelivery(ctx context.Context, orgID, deliveryID string) (events.Delivery, events.EventSnapshot, []events.Attempt, error)
	Replay(ctx context.Context, orgID, deliveryID string) error
}

// OrgResolver extracts the authenticated caller's org id from the request. Return
// a non-nil error to reject the request as unauthorized.
type OrgResolver func(r *http.Request) (orgID string, err error)

// Handler serves the admin API. Construct it with New.
type Handler struct {
	api     API
	org     OrgResolver
	mux     *http.ServeMux
	catalog *events.Catalog
}

// Option customizes a Handler at construction time.
type Option func(*Handler)

// WithCatalog wires an event-type Catalog so the admin surface serves
// GET /event-types (the service's declared event vocabulary). Without it, that
// endpoint returns an empty list.
func WithCatalog(c *events.Catalog) Option {
	return func(h *Handler) { h.catalog = c }
}

// New builds the admin Handler.
func New(api API, org OrgResolver, opts ...Option) *Handler {
	h := &Handler{api: api, org: org, mux: http.NewServeMux()}
	for _, o := range opts {
		o(h)
	}
	h.mux.HandleFunc("POST /subscriptions", h.createSubscription)
	h.mux.HandleFunc("GET /subscriptions", h.listSubscriptions)
	h.mux.HandleFunc("GET /subscriptions/{id}", h.getSubscription)
	h.mux.HandleFunc("DELETE /subscriptions/{id}", h.deleteSubscription)
	h.mux.HandleFunc("POST /subscriptions/{id}/rotate-secret", h.rotateSecret)
	h.mux.HandleFunc("POST /subscriptions/{id}/ping", h.ping)
	h.mux.HandleFunc("GET /event-types", h.listEventTypes)
	h.mux.HandleFunc("GET /deliveries", h.listDeliveries)
	h.mux.HandleFunc("GET /deliveries/{id}", h.getDelivery)
	h.mux.HandleFunc("POST /deliveries/{id}/replay", h.replay)
	h.mux.HandleFunc("GET /ui", h.ui)
	return h
}

// listEventTypes serves the service's declared event vocabulary. The catalog is
// service-global (identical for every org), but the endpoint still requires a
// resolved caller so it stays behind the same auth as the rest of the surface.
func (h *Handler) listEventTypes(w http.ResponseWriter, r *http.Request) {
	if _, ok := h.auth(w, r); !ok {
		return
	}
	types := []events.EventTypeDef{}
	if h.catalog != nil {
		types = h.catalog.List()
	}
	writeJSON(w, http.StatusOK, map[string]any{"eventTypes": types})
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) { h.mux.ServeHTTP(w, r) }

// --- subscription handlers ---

type createSubReq struct {
	EventTypes     []string               `json:"eventTypes"`
	URL            string                 `json:"url"`
	Secret         string                 `json:"secret,omitempty"`         // optional; generated if empty
	PayloadFilters []events.PayloadFilter `json:"payloadFilters,omitempty"` // optional content filters
}

type subResp struct {
	events.Subscription
	Secret string `json:"secret,omitempty"` // populated only on create/rotate
}

func (h *Handler) createSubscription(w http.ResponseWriter, r *http.Request) {
	org, ok := h.auth(w, r)
	if !ok {
		return
	}
	var body createSubReq
	if !decode(w, r, &body) {
		return
	}
	secret := body.Secret
	if secret == "" {
		s, err := generateSecret()
		if err != nil {
			writeErr(w, err)
			return
		}
		secret = s
	}
	sub, err := h.api.Subscribe(r.Context(), events.NewSubscription{
		OrgID: org, EventTypes: body.EventTypes, URL: body.URL, Secret: secret,
		PayloadFilters: body.PayloadFilters,
	})
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, subResp{Subscription: sub, Secret: secret})
}

func (h *Handler) listSubscriptions(w http.ResponseWriter, r *http.Request) {
	org, ok := h.auth(w, r)
	if !ok {
		return
	}
	subs, err := h.api.ListSubscriptions(r.Context(), org)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"subscriptions": subs})
}

func (h *Handler) getSubscription(w http.ResponseWriter, r *http.Request) {
	org, ok := h.auth(w, r)
	if !ok {
		return
	}
	sub, err := h.api.GetSubscription(r.Context(), org, r.PathValue("id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, sub) // Secret is json:"-" — never leaked
}

func (h *Handler) deleteSubscription(w http.ResponseWriter, r *http.Request) {
	org, ok := h.auth(w, r)
	if !ok {
		return
	}
	if err := h.api.Unsubscribe(r.Context(), org, r.PathValue("id")); err != nil {
		writeErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) rotateSecret(w http.ResponseWriter, r *http.Request) {
	org, ok := h.auth(w, r)
	if !ok {
		return
	}
	secret, err := h.api.RotateSecret(r.Context(), org, r.PathValue("id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"secret": secret})
}

func (h *Handler) ping(w http.ResponseWriter, r *http.Request) {
	org, ok := h.auth(w, r)
	if !ok {
		return
	}
	del, err := h.api.Ping(r.Context(), org, r.PathValue("id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"status": "queued", "delivery": del})
}

// --- delivery handlers ---

func (h *Handler) listDeliveries(w http.ResponseWriter, r *http.Request) {
	org, ok := h.auth(w, r)
	if !ok {
		return
	}
	page, cursor, err := h.api.ListDeliveries(r.Context(), org, deliveryFilter(r))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"deliveries": page, "nextCursor": cursor})
}

type deliveryDetail struct {
	Delivery events.Delivery      `json:"delivery"`
	Event    events.EventSnapshot `json:"event"`
	Attempts []events.Attempt     `json:"attempts"`
}

func (h *Handler) getDelivery(w http.ResponseWriter, r *http.Request) {
	org, ok := h.auth(w, r)
	if !ok {
		return
	}
	del, snap, atts, err := h.api.GetDelivery(r.Context(), org, r.PathValue("id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, deliveryDetail{Delivery: del, Event: snap, Attempts: atts})
}

func (h *Handler) replay(w http.ResponseWriter, r *http.Request) {
	org, ok := h.auth(w, r)
	if !ok {
		return
	}
	if err := h.api.Replay(r.Context(), org, r.PathValue("id")); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "queued"})
}

func deliveryFilter(r *http.Request) events.DeliveryFilter {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	return events.DeliveryFilter{
		SubscriptionID: q.Get("subscriptionId"),
		Status:         events.DeliveryStatus(q.Get("status")),
		Cursor:         q.Get("cursor"),
		Limit:          limit,
	}
}

// --- helpers ---

func (h *Handler) auth(w http.ResponseWriter, r *http.Request) (string, bool) {
	org, err := h.org(r)
	if err != nil || org == "" {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
		return "", false
	}
	return org, true
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(v); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// writeErr maps domain errors to HTTP status codes.
func writeErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, events.ErrNotFound):
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
	case errors.Is(err, events.ErrInvalidSubscription), errors.Is(err, events.ErrInvalidEvent):
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
	case errors.Is(err, events.ErrTenancyViolation):
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "forbidden"})
	default:
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
	}
}
