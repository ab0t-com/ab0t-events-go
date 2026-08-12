// Package memory provides in-memory SubscriptionStore and DeliveryLog adapters
// for ab0t-events-go. They are the reference implementations of the store ports:
// production uses the dynamo adapter, but memory is ideal for tests, examples,
// and single-process deployments. Both are org-partitioned and concurrency-safe.
package memory

import (
	"context"
	"sort"
	"strconv"
	"sync"

	"github.com/ab0t-com/ab0t-events-go/events"
)

// Store is an in-memory events.SubscriptionStore, partitioned by orgID.
type Store struct {
	mu    sync.RWMutex
	byOrg map[string]map[string]events.Subscription // orgID -> subID -> sub
}

var _ events.SubscriptionStore = (*Store)(nil)

// NewStore returns an empty subscription store.
func NewStore() *Store {
	return &Store{byOrg: map[string]map[string]events.Subscription{}}
}

func (s *Store) ListForOrgAndType(_ context.Context, orgID, eventType string) ([]events.Subscription, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []events.Subscription
	for _, sub := range s.byOrg[orgID] {
		if sub.Matches(eventType) {
			out = append(out, sub)
		}
	}
	sortSubs(out)
	return out, nil
}

func (s *Store) ListForOrg(_ context.Context, orgID string) ([]events.Subscription, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []events.Subscription
	for _, sub := range s.byOrg[orgID] {
		out = append(out, sub)
	}
	sortSubs(out)
	return out, nil
}

func (s *Store) Get(_ context.Context, orgID, subID string) (events.Subscription, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if m, ok := s.byOrg[orgID]; ok {
		if sub, ok := m[subID]; ok {
			return sub, nil
		}
	}
	return events.Subscription{}, events.ErrNotFound
}

func (s *Store) Put(_ context.Context, sub events.Subscription) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.byOrg[sub.OrgID] == nil {
		s.byOrg[sub.OrgID] = map[string]events.Subscription{}
	}
	s.byOrg[sub.OrgID][sub.ID] = sub
	return nil
}

func (s *Store) Delete(_ context.Context, orgID, subID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if m, ok := s.byOrg[orgID]; ok {
		delete(m, subID)
	}
	return nil
}

func sortSubs(subs []events.Subscription) {
	sort.Slice(subs, func(i, j int) bool { return subs[i].ID < subs[j].ID })
}

// Log is an in-memory events.DeliveryLog, partitioned by orgID.
type Log struct {
	mu    sync.RWMutex
	byOrg map[string]map[string]*logEntry // orgID -> deliveryID -> entry
	order map[string][]string             // orgID -> deliveryIDs in first-seen order
}

type logEntry struct {
	delivery events.Delivery
	event    events.EventSnapshot
	attempts []events.Attempt
}

var _ events.DeliveryLog = (*Log)(nil)

// NewLog returns an empty delivery log.
func NewLog() *Log {
	return &Log{
		byOrg: map[string]map[string]*logEntry{},
		order: map[string][]string{},
	}
}

func (l *Log) Record(_ context.Context, rec events.DeliveryRecord) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	org := rec.Delivery.OrgID
	if l.byOrg[org] == nil {
		l.byOrg[org] = map[string]*logEntry{}
	}
	e, ok := l.byOrg[org][rec.Delivery.ID]
	if !ok {
		e = &logEntry{event: rec.Event}
		l.byOrg[org][rec.Delivery.ID] = e
		l.order[org] = append(l.order[org], rec.Delivery.ID)
	}
	e.delivery = rec.Delivery
	if e.event.ID == "" && rec.Event.ID != "" {
		e.event = rec.Event
	}
	if rec.Attempt.N > 0 {
		e.attempts = append(e.attempts, rec.Attempt)
	}
	return nil
}

func (l *Log) Get(_ context.Context, orgID, deliveryID string) (events.Delivery, events.EventSnapshot, []events.Attempt, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if m, ok := l.byOrg[orgID]; ok {
		if e, ok := m[deliveryID]; ok {
			atts := append([]events.Attempt(nil), e.attempts...)
			return e.delivery, e.event, atts, nil
		}
	}
	return events.Delivery{}, events.EventSnapshot{}, nil, events.ErrNotFound
}

func (l *Log) List(_ context.Context, orgID string, f events.DeliveryFilter) ([]events.Delivery, string, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	ids := l.order[orgID]
	start := 0
	if f.Cursor != "" {
		if n, err := strconv.Atoi(f.Cursor); err == nil && n >= 0 {
			start = n
		}
	}
	limit := f.Limit
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	var out []events.Delivery
	i := start
	for ; i < len(ids) && len(out) < limit; i++ {
		e := l.byOrg[orgID][ids[i]]
		if f.SubscriptionID != "" && e.delivery.SubscriptionID != f.SubscriptionID {
			continue
		}
		if f.Status != "" && e.delivery.Status != f.Status {
			continue
		}
		out = append(out, e.delivery)
	}
	next := ""
	if i < len(ids) {
		next = strconv.Itoa(i)
	}
	return out, next, nil
}
