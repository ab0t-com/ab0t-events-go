package memory

import (
	"context"
	"testing"
	"time"

	"github.com/ab0t-com/ab0t-events-go/events"
)

func TestStore_CRUDAndOrgIsolation(t *testing.T) {
	ctx := context.Background()
	s := NewStore()
	_ = s.Put(ctx, events.Subscription{ID: "a1", OrgID: "orgA", EventTypes: []string{"drive.*"}, URL: "u", Active: true})
	_ = s.Put(ctx, events.Subscription{ID: "a2", OrgID: "orgA", EventTypes: []string{"mail.sent"}, URL: "u", Active: true})
	_ = s.Put(ctx, events.Subscription{ID: "b1", OrgID: "orgB", EventTypes: []string{"*"}, URL: "u", Active: true})

	if list, _ := s.ListForOrg(ctx, "orgA"); len(list) != 2 {
		t.Fatalf("orgA list = %d, want 2", len(list))
	}
	// orgB's wildcard must never surface under orgA.
	if list, _ := s.ListForOrgAndType(ctx, "orgA", "drive.file.created"); len(list) != 1 || list[0].ID != "a1" {
		t.Fatalf("type match wrong: %+v", list)
	}
	if _, err := s.Get(ctx, "orgB", "a1"); err != events.ErrNotFound {
		t.Fatalf("cross-org Get should be ErrNotFound, got %v", err)
	}
	_ = s.Delete(ctx, "orgA", "a1")
	if _, err := s.Get(ctx, "orgA", "a1"); err != events.ErrNotFound {
		t.Fatalf("after delete want ErrNotFound, got %v", err)
	}
}

func TestLog_RecordGetAndAttemptAppend(t *testing.T) {
	ctx := context.Background()
	l := NewLog()
	now := time.Unix(1000, 0).UTC()
	del := events.Delivery{ID: "d1", OrgID: "orgA", SubscriptionID: "s1", Status: events.StatusPending, CreatedAt: now}
	snap := events.EventSnapshot{ID: "e1", Type: "t", OrgID: "orgA", Payload: []byte(`{}`)}

	// First record stores the snapshot; subsequent records append attempts.
	_ = l.Record(ctx, events.DeliveryRecord{Delivery: del, Event: snap})
	del.Status = events.StatusFailed
	_ = l.Record(ctx, events.DeliveryRecord{Delivery: del, Attempt: events.Attempt{N: 1, Code: 500}})
	del.Status = events.StatusDead
	_ = l.Record(ctx, events.DeliveryRecord{Delivery: del, Attempt: events.Attempt{N: 2, Code: 500}})

	gotDel, gotSnap, atts, err := l.Get(ctx, "orgA", "d1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if gotDel.Status != events.StatusDead {
		t.Fatalf("status = %s, want dead", gotDel.Status)
	}
	if gotSnap.ID != "e1" {
		t.Fatalf("snapshot not preserved: %+v", gotSnap)
	}
	if len(atts) != 2 {
		t.Fatalf("attempts = %d, want 2", len(atts))
	}
	// Cross-org Get is denied.
	if _, _, _, err := l.Get(ctx, "orgB", "d1"); err != events.ErrNotFound {
		t.Fatalf("cross-org get want ErrNotFound, got %v", err)
	}
}

func TestLog_ListPaginationAndFilter(t *testing.T) {
	ctx := context.Background()
	l := NewLog()
	for i := 0; i < 5; i++ {
		id := string(rune('a' + i))
		sub := "s1"
		if i%2 == 1 {
			sub = "s2"
		}
		_ = l.Record(ctx, events.DeliveryRecord{Delivery: events.Delivery{ID: id, OrgID: "orgA", SubscriptionID: sub}})
	}

	// Page 1 of 2.
	page, cursor, err := l.List(ctx, "orgA", events.DeliveryFilter{Limit: 2})
	if err != nil || len(page) != 2 || cursor == "" {
		t.Fatalf("page1 = %d cursor=%q err=%v", len(page), cursor, err)
	}
	// Walk the cursor to exhaustion.
	total := len(page)
	for cursor != "" {
		page, cursor, _ = l.List(ctx, "orgA", events.DeliveryFilter{Limit: 2, Cursor: cursor})
		total += len(page)
	}
	if total != 5 {
		t.Fatalf("paged total = %d, want 5", total)
	}
	// Subscription filter.
	only, _, _ := l.List(ctx, "orgA", events.DeliveryFilter{SubscriptionID: "s2", Limit: 100})
	if len(only) != 2 {
		t.Fatalf("filter s2 = %d, want 2", len(only))
	}
	// Empty org.
	if p, _, _ := l.List(ctx, "orgZ", events.DeliveryFilter{}); len(p) != 0 {
		t.Fatalf("unknown org returned %d deliveries", len(p))
	}
}
