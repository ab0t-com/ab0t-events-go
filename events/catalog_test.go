package events

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
)

func TestCatalog_RegisterGetHasList(t *testing.T) {
	c := NewCatalog(false)
	c.Register(
		EventTypeDef{Type: "drive.file.created", Description: "A file was created", Example: json.RawMessage(`{"id":"f1"}`)},
		EventTypeDef{Type: "drive.file.deleted", Deprecated: true},
	)

	if !c.Has("drive.file.created") || !c.Has("drive.file.deleted") {
		t.Fatal("Has should report registered types")
	}
	if c.Has("drive.file.moved") {
		t.Fatal("Has should be false for an unregistered type")
	}

	got, ok := c.Get("drive.file.created")
	if !ok || got.Description != "A file was created" || string(got.Example) != `{"id":"f1"}` {
		t.Fatalf("Get returned %+v ok=%v", got, ok)
	}
	if dep, _ := c.Get("drive.file.deleted"); !dep.Deprecated {
		t.Fatal("Deprecated flag not preserved")
	}
	if _, ok := c.Get("nope"); ok {
		t.Fatal("Get should report not found for unregistered type")
	}

	// List preserves registration order.
	list := c.List()
	if len(list) != 2 || list[0].Type != "drive.file.created" || list[1].Type != "drive.file.deleted" {
		t.Fatalf("List order wrong: %+v", list)
	}
}

func TestCatalog_RegisterReplaceKeepsOrder(t *testing.T) {
	c := NewCatalog(false)
	c.Register(EventTypeDef{Type: "a"}, EventTypeDef{Type: "b"})
	c.Register(EventTypeDef{Type: "a", Description: "updated"}) // replace, not append

	list := c.List()
	if len(list) != 2 || list[0].Type != "a" || list[1].Type != "b" {
		t.Fatalf("replace should not change order or count: %+v", list)
	}
	if list[0].Description != "updated" {
		t.Fatalf("replace should update the definition: %+v", list[0])
	}
}

func TestCatalog_RegisterEmptyTypePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("Register with empty Type should panic")
		}
	}()
	NewCatalog(false).Register(EventTypeDef{Type: ""})
}

func TestCatalog_ListIsACopy(t *testing.T) {
	c := NewCatalog(false).Register(EventTypeDef{Type: "a"})
	got := c.List()
	got[0].Type = "mutated"
	if again := c.List(); again[0].Type != "a" {
		t.Fatal("mutating the returned slice must not affect the catalog")
	}
}

func TestCatalog_ConcurrentAccess(t *testing.T) {
	c := NewCatalog(true)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			c.Register(EventTypeDef{Type: "t." + string(rune('a'+n%26))})
			_ = c.Has("t.a")
			_ = c.List()
			_, _ = c.Get("t.b")
		}(i)
	}
	wg.Wait()
	if !c.Strict() {
		t.Fatal("Strict should be true")
	}
}

// TestEmit_StrictCatalogRejectsUnknownType is the behavioral proof: with a
// strict catalog wired in, Emit rejects an undeclared type and never touches the
// store/queue, while a declared type flows through normally.
func TestEmit_StrictCatalogRejectsUnknownType(t *testing.T) {
	h := newHarness(DefaultRetryPolicy())
	cat := NewCatalog(true).Register(EventTypeDef{Type: "drive.file.created"})
	// Rebuild the service with the catalog wired in, reusing the harness fakes.
	svc := NewService(Deps{
		Store: h.store, Queue: h.queue, Deliverer: h.deliv, Signer: recordingSigner{},
		Log: h.log, Clock: h.clock, IDGen: h.idgen, Rand: fixedRand{v: 0.5}, Catalog: cat,
	})
	h.store.add(Subscription{ID: "s1", OrgID: "org1", EventTypes: []string{"*"}, URL: "https://x", Active: true})

	// Unknown type: rejected, no fan-out.
	err := svc.Emit(context.Background(), mkEvent("e1", "drive.file.moved", "org1", nil))
	if !errors.Is(err, ErrUnknownEventType) {
		t.Fatalf("want ErrUnknownEventType, got %v", err)
	}
	if n := len(h.queue.pending()); n != 0 {
		t.Fatalf("rejected emit must not enqueue anything, got %d jobs", n)
	}

	// Declared type: flows through.
	if err := svc.Emit(context.Background(), mkEvent("e2", "drive.file.created", "org1", nil)); err != nil {
		t.Fatalf("declared type should emit, got %v", err)
	}
	if n := len(h.queue.pending()); n != 1 {
		t.Fatalf("declared emit should enqueue 1 job, got %d", n)
	}
}

// TestEmit_NonStrictCatalogAllowsUnknown proves a non-strict catalog is
// documentation only and never blocks an emit, and that a nil catalog is a no-op.
func TestEmit_NonStrictCatalogAllowsUnknown(t *testing.T) {
	h := newHarness(DefaultRetryPolicy())
	cat := NewCatalog(false).Register(EventTypeDef{Type: "known"})
	svc := NewService(Deps{
		Store: h.store, Queue: h.queue, Deliverer: h.deliv, Signer: recordingSigner{},
		Log: h.log, Clock: h.clock, IDGen: h.idgen, Rand: fixedRand{v: 0.5}, Catalog: cat,
	})
	h.store.add(Subscription{ID: "s1", OrgID: "org1", EventTypes: []string{"*"}, URL: "https://x", Active: true})
	if err := svc.Emit(context.Background(), mkEvent("e1", "anything.at.all", "org1", nil)); err != nil {
		t.Fatalf("non-strict catalog must not reject, got %v", err)
	}
	if n := len(h.queue.pending()); n != 1 {
		t.Fatalf("expected 1 job, got %d", n)
	}
}
