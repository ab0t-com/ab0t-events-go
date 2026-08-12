package std

import (
	"strings"
	"testing"
	"time"
)

func TestIDGen_FormatAndUniqueness(t *testing.T) {
	g := IDGen{}
	seen := map[string]bool{}
	for i := 0; i < 1000; i++ {
		id := g.NewID()
		if !strings.HasPrefix(id, "evt_") {
			t.Fatalf("missing default prefix: %q", id)
		}
		if len(id) != len("evt_")+32 { // 16 random bytes hex-encoded
			t.Fatalf("unexpected id length %d: %q", len(id), id)
		}
		if seen[id] {
			t.Fatalf("duplicate id generated: %q", id)
		}
		seen[id] = true
	}
	if got := (IDGen{Prefix: "sub_"}).NewID(); !strings.HasPrefix(got, "sub_") {
		t.Fatalf("custom prefix ignored: %q", got)
	}
}

func TestRand_InRange(t *testing.T) {
	r := Rand{}
	for i := 0; i < 10000; i++ {
		v := r.Float64()
		if v < 0 || v >= 1 {
			t.Fatalf("Float64 out of [0,1): %v", v)
		}
	}
}

func TestClock_NowUTCAndMonotonic(t *testing.T) {
	c := Clock{}
	a := c.Now()
	if a.Location() != time.UTC {
		t.Fatalf("clock not UTC: %v", a.Location())
	}
	b := c.Now()
	if b.Before(a) {
		t.Fatal("clock went backwards")
	}
}
