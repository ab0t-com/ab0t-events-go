package httpdeliver

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ab0t-com/ab0t-events-go/events"
)

func TestSSRFGuard_BlocksLoopback(t *testing.T) {
	// httptest binds to 127.0.0.1, which the guard must refuse.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))
	defer srv.Close()

	guarded := NewSecure()
	_, err := guarded.Post(context.Background(), events.DeliveryRequest{URL: srv.URL, Body: []byte("{}")})
	if !errors.Is(err, ErrBlockedAddress) {
		t.Fatalf("guarded deliverer allowed loopback: %v", err)
	}

	// With the guard off, the same request must succeed.
	open := New(Config{GuardPrivate: false})
	resp, err := open.Post(context.Background(), events.DeliveryRequest{URL: srv.URL, Body: []byte("{}")})
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("unguarded deliverer failed: code=%d err=%v", resp.StatusCode, err)
	}
}

// TestBlocked_SSRFRanges is the proving test for the audit SSRF findings: the
// guard's allowlist rejects the special-use ranges the old denylist missed while
// still permitting ordinary public addresses.
func TestBlocked_SSRFRanges(t *testing.T) {
	blockedIPs := []string{
		"127.0.0.1",                             // loopback
		"10.0.0.5", "192.168.1.1", "172.16.0.1", // RFC 1918 private
		"169.254.169.254",    // link-local (AWS metadata)
		"0.0.0.0", "0.0.0.1", // unspecified + "this host" block (NEW: 0.0.0.1)
		"100.64.0.1", "100.127.255.255", // CGNAT 100.64.0.0/10 (NEW)
		"192.0.2.5",          // TEST-NET-1 (NEW)
		"198.18.0.1",         // benchmarking (NEW)
		"255.255.255.255",    // broadcast (NEW)
		"fc00::1",            // IPv6 ULA (private)
		"fe80::1",            // IPv6 link-local
		"64:ff9b::a9fe:a9fe", // NAT64-wrapped 169.254.169.254 (NEW)
		"::1",                // IPv6 loopback
	}
	for _, s := range blockedIPs {
		ip := net.ParseIP(s)
		if ip == nil {
			t.Fatalf("bad test IP %q", s)
		}
		if !blocked(ip) {
			t.Errorf("expected %s to be blocked by the SSRF guard", s)
		}
	}

	allowed := []string{"8.8.8.8", "93.184.216.34", "1.1.1.1", "2606:2800:220:1:248:1893:25c8:1946"}
	for _, s := range allowed {
		if blocked(net.ParseIP(s)) {
			t.Errorf("expected public address %s to be allowed", s)
		}
	}
}

func TestValidateScheme_RequireHTTPS(t *testing.T) {
	// With RequireHTTPS, plaintext http to a public host is rejected...
	if err := validateScheme("http://reports.example.com/hook", true); !errors.Is(err, ErrInsecureScheme) {
		t.Fatalf("want ErrInsecureScheme for http public host, got %v", err)
	}
	// ...https is fine...
	if err := validateScheme("https://reports.example.com/hook", true); err != nil {
		t.Fatalf("https should pass, got %v", err)
	}
	// ...and http to loopback is allowed (local dev), since the guard blocks it in prod anyway.
	for _, u := range []string{"http://127.0.0.1:8080/x", "http://localhost:9000/y", "http://[::1]:7000/z"} {
		if err := validateScheme(u, true); err != nil {
			t.Fatalf("loopback http should be allowed under RequireHTTPS: %q -> %v", u, err)
		}
	}
	// Without RequireHTTPS, plaintext http is fine.
	if err := validateScheme("http://reports.example.com/hook", false); err != nil {
		t.Fatalf("http should pass without RequireHTTPS, got %v", err)
	}
}

func TestPost_RejectsBadSchemes(t *testing.T) {
	d := New(Config{GuardPrivate: false})
	for _, url := range []string{"ftp://host/x", "file:///etc/passwd", "gopher://h", "://nohost", "https://"} {
		if _, err := d.Post(context.Background(), events.DeliveryRequest{URL: url}); err == nil {
			t.Errorf("expected error for url %q", url)
		}
	}
}

func TestPost_CapturesSnippetOnFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom: downstream unavailable", http.StatusBadGateway)
	}))
	defer srv.Close()

	d := New(Config{GuardPrivate: false})
	resp, err := d.Post(context.Background(), events.DeliveryRequest{URL: srv.URL, Body: []byte("{}")})
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("code = %d, want 502", resp.StatusCode)
	}
	if !strings.Contains(string(resp.Body), "downstream unavailable") {
		t.Fatalf("snippet not captured: %q", resp.Body)
	}
	if resp.Latency <= 0 {
		t.Fatal("latency not measured")
	}
}

func TestPost_ParsesRetryAfterSeconds(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "7")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	d := New(Config{GuardPrivate: false})
	resp, err := d.Post(context.Background(), events.DeliveryRequest{URL: srv.URL, Body: []byte("{}")})
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	if resp.RetryAfter != 7*time.Second {
		t.Fatalf("RetryAfter = %v, want 7s", resp.RetryAfter)
	}
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	cases := []struct {
		in   string
		want time.Duration
	}{
		{"", 0},
		{"0", 0},
		{"-5", 0},
		{"30", 30 * time.Second},
		{"garbage", 0},
		{now.Add(90 * time.Second).UTC().Format(http.TimeFormat), 90 * time.Second},
		{now.Add(-time.Minute).UTC().Format(http.TimeFormat), 0}, // past date
	}
	for _, c := range cases {
		if got := parseRetryAfter(c.in, now); got != c.want {
			t.Errorf("parseRetryAfter(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

func TestPost_DoesNotFollowRedirects(t *testing.T) {
	var innerHit bool
	inner := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		innerHit = true
		w.WriteHeader(200)
	}))
	defer inner.Close()
	redir := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, inner.URL, http.StatusFound)
	}))
	defer redir.Close()

	d := New(Config{GuardPrivate: false})
	resp, err := d.Post(context.Background(), events.DeliveryRequest{URL: redir.URL, Body: []byte("{}")})
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	if resp.StatusCode != http.StatusFound {
		t.Fatalf("code = %d, want 302 (redirect not followed)", resp.StatusCode)
	}
	if innerHit {
		t.Fatal("deliverer followed a redirect to an internal host — SSRF risk")
	}
}

func TestPost_SendsHeadersAndBody(t *testing.T) {
	var gotHeader, gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeader = r.Header.Get("X-Signature")
		b := make([]byte, r.ContentLength)
		_, _ = r.Body.Read(b)
		gotBody = string(b)
		w.WriteHeader(204)
	}))
	defer srv.Close()

	d := New(Config{GuardPrivate: false})
	_, err := d.Post(context.Background(), events.DeliveryRequest{
		URL: srv.URL, Body: []byte(`{"hi":1}`), Headers: map[string]string{"X-Signature": "sha256=abc"},
	})
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	if gotHeader != "sha256=abc" {
		t.Fatalf("header not sent: %q", gotHeader)
	}
	if gotBody != `{"hi":1}` {
		t.Fatalf("body not sent: %q", gotBody)
	}
}

func TestPost_SetsUserAgent(t *testing.T) {
	var ua, customUA string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if customUA == "" {
			ua = r.Header.Get("User-Agent")
		} else {
			customUA = r.Header.Get("User-Agent")
		}
		w.WriteHeader(200)
	}))
	defer srv.Close()

	// Default UA.
	d := New(Config{GuardPrivate: false})
	_, _ = d.Post(context.Background(), events.DeliveryRequest{URL: srv.URL, Body: []byte("{}")})
	if ua != DefaultUserAgent {
		t.Fatalf("User-Agent = %q, want %q", ua, DefaultUserAgent)
	}

	// A caller-supplied User-Agent header is respected.
	customUA = "pending"
	d2 := New(Config{GuardPrivate: false})
	_, _ = d2.Post(context.Background(), events.DeliveryRequest{URL: srv.URL, Body: []byte("{}"), Headers: map[string]string{"User-Agent": "my-service/2.0"}})
	if customUA != "my-service/2.0" {
		t.Fatalf("caller User-Agent overridden: %q", customUA)
	}
}

func TestPost_HonoursContextCancellation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(200)
	}))
	defer srv.Close()

	d := New(Config{GuardPrivate: false, Timeout: time.Second})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := d.Post(ctx, events.DeliveryRequest{URL: srv.URL, Body: []byte("{}")}); err == nil {
		t.Fatal("expected context deadline error")
	}
}
