package httpdeliver_test

// Use-case scenario tests, 2026-11-22: each test is one job a service or its customer does with the library.
//
// Job: "a multi-tenant SaaS lets every org register ANY webhook URL — and that must never become a way to reach our
// own network". The tenant-registered URLs below are the ones a real attacker tries first. Every target is an IP
// literal, so no DNS lookup and no network traffic happens: the guard must refuse each one before dialing.

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ab0t-com/ab0t-events-go/adapters/hmacsign"
	"github.com/ab0t-com/ab0t-events-go/adapters/httpdeliver"
	"github.com/ab0t-com/ab0t-events-go/adapters/memory"
	"github.com/ab0t-com/ab0t-events-go/adapters/std"
	"github.com/ab0t-com/ab0t-events-go/events"
)

// ucSkipIfProxied: with HTTP(S)_PROXY set, net/http dials the PROXY, so the guard vets the proxy's address and never
// sees the target (the guard does not cover an egress proxy). The assertions below would then test the proxy.
func ucSkipIfProxied(t *testing.T) {
	t.Helper()
	for _, k := range []string{"HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy"} {
		if os.Getenv(k) != "" {
			t.Skipf("%s is set: the SSRF guard would vet the proxy address, not the target", k)
		}
	}
}

func TestUseCase_SSRF_TenantURLsPointingInsideTheNetworkAreNeverDialed(t *testing.T) {
	ucSkipIfProxied(t)
	d := httpdeliver.NewSecure()
	targets := []struct{ name, url string }{
		{"loopback", "https://127.0.0.1/hook"},
		{"rfc1918 10/8", "https://10.0.0.7/hook"},
		{"rfc1918 172.16/12 with port", "https://172.16.5.4:8443/hook"},
		{"rfc1918 192.168/16", "https://192.168.0.10/hook"},
		{"aws imds v4", "https://169.254.169.254/latest/meta-data/iam/security-credentials/"},
		{"ecs task credentials", "https://169.254.170.2/v2/credentials"},
		{"aws imds v6 (ULA)", "https://[fd00:ec2::254]/latest/meta-data/"},
		{"ipv6 loopback", "https://[::1]/hook"},
		{"ipv6 link-local", "https://[fe80::1]/hook"},
		{"cgnat (eks/k8s pod ranges)", "https://100.64.12.34/hook"},
		{"this-host block 0/8", "https://0.0.0.1/hook"},
		{"nat64-wrapped imds", "https://[64:ff9b::a9fe:a9fe]/hook"},
		{"ipv4-mapped imds", "https://[::ffff:169.254.169.254]/hook"},
		{"ipv4-mapped rfc1918", "https://[::ffff:10.0.0.1]/hook"},
		{"benchmark range", "https://198.18.0.1/hook"},
	}
	for _, tc := range targets {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := d.Post(context.Background(), events.DeliveryRequest{URL: tc.url, Body: []byte(`{}`)})
			if !errors.Is(err, httpdeliver.ErrBlockedAddress) {
				t.Fatalf("%s: want ErrBlockedAddress, got code=%d err=%v", tc.url, resp.StatusCode, err)
			}
			if resp.StatusCode != 0 {
				t.Fatalf("%s: a blocked target produced HTTP status %d", tc.url, resp.StatusCode)
			}
		})
	}

	// A REAL internal service listening on loopback sees zero requests (plaintext http to loopback passes the
	// scheme check by design — the address guard is what stops it).
	var hits atomic.Int32
	internal := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer internal.Close()
	if _, err := d.Post(context.Background(), events.DeliveryRequest{URL: internal.URL + "/admin/flush-cache", Body: []byte(`{}`)}); !errors.Is(err, httpdeliver.ErrBlockedAddress) {
		t.Fatalf("loopback service: want ErrBlockedAddress, got %v", err)
	}
	if n := hits.Load(); n != 0 {
		t.Fatalf("internal service received %d requests through the guarded deliverer", n)
	}
}

// TestUseCase_HTTPSOnly_PlaintextEndpointRefusedBeforeAnyLookup: a customer pastes an http:// URL. NewSecure refuses
// it before resolving the name, so the payload is never sent in cleartext and no DNS query leaks.
func TestUseCase_HTTPSOnly_PlaintextEndpointRefusedBeforeAnyLookup(t *testing.T) {
	d := httpdeliver.NewSecure()
	for _, u := range []string{
		"http://customer.example/webhooks",
		"http://10.0.0.7/hook",
		"http://169.254.169.254/latest/meta-data/",
	} {
		resp, err := d.Post(context.Background(), events.DeliveryRequest{URL: u, Body: []byte(`{"pii":"placeholder"}`)})
		if !errors.Is(err, httpdeliver.ErrInsecureScheme) {
			t.Fatalf("%s: want ErrInsecureScheme, got code=%d err=%v", u, resp.StatusCode, err)
		}
	}
}

// ucSyncQueue runs the pipeline synchronously so the end-to-end story below needs no goroutines or sleeps.
type ucSyncQueue struct {
	mu   sync.Mutex
	jobs []events.Job
}

func (q *ucSyncQueue) Enqueue(_ context.Context, j events.Job) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.jobs = append(q.jobs, j)
	return nil
}

func (q *ucSyncQueue) drain(t *testing.T, svc *events.Service, max int) {
	t.Helper()
	for i := 0; i < max; i++ {
		q.mu.Lock()
		if len(q.jobs) == 0 {
			q.mu.Unlock()
			return
		}
		j := q.jobs[0]
		q.jobs = q.jobs[1:]
		q.mu.Unlock()
		if err := svc.Process(context.Background(), j); err != nil {
			t.Fatalf("process: %v", err)
		}
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.jobs) != 0 {
		t.Fatalf("queue still holds %d jobs after %d", len(q.jobs), max)
	}
}

// TestUseCase_SSRF_RedirectToTheMetadataServiceIsNotFollowedAndDeadLetters: a tenant registers a public-looking
// endpoint that answers every webhook with "307 → http://169.254.169.254/...". Following it would POST our signed
// payload to the metadata service and hand the response back through the delivery log. The deliverer must not
// follow it, and the core must dead-letter (a 3xx is a permanent answer) instead of retrying.
func TestUseCase_SSRF_RedirectToTheMetadataServiceIsNotFollowedAndDeadLetters(t *testing.T) {
	var metadataHits atomic.Int32
	metadata := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		metadataHits.Add(1)
		_, _ = w.Write([]byte(`{"Code":"Success","Token":"placeholder"}`))
	}))
	defer metadata.Close()
	var tenantHits atomic.Int32
	tenant := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tenantHits.Add(1)
		http.Redirect(w, r, metadata.URL+"/latest/meta-data/iam/security-credentials/", http.StatusTemporaryRedirect)
	}))
	defer tenant.Close()

	q := &ucSyncQueue{}
	log := memory.NewLog()
	svc := events.NewService(events.Deps{
		Store: memory.NewStore(), Queue: q, Log: log, Signer: hmacsign.New(),
		// Both servers are loopback, so the address guard is off here: this isolates the redirect policy, which
		// applies in every mode.
		Deliverer: httpdeliver.New(httpdeliver.Config{Timeout: 2 * time.Second}),
		Clock:     std.Clock{}, IDGen: std.IDGen{}, Rand: std.Rand{},
		Policy: events.DefaultRetryPolicy(),
	})
	ctx := context.Background()
	sub, err := svc.Subscribe(ctx, events.NewSubscription{OrgID: "org_mallory", EventTypes: []string{"*"}, URL: tenant.URL + "/hook", Secret: "test-placeholder-secret"})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	ev := events.Event{ID: "evt_redirect_1", Type: "user.created", OrgID: "org_mallory", OccurredAt: time.Now().UTC(), Payload: []byte(`{"userId":"u_1"}`)}
	if err := svc.Emit(ctx, ev); err != nil {
		t.Fatalf("emit: %v", err)
	}
	q.drain(t, svc, 10)

	d, _, atts, err := log.Get(ctx, "org_mallory", events.DeliveryID(sub.ID, ev.ID))
	if err != nil {
		t.Fatalf("log get: %v", err)
	}
	if d.Status != events.StatusDead || len(atts) != 1 {
		t.Fatalf("status=%s attempts=%d, want dead after exactly 1 attempt (3xx is neither followed nor retried)", d.Status, len(atts))
	}
	if atts[0].Code != http.StatusTemporaryRedirect || !strings.HasPrefix(d.LastError, "http 307") {
		t.Fatalf("attempt code=%d lastError=%q, want the 307 recorded", atts[0].Code, d.LastError)
	}
	if tenantHits.Load() != 1 || metadataHits.Load() != 0 {
		t.Fatalf("tenant hits=%d metadata hits=%d, want 1 and 0", tenantHits.Load(), metadataHits.Load())
	}
}
