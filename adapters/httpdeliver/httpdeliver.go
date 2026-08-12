// Package httpdeliver is the net/http Deliverer adapter for ab0t-events-go. It
// POSTs a signed body to a subscriber URL under a per-request timeout.
//
// # SSRF guard (required in production)
//
// A subscription URL is attacker-influenced data: an org can register any URL.
// Without a guard, the deliverer becomes an SSRF primitive that can reach
// internal services (169.254.169.254 metadata, 127.0.0.1, 10.0.0.0/8, etc.).
// This adapter therefore refuses non-http(s) schemes and, when GuardPrivate is
// enabled (the default via NewSecure), resolves the host and permits ONLY public
// global-unicast addresses before dialing — rejecting loopback, link-local,
// private, unspecified, multicast, and a set of special-use ranges that are
// technically global-unicast but reach internal hosts (CGNAT 100.64.0.0/10, the
// 0.0.0.0/8 "this host" block, NAT64 64:ff9b::/96, and the IETF test/benchmark
// ranges). It pins the connection to the vetted IP so a DNS-rebinding retry
// cannot slip past the check. NewSecure also requires https (see RequireHTTPS),
// so payloads are never delivered in cleartext. Do NOT disable the guard for
// internet-facing endpoints.
package httpdeliver

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/ab0t-com/ab0t-events-go/events"
)

// DefaultUserAgent identifies deliveries from this library to subscribers.
const DefaultUserAgent = "ab0t-events-go"

// Deliverer implements events.Deliverer over net/http.
type Deliverer struct {
	client       *http.Client
	timeout      time.Duration
	userAgent    string
	requireHTTPS bool
}

var _ events.Deliverer = (*Deliverer)(nil)

// Config configures the Deliverer.
type Config struct {
	Timeout      time.Duration // per-request timeout; default 10s
	GuardPrivate bool          // reject private/loopback/link-local/special hosts (SSRF guard)
	RequireHTTPS bool          // reject plaintext http:// (except to loopback, for local dev)
	MaxBodyBytes int64         // cap on the response body drained; default 64 KiB
	UserAgent    string        // User-Agent header; default DefaultUserAgent
}

// NewSecure returns a Deliverer with the SSRF guard enabled, https required, and
// a 10s timeout. This is the recommended constructor for internet-facing use.
func NewSecure() *Deliverer { return New(Config{GuardPrivate: true, RequireHTTPS: true}) }

// New returns a Deliverer with the given config.
func New(cfg Config) *Deliverer {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 10 * time.Second
	}
	if cfg.MaxBodyBytes <= 0 {
		cfg.MaxBodyBytes = 64 << 10
	}
	if cfg.UserAgent == "" {
		cfg.UserAgent = DefaultUserAgent
	}
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   cfg.Timeout,
		ExpectContinueTimeout: time.Second,
	}
	if cfg.GuardPrivate {
		transport.DialContext = guardedDialContext(cfg.Timeout)
	} else {
		transport.DialContext = (&net.Dialer{Timeout: cfg.Timeout}).DialContext
	}
	d := &Deliverer{
		client: &http.Client{
			Timeout:   cfg.Timeout,
			Transport: transport,
			// Do not follow redirects: a 3xx to an internal host would bypass
			// the URL vetting the subscription went through.
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		timeout:      cfg.Timeout,
		userAgent:    cfg.UserAgent,
		requireHTTPS: cfg.RequireHTTPS,
	}
	return d
}

// Post sends the request and returns the status code and round-trip latency.
func (d *Deliverer) Post(ctx context.Context, req events.DeliveryRequest) (events.DeliveryResponse, error) {
	if err := validateScheme(req.URL, d.requireHTTPS); err != nil {
		return events.DeliveryResponse{}, err
	}
	start := time.Now()
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, req.URL, bytes.NewReader(req.Body))
	if err != nil {
		return events.DeliveryResponse{Latency: time.Since(start)}, err
	}
	for k, v := range req.Headers {
		httpReq.Header.Set(k, v)
	}
	if httpReq.Header.Get("User-Agent") == "" {
		httpReq.Header.Set("User-Agent", d.userAgent)
	}
	resp, err := d.client.Do(httpReq)
	if err != nil {
		return events.DeliveryResponse{Latency: time.Since(start)}, err
	}
	defer resp.Body.Close()

	out := events.DeliveryResponse{StatusCode: resp.StatusCode, Latency: time.Since(start)}
	// On failure, keep a small snippet of the body to aid debugging; on success
	// just drain (bounded) so the connection can be reused.
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		const snippetMax = 512
		body, _ := io.ReadAll(io.LimitReader(resp.Body, snippetMax))
		out.Body = body
		out.RetryAfter = parseRetryAfter(resp.Header.Get("Retry-After"), time.Now())
		_, _ = io.CopyN(io.Discard, resp.Body, 64<<10)
		out.Latency = time.Since(start)
	} else {
		_, _ = io.CopyN(io.Discard, resp.Body, 64<<10)
	}
	return out, nil
}

// parseRetryAfter interprets a Retry-After header value, which is either a number
// of seconds or an HTTP date. It returns 0 for absent/invalid/past values.
func parseRetryAfter(v string, now time.Time) time.Duration {
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil {
		if secs <= 0 {
			return 0
		}
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := t.Sub(now); d > 0 {
			return d
		}
	}
	return 0
}

// ErrInsecureScheme is returned when RequireHTTPS is set and a plaintext http://
// URL (to a non-loopback host) is delivered.
var ErrInsecureScheme = errors.New("httpdeliver: https required (plaintext http is not allowed for this host)")

func validateScheme(raw string, requireHTTPS bool) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("httpdeliver: bad url: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("httpdeliver: unsupported url scheme %q", u.Scheme)
	}
	if u.Host == "" {
		return errors.New("httpdeliver: url has no host")
	}
	// Require TLS unless the target is loopback (kept allowed for local dev/tests;
	// loopback is refused anyway by the SSRF guard in production).
	if requireHTTPS && u.Scheme != "https" && !isLoopbackHost(u.Hostname()) {
		return ErrInsecureScheme
	}
	return nil
}

// isLoopbackHost reports whether a URL host is the local machine.
func isLoopbackHost(host string) bool {
	if host == "localhost" {
		return true
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback()
	}
	return false
}

// ErrBlockedAddress is returned when the SSRF guard refuses a target address.
var ErrBlockedAddress = errors.New("httpdeliver: destination address is blocked (SSRF guard)")

// guardedDialContext resolves the target, permits only public global-unicast
// addresses (rejecting private / loopback / link-local / special-use ranges),
// and dials the vetted IP directly so the connection cannot be rebound to a
// different host after the check.
func guardedDialContext(timeout time.Duration) func(context.Context, string, string) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: timeout}
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, err
		}
		ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
		if err != nil {
			return nil, err
		}
		for _, ip := range ips {
			if blocked(ip.IP) {
				return nil, fmt.Errorf("%w: %s", ErrBlockedAddress, ip.IP)
			}
		}
		if len(ips) == 0 {
			return nil, ErrBlockedAddress
		}
		return dialer.DialContext(ctx, network, net.JoinHostPort(ips[0].IP.String(), port))
	}
}

// blocked is an allowlist: only a public, global-unicast, non-private address is
// permitted. Everything else — loopback, link-local, multicast, unspecified,
// private (RFC 1918 / ULA) — is rejected, plus the special-use ranges below that
// are technically global-unicast but reach internal or unroutable hosts.
func blocked(ip net.IP) bool {
	if !ip.IsGlobalUnicast() || ip.IsPrivate() {
		return true
	}
	for _, n := range extraBlockedCIDRs {
		if n.Contains(ip) {
			return true
		}
	}
	return false
}

// extraBlockedCIDRs are ranges net.IP.IsGlobalUnicast()/IsPrivate() do not catch
// but that must not be reachable via webhook egress.
var extraBlockedCIDRs = mustCIDRs(
	"0.0.0.0/8",          // "this host on this network" (0.0.0.1+ routes to local on Linux)
	"100.64.0.0/10",      // RFC 6598 carrier-grade NAT / shared address space
	"192.0.0.0/24",       // RFC 6890 IETF protocol assignments
	"192.0.2.0/24",       // TEST-NET-1
	"198.18.0.0/15",      // RFC 2544 benchmarking
	"198.51.100.0/24",    // TEST-NET-2
	"203.0.113.0/24",     // TEST-NET-3
	"240.0.0.0/4",        // reserved (former class E)
	"255.255.255.255/32", // limited broadcast
	"64:ff9b::/96",       // RFC 6052 NAT64 (wraps arbitrary IPv4, incl. metadata)
	"64:ff9b:1::/48",     // RFC 8215 local-use NAT64
	"2001:db8::/32",      // documentation
)

func mustCIDRs(cidrs ...string) []*net.IPNet {
	out := make([]*net.IPNet, 0, len(cidrs))
	for _, c := range cidrs {
		_, n, err := net.ParseCIDR(c)
		if err != nil {
			panic("httpdeliver: bad built-in CIDR " + c + ": " + err.Error())
		}
		out = append(out, n)
	}
	return out
}
