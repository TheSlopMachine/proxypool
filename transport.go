package proxypool

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// parsedProxy carries the dial-relevant parts of a normalized proxy URL.
type parsedProxy struct {
	scheme   string
	host     string
	port     int
	username string
	password string
	hasAuth  bool
}

// parseProxyURL normalizes raw and splits out scheme, address, and credentials.
func parseProxyURL(raw string) (parsedProxy, error) {
	canonical, host, port, err := normalizeProxyURL(raw)
	if err != nil {
		return parsedProxy{}, err
	}
	u, err := url.Parse(canonical)
	if err != nil {
		return parsedProxy{}, err
	}
	p := parsedProxy{scheme: u.Scheme, host: host, port: port}
	if u.User != nil {
		p.username = u.User.Username()
		p.password, _ = u.User.Password()
		p.hasAuth = true
	}
	return p, nil
}

// address returns the TCP dial address of the proxy itself.
func (p parsedProxy) address() string {
	return net.JoinHostPort(p.host, strconv.Itoa(p.port))
}

// TransportFor builds an HTTP transport routing through the given proxy URL.
// Supported schemes: http, https, socks4 (socks4a canonicalizes to socks4),
// socks5. Credentials embedded as userinfo apply to every scheme.
//
// Every network phase is bounded by timeout: TCP connect (Dialer.Timeout),
// TLS handshake (TLSHandshakeTimeout), first response byte
// (ResponseHeaderTimeout), and the SOCKS greeting/CONNECT exchange (absolute
// conn deadline in dialContext). This closes the hang where a proxy accepts
// TCP then stalls forever — previously severing the host network (WireGuard
// flap) was the only thing that unblocked parked workers.
func TransportFor(proxyURL string, timeout time.Duration) (*http.Transport, error) {
	p, err := parseProxyURL(proxyURL)
	if err != nil {
		return nil, err
	}
	transport := &http.Transport{
		DisableKeepAlives:       true,
		ResponseHeaderTimeout:   timeout,
		TLSHandshakeTimeout:     timeout,
		ExpectContinueTimeout:   1 * time.Second,
		IdleConnTimeout:         30 * time.Second,
		MaxIdleConns:            0,
		DisableCompression:      false,
		DialContext: (&net.Dialer{
			Timeout: timeout,
		}).DialContext,
	}
	switch p.scheme {
	case "http", "https":
		parsed, err := url.Parse(proxyURL)
		if err != nil {
			return nil, fmt.Errorf("invalid proxy URL: %w", err)
		}
		transport.Proxy = http.ProxyURL(parsed)
	case "socks4", "socks5":
		transport.Proxy = nil
		transport.DialContext = p.dialContext(timeout)
	default:
		return nil, fmt.Errorf("unsupported proxy URL %q", proxyURL)
	}
	return transport, nil
}

// dialContext returns a DialContext func tunneling through the SOCKS proxy.
// It honors ctx cancellation (from http.Client.Timeout) and enforces an
// absolute conn deadline over the whole handshake so tarpits that accept TCP
// but never answer fail within timeout instead of parking a worker forever.
func (p parsedProxy) dialContext(timeout time.Duration) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, target string) (net.Conn, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		effective := effectiveTimeout(ctx, timeout)
		if p.scheme == "socks5" {
			return p.dialSOCKS5(ctx, target, effective)
		}
		return dialSOCKS4(ctx, p.address(), target, p.username, effective)
	}
}

// effectiveTimeout returns the sooner of the configured timeout and the
// remaining request-context deadline (set by http.Client.Timeout). This lets
// Client.Timeout actually bound the custom SOCKS dial instead of being
// silently ignored.
func effectiveTimeout(ctx context.Context, timeout time.Duration) time.Duration {
	if deadline, ok := ctx.Deadline(); ok {
		if remaining := time.Until(deadline); remaining < timeout {
			if remaining <= 0 {
				return time.Millisecond
			}
			return remaining
		}
	}
	return timeout
}

// dialSOCKS5 opens target through the SOCKS5 proxy (RFC 1928), with optional
// username/password authentication (RFC 1929). The entire exchange runs
// under an absolute conn deadline derived from timeout, so half-open
// proxies fail fast. Deadline is cleared before returning a usable conn.
func (p parsedProxy) dialSOCKS5(ctx context.Context, target string, timeout time.Duration) (net.Conn, error) {
	host, portStr, err := net.SplitHostPort(target)
	if err != nil {
		return nil, fmt.Errorf("invalid target %q: %w", target, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port <= 0 || port > 65535 {
		return nil, fmt.Errorf("invalid target port: %s", portStr)
	}

	dialer := &net.Dialer{Timeout: timeout}
	conn, err := dialer.DialContext(ctx, "tcp", p.address())
	if err != nil {
		return nil, err
	}
	deadline := time.Now().Add(timeout)
	_ = conn.SetDeadline(deadline)

	fail := func(err error) (net.Conn, error) {
		conn.Close()
		return nil, err
	}

	// Greeting: VER=0x05, methods (0x00 no-auth, plus 0x02 if credentials).
	var greeting []byte
	if p.hasAuth {
		greeting = []byte{0x05, 0x02, 0x00, 0x02}
	} else {
		greeting = []byte{0x05, 0x01, 0x00}
	}
	if _, err := conn.Write(greeting); err != nil {
		return fail(err)
	}
	methodReply := make([]byte, 2)
	if _, err := readFullCtx(ctx, conn, methodReply); err != nil {
		return fail(err)
	}
	if methodReply[0] != 0x05 {
		return fail(fmt.Errorf("socks5 bad version 0x%02x", methodReply[0]))
	}
	switch methodReply[1] {
	case 0x00:
		// No auth required.
	case 0x02:
		if !p.hasAuth {
			return fail(fmt.Errorf("socks5 proxy requires auth"))
		}
		user := []byte(p.username)
		pass := []byte(p.password)
		if len(user) > 255 || len(pass) > 255 {
			return fail(fmt.Errorf("socks5 credentials too long"))
		}
		authReq := []byte{0x01, byte(len(user))}
		authReq = append(authReq, user...)
		authReq = append(authReq, byte(len(pass)))
		authReq = append(authReq, pass...)
		if _, err := conn.Write(authReq); err != nil {
			return fail(err)
		}
		authReply := make([]byte, 2)
		if _, err := readFullCtx(ctx, conn, authReply); err != nil {
			return fail(err)
		}
		if authReply[1] != 0x00 {
			return fail(fmt.Errorf("socks5 auth rejected (0x%02x)", authReply[1]))
		}
	default:
		return fail(fmt.Errorf("socks5 no acceptable auth (0x%02x)", methodReply[1]))
	}

	// CONNECT request: VER CMD RSV ATYP ADDR PORT.
	req := []byte{0x05, 0x01, 0x00}
	if ip4 := net.ParseIP(host).To4(); ip4 != nil {
		req = append(req, 0x01)
		req = append(req, ip4...)
	} else if ip16 := net.ParseIP(host); ip16 != nil {
		req = append(req, 0x04)
		req = append(req, ip16.To16()...)
	} else {
		if len(host) == 0 || len(host) > 255 {
			return fail(fmt.Errorf("socks5 invalid domain %q", host))
		}
		req = append(req, 0x03, byte(len(host)))
		req = append(req, host...)
	}
	req = append(req, byte(port>>8), byte(port))
	if _, err := conn.Write(req); err != nil {
		return fail(err)
	}

	// CONNECT reply: VER REP RSV ATYP BND.ADDR BND.PORT.
	hdr := make([]byte, 4)
	if _, err := readFullCtx(ctx, conn, hdr); err != nil {
		return fail(err)
	}
	if hdr[0] != 0x05 {
		return fail(fmt.Errorf("socks5 bad reply version 0x%02x", hdr[0]))
	}
	if hdr[1] != 0x00 {
		return fail(fmt.Errorf("socks5 connect rejected (0x%02x)", hdr[1]))
	}
	var addrLen int
	switch hdr[3] {
	case 0x01:
		addrLen = 4
	case 0x04:
		addrLen = 16
	case 0x03:
		ln := make([]byte, 1)
		if _, err := readFullCtx(ctx, conn, ln); err != nil {
			return fail(err)
		}
		addrLen = int(ln[0])
	default:
		return fail(fmt.Errorf("socks5 bad atyp 0x%02x", hdr[3]))
	}
	rest := make([]byte, addrLen+2)
	if _, err := readFullCtx(ctx, conn, rest); err != nil {
		return fail(err)
	}
	_ = conn.SetDeadline(time.Time{})
	return conn, nil
}

// dialSOCKS4 opens target through the SOCKS4 proxy. Hostnames resolve
// remotely (SOCKS4a framing: 0.0.0.x marker plus domain suffix).
func dialSOCKS4(ctx context.Context, proxyAddr, target, username string, timeout time.Duration) (net.Conn, error) {
	host, portStr, err := net.SplitHostPort(target)
	if err != nil {
		return nil, fmt.Errorf("invalid target %q: %w", target, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port <= 0 || port > 65535 {
		return nil, fmt.Errorf("invalid target port: %s", portStr)
	}

	dialer := &net.Dialer{Timeout: timeout}
	conn, err := dialer.DialContext(ctx, "tcp", proxyAddr)
	if err != nil {
		return nil, err
	}
	_ = conn.SetDeadline(time.Now().Add(timeout))

	req := []byte{0x04, 0x01, byte(port >> 8), byte(port)}
	domain := ""
	if ip4 := net.ParseIP(host).To4(); ip4 != nil {
		req = append(req, ip4...)
	} else {
		req = append(req, 0, 0, 0, 1)
		domain = host
	}
	req = append(req, username...)
	req = append(req, 0x00)
	if domain != "" {
		req = append(req, domain...)
		req = append(req, 0x00)
	}
	if _, err := conn.Write(req); err != nil {
		conn.Close()
		return nil, err
	}

	reply := make([]byte, 8)
	if _, err := readFullCtx(ctx, conn, reply); err != nil {
		conn.Close()
		return nil, err
	}
	if reply[0] != 0x00 || reply[1] != 0x5A {
		conn.Close()
		return nil, fmt.Errorf("socks4 request rejected (code 0x%02x)", reply[1])
	}
	_ = conn.SetDeadline(time.Time{})
	return conn, nil
}

// readFull reads exactly len(buf) bytes.
func readFull(conn net.Conn, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := conn.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// readFullCtx reads exactly len(buf) bytes, aborting early if ctx is done.
// The conn deadline normally fires first; this is a second guard so a
// cancelled request context (http.Client.Timeout) never parks a worker.
func readFullCtx(ctx context.Context, conn net.Conn, buf []byte) (int, error) {
	type result struct {
		n   int
		err error
	}
	done := make(chan result, 1)
	go func() {
		n, err := readFull(conn, buf)
		done <- result{n: n, err: err}
	}()
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case r := <-done:
		return r.n, r.err
	}
}
