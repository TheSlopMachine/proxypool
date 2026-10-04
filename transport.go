package proxypool

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"golang.org/x/net/proxy"
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
func TransportFor(proxyURL string, timeout time.Duration) (*http.Transport, error) {
	p, err := parseProxyURL(proxyURL)
	if err != nil {
		return nil, err
	}
	transport := &http.Transport{
		DisableKeepAlives:     true,
		ResponseHeaderTimeout: timeout,
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
func (p parsedProxy) dialContext(timeout time.Duration) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, target string) (net.Conn, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if p.scheme == "socks5" {
			return p.dialSOCKS5(ctx, target, timeout)
		}
		return dialSOCKS4(ctx, p.address(), target, p.username, timeout)
	}
}

// dialSOCKS5 opens target through the SOCKS5 proxy (RFC 1928), with optional
// username/password authentication (RFC 1929).
func (p parsedProxy) dialSOCKS5(ctx context.Context, target string, timeout time.Duration) (net.Conn, error) {
	var auth *proxy.Auth
	if p.hasAuth {
		auth = &proxy.Auth{User: p.username, Password: p.password}
	}
	dialer, err := proxy.SOCKS5("tcp", p.address(), auth, &net.Dialer{Timeout: timeout})
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return dialer.Dial("tcp", target)
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
	if _, err := readFull(conn, reply); err != nil {
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
