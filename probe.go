package proxypool

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	handshakeTarget = "1.1.1.1:443"
	traceURL        = "https://www.cloudflare.com/cdn-cgi/trace"
	generate204URL  = "http://cp.cloudflare.com/generate_204"
)

// verifyHandshake confirms the proxy tunnels to the handshake target.
// HTTP(S) proxies answer a raw CONNECT; SOCKS proxies open the same target
// through their own handshake, with userinfo credentials when present.
// ctx carries the per-proxy deadline so tarpits fail fast instead of
// parking a handshake worker indefinitely.
func verifyHandshake(ctx context.Context, proxyURL string, timeout time.Duration) error {
	p, err := parseProxyURL(proxyURL)
	if err != nil {
		return err
	}
	switch p.scheme {
	// Exhaustive over SupportedSchemes (parseProxyURL rejects the rest,
	// so default is unreachable defense).
	case "http", "https":
		return verifyHTTPHandshake(ctx, p.address(), timeout)
	case "socks4", "socks5":
		dial := p.dialSOCKS4
		if p.scheme == "socks5" {
			dial = p.dialSOCKS5
		}
		conn, err := dial(ctx, handshakeTarget, timeout)
		if err != nil {
			return err
		}
		conn.Close()
		return nil
	default:
		return fmt.Errorf("unsupported protocol: %s", p.scheme)
	}
}

// verifyHTTPHandshake performs a raw TCP HTTP CONNECT tunnel verification
// under ctx + an absolute conn deadline covering dial, write, and reply read.
func verifyHTTPHandshake(ctx context.Context, proxyAddr string, timeout time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	dialer := &net.Dialer{Timeout: timeout}
	conn, err := dialer.DialContext(ctx, "tcp", proxyAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	// A failed deadline silently removes hang protection: fail fast instead.
	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return err
	}

	req := "CONNECT " + handshakeTarget + " HTTP/1.1\r\nHost: " + handshakeTarget + "\r\nProxy-Connection: close\r\n\r\n"
	if _, err := io.WriteString(conn, req); err != nil {
		return err
	}

	var buf [256]byte
	total := 0
	var readErr error
	for total < len(buf) {
		n, rErr := conn.Read(buf[total:])
		if n > 0 {
			total += n
			if bytes.Contains(buf[:total], []byte("\n")) {
				break
			}
		}
		if rErr != nil {
			readErr = rErr
			break
		}
	}

	if total > 0 && is2xxResponse(buf[:total]) {
		return nil
	}
	if total > 0 {
		return &checkError{
			Phase:  "handshake",
			Reason: FailRejected,
			Err:    fmt.Errorf("%w: CONNECT reply %q", errProxyRejected, strings.TrimSpace(string(buf[:total]))),
		}
	}
	if readErr != nil {
		return readErr
	}
	return io.ErrUnexpectedEOF
}

func is2xxResponse(b []byte) bool {
	if !bytes.HasPrefix(b, []byte("HTTP/")) {
		return false
	}
	spaceIdx := bytes.IndexByte(b, ' ')
	if spaceIdx == -1 {
		return false
	}
	fields := bytes.Fields(b[spaceIdx:])
	if len(fields) == 0 || len(fields[0]) != 3 {
		return false
	}
	code, err := strconv.Atoi(string(fields[0]))
	if err != nil {
		return false
	}
	return code >= 200 && code < 300
}

// probeEndpoint conducts the dual-endpoint latency and location test
// through the proxy's own transport (HTTP CONNECT or SOCKS tunnel).
func probeEndpoint(ctx context.Context, proxyURI string, needLocation bool, timeout time.Duration) (time.Duration, string, error) {
	transport, err := TransportFor(proxyURI, timeout)
	if err != nil {
		return 0, "", err
	}
	defer transport.CloseIdleConnections()

	client := &http.Client{
		Transport: transport,
		Timeout:   timeout,
	}

	target := generate204URL
	if needLocation {
		target = traceURL
	}

	req, err := http.NewRequestWithContext(ctx, "GET", target, nil)
	if err != nil {
		return 0, "", err
	}
	req.Header.Set("User-Agent", "curl/8.7.1")

	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 400 {
		return 0, "", fmt.Errorf("%w: %d", errBadProbeStatus, resp.StatusCode)
	}

	latency := time.Since(start)
	discoveredLocation := ""

	if needLocation {
		// Read Cloudflare trace response body (typically ~200 bytes)
		lr := io.LimitReader(resp.Body, 2048)
		scanner := bufio.NewScanner(lr)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "loc=") {
				discoveredLocation = strings.ToUpper(strings.TrimPrefix(line, "loc="))
				break
			}
		}
	}

	return latency, discoveredLocation, nil
}

// deltaFor normalizes reputation movement by elapsed observation time:
// immunity against rapid refresh spam and long pauses alike.
func deltaFor(lastCheckedAt, now time.Time) float64 {
	delta := 1.0
	if !lastCheckedAt.IsZero() {
		delta = math.Min(1.0, math.Max(0.01, now.Sub(lastCheckedAt).Minutes()))
	}
	return delta
}

// classifyRevival snapshots a candidate's pre-probe lifecycle position for
// post-probe Died/Revived edge detection.
func classifyRevival(item ProxyState, now time.Time) (wasDead, wasRevived bool) {
	wasDead = item.IsDead
	wasRevived = item.IsDead && !item.ReviveAt.IsZero() && !now.Before(item.ReviveAt)
	return wasDead, wasRevived
}

// applySuccess updates state after passing handshake + latency check.
func applySuccess(state *ProxyState, now time.Time, latency time.Duration, location string) {
	delta := deltaFor(state.LastCheckedAt, now)

	state.Score = math.Min(10.0, state.Score+delta)
	state.Penalty = math.Max(0.0, state.Penalty-delta)
	state.IsDead = false
	state.FailReason = FailNone
	state.SoftFails = 0
	state.Suspect = false
	state.LastCheckedAt = now
	state.ReviveAt = time.Time{}
	state.Latency = latency

	if location != "" {
		state.Location = location
	}
}

// applyFailureV2 applies the step-3 failure policy. Forced checks only record
// the failure metadata; normal soft failures use a separate exponential ban
// budget, while hard failures retain the legacy reputation-based backoff.
func applyFailureV2(state *ProxyState, now time.Time, reason FailReason, net NetSnapshot, forced bool) {
	applyFailureV2Config(state, now, reason, net, forced, DefaultConfig())
}

func applyFailureV2Config(state *ProxyState, now time.Time, reason FailReason, net NetSnapshot, forced bool, cfg Config) {
	if forced {
		state.FailReason = reason
		state.LastCheckedAt = now
		return
	}
	if reason.Soft() && net.State != NetGood {
		return
	}

	lastCheckedAt := state.LastCheckedAt
	state.FailReason = reason
	state.LastCheckedAt = now
	if reason.Soft() {
		state.SoftFails++
		failCount := state.SoftFails
		duration := cfg.SoftBanBase
		for n := 1; n < failCount && duration < cfg.SoftBanMax; n++ {
			if duration > cfg.SoftBanMax/2 {
				duration = cfg.SoftBanMax
				break
			}
			duration *= 2
		}
		if duration > cfg.SoftBanMax {
			duration = cfg.SoftBanMax
		}
		jitter := 0.85 + 0.30*rand.Float64()
		state.IsDead = true
		state.Suspect = false
		state.ReviveAt = now.Add(time.Duration(float64(duration) * jitter))
		return
	}

	delta := deltaFor(lastCheckedAt, now)
	state.SoftFails = 0
	state.Penalty += delta
	state.Score = math.Max(0.0, state.Score-delta)
	state.IsDead = true
	state.Suspect = false

	rawMinutes := 15.0 * math.Pow(2.0, state.Penalty) / math.Pow(2.5, state.Score)
	baseMinutes := math.Max(1.0, math.Min(2880.0, rawMinutes))
	jitter := 0.85 + (0.30 * rand.Float64())
	state.ReviveAt = now.Add(time.Duration(baseMinutes * jitter * float64(time.Minute)))
}
