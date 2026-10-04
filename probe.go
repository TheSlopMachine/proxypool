package proxypool

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
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
	for total < len(buf) {
		n, rErr := conn.Read(buf[total:])
		if n > 0 {
			total += n
			if bytes.Contains(buf[:total], []byte("\n")) {
				break
			}
		}
		if rErr != nil {
			break
		}
	}

	if total == 0 || !is2xxResponse(buf[:total]) {
		return errors.New("proxy handshake failed or returned non-2xx")
	}
	return nil
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
		return 0, "", fmt.Errorf("unexpected status code: %d", resp.StatusCode)
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
	state.LastCheckedAt = now
	state.ReviveAt = time.Time{}
	state.Latency = latency

	if location != "" {
		state.Location = location
	}
}

// applyFailure updates state and schedules a revival timestamp using jitter dispersion.
func applyFailure(state *ProxyState, now time.Time) {
	delta := deltaFor(state.LastCheckedAt, now)

	state.Penalty += delta
	state.Score = math.Max(0.0, state.Score-delta)
	state.IsDead = true
	state.LastCheckedAt = now

	// Calculate base ban duration using the ratio model
	rawMinutes := 15.0 * math.Pow(2.0, state.Penalty) / math.Pow(2.5, state.Score)
	baseMinutes := math.Max(1.0, math.Min(2880.0, rawMinutes))

	// Apply +/-15% randomized jitter dispersion to bans > 1 min
	banMinutes := baseMinutes
	if baseMinutes > 1.0 {
		jitter := 0.85 + (0.30 * rand.Float64())
		banMinutes = math.Max(1.0, baseMinutes*jitter)
	}

	state.ReviveAt = now.Add(time.Duration(banMinutes * float64(time.Minute)))
}

// ProbeOutcome pairs a probed proxy's new state with its telemetry report.
// The two stay index-aligned by construction: one outcome per candidate.
type ProbeOutcome struct {
	State  ProxyState
	Report ProxyReport
}

// executePipeline coordinates concurrent Phase 1 (Handshake) and Phase 2 (Probe) checks.
// Handshake and probe budgets come from cfg.Timeouts: handshake bounds the
// fast TCP/CONNECT/SOCKS greeting filter, probe bounds the full HTTP/TLS
// exchange. Probe workers equal handshake workers (no /2 throttle) so
// DefaultConcurrency-way concurrency sustains full dial throughput on stock
// Windows. Every wait is ctx/deadline-bounded; progress flows through the
// reporter every 5000 completions so silence means idleness, not a parked
// pipeline.
func executePipeline(candidates []ProxyState, cfg PipelineConfig) []ProbeOutcome {
	cfg = cfg.Resolve()
	if len(candidates) == 0 {
		return nil
	}

	type probeResult struct {
		state      ProxyState
		success    bool
		latency    time.Duration
		location   string
		wasDead    bool
		wasRevived bool
	}

	// ------------------------------------------------------------------------
	// Phase 1: Fast TCP CONNECT Handshake
	// ------------------------------------------------------------------------
	handshakeWorkers := min(cfg.Concurrency, len(candidates))
	jobs := make(chan ProxyState, len(candidates))
	handshakePassChan := make(chan ProxyState, len(candidates))
	resultsChan := make(chan probeResult, len(candidates))

	for _, c := range candidates {
		jobs <- c
	}
	close(jobs)

	var wgHandshake sync.WaitGroup
	for i := 0; i < handshakeWorkers; i++ {
		wgHandshake.Add(1)
		go func() {
			defer wgHandshake.Done()
			for item := range jobs {
				wasDead, wasRevived := classifyRevival(item, cfg.Now)

				hctx, cancel := context.WithTimeout(context.Background(), cfg.Timeouts.Handshake)
				err := verifyHandshake(hctx, item.URL, cfg.Timeouts.Handshake)
				cancel()
				if err == nil {
					handshakePassChan <- item
				} else {
					resultsChan <- probeResult{
						state:      item,
						success:    false,
						wasDead:    wasDead,
						wasRevived: wasRevived,
					}
				}
			}
		}()
	}

	go func() {
		wgHandshake.Wait()
		close(handshakePassChan)
	}()

	// ------------------------------------------------------------------------
	// Phase 2: Dual-Endpoint Probe (/trace vs generate_204)
	// ------------------------------------------------------------------------
	// Full-width workers: halving here (concurrency/2) collapsed throughput
	// once SOCKS tarpits parked Phase-2 goroutines. Stock Windows sustains
	// 1000 concurrent dials; TIME_WAIT churn stays safe because only the
	// ~2% that pass the handshake hold a connected socket.
	probeWorkers := max(1, min(cfg.Concurrency, len(candidates)))
	var wgProbe sync.WaitGroup

	for i := 0; i < probeWorkers; i++ {
		wgProbe.Add(1)
		go func() {
			defer wgProbe.Done()
			for item := range handshakePassChan {
				wasDead, wasRevived := classifyRevival(item, cfg.Now)
				needLocation := item.Location == ""

				pctx, cancel := context.WithTimeout(context.Background(), cfg.Timeouts.Probe)
				lat, loc, err := probeEndpoint(pctx, item.URL, needLocation, cfg.Timeouts.Probe)
				cancel()
				if err == nil {
					resultsChan <- probeResult{
						state:      item,
						success:    true,
						latency:    lat,
						location:   loc,
						wasDead:    wasDead,
						wasRevived: wasRevived,
					}
				} else {
					resultsChan <- probeResult{
						state:      item,
						success:    false,
						wasDead:    wasDead,
						wasRevived: wasRevived,
					}
				}
			}
		}()
	}

	go func() {
		wgProbe.Wait()
		close(resultsChan)
	}()

	// ------------------------------------------------------------------------
	// Aggregate State Mutations & Telemetry Reports
	// ------------------------------------------------------------------------
	outcomes := make([]ProbeOutcome, 0, len(candidates))

	completed := 0
	for res := range resultsChan {
		st := res.state
		var died, revived bool

		if res.success {
			applySuccess(&st, cfg.Now, res.latency, res.location)
			if res.wasDead {
				revived = true
			}
		} else {
			applyFailure(&st, cfg.Now)
			if !res.wasDead {
				died = true
			}
			if res.wasRevived {
				revived = true
			}
		}

		outcomes = append(outcomes, ProbeOutcome{
			State: st,
			Report: ProxyReport{
				Timestamp: cfg.Now,
				URL:       st.URL,
				Location:  st.Location,
				IsDead:    st.IsDead,
				Score:     st.Score,
				Penalty:   st.Penalty,
				ReviveAt:  st.ReviveAt,
				Latency:   st.Latency,
				Died:      died,
				Revived:   revived,
			},
		})

		completed++
		if completed%5000 == 0 || completed == len(candidates) {
			cfg.Reporter.ReportProgress(RefreshProgressReport{
				Timestamp: cfg.Now,
				Completed: completed,
				Total:     len(candidates),
				Elapsed:   time.Since(cfg.CycleStart).Round(time.Second),
			})
		}
	}

	return outcomes
}
