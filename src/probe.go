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
	"net/url"
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

// verifyHandshake performs a raw TCP HTTP CONNECT tunnel verification.
func verifyHandshake(proxyAddr string, timeout time.Duration) error {
	dialer := &net.Dialer{Timeout: timeout}
	conn, err := dialer.Dial("tcp", proxyAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	_ = conn.SetDeadline(time.Now().Add(timeout))

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
	for spaceIdx < len(b) && b[spaceIdx] == ' ' {
		spaceIdx++
	}
	if spaceIdx+3 > len(b) {
		return false
	}
	code := 0
	for i := 0; i < 3; i++ {
		c := b[spaceIdx+i]
		if c < '0' || c > '9' {
			return false
		}
		code = code*10 + int(c-'0')
	}
	return code >= 200 && code < 300
}

// probeEndpoint conducts the dual-endpoint HTTP latency and location test.
func probeEndpoint(ctx context.Context, proxyURI string, needLocation bool, timeout time.Duration) (time.Duration, string, error) {
	parsedProxy, err := url.Parse(proxyURI)
	if err != nil {
		return 0, "", err
	}

	transport := &http.Transport{
		Proxy:             http.ProxyURL(parsedProxy),
		DisableKeepAlives: true,
		DialContext: (&net.Dialer{
			Timeout: timeout,
		}).DialContext,
		ResponseHeaderTimeout: timeout,
	}

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

// applySuccess updates state after passing handshake + latency check.
func applySuccess(state *ProxyState, now time.Time, latency time.Duration, location string) {
	delta := 1.0
	if !state.LastCheckedAt.IsZero() {
		elapsed := now.Sub(state.LastCheckedAt).Minutes()
		delta = math.Min(1.0, math.Max(0.01, elapsed))
	}

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
	delta := 1.0
	if !state.LastCheckedAt.IsZero() {
		elapsed := now.Sub(state.LastCheckedAt).Minutes()
		delta = math.Min(1.0, math.Max(0.01, elapsed))
	}

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

// executePipeline coordinates concurrent Phase 1 (Handshake) and Phase 2 (Probe) checks.
func executePipeline(candidates []ProxyState, concurrency int, timeout time.Duration, now time.Time) ([]ProxyState, []ProxyReport) {
	if len(candidates) == 0 {
		return nil, nil
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
	handshakeWorkers := min(concurrency, len(candidates))
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
				proxyAddr := net.JoinHostPort(item.IP, strconv.Itoa(item.Port))
				wasDead := item.IsDead
				wasRevived := item.IsDead && !item.ReviveAt.IsZero() && !now.Before(item.ReviveAt)

				if err := verifyHandshake(proxyAddr, timeout); err == nil {
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
	probeWorkers := max(1, min(concurrency/2, len(candidates)))
	var wgProbe sync.WaitGroup

	for i := 0; i < probeWorkers; i++ {
		wgProbe.Add(1)
		go func() {
			defer wgProbe.Done()
			ctx := context.Background()
			for item := range handshakePassChan {
				wasDead := item.IsDead
				wasRevived := item.IsDead && !item.ReviveAt.IsZero() && !now.Before(item.ReviveAt)
				needLocation := item.Location == ""

				lat, loc, err := probeEndpoint(ctx, item.URL, needLocation, timeout)
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
	updatedStates := make([]ProxyState, 0, len(candidates))
	reports := make([]ProxyReport, 0, len(candidates))

	for res := range resultsChan {
		st := res.state
		var died, revived bool

		if res.success {
			applySuccess(&st, now, res.latency, res.location)
			if res.wasDead {
				revived = true
			}
		} else {
			applyFailure(&st, now)
			if !res.wasDead {
				died = true
			}
			if res.wasRevived {
				revived = true
			}
		}

		updatedStates = append(updatedStates, st)
		reports = append(reports, ProxyReport{
			Timestamp: now,
			URL:       st.URL,
			Location:  st.Location,
			IsDead:    st.IsDead,
			Score:     st.Score,
			Penalty:   st.Penalty,
			ReviveAt:  st.ReviveAt,
			Latency:   st.Latency,
			Died:      died,
			Revived:   revived,
		})
	}

	return updatedStates, reports
}