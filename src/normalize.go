package proxypool

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// normalizeProxyURL parses, normalizes, and extracts connection details.
func normalizeProxyURL(raw string) (canonicalURL string, host string, port int, err error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", "", 0, errors.New("empty proxy URL")
	}

	if !strings.Contains(raw, "://") {
		raw = "http://" + raw
	}

	u, err := url.Parse(raw)
	if err != nil {
		return "", "", 0, err
	}

	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return "", "", 0, fmt.Errorf("unsupported protocol: %s", scheme)
	}

	h, pStr, err := net.SplitHostPort(u.Host)
	if err != nil {
		h = u.Host
		if scheme == "https" {
			port = 443
		} else {
			port = 80
		}
	} else {
		port, err = strconv.Atoi(pStr)
		if err != nil || port <= 0 || port > 65535 {
			return "", "", 0, fmt.Errorf("invalid port: %s", pStr)
		}
	}

	canonicalURL = fmt.Sprintf("%s://%s", scheme, net.JoinHostPort(h, strconv.Itoa(port)))
	return canonicalURL, h, port, nil
}

// medianDuration returns the 50th percentile duration from a slice.
func medianDuration(vals []time.Duration) time.Duration {
	if len(vals) == 0 {
		return 0
	}
	sorted := make([]time.Duration, len(vals))
	copy(sorted, vals)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	n := len(sorted)
	if n%2 == 1 {
		return sorted[n/2]
	}
	return (sorted[n/2-1] + sorted[n/2]) / 2
}

// meanFloat calculates the arithmetic mean of a float slice.
func meanFloat(vals []float64) float64 {
	if len(vals) == 0 {
		return 0.0
	}
	total := 0.0
	for _, v := range vals {
		total += v
	}
	return total / float64(len(vals))
}
