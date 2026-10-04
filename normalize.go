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

// defaultPortByScheme supplies the implicit port when a proxy URL omits one.
var defaultPortByScheme = map[string]int{
	"http":   80,
	"https":  443,
	"socks4": 1080,
	"socks5": 1080,
}

// normalizeProxyURL parses, normalizes, and extracts connection details.
// Accepted schemes: http, https, socks4(+socks4a alias), socks5. Embedded
// userinfo (user[:pass]@) is preserved in the canonical URL; socks4a
// canonicalizes to socks4.
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
	if scheme == "socks4a" {
		scheme = "socks4"
	}
	defaultPort, ok := defaultPortByScheme[scheme]
	if !ok {
		return "", "", 0, fmt.Errorf("unsupported protocol: %s", scheme)
	}

	host = u.Hostname()
	if host == "" {
		return "", "", 0, fmt.Errorf("missing host: %s", raw)
	}
	if portStr := u.Port(); portStr == "" {
		port = defaultPort
	} else if port, err = strconv.Atoi(portStr); err != nil || port <= 0 || port > 65535 {
		return "", "", 0, fmt.Errorf("invalid port: %s", portStr)
	}

	authority := net.JoinHostPort(host, strconv.Itoa(port))
	if u.User != nil {
		authority = u.User.String() + "@" + authority
	}
	canonicalURL = fmt.Sprintf("%s://%s", scheme, authority)
	return canonicalURL, host, port, nil
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
