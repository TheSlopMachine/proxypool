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

// SupportedSchemes lists the proxy schemes the prober can handle, in
// canonical form. "socks4a" is accepted as an alias and canonicalizes to
// "socks4". Feeds filter against IsSupportedScheme; transports switch on
// the canonical scheme.
func SupportedSchemes() []string {
	return []string{"http", "https", "socks4", "socks5"}
}

// IsSupportedScheme reports whether scheme names a probed protocol,
// canonicalizing case and the socks4a alias.
func IsSupportedScheme(scheme string) bool {
	scheme = strings.ToLower(strings.TrimSpace(scheme))
	if scheme == "socks4a" {
		scheme = "socks4"
	}
	_, ok := defaultPortByScheme[scheme]
	return ok
}

// ParsedEndpoint is a normalized proxy endpoint: canonical URL plus the
// dial-relevant host and port.
type ParsedEndpoint struct {
	Canonical string // e.g. "socks5://user:pass@1.2.3.4:1080"
	Host      string // target IP or hostname
	Port      int    // target port
}

// normalizeProxyURL parses, normalizes, and extracts connection details.
// Accepted schemes: http, https, socks4(+socks4a alias), socks5. Embedded
// userinfo (user[:pass]@) is preserved in the canonical URL; socks4a
// canonicalizes to socks4.
func normalizeProxyURL(raw string) (ParsedEndpoint, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ParsedEndpoint{}, errors.New("empty proxy URL")
	}

	if !strings.Contains(raw, "://") {
		raw = "http://" + raw
	}

	u, err := url.Parse(raw)
	if err != nil {
		return ParsedEndpoint{}, err
	}

	scheme := strings.ToLower(u.Scheme)
	if scheme == "socks4a" {
		scheme = "socks4"
	}
	defaultPort, ok := defaultPortByScheme[scheme]
	if !ok {
		return ParsedEndpoint{}, fmt.Errorf("unsupported protocol: %s", scheme)
	}

	host := u.Hostname()
	if host == "" {
		return ParsedEndpoint{}, fmt.Errorf("missing host: %s", raw)
	}
	var port int
	if portStr := u.Port(); portStr == "" {
		port = defaultPort
	} else if port, err = strconv.Atoi(portStr); err != nil || port <= 0 || port > 65535 {
		return ParsedEndpoint{}, fmt.Errorf("invalid port: %s", portStr)
	}

	authority := net.JoinHostPort(host, strconv.Itoa(port))
	if u.User != nil {
		authority = u.User.String() + "@" + authority
	}
	return ParsedEndpoint{
		Canonical: fmt.Sprintf("%s://%s", scheme, authority),
		Host:      host,
		Port:      port,
	}, nil
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
