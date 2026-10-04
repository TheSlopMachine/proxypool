package proxypool

import (
	"testing"
)

func TestNormalizeProxyURL(t *testing.T) {
	cases := []struct {
		name      string
		raw       string
		canonical string
		host      string
		port      int
		wantErr   bool
	}{
		{name: "bare host port defaults to http", raw: "1.2.3.4:8080", canonical: "http://1.2.3.4:8080", host: "1.2.3.4", port: 8080},
		{name: "http explicit", raw: "http://1.2.3.4:8080", canonical: "http://1.2.3.4:8080", host: "1.2.3.4", port: 8080},
		{name: "scheme case folds", raw: "HTTP://1.2.3.4:8080", canonical: "http://1.2.3.4:8080", host: "1.2.3.4", port: 8080},
		{name: "https default port", raw: "https://example.com", canonical: "https://example.com:443", host: "example.com", port: 443},
		{name: "http default port", raw: "http://example.com", canonical: "http://example.com:80", host: "example.com", port: 80},
		{name: "socks5 keeps scheme and port", raw: "socks5://1.2.3.4:1080", canonical: "socks5://1.2.3.4:1080", host: "1.2.3.4", port: 1080},
		{name: "socks5 default port", raw: "socks5://1.2.3.4", canonical: "socks5://1.2.3.4:1080", host: "1.2.3.4", port: 1080},
		{name: "socks4 keeps scheme", raw: "socks4://1.2.3.4:9050", canonical: "socks4://1.2.3.4:9050", host: "1.2.3.4", port: 9050},
		{name: "socks4 default port", raw: "socks4://1.2.3.4", canonical: "socks4://1.2.3.4:1080", host: "1.2.3.4", port: 1080},
		{name: "socks4a canonicalizes to socks4", raw: "socks4a://1.2.3.4:1080", canonical: "socks4://1.2.3.4:1080", host: "1.2.3.4", port: 1080},
		{name: "socks5 userinfo preserved", raw: "socks5://user:pass@1.2.3.4:1080", canonical: "socks5://user:pass@1.2.3.4:1080", host: "1.2.3.4", port: 1080},
		{name: "http user without password preserved", raw: "http://user@1.2.3.4:8080", canonical: "http://user@1.2.3.4:8080", host: "1.2.3.4", port: 8080},
		{name: "https userinfo preserved", raw: "https://user:pass@example.com:8443", canonical: "https://user:pass@example.com:8443", host: "example.com", port: 8443},
		{name: "empty rejected", raw: "", wantErr: true},
		{name: "blank rejected", raw: "   ", wantErr: true},
		{name: "ftp rejected", raw: "ftp://1.2.3.4:21", wantErr: true},
		{name: "socks6 rejected", raw: "socks6://1.2.3.4:1080", wantErr: true},
		{name: "port overflow rejected", raw: "http://1.2.3.4:99999", wantErr: true},
		{name: "port zero rejected", raw: "http://1.2.3.4:0", wantErr: true},
		{name: "missing host rejected", raw: "http://:8080", wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ep, err := normalizeProxyURL(tc.raw)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for %q", tc.raw)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error for %q: %v", tc.raw, err)
			}
			if ep.Canonical != tc.canonical || ep.Host != tc.host || ep.Port != tc.port {
				t.Fatalf("got %q/%q/%d, want %q/%q/%d", ep.Canonical, ep.Host, ep.Port, tc.canonical, tc.host, tc.port)
			}
		})
	}
}

func TestParseProxyURLAuth(t *testing.T) {
	p, err := parseProxyURL("socks5://alice:s3cret@1.2.3.4:1080")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !p.hasAuth || p.username != "alice" || p.password != "s3cret" {
		t.Fatalf("auth not parsed: %+v", p)
	}
	if p.scheme != "socks5" || p.address() != "1.2.3.4:1080" {
		t.Fatalf("address not parsed: %+v", p)
	}

	p, err = parseProxyURL("http://1.2.3.4:8080")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.hasAuth {
		t.Fatalf("bare URL must not report auth: %+v", p)
	}
}
