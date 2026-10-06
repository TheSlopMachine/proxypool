package proxypool

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"
)

type tcpProxySource struct{ url string }

func (s tcpProxySource) Name() string        { return "fake" }
func (s tcpProxySource) FetchList() []string { return []string{s.url} }

func startFakeHTTPProxy(t *testing.T) (string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				select {
				case <-stop:
					return
				default:
				}
				continue
			}
			go func(c net.Conn) {
				defer c.Close()
				_ = c.SetDeadline(time.Now().Add(2 * time.Second))
				r := bufio.NewReader(c)
				line, err := r.ReadString('\n')
				if err != nil {
					return
				}
				if !strings.HasPrefix(line, "CONNECT ") {
					return
				}
				for {
					line, err = r.ReadString('\n')
					if err != nil {
						return
					}
					if line == "\r\n" {
						break
					}
				}
				_, _ = fmt.Fprint(c, "HTTP/1.1 200 Connection Established\r\nProxy-Agent: test\r\n\r\n")
			}(conn)
		}
	}()
	return "http://" + ln.Addr().String(), func() { close(stop); _ = ln.Close() }
}

func TestRefreshChecksCandidateAndPersistsResult(t *testing.T) {
	proxyURL, cleanup := startFakeHTTPProxy(t)
	defer cleanup()
	p := NewPool()
	p.SetTimeout(TimeoutConfig{Handshake: 100 * time.Millisecond, Probe: 100 * time.Millisecond})
	p.SetLimits(1, 1, 1)
	p.RegisterProxySource(tcpProxySource{url: proxyURL})
	p.Refresh()
	state, ok := p.cache.Get(proxyURL)
	if !ok {
		t.Fatal("checked candidate must be persisted immediately")
	}
	if state.LastCheckedAt.IsZero() || state.FailReason == FailNone {
		t.Fatalf("expected a completed failed check, got %+v", state)
	}
}

func TestDropUncheckedRemovesLegacyRecords(t *testing.T) {
	cache := newMemoryCache()
	cache.Set(ProxyState{URL: "legacy", LastSeenInSource: time.Time{}})
	cache.Set(ProxyState{URL: "checked", LastCheckedAt: time.Now().UTC()})
	dropUnchecked(cache, time.Now().UTC())
	if _, ok := cache.Get("legacy"); ok {
		t.Fatal("unchecked legacy record must be deleted")
	}
	state, ok := cache.Get("checked")
	if !ok || state.LastSeenInSource.IsZero() {
		t.Fatalf("checked record must gain a source timestamp, got %+v/%v", state, ok)
	}
}
