// Command proxypoold is the bundled sidecar for opencode's proxy pool.
// It wires the three public feeds (Proxifly/SpeedX/Monosans) into a
// proxypool.Pool and exposes a localhost-only HTTP control plane:
//
//	GET  /healthz
//	GET  /stats                       -> proxypool.PoolStats (JSON)
//	GET  /proxies?limit=&country=     -> {proxies,total} (JSON)
//	POST /refresh                     -> async source ingest
//	POST /mark-dead {url,reason}      -> Pool.MarkDead
//	POST /mark-limited {url,reason}   -> Pool.MarkDead + limited registry (failover signal)
//	POST /suspect {url}               -> Pool.Suspect
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/TheSlopMachine/proxypool"
)

const (
	proxiflyAllURL  = "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/all/data.json"
	speedXHTTPURL   = "https://cdn.jsdelivr.net/gh/TheSpeedX/PROXY-List@master/http.txt"
	speedXSOCKS4URL = "https://cdn.jsdelivr.net/gh/TheSpeedX/PROXY-List@master/socks4.txt"
	speedXSOCKS5URL = "https://cdn.jsdelivr.net/gh/TheSpeedX/PROXY-List@master/socks5.txt"
	monosansAllURL  = "https://cdn.jsdelivr.net/gh/monosans/proxy-list@main/proxies/all.txt"
)

func isAcceptedProtocol(scheme string) bool { return proxypool.IsSupportedScheme(scheme) }

type conditionalGet struct {
	etag    string
	lastMod string
	status  int
	client  *http.Client
}

func (c *conditionalGet) Get(url string) ([]byte, int) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		c.status = 500
		return nil, 500
	}
	if c.etag != "" {
		req.Header.Set("If-None-Match", c.etag)
	}
	if c.lastMod != "" {
		req.Header.Set("If-Modified-Since", c.lastMod)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		c.status = 500
		return nil, 500
	}
	defer resp.Body.Close()
	c.status = resp.StatusCode
	if resp.StatusCode == http.StatusNotModified {
		if v := resp.Header.Get("ETag"); v != "" {
			c.etag = v
		}
		if v := resp.Header.Get("Last-Modified"); v != "" {
			c.lastMod = v
		}
		return nil, http.StatusNotModified
	}
	if resp.StatusCode != http.StatusOK {
		return nil, resp.StatusCode
	}
	if v := resp.Header.Get("ETag"); v != "" {
		c.etag = v
	} else {
		c.etag = ""
	}
	if v := resp.Header.Get("Last-Modified"); v != "" {
		c.lastMod = v
	} else {
		c.lastMod = ""
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil || len(body) == 0 {
		c.status = 500
		return nil, 500
	}
	return body, http.StatusOK
}

type ProxiflySource struct{ fetch *conditionalGet }

func (s *ProxiflySource) Name() string { return "Proxifly" }
func (s *ProxiflySource) FetchList() []string {
	body, status := s.fetch.Get(proxiflyAllURL)
	if status != http.StatusOK {
		return nil
	}
	var items []struct {
		Proxy    string `json:"proxy"`
		Protocol string `json:"protocol"`
	}
	if err := json.Unmarshal(body, &items); err != nil {
		return nil
	}
	urls := make([]string, 0, len(items))
	for _, item := range items {
		if !isAcceptedProtocol(item.Protocol) || strings.TrimSpace(item.Proxy) == "" {
			continue
		}
		urls = append(urls, item.Proxy)
	}
	return urls
}

type speedXFeed struct {
	url    string
	scheme string
	fetch  *conditionalGet
}

type SpeedXSource struct{ feeds []speedXFeed }

func (s *SpeedXSource) Name() string { return "SpeedX" }
func (s *SpeedXSource) FetchList() []string {
	results := make([][]string, len(s.feeds))
	var wg sync.WaitGroup
	for i := range s.feeds {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f := &s.feeds[i]
			body, status := f.fetch.Get(f.url)
			if status != http.StatusOK {
				return
			}
			var urls []string
			for _, line := range cleanFeedLines(body) {
				sep := strings.LastIndex(line, ":")
				if sep <= 0 {
					continue
				}
				host, portStr := line[:sep], line[sep+1:]
				port, err := strconv.Atoi(portStr)
				if strings.TrimSpace(host) == "" || err != nil || port < 1 || port > 65535 {
					continue
				}
				urls = append(urls, f.scheme+"://"+line)
			}
			results[i] = urls
		}()
	}
	wg.Wait()
	var urls []string
	for _, part := range results {
		urls = append(urls, part...)
	}
	if len(urls) == 0 {
		return nil
	}
	return urls
}

func cleanFeedLines(body []byte) []string {
	var lines []string
	for _, line := range strings.Split(string(body), "\n") {
		line = strings.TrimSpace(strings.ReplaceAll(line, "\r", ""))
		if line == "" {
			continue
		}
		lines = append(lines, line)
	}
	return lines
}

type MonosansSource struct{ fetch *conditionalGet }

func (s *MonosansSource) Name() string { return "Monosans" }
func (s *MonosansSource) FetchList() []string {
	body, status := s.fetch.Get(monosansAllURL)
	if status != http.StatusOK {
		return nil
	}
	var urls []string
	for _, line := range cleanFeedLines(body) {
		scheme, _, ok := strings.Cut(line, "://")
		if !ok {
			scheme = "http"
			line = scheme + "://" + line
		}
		if !isAcceptedProtocol(scheme) {
			continue
		}
		urls = append(urls, line)
	}
	if len(urls) == 0 {
		return nil
	}
	return urls
}

func main() {
	portFlag := flag.String("port", "0", "localhost TCP port to bind (0 picks an ephemeral port)")
	tokenFlag := flag.String("token", "", "bearer token required on every request (empty disables auth)")
	flag.Parse()

	pool, err := proxypool.NewPool(proxypool.NewMemoryCache())
	if err != nil {
		fmt.Fprintf(os.Stderr, "proxypoold: %v\n", err)
		os.Exit(1)
	}
	feedTransport := &http.Transport{ResponseHeaderTimeout: 10 * time.Second, TLSHandshakeTimeout: 10 * time.Second}
	feedClient := &http.Client{Timeout: 30 * time.Second, Transport: feedTransport}
	pool.RegisterProxySource(&ProxiflySource{fetch: &conditionalGet{client: feedClient}})
	pool.RegisterProxySource(&SpeedXSource{feeds: []speedXFeed{
		{url: speedXHTTPURL, scheme: "http", fetch: &conditionalGet{client: feedClient}},
		{url: speedXSOCKS4URL, scheme: "socks4", fetch: &conditionalGet{client: feedClient}},
		{url: speedXSOCKS5URL, scheme: "socks5", fetch: &conditionalGet{client: feedClient}},
	}})
	pool.RegisterProxySource(&MonosansSource{fetch: &conditionalGet{client: feedClient}})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool.Start(ctx)
	defer pool.Stop()

	mux := http.NewServeMux()
	withAuth := func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if *tokenFlag != "" && r.Header.Get("Authorization") != "Bearer "+*tokenFlag {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			next(w, r)
		}
	}
	mux.HandleFunc("/healthz", withAuth(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"ok":true}`)
	}))
	// limited tracks proxies flagged by upstream-observed failures (HTTP
	// 429/407/403/5xx). Marking goes through Pool.MarkDead so the proxy
	// leaves the alive index on the existing escalating schedule; this map
	// only counts recent limited marks for the status surface.
	var limitedMu sync.Mutex
	limited := make(map[string]time.Time)
	pruneLimited := func() {
		cutoff := time.Now().Add(-24 * time.Hour)
		for url, at := range limited {
			if at.Before(cutoff) {
				delete(limited, url)
			}
		}
	}
	mux.HandleFunc("/stats", withAuth(func(w http.ResponseWriter, _ *http.Request) {
		stats := pool.Snapshot()
		limitedMu.Lock()
		pruneLimited()
		limitedCount := len(limited)
		limitedMu.Unlock()
		out := map[string]any{
			"enabled": true,
			"alive":   stats.Alive,
			"suspect": stats.Suspect,
			"banned":  stats.Banned,
			"queued":  stats.Queued,
			"limited": limitedCount,
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	}))
	mux.HandleFunc("/proxies", withAuth(func(w http.ResponseWriter, r *http.Request) {
		limit := 50
		if v := r.URL.Query().Get("limit"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 500 {
				limit = n
			}
		}
		var filter proxypool.ProxyFilter
		filter.Limit = &limit
		if country := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("country"))); len(country) == 2 {
			filter.Location = &country
		}
		infos := pool.ListProxies(filter)
		type item struct {
			URL         string  `json:"url"`
			Location    string  `json:"location,omitempty"`
			LatencyMs   int64   `json:"latencyMs,omitempty"`
			Score       float64 `json:"score,omitempty"`
			Source      string  `json:"source,omitempty"`
			LastChecked int64   `json:"lastChecked,omitempty"`
		}
		items := make([]item, 0, len(infos))
		for _, p := range infos {
			items = append(items, item{
				URL:         p.URL,
				Location:    p.Location,
				LatencyMs:   p.Latency.Milliseconds(),
				Score:       p.Score,
				LastChecked: p.LastChecked.UnixMilli(),
			})
			_ = p
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"proxies": items, "total": len(items)})
	}))
	mux.HandleFunc("/refresh", withAuth(func(w http.ResponseWriter, _ *http.Request) {
		pool.RequestIngest()
		w.WriteHeader(http.StatusNoContent)
	}))
	mux.HandleFunc("/mark-dead", withAuth(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			URL    string `json:"url"`
			Reason string `json:"reason"`
		}
		_ = json.NewDecoder(io.LimitReader(r.Body, 64<<10)).Decode(&body)
		if body.URL != "" {
			pool.MarkDead(body.URL, body.Reason)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	mux.HandleFunc("/mark-limited", withAuth(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			URL    string `json:"url"`
			Reason string `json:"reason"`
		}
		_ = json.NewDecoder(io.LimitReader(r.Body, 64<<10)).Decode(&body)
		if body.URL != "" {
			pool.MarkDead(body.URL, body.Reason)
			limitedMu.Lock()
			limited[body.URL] = time.Now()
			limitedMu.Unlock()
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	mux.HandleFunc("/suspect", withAuth(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			URL string `json:"url"`
		}
		_ = json.NewDecoder(io.LimitReader(r.Body, 64<<10)).Decode(&body)
		if body.URL != "" {
			pool.Suspect(body.URL, "")
		}
		w.WriteHeader(http.StatusNoContent)
	}))

	addr := "127.0.0.1:" + *portFlag
	if *portFlag == "0" {
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "proxypoold listen: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("proxypoold listening on http://%s\n", ln.Addr().String())
		_ = os.Stdout.Sync()
		server := &http.Server{Handler: mux}
		go func() {
			sig := make(chan os.Signal, 1)
			signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
			<-sig
			cancel()
			_ = server.Close()
		}()
		_ = server.Serve(ln)
		return
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "proxypoold listen: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("proxypoold listening on http://%s\n", ln.Addr().String())
	_ = os.Stdout.Sync()
	server := &http.Server{Handler: mux}
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
		<-sig
		cancel()
		_ = server.Close()
	}()
	_ = server.Serve(ln)
}
