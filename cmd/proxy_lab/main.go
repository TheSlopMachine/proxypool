package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/TheSlopMachine/proxypool"
)

// ============================================================================
// 1. Upstream Proxy Sources (native Go reissues of the plugin feeds)
// ============================================================================

const (
	proxiflyAllURL  = "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/all/data.json"
	speedXHTTPURL   = "https://cdn.jsdelivr.net/gh/TheSpeedX/PROXY-List@master/http.txt"
	speedXSOCKS4URL = "https://cdn.jsdelivr.net/gh/TheSpeedX/PROXY-List@master/socks4.txt"
	speedXSOCKS5URL = "https://cdn.jsdelivr.net/gh/TheSpeedX/PROXY-List@master/socks5.txt"
	monosansAllURL  = "https://cdn.jsdelivr.net/gh/monosans/proxy-list@main/proxies/all.txt"
)

// isAcceptedProtocol mirrors the plugin contract: rows outside the pool's
// supported schemes count as unsupported. It delegates to the canonical
// table so feeds and prober can never disagree (socks4a reads as socks4).
func isAcceptedProtocol(scheme string) bool {
	return proxypool.IsSupportedScheme(scheme)
}

// labSource is the status surface the reporter needs from every feed.
type labSource interface {
	Name() string
	LastStatus() int
}

// conditionalGet issues ETag/Last-Modified conditional requests against one
// feed URL. A 304 returns no body; any non-200 returns no body either.
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
	if err != nil {
		fmt.Fprintf(os.Stderr, "feed %s: body read failed: %v\n", url, err)
		c.status = 500
		return nil, 500
	}
	if len(body) == 0 {
		fmt.Fprintf(os.Stderr, "feed %s: empty body\n", url)
		c.status = 500
		return nil, 500
	}
	return body, http.StatusOK
}

func (c *conditionalGet) LastStatus() int {
	if c.status == 0 {
		return 200
	}
	return c.status
}

// collapseStatus reduces several feed statuses to the single HTTP column:
// 200 when any feed delivered fresh data, 304 when every feed stayed cached,
// otherwise the first error status.
func collapseStatus(statuses ...int) int {
	fallback := 0
	saw304 := false
	for _, s := range statuses {
		switch {
		case s == 200:
			return 200
		case s == 0:
			continue
		case s == 304:
			saw304 = true
		case fallback == 0:
			fallback = s
		}
	}
	if fallback != 0 {
		return fallback
	}
	if saw304 {
		return 304
	}
	return 200
}

// ProxiflySource serves the all-protocols JSON feed with ETag / 304 support.
type ProxiflySource struct {
	fetch *conditionalGet
}

func NewProxiflySource(client *http.Client) *ProxiflySource {
	return &ProxiflySource{fetch: &conditionalGet{client: client}}
}

func (s *ProxiflySource) Name() string { return "Proxifly" }

func (s *ProxiflySource) LastStatus() int { return s.fetch.LastStatus() }

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
		fmt.Fprintf(os.Stderr, "proxifly feed: dropped %d-byte malformed body: %v\n", len(body), err)
		return nil
	}

	urls := make([]string, 0, len(items))
	for _, item := range items {
		if !isAcceptedProtocol(item.Protocol) {
			continue
		}
		if strings.TrimSpace(item.Proxy) == "" {
			continue
		}
		urls = append(urls, item.Proxy)
	}
	return urls
}

// speedXFeed pairs one text feed URL with the scheme its bare host:port
// lines stand for.
type speedXFeed struct {
	url    string
	scheme string
	fetch  *conditionalGet
}

// SpeedXSource serves the http/socks4/socks5 text feeds, each with its own
// ETag / Last-Modified validators.
type SpeedXSource struct {
	feeds []speedXFeed
}

func NewSpeedXSource(client *http.Client) *SpeedXSource {
	return &SpeedXSource{feeds: []speedXFeed{
		{url: speedXHTTPURL, scheme: "http", fetch: &conditionalGet{client: client}},
		{url: speedXSOCKS4URL, scheme: "socks4", fetch: &conditionalGet{client: client}},
		{url: speedXSOCKS5URL, scheme: "socks5", fetch: &conditionalGet{client: client}},
	}}
}

func (s *SpeedXSource) Name() string { return "SpeedX" }

func (s *SpeedXSource) LastStatus() int {
	statuses := make([]int, 0, len(s.feeds))
	for _, f := range s.feeds {
		statuses = append(statuses, f.fetch.LastStatus())
	}
	return collapseStatus(statuses...)
}

func (s *SpeedXSource) FetchList() []string {
	// Fetch the 3 text feeds concurrently: sequential 3x30s worst-case
	// becomes ~30s, well inside the 10min cycle budget.
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
	total := 0
	for _, part := range results {
		total += len(part)
	}
	if total == 0 {
		return nil
	}
	urls := make([]string, 0, total)
	for _, part := range results {
		urls = append(urls, part...)
	}
	return urls
}

// cleanFeedLines splits a text feed body into non-empty trimmed lines,
// shared by the SpeedX and Monosans parsers.
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

// MonosansSource serves the all-protocols text feed whose lines already
// carry scheme://host:port endpoints.
type MonosansSource struct {
	fetch *conditionalGet
}

func NewMonosansSource(client *http.Client) *MonosansSource {
	return &MonosansSource{fetch: &conditionalGet{client: client}}
}

func (s *MonosansSource) Name() string { return "Monosans" }

func (s *MonosansSource) LastStatus() int { return s.fetch.LastStatus() }

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

// ============================================================================
// 2. Metrics & Telemetry Reporter (CSV Streamer + 3-Letter Namer)
// ============================================================================

// maxNamesPerCell caps the DiedNames/RevivedNames columns for large pools;
// the numeric Died/Revived columns keep the full counts.
const maxNamesPerCell = 20

const csvHeader = "Timestamp|HTTP|Total|New|Skip|Revive|Probed|Survived|Died|Alive|MinLat|MedLat|DiedNames|RevivedNames|MaxScore|AvgScore|MaxPenalty|AvgPenalty|Duration|SrcProxifly|SrcSpeedX|SrcMonosans\n"

type LabReporter struct {
	mu           sync.Mutex
	tableFile    *os.File
	sources      []labSource
	nameMap      map[string]string
	nameCounter  int
	diedNames    []string
	revivedNames []string
	writeErrs    int
}

func NewLabReporter(file *os.File, sources ...labSource) *LabReporter {
	return &LabReporter{
		tableFile: file,
		sources:   sources,
		nameMap:   make(map[string]string),
	}
}

func (r *LabReporter) ReportProxy(p proxypool.ProxyReport) {
	r.mu.Lock()
	defer r.mu.Unlock()

	name, exists := r.nameMap[p.URL]
	if !exists {
		name = generateName(r.nameCounter)
		r.nameCounter++
		r.nameMap[p.URL] = name
	}

	if p.Died {
		r.diedNames = append(r.diedNames, name)
	}
	if p.Revived {
		r.revivedNames = append(r.revivedNames, name)
	}
}

// formatNames sorts the per-cycle names and caps the cell at maxNamesPerCell
// entries with a single +N-more overflow token.
func formatNames(names []string) string {
	if len(names) == 0 {
		return "-"
	}
	sort.Strings(names)
	if len(names) > maxNamesPerCell {
		overflow := len(names) - maxNamesPerCell
		capped := make([]string, 0, maxNamesPerCell+1)
		capped = append(capped, names[:maxNamesPerCell]...)
		capped = append(capped, fmt.Sprintf("+%d-more", overflow))
		names = capped
	}
	return strings.Join(names, " ")
}

func (r *LabReporter) ReportStats(report proxypool.PoolStats) {
	r.mu.Lock()
	defer r.mu.Unlock()
	statuses := make([]int, 0, len(r.sources))
	for _, source := range r.sources {
		statuses = append(statuses, source.LastStatus())
	}
	total := report.Alive + report.Suspect + report.Banned + report.Queued + report.Inflight
	row := fmt.Sprintf("%s|%d|%d|%d|%d|%d|%d|%d|%d|%d|%dms|%dms|%s|%s|%.2f|%.2f|%.2f|%.2f|%s|%s\n",
		report.Time.Format("2006-01-02 15:04:05"),
		collapseStatus(statuses...),
		total,
		report.Queued,
		0,
		0,
		report.Inflight,
		report.Alive,
		report.Suspect,
		report.Alive,
		0,
		0,
		formatNames(r.diedNames),
		formatNames(r.revivedNames),
		0.0,
		0.0,
		0.0,
		0.0,
		"-",
		joinStatuses(statuses),
	)
	fmt.Print(row)
	if r.tableFile != nil {
		if _, err := r.tableFile.WriteString(row); err != nil {
			r.writeErrs++
			fmt.Fprintf(os.Stderr, "table write failed (%d total): %v\n", r.writeErrs, err)
		} else if err := r.tableFile.Sync(); err != nil {
			r.writeErrs++
			fmt.Fprintf(os.Stderr, "table sync failed (%d total): %v\n", r.writeErrs, err)
		}
	}
	r.diedNames = nil
	r.revivedNames = nil
}

func (r *LabReporter) ReportEvent(_ proxypool.PoolEvent) {}

func joinStatuses(statuses []int) string {
	parts := make([]string, 0, len(statuses))
	for _, s := range statuses {
		parts = append(parts, fmt.Sprintf("%d", s))
	}
	return strings.Join(parts, "|")
}

// generateName assigns 3-letter alphabetical names: 0->AAA, 1->AAB, ..., 17575->ZZZ
func generateName(idx int) string {
	if idx < 26*26*26 {
		c1 := byte('A' + (idx/(26*26))%26)
		c2 := byte('A' + (idx/26)%26)
		c3 := byte('A' + idx%26)
		return string([]byte{c1, c2, c3})
	}
	// Fallback expansion for >17,575 items
	var sb strings.Builder
	var rev []byte
	for {
		rev = append(rev, byte('A'+(idx%26)))
		idx = (idx / 26) - 1
		if idx < 0 {
			break
		}
	}
	for i := len(rev) - 1; i >= 0; i-- {
		sb.WriteByte(rev[i])
	}
	return sb.String()
}

// ============================================================================
// 3. Application Entrypoint
// ============================================================================

func main() {
	intervalFlag := flag.Duration("interval", 15*time.Second, "Interval between refresh checks (e.g. 15s, 1m, 5m)")
	samplesFlag := flag.Int("samples", 100, "How many samples to collect before exiting (0 for infinite)")
	tableFlag := flag.String("table", "table.csv", "Path where to output pipe-separated CSV logs")
	concurrencyFlag := flag.Int("concurrency", 1000, "Max concurrent probe workers (stock-Windows-safe default; 2000+ needs ephemeral-port tuning)")
	timeoutFlag := flag.Duration("timeout", 5*time.Second, "Per-probe HTTP/TLS budget (also sets handshake when -handshake-timeout unset)")
	handshakeFlag := flag.Duration("handshake-timeout", 3*time.Second, "Phase-1 TCP/CONNECT/SOCKS greeting budget")
	flag.Parse()

	// 1. Open destination table file
	tableFile, err := os.OpenFile(*tableFlag, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal: failed to open log file %s: %v\n", *tableFlag, err)
		os.Exit(1)
	}
	defer tableFile.Close()

	// 2. Write CSV header if the file is brand new
	fi, err := tableFile.Stat()
	if err == nil && fi.Size() == 0 {
		if _, err := tableFile.WriteString(csvHeader); err != nil {
			fmt.Fprintf(os.Stderr, "Fatal: failed to write CSV header: %v\n", err)
			os.Exit(1)
		}
		if err := tableFile.Sync(); err != nil {
			fmt.Fprintf(os.Stderr, "Fatal: failed to sync CSV header: %v\n", err)
			os.Exit(1)
		}
		fmt.Print(csvHeader)
	}

	// 3. Setup proxy pool, embedded upstream sources, and reporter
	pool := proxypool.NewPool()
	pool.SetLimits(*concurrencyFlag, *concurrencyFlag, *concurrencyFlag)
	pool.SetTimeout(proxypool.TimeoutConfig{Handshake: *handshakeFlag, Probe: *timeoutFlag})
	// Feed client: whole-request 30s cap plus 10s TLS/header caps so a
	// stalled CDN edge fails fast instead of blocking ingestion.
	feedTransport := &http.Transport{
		ResponseHeaderTimeout: 10 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	client := &http.Client{Timeout: 30 * time.Second, Transport: feedTransport}
	proxifly := NewProxiflySource(client)
	speedx := NewSpeedXSource(client)
	monosans := NewMonosansSource(client)
	pool.RegisterProxySource(proxifly)
	pool.RegisterProxySource(speedx)
	pool.RegisterProxySource(monosans)

	reporter := NewLabReporter(tableFile, proxifly, speedx, monosans)
	pool.RegisterReporter(reporter)

	fmt.Fprintf(os.Stderr, "Proxy Lab started.\n -> Interval: %v\n -> Samples:  %d\n -> Table:    %s\n -> Concurrency: %d\n -> Probe timeout: %v\n -> Handshake timeout: %v\n\n", *intervalFlag, *samplesFlag, *tableFlag, *concurrencyFlag, *timeoutFlag, *handshakeFlag)

	// Graceful shutdown on Ctrl+C
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	sampleCount := 0
	ticker := time.NewTicker(*intervalFlag)
	defer ticker.Stop()

	// Run initial check immediately
	sampleCount++
	pool.Refresh()
	reporter.ReportStats(pool.Snapshot())

	for {
		if *samplesFlag > 0 && sampleCount >= *samplesFlag {
			fmt.Fprintf(os.Stderr, "\nCompleted %d samples. Exiting.\n", *samplesFlag)
			break
		}

		select {
		case <-sigChan:
			fmt.Fprintln(os.Stderr, "\nReceived shutdown signal. Exiting.")
			return
		case <-ticker.C:
			sampleCount++
			pool.Refresh()
			reporter.ReportStats(pool.Snapshot())
		}
	}
}
