package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/TheSlopMachine/proxypool"
)

// ============================================================================
// 1. Upstream Proxy Source (Proxifly with ETag / 304 support)
// ============================================================================

type ProxiflySource struct {
	url        string
	etag       string
	lastMod    string
	lastStatus int
	client     *http.Client
}

func NewProxiflySource(url string) *ProxiflySource {
	return &ProxiflySource{
		url:    url,
		client: &http.Client{Timeout: 15 * time.Second},
	}
}

func (s *ProxiflySource) Name() string { return "Proxifly-HTTP" }

func (s *ProxiflySource) LastStatus() int {
	if s.lastStatus == 0 {
		return 200
	}
	return s.lastStatus
}

func (s *ProxiflySource) FetchList() []string {
	req, err := http.NewRequest("GET", s.url, nil)
	if err != nil {
		s.lastStatus = 500
		return nil
	}
	if s.etag != "" {
		req.Header.Set("If-None-Match", s.etag)
	}
	if s.lastMod != "" {
		req.Header.Set("If-Modified-Since", s.lastMod)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		s.lastStatus = 500
		return nil
	}
	defer resp.Body.Close()

	s.lastStatus = resp.StatusCode
	if resp.StatusCode == http.StatusNotModified {
		return nil // 304 Not Modified
	}
	if resp.StatusCode != http.StatusOK {
		return nil
	}

	s.etag = resp.Header.Get("ETag")
	s.lastMod = resp.Header.Get("Last-Modified")

	var items []struct {
		Proxy    string `json:"proxy"`
		Protocol string `json:"protocol"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&items); err != nil {
		return nil
	}

	urls := make([]string, 0, len(items))
	for _, item := range items {
		proto := strings.ToLower(strings.TrimSpace(item.Protocol))
		if proto != "" && proto != "http" && proto != "https" {
			continue
		}
		if strings.HasPrefix(item.Proxy, "socks4://") || strings.HasPrefix(item.Proxy, "socks5://") {
			continue
		}
		urls = append(urls, item.Proxy)
	}
	return urls
}

// ============================================================================
// 2. Metrics & Telemetry Reporter (CSV Streamer + 3-Letter Namer)
// ============================================================================

type LabReporter struct {
	mu           sync.Mutex
	tableFile    *os.File
	source       *ProxiflySource
	nameMap      map[string]string
	nameCounter  int
	diedNames    []string
	revivedNames []string
}

func NewLabReporter(file *os.File, source *ProxiflySource) *LabReporter {
	return &LabReporter{
		tableFile: file,
		source:    source,
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

func (r *LabReporter) Report(report proxypool.RefreshReport) {
	r.mu.Lock()
	defer r.mu.Unlock()

	sort.Strings(r.diedNames)
	sort.Strings(r.revivedNames)

	diedStr := strings.Join(r.diedNames, " ")
	if diedStr == "" {
		diedStr = "-"
	}

	revivedStr := strings.Join(r.revivedNames, " ")
	if revivedStr == "" {
		revivedStr = "-"
	}

	// Format row strictly matching:
	// Timestamp|HTTP|Total|New|Skip|Revive|Probed|Survived|Died|Alive|MinLat|MedLat|DiedNames|RevivedNames|MaxScore|AvgScore|MaxPenalty|AvgPenalty|Duration
	row := fmt.Sprintf("%s|%d|%d|%d|%d|%d|%d|%d|%d|%d|%dms|%dms|%s|%s|%.2f|%.2f|%.2f|%.2f|%.2fs\n",
		report.Timestamp.Format("2006-01-02 15:04:05"),
		r.source.LastStatus(),
		report.ProxiesTotal,
		report.ProxiesTotalNew,
		report.ProxiesTotalSkipped,
		report.ProxiesTotalRevived,
		report.ProxiesTotalProbed,
		report.ProxiesTotalSurvived,
		report.ProxiesTotalDied,
		report.ProxiesTotalAlive,
		report.LatencyMinimum.Milliseconds(),
		report.LatencyMedian.Milliseconds(),
		diedStr,
		revivedStr,
		report.ScoreMaximum,
		report.ScoreAverage,
		report.PenaltyMaximum,
		report.PenaltyAverage,
		report.Duration.Seconds(),
	)

	// Stream to stdout
	fmt.Print(row)

	// Write to table file and flush immediately
	if r.tableFile != nil {
		_, _ = r.tableFile.WriteString(row)
		_ = r.tableFile.Sync()
	}

	// Reset per-cycle collections
	r.diedNames = nil
	r.revivedNames = nil
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
	var b []byte
	for idx >= 0 {
		b = append([]byte{byte('A' + (idx % 26))}, b...)
		idx = (idx / 26) - 1
	}
	return string(b)
}

// ============================================================================
// 3. Application Entrypoint
// ============================================================================

func main() {
	intervalFlag := flag.Duration("interval", 15*time.Second, "Interval between refresh checks (e.g. 15s, 1m, 5m)")
	samplesFlag := flag.Int("samples", 100, "How many samples to collect before exiting (0 for infinite)")
	tableFlag := flag.String("table", "table.csv", "Path where to output pipe-separated CSV logs")
	sourceURL := flag.String("source", "https://raw.githubusercontent.com/proxifly/free-proxy-list/refs/heads/main/proxies/protocols/http/data.json", "Upstream proxy JSON feed")
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
	header := "Timestamp|HTTP|Total|New|Skip|Revive|Probed|Survived|Died|Alive|MinLat|MedLat|DiedNames|RevivedNames|MaxScore|AvgScore|MaxPenalty|AvgPenalty|Duration\n"
	if err == nil && fi.Size() == 0 {
		_, _ = tableFile.WriteString(header)
		_ = tableFile.Sync()
		fmt.Print(header)
	}

	// 3. Setup proxy pool, upstream source, and reporter
	pool := proxypool.NewPool()
	src := NewProxiflySource(*sourceURL)
	pool.RegisterProxySource(src)

	reporter := NewLabReporter(tableFile, src)
	pool.RegisterReporter(reporter)

	fmt.Fprintf(os.Stderr, "Proxy Lab started.\n -> Interval: %v\n -> Samples:  %d\n -> Table:    %s\n\n", *intervalFlag, *samplesFlag, *tableFlag)

	// Graceful shutdown on Ctrl+C
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	sampleCount := 0
	ticker := time.NewTicker(*intervalFlag)
	defer ticker.Stop()

	// Run initial check immediately
	sampleCount++
	pool.Refresh()

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
		}
	}
}
