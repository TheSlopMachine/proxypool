# proxypool

A high-performance, modular Go proxy pool library designed for public and private proxy management. Built to handle the harsh realities of public proxy scraping: extreme mortality rates, ephemeral flapping, and zombie server accumulation.

---

## Features

* **Two-Phase Fast Verification:** Fast TCP `CONNECT` tunnel handshakes to weed out dead nodes with zero heap allocations, followed by dual-endpoint HTTP probing.
* **Auto-Discovery of Geolocation:** Discovers proxy exit countries on-the-fly using Cloudflare's trace diagnostic endpoint without external GeoIP databases.
* **Continuous Time-Normalized Scoring ($\Delta t$):** Immunity against rapid refresh spam and long pauses; reputation scales strictly with elapsed observation time.
* **Grace Buffering for Veteran Proxies:** A reputation credit system shields reliable servers from being exiled over transient network drops or short outages.
* **Truncated Exponential Backoff with Jitter:** De-synchronizes dead "zombie" revival spikes and pushes offline servers into sleep intervals of up to 48 hours.
* **Pluggable Architecture:** Fully decoupled `ProxySource`, `CacheSource`, and `RefreshReporter` contracts.

---

## Installation

```bash
go get github.com/TheSlopMachine/proxypool
```

---

## Quickstart

```go
package main

import (
	"fmt"
	"time"

	"github.com/TheSlopMachine/proxypool"
)

func main() {
	// 1. Initialize pool (defaults to thread-safe in-memory cache)
	pool := proxypool.NewPool()

	// 2. Register one or more proxy sources
	pool.RegisterProxySource(NewMyProxySource())

	// 3. Trigger verification cycle
	pool.Refresh()

	// 4. Query working proxies with optional criteria
	targetCountry := "DE"
	maxLatency := 600 * time.Millisecond
	minScore := 5.0
	limit := 10

	proxies := pool.ListProxies(proxypool.ProxyFilter{
		Location:   &targetCountry,
		MaxLatency: &maxLatency,
		MinScore:   &minScore,
		Limit:      &limit,
	})

	for _, p := range proxies {
		fmt.Printf("Proxy: %s | Country: %s | Latency: %v | Score: %.2f\n",
			p.URL, p.Location, p.Latency, p.Score)
	}
}
```

---

## Architecture & Extensibility

The library delegates storage, feed acquisition, and telemetry to clean interfaces, while retaining internal control over proxy validation and scoring.

### 1. `ProxySource` Contract

A `ProxySource` provides raw proxy URLs to the pool. It is source-agnostic—whether it scrapes an HTML table, reads a local file, or polls an API with conditional headers:

```go
type ProxySource interface {
	Name() string
	FetchList() []string // Return empty slice if no changes occurred
}
```

#### Traffic Optimization (`200 OK` vs. `304 Not Modified`)
Because the pool is completely agnostic to upstream transport protocols, **conditional HTTP caching is the source's responsibility**. If your upstream provider supports `ETag` or `If-Modified-Since`, track these headers inside your `ProxySource`:

```go
type MyAPISource struct {
	url     string
	etag    string
	lastMod string
}

func (s *MyAPISource) FetchList() []string {
	req, _ := http.NewRequest("GET", s.url, nil)
	if s.etag != "" {
		req.Header.Set("If-None-Match", s.etag)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode == http.StatusNotModified {
		return nil // 304: zero JSON downloaded or parsed
	}
	defer resp.Body.Close()

	s.etag = resp.Header.Get("ETag")
	// parse and return []string...
}
```

When an empty slice is returned, the pool bypasses ingestion and evaluates only local proxies due for health checks or revivals.

Incoming raw URLs are automatically canonicalized by the pool (enforcing lowercase schemes, stripping trailing slashes, and extracting standard `host:port`), ensuring deduplication across multiple sources.

---

### 2. `CacheSource` Contract

`CacheSource` is a passive key-value state store. It must **not** alter, mutate, or validate incoming data.

```go
type CacheSource interface {
	Get(url string) (ProxyState, bool)
	Set(state ProxyState)
	All() []ProxyState
	Clear()
}
```

* **Default:** `NewPool()` starts with an internal, concurrent-safe in-memory cache.
* **Hot Migration:** Calling `pool.RegisterCacheSource(customCache)` automatically reads `All()` from the existing cache and migrates every entry into the new storage backend (e.g., Redis, SQLite, PostgreSQL) without dropping pool reputation states.

---

### 3. `RefreshReporter` Contract

Instead of binding to a rigid logging framework, the pool exposes structured telemetry hooks:

```go
type RefreshReporter interface {
	ReportProxy(report ProxyReport)
	Report(report RefreshReport)
}
```

* `ReportProxy`: Emits per-proxy evaluation events (`Died`, `Revived`, current score/penalty, latency).
* `Report`: Emits aggregated cycle statistics (min/max/median latency, average health score, cycle duration, alive counts).

---

## The Mathematics & Design

Public proxy lists exhibit extreme behavior: **~98% of scraped proxies are dead servers**, newly discovered proxies have a **75%+ first-minute mortality rate**, and fewer than **1% remain viable over 8+ hours**. 

The engine uses several targeted techniques to stabilize this environment:

### 1. Two-Phase Dual-Endpoint Probing

1. **Phase 1: TCP CONNECT Handshake (Fast Filter)**  
   Attempts a raw TCP tunnel handshake to `1.1.1.1:443` with a strict 4-second timeout. Reads the first status line (`HTTP/1.x 2xx`) from a stack-allocated buffer without heap allocations or TLS overhead. Fails dead nodes in milliseconds.
2. **Phase 2: Dual-Endpoint HTTP Probe**  
   * **If `Location == ""` (New Node):** Probes `https://www.cloudflare.com/cdn-cgi/trace`. Measures real HTTPS round-trip time and parses `loc=XX` to discover the exit country.
   * **If `Location != ""` (Known Node):** Probes `http://cp.cloudflare.com/generate_204`. A zero-body HTTP 204 check that updates latency and uptime scores with minimal bandwidth.

---

### 2. Continuous Time-Normalized Scaling ($\Delta t$)

To prevent rapid polling loops (e.g., a 5-second tick) from inflating scores or penalizing momentary drops too heavily, state transitions scale with elapsed time:

$$\Delta t = \frac{\text{now} - \text{lastCheckedAt}}{1\text{ minute}}$$

$$\delta = \begin{cases} 
1.0 & \text{if first check (lastCheckedAt is zero)} \\
\min(1.0, \; \max(0.01, \; \Delta t)) & \text{otherwise}
\end{cases}$$

* **Anti-Spam ($\Delta t < 1.0$):** Twelve consecutive 5-second checks only accumulate $\mathbf{1.0}$ point total.
* **Stale Absence Cap ($\Delta t > 1.0$):** Capping $\delta$ at $1.0$ guarantees that after an offline weekend, a single check cannot award or deduct thousands of points.

---

### 3. The Score vs. Penalty Ratio Model

A penalty-only backoff punishes good proxies unfairly during momentary router restarts or transient Cloudflare rate limits. 

We decouple health into two interacting variables:
* **`Score` $[0.0, 10.0]$:** Earned incrementally on successful checks.
* **`Penalty` $[0.0, \infty)$:** Incremented on failures.

$$\text{baseMinutes} = \max\left(1.0, \; \min\left(2880.0, \; 15.0 \cdot \frac{2^{\text{Penalty}}}{2.5^{\text{Score}}}\right)\right)$$

Because **$2.5 > 2.0$**, score is weighted more heavily than penalty:
* **The Veteran Shield:** A proxy with `Score = 10.0` that fails will evaluate to a **1-minute ban** ($\text{base} \approx 0.007\text{m}$, clamped to $1.0\text{m}$). It stays in the active retry loop to catch quick recoveries. If the outage continues, its score gradually depletes until the exponential penalty takes over.
* **The Zombie Hammer:** A dead proxy with `Score = 0.0` ramps exponentially on consecutive failures ($30\text{m} \to 60\text{m} \to 120\text{m} \to 240\text{m}$) up to a **48-hour ceiling (2,880m)**.

---

### 4. Randomized Jitter Dispersion

When thousands of dead proxies are imported in an initial batch, a deterministic backoff causes them to revive at the exact same minute ($+30\text{m}$, $+60\text{m}$, $+120\text{m}$), producing massive network spikes.

The engine applies a continuous $\pm 15\%$ uniform random jitter to all bans over 1 minute:

$$\text{jitter} = 0.85 + (0.30 \times \text{rand}())$$

$$\text{banMinutes} = \max(1.0, \; \text{baseMinutes} \times \text{jitter})$$

This smooths out periodic surges into a continuous, low-overhead background stream.

---

## Research: Proxy Lab & Visualizer

The repository includes a companion research tool in `cmd/proxy_lab/` used to run empirical benchmarks and tune scoring parameters.

It streams pipe-delimited telemetry rows into a CSV file, assigning persistent sequential 3-letter identifiers (`AAA`, `AAB` ... `ZZZ`) to individual proxies to monitor long-term behavior.

```powershell
go run .\cmd\proxy_lab\main.go -interval 5m -samples 100 -table table.csv
```

You can inspect the generated telemetry using the standalone web dashboard:

👉 **Interactive Dashboard Visualizer:** [theslopmachine.github.io/proxypool/web/proxy_viewer.html](https://theslopmachine.github.io/proxypool/web/proxy_viewer.html)

*(The visualizer runs client-side in your browser; drop your generated CSV into the page to plot latency distributions, pool churn, and stability curves).*

---

## License

This project is licensed under the [MIT License](LICENSE.txt).