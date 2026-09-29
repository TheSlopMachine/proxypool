package proxypool

import (
	"time"
)

// ============================================================================
// 1. Data Models
// ============================================================================

// ProxyState holds internal lifecycle, scoring, and operational metadata.
// Stored inside CacheSource and managed solely by ProxyPool.
type ProxyState struct {
	URL           string        // Canonical proxy URL (e.g., "http://1.2.3.4:8080")
	IP            string        // Target IP or hostname
	Port          int           // Target port
	Location      string        // 2-letter ISO country code discovered via Cloudflare trace (e.g., "DE")
	IsDead        bool          // Operational status
	Penalty       float64       // Accumulated backoff penalty weight
	Score         float64       // Reputation credit [0.0 - 10.0]
	ReviveAt      time.Time     // Timestamp when dead proxy can be re-tested
	LastCheckedAt time.Time     // Timestamp of last probe (used for delta normalization)
	Latency       time.Duration // Last measured round-trip time
}

// ProxyInfo represents a validated, consumer-ready proxy returned by ListProxies.
type ProxyInfo struct {
	URL         string        `json:"url"`
	IP          string        `json:"ip"`
	Port        int           `json:"port"`
	Location    string        `json:"location"`
	Latency     time.Duration `json:"latency"`
	Score       float64       `json:"score"`
	LastChecked time.Time     `json:"last_checked"`
}

// ProxyFilter defines search criteria used when querying the pool.
// All fields are optional pointers; nil allows all values.
type ProxyFilter struct {
	Location   *string        // ISO country code (e.g., "DE", "US")
	MaxLatency *time.Duration // Upper latency threshold
	MinScore   *float64       // Minimum reputation threshold (e.g., 5.0)
	Limit      *int           // Cap the maximum number of results returned
}

// ============================================================================
// 2. Telemetry Models
// ============================================================================

// RefreshReport contains aggregated cycle statistics.
type RefreshReport struct {
	Timestamp            time.Time     // UTC timestamp when the refresh cycle started
	Duration             time.Duration // Total execution duration of the cycle
	ProxiesTotal         int           // Total known proxies in cache
	ProxiesTotalNew      int           // New proxies discovered during this cycle
	ProxiesTotalSkipped  int           // Dead proxies skipped (in backoff cooldown)
	ProxiesTotalRevived  int           // Dead proxies that reached revive_at and were re-probed
	ProxiesTotalProbed   int           // Candidates sent to verification
	ProxiesTotalSurvived int           // Proxies that passed handshake and latency check
	ProxiesTotalDied     int           // Previously alive proxies that failed this round
	ProxiesTotalAlive    int           // Total working proxies currently in pool

	// Latency (Active Pool)
	LatencyMinimum time.Duration // Lowest active proxy latency
	LatencyMaximum time.Duration // Highest active proxy latency
	LatencyMedian  time.Duration // P50 latency (superior to mean for network RTT)

	// Score (Active Pool, [0.0 - 10.0])
	ScoreMaximum float64 // Highest score in pool
	ScoreAverage float64 // Mean score (acts as general pool health index)

	// Penalty (Active Pool)
	PenaltyMaximum float64 // Highest residual penalty among active proxies
	PenaltyAverage float64 // Mean residual penalty among active proxies
}

// ProxyReport contains telemetry for a single proxy evaluated during a cycle.
type ProxyReport struct {
	Timestamp time.Time     // UTC timestamp of the event
	URL       string        // Normalized proxy URL
	Location  string        // Discovered ISO country code
	IsDead    bool          // Operational status after check
	Score     float64       // Reputation score [0.0 - 10.0]
	Penalty   float64       // Current backoff penalty weight
	ReviveAt  time.Time     // Scheduled revival timestamp if dead
	Latency   time.Duration // Measured latency during check (0 if failed/skipped)
	Died      bool          // True if transitioned from Alive -> Dead
	Revived   bool          // True if transitioned from Dead -> Probed/Alive
}

// ============================================================================
// 3. Interfaces
// ============================================================================

// CacheSource defines passive storage for proxy states.
// Implementations must not alter or mutate incoming data.
type CacheSource interface {
	Get(url string) (ProxyState, bool)
	Set(state ProxyState)
	All() []ProxyState
	Clear()
}

// ProxySource provides raw proxy URLs from any upstream feed.
// Upstream rate-limiting, ETag/Last-Modified caching, and format decoding
// belong entirely inside the source implementation.
type ProxySource interface {
	Name() string        // Unique identifier for logging/metrics
	FetchList() []string // Returns raw proxy URLs; empty slice means no changes (e.g., 304)
}

// RefreshReporter receives structured metrics during and after a refresh cycle.
type RefreshReporter interface {
	ReportProxy(report ProxyReport)
	Report(report RefreshReport)
}

// noopReporter ensures safe execution when no custom reporter is registered.
type noopReporter struct{}

func (n *noopReporter) ReportProxy(_ ProxyReport) {}
func (n *noopReporter) Report(_ RefreshReport)      {}