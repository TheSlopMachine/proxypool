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
	URL      string // Canonical proxy URL (e.g., "http://1.2.3.4:8080", "socks5://user:pass@1.2.3.4:1080")
	IP       string // Target IP or hostname
	Port     int    // Target port
	Location string // 2-letter ISO country code discovered via Cloudflare trace (e.g., "DE")
	IsDead   bool   // Operational status
	// Source is the name of the first source that reported this proxy.
	Source string
	// FailReason classifies the last failed check; empty when the last check passed.
	FailReason FailReason
	// SoftFails counts consecutive soft failures; reset on success.
	SoftFails int
	// Suspect excludes the proxy from listings and queues an immediate recheck.
	Suspect bool
	// LastSeenInSource is the last time any source reported this URL.
	LastSeenInSource time.Time
	Penalty          float64           // Accumulated backoff penalty weight
	Score            float64           // Reputation credit [0.0 - 10.0]
	ReviveAt         time.Time         // Timestamp when dead proxy can be re-tested
	LastCheckedAt    time.Time         // Timestamp of last probe (used for delta normalization)
	Latency          time.Duration     // Last measured round-trip time
	Metadata         map[string]string // Optional user-attached custom metadata (nil = none)
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
// BanFilter selects banned proxies for manual revival checks. Empty fields match any value.
type BanFilter struct {
	Reason FailReason
	Source string
}

type ProxyFilter struct {
	Location   *string        // ISO country code (e.g., "DE", "US")
	MaxLatency *time.Duration // Upper latency threshold
	MinScore   *float64       // Minimum reputation threshold (e.g., 5.0)
	Limit      *int           // Cap the maximum number of results returned
}

// ============================================================================
// 2. Telemetry Models
// ============================================================================

// ProxyReport contains telemetry for a single proxy evaluated during a cycle.
type ProxyReport struct {
	Timestamp  time.Time     // UTC timestamp of the event
	URL        string        // Normalized proxy URL
	Source     string        // Name of the source that first reported the proxy
	Location   string        // Discovered ISO country code
	FailReason FailReason    // Classification of the failed check; empty on success
	Lane       string        // Scheduler lane that ran the check; empty before the lane scheduler
	IsDead     bool          // Operational status after check
	Score      float64       // Reputation score [0.0 - 10.0]
	Penalty    float64       // Current backoff penalty weight
	ReviveAt   time.Time     // Scheduled revival timestamp if dead
	Latency    time.Duration // Measured latency during check (0 if failed/skipped)
	Died       bool          // True if transitioned from Alive -> Dead
	Revived    bool          // True if transitioned from Dead -> Probed/Alive
}

// Mode is the pool scheduling mode.
type Mode string // "foreground", "background"

const (
	ModeForeground Mode = "foreground"
	ModeBackground Mode = "background"
)

// NetState is the state of the local network connection of the host.
type NetState string // "good", "degraded", "down"

// PoolEvent records a pool state transition.
type PoolEvent struct {
	Time   time.Time         // UTC timestamp of the event
	Kind   string            // "mode_change", "net_change", "limit_change", "ingest_done", "breaker_open", "breaker_close"
	Fields map[string]string // Stringified details, e.g. {"from":"foreground","to":"background"}
}

// NetSnapshot is the last published network health sample. Defined here
// because PoolStats references it; the publishing prober lives in nethealth.go.
type NetSnapshot struct {
	State     NetState      // "good", "degraded", "down"
	RTT       time.Duration // Median of the last 3 successful samples, 0 if none
	UpdatedAt time.Time     // UTC timestamp of the last sample
}

// SourceStats aggregates pool composition per source.
type SourceStats struct {
	Source     string
	Alive      int
	Suspect    int
	Banned     int
	Queued     int                // Candidates of this source still in the candidate queue
	BanReasons map[FailReason]int // Among banned
}

// LaneStats reports one scheduler lane's load.
type LaneStats struct {
	Lane     string
	Inflight int
	Queued   int // Due items
}

// PoolStats is a point-in-time pool snapshot reported on StatsInterval.
type PoolStats struct {
	Time         time.Time
	Mode         Mode
	Net          NetSnapshot
	Limit        int
	Inflight     int
	Alive        int
	Suspect      int
	Banned       int
	Queued       int // Candidate queue length
	Lanes        []LaneStats
	Sources      []SourceStats // Sorted by Source
	BanReasons   map[FailReason]int
	LastIngestAt time.Time
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
	Delete(url string)
	Clear()
}

// ProxySource provides raw proxy URLs from any upstream feed.
// Upstream rate-limiting, ETag/Last-Modified caching, and format decoding
// belong entirely inside the source implementation.
type ProxySource interface {
	Name() string        // Unique identifier for logging/metrics
	FetchList() []string // Returns raw proxy URLs; empty slice means no changes (e.g., 304)
}

// TaggedURL associates one source identity with a raw URL.
type TaggedURL struct {
	URL    string
	Source string
}

// TaggedSource optionally supplies per-URL source tags during ingest.
type TaggedSource interface {
	FetchTagged() []TaggedURL
}

// TimeoutConfig bounds the two probe phases independently. It is the single
// source of truth for network budgets.
type TimeoutConfig struct {
	Handshake time.Duration
	Probe     time.Duration
}

// DefaultTimeoutConfig returns the default handshake and probe budgets.
func DefaultTimeoutConfig() TimeoutConfig {
	return TimeoutConfig{Handshake: 3 * time.Second, Probe: 5 * time.Second}
}

// Resolve fills non-positive timeout fields with defaults.
func (c TimeoutConfig) Resolve() TimeoutConfig {
	def := DefaultTimeoutConfig()
	if c.Handshake <= 0 {
		c.Handshake = def.Handshake
	}
	if c.Probe <= 0 {
		c.Probe = def.Probe
	}
	return c
}

// Reporter receives structured pool telemetry.
type Reporter interface {
	ReportProxy(ProxyReport) // One call per finished check
	ReportStats(PoolStats)   // Every Config.StatsInterval
	ReportEvent(PoolEvent)   // State transitions
}

// noopReporter ensures safe execution when no custom reporter is registered.
type noopReporter struct{}

func (n *noopReporter) ReportProxy(_ ProxyReport) {}
func (n *noopReporter) ReportStats(_ PoolStats)   {}
func (n *noopReporter) ReportEvent(_ PoolEvent)   {}
