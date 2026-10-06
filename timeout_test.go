package proxypool

import (
	"context"
	"testing"
	"time"
)

func TestTimeoutConfigResolve(t *testing.T) {
	def := DefaultTimeoutConfig()
	if def.Handshake != 3*time.Second || def.Probe != 5*time.Second {
		t.Fatalf("defaults changed: %+v", def)
	}
	if got := (TimeoutConfig{}).Resolve(); got != def {
		t.Fatalf("zero must resolve to defaults, got %+v", got)
	}
	custom := TimeoutConfig{Handshake: time.Second, Probe: 7 * time.Second}
	if got := custom.Resolve(); got != custom {
		t.Fatalf("valid config must pass through, got %+v", got)
	}
}

func TestConfigDefaults(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.MinLimit != 10 || cfg.InitialLimit != 50 || cfg.MaxLimit != 500 {
		t.Fatalf("limit defaults changed: %+v", cfg)
	}
	if cfg.LowWater != 20 || cfg.HighWater != 100 {
		t.Fatalf("watermarks changed: %+v", cfg)
	}
	if cfg.LivenessInterval != 3*time.Minute || cfg.IngestInterval != 10*time.Minute || cfg.StatsInterval != 30*time.Second {
		t.Fatalf("interval defaults changed: %+v", cfg)
	}
	if cfg.SoftBanBase != 5*time.Minute || cfg.SoftBanMax != 6*time.Hour {
		t.Fatalf("soft-ban defaults changed: %+v", cfg)
	}
	if cfg.NetProbeInterval != 5*time.Second || cfg.NetDegradedRTT != 1500*time.Millisecond || cfg.SourceGCAge != 168*time.Hour {
		t.Fatalf("network/source defaults changed: %+v", cfg)
	}
	zero := (Config{}).Resolve()
	if zero != cfg {
		t.Fatalf("zero config must resolve to defaults, got %+v", zero)
	}
}

func TestLimiterRespectsLimitAndContext(t *testing.T) {
	l := newLimiter(1)
	if err := l.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := l.Acquire(ctx); err == nil {
		t.Fatal("second acquire must block until a slot is released")
	}
	if l.Inflight() != 1 {
		t.Fatalf("expected one inflight slot, got %d", l.Inflight())
	}
	l.SetLimit(2)
	if err := l.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	l.Release()
	l.Release()
	if got := l.Inflight(); got != 0 {
		t.Fatalf("expected zero inflight, got %d", got)
	}
}
