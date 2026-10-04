package proxypool

import (
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
	if got := (TimeoutConfig{Handshake: -1, Probe: 0}).Resolve(); got != def {
		t.Fatalf("non-positive must resolve to defaults, got %+v", got)
	}
	custom := TimeoutConfig{Handshake: time.Second, Probe: 7 * time.Second}
	if got := custom.Resolve(); got != custom {
		t.Fatalf("valid config must pass through, got %+v", got)
	}
	if got := (TimeoutConfig{Handshake: time.Second}).Resolve(); got.Handshake != time.Second || got.Probe != def.Probe {
		t.Fatalf("partial config must fill only the gap, got %+v", got)
	}
}

func TestPipelineConfigResolve(t *testing.T) {
	got := PipelineConfig{}.Resolve()
	if got.Concurrency != DefaultConcurrency {
		t.Fatalf("concurrency must default to %d, got %d", DefaultConcurrency, got.Concurrency)
	}
	if got.Timeouts != DefaultTimeoutConfig() {
		t.Fatalf("timeouts must default, got %+v", got.Timeouts)
	}
	if got.Reporter == nil {
		t.Fatal("reporter must default to non-nil")
	}
	if got.CycleStart.IsZero() || got.Now.IsZero() {
		t.Fatalf("timestamps must default to non-zero: %+v", got)
	}

	custom := PipelineConfig{Concurrency: 8, Timeouts: TimeoutConfig{Handshake: time.Second, Probe: 2 * time.Second}}
	if got := custom.Resolve(); got.Concurrency != 8 || got.Timeouts != custom.Timeouts {
		t.Fatalf("valid config must pass through, got %+v", got)
	}
}
