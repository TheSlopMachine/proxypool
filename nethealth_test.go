package proxypool

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestNetHealthProbeUsesInjectedClient(t *testing.T) {
	var seenMethod string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenMethod = r.Method
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	h := newNetHealth()
	h.client = server.Client()
	rtt, err := h.probeURL(context.Background(), server.URL)
	if err != nil {
		t.Fatalf("probe must succeed: %v", err)
	}
	if rtt <= 0 {
		t.Fatal("probe RTT must be positive")
	}
	if seenMethod != http.MethodGet {
		t.Fatalf("expected GET, got %q", seenMethod)
	}
}

func TestNetHealthProbeRejectsUnexpectedStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer server.Close()

	h := newNetHealth()
	h.client = server.Client()
	_, err := h.probeURL(context.Background(), server.URL)
	if !errors.Is(err, errBadProbeStatus) {
		t.Fatalf("expected bad probe status, got %v", err)
	}
}

func TestSummarizeNetSamples(t *testing.T) {
	cfg := DefaultConfig()
	fast := 100 * time.Millisecond
	slow := 2 * time.Second
	tests := []struct {
		name    string
		samples []netSample
		want    NetState
	}{
		{name: "startup", samples: []netSample{{ok: false}, {ok: false}}, want: NetGood},
		{name: "all failed", samples: []netSample{{ok: false}, {ok: false}, {ok: false}}, want: NetDown},
		{name: "mixed", samples: []netSample{{ok: true, rtt: fast}, {ok: false}, {ok: true, rtt: fast}}, want: NetDegraded},
		{name: "slow", samples: []netSample{{ok: true, rtt: slow}, {ok: true, rtt: slow}, {ok: true, rtt: slow}}, want: NetDegraded},
		{name: "good", samples: []netSample{{ok: true, rtt: fast}, {ok: true, rtt: fast}, {ok: true, rtt: fast}}, want: NetGood},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := summarizeNetSamples(tt.samples, cfg.NetDegradedRTT); got != tt.want {
				t.Fatalf("state = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestNextLimitAIMD(t *testing.T) {
	cfg := DefaultConfig()
	got, streak := nextLimit(50, NetGood, 0, cfg)
	if got != 50 || streak != 1 {
		t.Fatalf("first good sample = (%d, %d), want (50, 1)", got, streak)
	}
	got, streak = nextLimit(got, NetGood, streak, cfg)
	if got != 55 || streak != 0 {
		t.Fatalf("second good sample = (%d, %d), want (55, 0)", got, streak)
	}
	got, streak = nextLimit(55, NetDegraded, streak, cfg)
	if got != 27 || streak != 0 {
		t.Fatalf("degraded = (%d, %d), want (27, 0)", got, streak)
	}
	got, streak = nextLimit(27, NetDown, streak, cfg)
	if got != cfg.MinLimit || streak != 0 {
		t.Fatalf("down = (%d, %d), want (%d, 0)", got, streak, cfg.MinLimit)
	}
	got, _ = nextLimit(cfg.MaxLimit, NetGood, 1, cfg)
	if got != cfg.MaxLimit {
		t.Fatalf("good at cap = %d, want %d", got, cfg.MaxLimit)
	}
}
