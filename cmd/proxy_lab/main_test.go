package main

import (
	"io"
	"os"
	"strings"
	"testing"

	"github.com/TheSlopMachine/proxypool"
)

type stubSource struct{ status int }

func (s stubSource) Name() string    { return "stub" }
func (s stubSource) LastStatus() int { return s.status }

// TestCSVRowMatchesHeader guards the positional csvHeader/Report contract:
// the formatted row must carry exactly the header's pipes plus one extra
// pipe per additional source (joinStatuses embeds N-1 pipes in the last
// column). Column drift between header and row is otherwise silent.
func TestCSVRowMatchesHeader(t *testing.T) {
	headerPipes := strings.Count(csvHeader, "|")

	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stdout = w
	// Production wires exactly 3 sources (proxifly/speedx/monosans) matching
	// the 3 trailing header columns; joinStatuses embeds N-1 pipes, so the
	// row matches the header only when N == 3.
	reporter := NewLabReporter(nil, stubSource{200}, stubSource{304}, stubSource{500})
	reporter.Report(proxypool.RefreshReport{})
	_ = w.Close()
	os.Stdout = old
	out, _ := io.ReadAll(r)
	row := strings.TrimSpace(string(out))

	if got := strings.Count(row, "|"); got != headerPipes {
		t.Fatalf("row has %d pipes, want header's %d (3 sources): %q", got, headerPipes, row)
	}
}
