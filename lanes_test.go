package proxypool

import "testing"

func TestLaneCapsForeground(t *testing.T) {
	caps := laneCaps(ModeForeground, false, 100)
	want := map[lane]int{laneForeground: 80, laneBackground: 0, laneLiveness: 15, laneRevival: 5}
	for name, expected := range want {
		if got := caps[name]; got != expected {
			t.Fatalf("%s cap = %d, want %d", name, got, expected)
		}
	}
}

func TestLaneCapsBackground(t *testing.T) {
	caps := laneCaps(ModeBackground, false, 10)
	want := map[lane]int{laneForeground: 0, laneBackground: 2, laneLiveness: 5, laneRevival: 3}
	for name, expected := range want {
		if got := caps[name]; got != expected {
			t.Fatalf("%s cap = %d, want %d", name, got, expected)
		}
	}
}

func TestLaneCapsStarvedForeground(t *testing.T) {
	caps := laneCaps(ModeForeground, true, 10)
	want := map[lane]int{laneForeground: 0, laneBackground: 0, laneLiveness: 2, laneRevival: 9}
	for name, expected := range want {
		if got := caps[name]; got != expected {
			t.Fatalf("%s cap = %d, want %d", name, got, expected)
		}
	}
}

func TestLaneCapsMinimumOne(t *testing.T) {
	caps := laneCaps(ModeForeground, false, 1)
	if caps[laneForeground] != 1 || caps[laneLiveness] != 1 || caps[laneRevival] != 1 {
		t.Fatalf("expected minimum one for active non-empty lane caps: %+v", caps)
	}
}
