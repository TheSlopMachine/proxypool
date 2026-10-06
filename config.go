package proxypool

import "time"

// Config controls scheduler cadence, queue thresholds, and verification limits.
type Config struct {
	MinLimit         int
	InitialLimit     int
	MaxLimit         int
	LowWater         int
	HighWater        int
	LivenessInterval time.Duration
	IngestInterval   time.Duration
	StatsInterval    time.Duration
	SoftBanBase      time.Duration
	SoftBanMax       time.Duration
	NetProbeInterval time.Duration
	NetDegradedRTT   time.Duration
	SourceGCAge      time.Duration
}

// DefaultConfig returns the scheduler defaults defined by the rollout plan.
func DefaultConfig() Config {
	return Config{
		MinLimit:         10,
		InitialLimit:     50,
		MaxLimit:         500,
		LowWater:         20,
		HighWater:        100,
		LivenessInterval: 3 * time.Minute,
		IngestInterval:   10 * time.Minute,
		StatsInterval:    30 * time.Second,
		SoftBanBase:      5 * time.Minute,
		SoftBanMax:       6 * time.Hour,
		NetProbeInterval: 5 * time.Second,
		NetDegradedRTT:   1500 * time.Millisecond,
		SourceGCAge:      168 * time.Hour,
	}
}

// Resolve fills non-positive fields from DefaultConfig without mutating the receiver.
func (c Config) Resolve() Config {
	def := DefaultConfig()
	if c.MinLimit <= 0 {
		c.MinLimit = def.MinLimit
	}
	if c.InitialLimit <= 0 {
		c.InitialLimit = def.InitialLimit
	}
	if c.MaxLimit <= 0 {
		c.MaxLimit = def.MaxLimit
	}
	if c.LowWater <= 0 {
		c.LowWater = def.LowWater
	}
	if c.HighWater <= 0 {
		c.HighWater = def.HighWater
	}
	if c.LivenessInterval <= 0 {
		c.LivenessInterval = def.LivenessInterval
	}
	if c.IngestInterval <= 0 {
		c.IngestInterval = def.IngestInterval
	}
	if c.StatsInterval <= 0 {
		c.StatsInterval = def.StatsInterval
	}
	if c.SoftBanBase <= 0 {
		c.SoftBanBase = def.SoftBanBase
	}
	if c.SoftBanMax <= 0 {
		c.SoftBanMax = def.SoftBanMax
	}
	if c.NetProbeInterval <= 0 {
		c.NetProbeInterval = def.NetProbeInterval
	}
	if c.NetDegradedRTT <= 0 {
		c.NetDegradedRTT = def.NetDegradedRTT
	}
	if c.SourceGCAge <= 0 {
		c.SourceGCAge = def.SourceGCAge
	}
	if c.MinLimit > c.InitialLimit {
		c.InitialLimit = c.MinLimit
	}
	if c.InitialLimit > c.MaxLimit {
		c.MaxLimit = c.InitialLimit
	}
	return c
}
