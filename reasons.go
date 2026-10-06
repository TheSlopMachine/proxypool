package proxypool

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"syscall"
)

// FailReason classifies a failed proxy check.
type FailReason string

const (
	FailNone      FailReason = ""
	FailTimeout   FailReason = "timeout"
	FailRefused   FailReason = "refused"
	FailRejected  FailReason = "rejected" // CONNECT reply non-2xx, SOCKS reply != success
	FailEOF       FailReason = "eof_reset"
	FailDNS       FailReason = "dns"
	FailTLS       FailReason = "tls"
	FailBadStatus FailReason = "bad_status" // Probe HTTP status outside 200..399
	FailLocalNet  FailReason = "local_net"
	FailOther     FailReason = "other"
)

// Soft reports whether the reason is a transient local or upstream
// condition that must not hard-ban the proxy.
func (r FailReason) Soft() bool {
	return r == FailTimeout || r == FailDNS || r == FailLocalNet
}

// Valid reports whether the reason is a known non-empty constant.
func (r FailReason) Valid() bool {
	switch r {
	case FailTimeout, FailRefused, FailRejected, FailEOF, FailDNS, FailTLS, FailBadStatus, FailLocalNet, FailOther:
		return true
	}
	return false
}

// Sentinel errors carrying a known failure classification. classifyError
// maps them to their reasons; everything else is inspected via errors.Is
// and errors.As only.
var (
	errProxyRejected  = errors.New("proxy rejected the connection request")
	errBadProbeStatus = errors.New("probe endpoint returned an unexpected HTTP status")
)

// checkError attaches a check phase and a failure reason to a wrapped error.
type checkError struct {
	Phase  string
	Reason FailReason
	Err    error
}

func (e *checkError) Error() string { return e.Phase + " check failed: " + e.Err.Error() }
func (e *checkError) Unwrap() error { return e.Err }

// wrapCheckError classifies err and tags it with the check phase. An
// existing *checkError passes through unchanged so the original phase
// and reason are preserved.
func wrapCheckError(phase string, err error) error {
	if err == nil {
		return nil
	}
	var ce *checkError
	if errors.As(err, &ce) {
		return err
	}
	return &checkError{Phase: phase, Reason: classifyError(err), Err: err}
}

// classifyError maps an error to a FailReason using errors.Is and
// errors.As only; no string matching. A *checkError yields its own reason.
func classifyError(err error) FailReason {
	if err == nil {
		return FailNone
	}
	var ce *checkError
	if errors.As(err, &ce) {
		return ce.Reason
	}
	if errors.Is(err, errProxyRejected) {
		return FailRejected
	}
	if errors.Is(err, errBadProbeStatus) {
		return FailBadStatus
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return FailTimeout
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return FailTimeout
	}
	if errors.Is(err, syscall.ECONNREFUSED) {
		return FailRefused
	}
	if errors.Is(err, syscall.ECONNRESET) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return FailEOF
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return FailDNS
	}
	if errors.Is(err, syscall.ENETUNREACH) || errors.Is(err, syscall.EHOSTUNREACH) {
		return FailLocalNet
	}
	var certErr *tls.CertificateVerificationError
	if errors.As(err, &certErr) {
		return FailTLS
	}
	var recErr tls.RecordHeaderError
	if errors.As(err, &recErr) {
		return FailTLS
	}
	return FailOther
}
