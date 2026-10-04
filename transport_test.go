package proxypool

import (
	"net"
	"strings"
	"testing"
	"time"
)

const testTimeout = 3 * time.Second

// closedLoopback reserves a loopback port and closes it, so dials refuse.
func closedLoopback(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

// startHTTPConnectServer answers CONNECT with 200 (grant) or 403 (reject).
func startHTTPConnectServer(t *testing.T, grant bool) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				buf := make([]byte, 0, 512)
				tmp := make([]byte, 128)
				for !strings.Contains(string(buf), "\r\n\r\n") && len(buf) < 4096 {
					_ = c.SetReadDeadline(time.Now().Add(testTimeout))
					n, err := c.Read(tmp)
					if n > 0 {
						buf = append(buf, tmp[:n]...)
					}
					if err != nil {
						return
					}
				}
				if grant {
					_, _ = c.Write([]byte("HTTP/1.1 200 Connection established\r\n\r\n"))
				} else {
					_, _ = c.Write([]byte("HTTP/1.1 403 Forbidden\r\n\r\n"))
				}
			}(conn)
		}
	}()
	return ln.Addr().String()
}

// startSOCKS4Server answers SOCKS4 CONNECT with grant (0x5A) or reject.
// Received userids append to seen when non-nil.
func startSOCKS4Server(t *testing.T, grant bool, seen *[]string) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				head := make([]byte, 8)
				_ = c.SetReadDeadline(time.Now().Add(testTimeout))
				if _, err := readFull(c, head); err != nil {
					return
				}
				userid := readNullString(c)
				if seen != nil {
					*seen = append(*seen, userid)
				}
				if head[4] == 0 && head[5] == 0 && head[6] == 0 && head[7] != 0 {
					readNullString(c) // SOCKS4a domain suffix
				}
				code := byte(0x5B)
				if grant {
					code = 0x5A
				}
				_, _ = c.Write([]byte{0x00, code, 0, 0, 0, 0, 0, 0})
			}(conn)
		}
	}()
	return ln.Addr().String()
}

func readNullString(c net.Conn) string {
	var out []byte
	one := make([]byte, 1)
	for len(out) < 256 {
		_ = c.SetReadDeadline(time.Now().Add(testTimeout))
		if _, err := readFull(c, one); err != nil {
			break
		}
		if one[0] == 0x00 {
			break
		}
		out = append(out, one[0])
	}
	return string(out)
}

// startSOCKS5Server answers SOCKS5 CONNECT with success. When requireAuth is
// set, only method 0x02 with matching user/pass succeeds.
func startSOCKS5Server(t *testing.T, requireAuth bool, user, pass string) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				_ = c.SetReadDeadline(time.Now().Add(testTimeout))
				greet := make([]byte, 2)
				if _, err := readFull(c, greet); err != nil {
					return
				}
				methods := make([]byte, int(greet[1]))
				if _, err := readFull(c, methods); err != nil {
					return
				}
				if requireAuth {
					offered := false
					for _, m := range methods {
						if m == 0x02 {
							offered = true
						}
					}
					if !offered {
						_, _ = c.Write([]byte{0x05, 0xFF})
						return
					}
					_, _ = c.Write([]byte{0x05, 0x02})
					if !checkSOCKS5Auth(c, user, pass) {
						return
					}
				} else {
					_, _ = c.Write([]byte{0x05, 0x00})
				}
				head := make([]byte, 4)
				if _, err := readFull(c, head); err != nil {
					return
				}
				switch head[3] {
				case 0x01:
					if _, err := readFull(c, make([]byte, 6)); err != nil {
						return
					}
				case 0x03:
					ln := make([]byte, 1)
					if _, err := readFull(c, ln); err != nil {
						return
					}
					if _, err := readFull(c, make([]byte, int(ln[0])+2)); err != nil {
						return
					}
				case 0x04:
					if _, err := readFull(c, make([]byte, 18)); err != nil {
						return
					}
				default:
					return
				}
				_, _ = c.Write([]byte{0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
			}(conn)
		}
	}()
	return ln.Addr().String()
}

func checkSOCKS5Auth(c net.Conn, user, pass string) bool {
	head := make([]byte, 2)
	if _, err := readFull(c, head); err != nil {
		return false
	}
	ulen := int(head[1])
	rest := make([]byte, ulen+1)
	if _, err := readFull(c, rest); err != nil {
		_, _ = c.Write([]byte{0x01, 0x01})
		return false
	}
	gotUser := string(rest[:ulen])
	plen := int(rest[ulen])
	pw := make([]byte, plen)
	if _, err := readFull(c, pw); err != nil {
		_, _ = c.Write([]byte{0x01, 0x01})
		return false
	}
	if gotUser != user || string(pw) != pass {
		_, _ = c.Write([]byte{0x01, 0x01})
		return false
	}
	_, _ = c.Write([]byte{0x01, 0x00})
	return true
}

func TestVerifyHandshakeHTTP(t *testing.T) {
	addr := startHTTPConnectServer(t, true)
	if err := verifyHandshake("http://"+addr, testTimeout); err != nil {
		t.Fatalf("granting HTTP proxy must pass: %v", err)
	}
	if err := verifyHandshake("https://"+addr, testTimeout); err != nil {
		t.Fatalf("granting HTTPS proxy must pass: %v", err)
	}

	addr = startHTTPConnectServer(t, false)
	if err := verifyHandshake("http://"+addr, testTimeout); err == nil {
		t.Fatal("rejecting HTTP proxy must fail")
	}
	if err := verifyHandshake("http://"+closedLoopback(t), testTimeout); err == nil {
		t.Fatal("closed port must fail")
	}
}

func TestVerifyHandshakeSOCKS4(t *testing.T) {
	var seen []string
	addr := startSOCKS4Server(t, true, &seen)
	if err := verifyHandshake("socks4://tester@"+addr, testTimeout); err != nil {
		t.Fatalf("granting SOCKS4 proxy must pass: %v", err)
	}
	if len(seen) != 1 || seen[0] != "tester" {
		t.Fatalf("userid must reach the server, got %q", seen)
	}
	if err := verifyHandshake("socks4a://"+addr, testTimeout); err != nil {
		t.Fatalf("socks4a alias must pass: %v", err)
	}

	addr = startSOCKS4Server(t, false, nil)
	if err := verifyHandshake("socks4://"+addr, testTimeout); err == nil {
		t.Fatal("rejecting SOCKS4 proxy must fail")
	}
}

func TestVerifyHandshakeSOCKS5(t *testing.T) {
	addr := startSOCKS5Server(t, false, "", "")
	if err := verifyHandshake("socks5://"+addr, testTimeout); err != nil {
		t.Fatalf("granting SOCKS5 proxy must pass: %v", err)
	}

	addr = startSOCKS5Server(t, true, "alice", "s3cret")
	if err := verifyHandshake("socks5://alice:s3cret@"+addr, testTimeout); err != nil {
		t.Fatalf("SOCKS5 with valid credentials must pass: %v", err)
	}
	if err := verifyHandshake("socks5://alice:wrong@"+addr, testTimeout); err == nil {
		t.Fatal("SOCKS5 with wrong password must fail")
	}
	if err := verifyHandshake("socks5://"+addr, testTimeout); err == nil {
		t.Fatal("SOCKS5 without credentials against auth server must fail")
	}
}

func TestTransportFor(t *testing.T) {
	for _, raw := range []string{
		"http://1.2.3.4:8080",
		"https://1.2.3.4:8443",
		"socks4://1.2.3.4:1080",
		"socks4a://1.2.3.4:1080",
		"socks5://1.2.3.4:1080",
		"socks5://alice:s3cret@1.2.3.4:1080",
	} {
		tr, err := TransportFor(raw, testTimeout)
		if err != nil {
			t.Fatalf("TransportFor(%q) error: %v", raw, err)
		}
		if tr == nil || tr.DialContext == nil {
			t.Fatalf("TransportFor(%q) must return a usable transport", raw)
		}
	}
	if _, err := TransportFor("ftp://1.2.3.4:21", testTimeout); err == nil {
		t.Fatal("ftp must fail")
	}
	if _, err := TransportFor("http://1.2.3.4:99999", testTimeout); err == nil {
		t.Fatal("invalid port must fail")
	}
}

func TestTransportForSOCKS5EndToEnd(t *testing.T) {
	addr := startSOCKS5Server(t, false, "", "")
	tr, err := TransportFor("socks5://"+addr, testTimeout)
	if err != nil {
		t.Fatalf("TransportFor: %v", err)
	}
	conn, err := tr.DialContext(t.Context(), "tcp", "1.1.1.1:443")
	if err != nil {
		t.Fatalf("dial through SOCKS5 transport: %v", err)
	}
	conn.Close()
}

func TestTransportForSOCKS4EndToEnd(t *testing.T) {
	addr := startSOCKS4Server(t, true, nil)
	tr, err := TransportFor("socks4://"+addr, testTimeout)
	if err != nil {
		t.Fatalf("TransportFor: %v", err)
	}
	conn, err := tr.DialContext(t.Context(), "tcp", "1.1.1.1:443")
	if err != nil {
		t.Fatalf("dial through SOCKS4 transport: %v", err)
	}
	conn.Close()
}
